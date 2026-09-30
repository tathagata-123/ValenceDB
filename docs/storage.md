# ValenceDB storage format and physical I/O

This document describes the binary page foundation, persistent row storage, and table-index integration. Basic query execution and scratch lifecycle are described in [execution.md](execution.md).

## Page identities and files

Every page is exactly **4096 bytes**. `PageId` is `uint32_t`; `UINT32_MAX` is invalid. A `PageRef` is `(FileKind, PageId)`, where `FileKind` is `Database` or `Temporary`. The two files have independent page-number sequences: Database page 5 and Temporary page 5 are different pages. `PageBytes` is a fixed `std::array<std::byte, 4096>`.

The persistent file and temporary file are each a sequence of complete pages without a physical file header. The database creates a superblock and catalog in persistent pages 0 and 1. Allocation appends one complete zero-filled page and returns its zero-based PageId. Existing page files must have a length divisible by 4096; malformed lengths are rejected on open. The largest valid page count is `UINT32_MAX`, with valid IDs `0` through `UINT32_MAX - 1`.

`DiskManager` takes two distinct paths and an explicit `CreateNew` or `OpenExisting` mode for each. `CreateNew` rejects an existing file. `OpenExisting` preserves bytes and rejects missing or malformed files. `Read` and `Write` require an allocated PageId and transfer exactly one page; they never create sparse pages. `Flush`, `FlushAll`, and `Close` report failures through `DbError`; the destructor is nonthrowing. A clean flush/close/reopen preserves bytes. No `fsync`, crash atomicity, WAL, or crash recovery is promised. Simple counters record successful physical page operations and failed read/write attempts. A one-shot failure hook supports deterministic tests.

`ResetTemporary()` explicitly truncates and reopens only the scratch file. **BufferPool and query code must first ensure every Temporary page is unpinned and discarded from the cache.** DiskManager itself has no cache knowledge and performs no cache invalidation. DiskManager tests access it directly; ordinary database and temporary page content access goes through BufferPool apart from documented bootstrap and physical-allocation mechanics.

## Common page header

`WritePageHeader` writes these fields at fixed offsets; `ValidatePageHeader` checks magic, type, format version, page ID, and zero reserved bytes. Unknown expected type or invalid expected PageId means that particular identity is not compared, while the stored type and ID must still be valid. Allocation alone does not write this header; the owning page layer initializes it.

| Offset | Width | Field | Encoding |
|---:|---:|---|---|
| 0 | 4 | magic | `0x50424456` (`VDBP` bytes) |
| 4 | 2 | page type | reserved category ID |
| 6 | 2 | format | `1` |
| 8 | 4 | page ID | physical ID |
| 12 | 4 | reserved | zero |

The header is 16 bytes. The reserved page-type identities are Superblock, Catalog, TableMeta, Heap, IndexMeta, BPlusLeaf, BPlusInternal, and Temporary. The storage layer implements the first four layouts.

## RID and tuple format

A RID is a logical `(page_id:uint32, slot_id:uint16)` pair. Its eight-byte encoding is page ID at offset 0, slot ID at offset 4, and a zero `uint16` reserved field at offset 6. RID ordering compares the numbers, first page ID and then slot ID, independent of little-endian byte order.

All multibyte disk fields use **little-endian** encoding, independent of host endianness or C++ struct layout. INT64 uses its 64-bit representation. DOUBLE requires IEEE-754 binary64, enforced at compile time, and serializes the exact 64-bit bit pattern. Strings own arbitrary bytes, including embedded NUL, and have no terminator.

A tuple begins with `ncols:uint16` at offset 0 and `nullmask:uint16` at offset 2. Bit `i` marks column `i` as NULL. Non-NULL fields follow in schema order: INT64 and DOUBLE use eight bytes; VARCHAR uses a `uint16` byte length followed by precisely that many bytes. NULL fields have no payload. Decoder input must end exactly after the last field.

For example, schema `(id INT64, tag VARCHAR(4))` and values `(1, "A\0B")` encode as:

| Offset | Bytes (hex) | Meaning |
|---:|---|---|
| 0–1 | `02 00` | two columns |
| 2–3 | `00 00` | no NULLs |
| 4–11 | `01 00 00 00 00 00 00 00` | INT64 1 |
| 12–13 | `03 00` | three VARCHAR bytes |
| 14–16 | `41 00 42` | `A`, NUL, `B` |

The encoded length is 17 bytes. The NUL is data, not a terminator.

## Schema rules and errors

Base schemas have 1–8 nonnullable INT64/VARCHAR columns, VARCHAR limits of 1–256 bytes, and a maximum encoded tuple of 1024 bytes. Intermediate schemas have 1–16 columns, may additionally contain DOUBLE and nullable fields, and permit up to 2048 encoded bytes. Schema construction rejects configurations whose maximum possible encoded tuple exceeds the limit. Tuple encoding checks actual types, counts, lengths, NULL permissions, and size. Decoding checks all boundaries, count, mask, NULL permissions, lengths, and complete consumption. `Value` and `Tuple` own their data; decoding retains no view into the input buffer.

`DbError` reports validation, malformed data, invalid page access, resource limits, and file I/O errors. There is no recovery protocol after unexpected partial physical I/O.

## Slotted heap pages

A heap page extends the common header to 32 bytes. Its fields are `slot_count:uint16@16`, `lower:uint16@18`, `upper:uint16@20`, `live_count:uint16@22`, `next_page:uint32@24`, and `owner_meta_page:uint32@28`. Slots begin at byte 32 and each contains `offset:uint16,length:uint16`. Slot `i` is at `32 + 4*i`. Payloads grow backward from byte 4096. Always `lower = 32 + 4*slot_count` and `lower <= upper <= 4096`.

Insertion appends a slot and copies encoded tuple bytes below `upper`. A zero length marks a tombstone. Deleted slots are never reused, so a surviving `(PageId, slot_id)` RID remains stable. Delete does not immediately move bytes. When fragmented space can hold an insertion, `Compact` copies live payloads through a page-sized scratch buffer, updates only their slot offsets, and creates one contiguous free region. If the slot directory itself has no space, compaction cannot help; HeapFile appends a page.

Worked example: slots `0 -> A, 1 -> B, 2 -> C` become `0 -> A, 1 -> tombstone, 2 -> C` after deleting B. Compaction may move A and C, but their slot IDs stay 0 and 2. A later insertion receives slot 3.

Each persistent heap page stores its owning TableMeta PageId. Direct RID fetch/delete and every scan page validate the common header, page bounds, slot ranges, nonoverlapping live payloads, and owner. A foreign-table RID and cross-owner `next_page` link raise `DbError`. An absent or deleted slot is ordinary not-found. HeapFile tries only the tail page and maintains a forward chain; it does not search older pages for space or reclaim PageIds. An empty table allocates no heap page.

`HeapScan::Next()` follows the chain in slot order, skips tombstones, and returns an owned tuple with its RID. It holds no page pin between calls. `Reset()` restarts if no database mutation occurred. A mutation invalidates an existing scan; the engine is single-threaded with one active operation/query.

## Database metadata

Page 0 is Superblock: `page_size:uint32@16 = 4096`, `catalog_page:uint32@20 = 1`; all remaining bytes are zero. Reopen validates the common header and both fields. Page 1 is the Catalog directory: 32-byte header, `table_count:uint16@16`, then at most 16 entries of `name[32],descriptor_page:uint32` (36 bytes each). Names are byte strings, normally UTF-8, with an obligatory zero terminator; maximum is 31 bytes. Embedded NUL, empty, duplicate, and overlong names are rejected.

Each table owns one TableMeta page: `first_page:uint32@16`, `last_page:uint32@20`, `live_rows:uint64@24`, `heap_pages:uint32@32`, `index_col:uint16@36`, `column_count:uint16@38`, `index_meta:uint32@40`. Schema descriptors begin at byte 64. Each 32-byte descriptor is `name[24],type:uint8,flags:uint8,varchar_limit:uint16,reserved:uint32`. Column names have at most 23 bytes plus NUL. Base-table type codes are `0=INT64`, `1=VARCHAR`; base flags are zero. No-index state uses `index_col=UINT16_MAX` and `index_meta=INVALID_PAGE_ID`. Reserved IndexMeta fields `root:uint32@16`, `height:uint16@20`, `key_column:uint16@22`. [B+ trees](bplus_tree.md) use this layout; index creation publishes the configured index column and IndexMeta PageId in TableMeta.

`CreateTable` validates names, base schema, uniqueness, capacity, and descriptor fit before allocating its TableMeta page and publishing the catalog entry. New table endpoints are invalid and counts are zero. `Database::Insert` and `Database::Delete` are the user-facing mutation path; they call HeapFile and save updated TableMeta. The indexed-table path extends this with index maintenance, described below.

All persistent page contents are read and modified through BufferPool and PageGuard, with modified guards marked dirty. `Flush()` and `Close()` report write errors. Clean close/reopen preserves catalog, schema, endpoints, counts, RIDs, and tuples. No `fsync`, WAL, transaction rollback, crash recovery, free-space map, heap-page reclamation, slot reuse, or overflow tuple pages is provided. An unexpected multi-page I/O failure may leave an uncertain structural mutation; callers should abandon that database instance.

## One INT64 index per table

`TableInfo` now includes `index_col` and `index_meta`, using the reserved TableMeta offsets 36 and 40. Both are invalid before publication; both are valid afterward. Catalog decode rejects mismatched presence markers, out-of-range columns, non-INT64 columns, and invalid/self metadata references. Database reopen opens one stable BPlusTree handle per published index and checks its key column against TableMeta. It reads metadata/root pages without rescanning the heap or rebuilding the tree. Handles live until Database destruction, and the open database must outlive all index cursors.

`Database::CreateIndex(name,column)` resolves the table and rejects a second index, an invalid column, or a non-INT64 type before allocating anything. Base DOUBLE is already rejected by Schema. It creates a private tree, streams the existing `HeapScan::Next()->optional<pair<RID,Tuple>>`, and inserts each live row's exact `(tuple.value(column).as_int64(), RID)`. Tombstones are skipped. Every insertion must succeed and the number built must equal TableMeta.live_rows. Only then does it publish index_col/index_meta with Catalog::Save. No table-wide vector or executor is involved. An empty table publishes an empty leaf root at height 1. If build fails, its allocated pages may remain unreachable; TableMeta is not published early.

Successful user-facing mutations go through Database:

- Insert validates the entire tuple, extracts the configured key, inserts the heap tuple to obtain a new stable RID, inserts the physical pair, and saves TableMeta. An exact duplicate for a freshly allocated heap RID raises DbError as corruption.
- Delete fetches the old owned tuple, validating the heap page owner against the target TableMeta before touching an index. Absent/deleted RIDs return false. It extracts the old key, erases that exact pair, tombstones the heap row, and saves the reduced count. A missing pair for a live heap row raises DbError **before** heap deletion or count mutation. A foreign RID also fails before index mutation.

The consistency invariant is a bijection: every live indexed tuple has exactly one `(indexed value,RID)` entry, and every index entry identifies a live heap tuple with the same value. Logical duplicates are unrestricted. Test-side checking independently compares heap scans, reference RID-to-owned-Tuple maps, physical index traversal, individual heap fetches, row counts, equality/range results, and raw metadata.

`Database::Index(name)` exposes a const tree reference for inspection/lookup; `IndexEqualRange(name,key)` and `IndexRange(name,low,high)` return existing pin-free BPlusIterator cursors yielding owned key/RID pairs. A caller can stream each RID into Database::Fetch. Inclusive ranges retain duplicates and support both INT64 extremes without endpoint arithmetic. No production API collects an unbounded vector. These are storage access methods, not an IndexScan executor.

Single-threaded operation and heap-scan invalidation remain unchanged. Successful mutations invalidate existing heap scans; successful index insertion/deletion invalidates that index's cursors. QueryContext activates a Database flag that rejects Insert, Delete, CreateIndex, CreateTable, and Close during a query. It requires at least eight BufferPool frames, while storage-only small-pool use stays supported. Database::Scan and HeapScan accept an optional borrowed ScanRing for SeqScan; it must outlive the scan. No locks are added.

The operation order is deliberate but has no transactional atomicity. A heap insert followed by index failure, or index erase followed by heap/save failure, can leave inconsistent state. Unexpected structural I/O errors propagate as DbError; abandon/rebuild the affected instance instead of assuming rollback. There is no WAL, recovery, persistent page reuse, or compensation. Clean Close/OpenExisting preserves published metadata and repaired tree contents.

## Source, complexity, and self-check

Start with [EncodeTuple/DecodeTuple](../src/types/tuple.cpp), [page codecs](../src/storage/page_codec.cpp), [DiskManager](../src/storage/disk_manager.cpp), [SlottedPage](../src/storage/slotted_page.cpp), [HeapFile/HeapScan](../src/storage/heap_file.cpp), [Catalog](../src/storage/catalog.cpp), then [Database](../src/storage/database.cpp). Database owns disk/pool/catalog/index handles; HeapFile borrows a pool and a copied mutable TableInfo; HeapScan owns a metadata snapshot and borrows the database epoch. Guarded page views never escape into returned tuples.

Encoding/decoding is linear in encoded bytes/columns. Slot lookup is direct after validation; Validate sorts bounded live ranges to reject overlap, O(s log s) for s slots on one fixed page. Compact visits slots and copies live bytes through one page image. Heap traversal visits all slots/pages, while its tuple-at-a-time API may Fetch/validate the same cached page repeatedly. Thus unique-page scan volume and BufferPool request count differ. Catalog lookup is a linear scan of at most 16 entries. Indexed mutations additionally pay tree mutation cost.

Tail-only insertion and tombstones avoid a free-space map and preserve RID identity, at the cost of retained space and directory exhaustion. There are no overflow rows, UPDATE, slot reuse, or page reclamation. The storage and B+ tree tests independently exercise owner validation, slot/link/count checks, catalog publication, index metadata, and clean reopen.
