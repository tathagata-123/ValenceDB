# Disk-backed B+ tree (V1)

V1 is an educational, insertion-only INT64 B+ tree. It supports exact point
lookup, insertion, leaf/internal/cascading splits, root growth, linked leaves,
and streaming equality and inclusive range scans. Every metadata and node page
is persistent and accessed through BufferPool and PageGuard. There is no tree
deletion, redistribution, merge, root contraction, or tree-specific page retirement.
`Database::Delete` rejects indexed tables before accessing the heap; non-indexed
tables still support deletion.

The algorithm is in `src/index/bplus_tree.cpp`, serialization in `bplus_page.cpp`,
and streaming advancement in `bplus_iterator.cpp`. Database owns table indexes;
IndexScan and the optimizer continue using their existing lookup/order interfaces.
The [V1 Phase 1 report](v1_phase1_refactor.md) records the baseline, removed
mechanisms, contract changes, tests, and size comparison.

## Keys and duplicates

`BPlusKey { int64_t key; RID rid; }` orders numerically by signed logical key,
unsigned RID page ID, then unsigned slot ID. Serialized bytes are never compared
for order. Different RIDs with the same logical key can span many leaves.
`Insert` returns false for an existing exact pair without writes or epoch changes.
`Contains` tests an exact pair. All INT64 keys, including both extremes, are valid.
Invalid RID page IDs are rejected. Standalone trees accept other numeric RIDs
without fetching heap rows. `RID{0,0}` is the smallest RID for range initialization.

## Persistent layouts

All pages use the existing 16-byte common header, including magic, PageType, format version 1, PageId, and zero reserved bytes. Fields use explicit little-endian codecs; no C++ struct, vector, or pointer is serialized.

IndexMeta:

| Offset | Width | Field |
|---:|---:|---|
| 0 | 16 | common header, type IndexMeta |
| 16 | 4 | root PageId |
| 20 | 2 | height, counting both root and leaf levels |
| 22 | 2 | key column; 0–7 or UINT16_MAX for standalone use |
| 24 | 4072 | reserved, zero |

Leaf:

| Offset | Width | Field |
|---:|---:|---|
| 0 | 16 | common header, type BPlusLeaf |
| 16 | 2 | entry count |
| 18 | 2 | reserved, zero |
| 20 | 4 | next leaf PageId or INVALID_PAGE_ID |
| 24 | 8 | reserved, zero |
| 32 | 16 each | INT64 key followed by 8-byte RID |

The leaf maximum is `floor((4096 - 32) / 16) = 254` entries. Non-root minimum occupancy is 127. RID includes its existing two zero padding bytes.

Internal:

| Offset | Width | Field |
|---:|---:|---|
| 0 | 16 | common header, type BPlusInternal |
| 16 | 2 | child count |
| 18 | 2 | reserved, zero |
| 20 | 4 | first child PageId |
| 24 | 8 | reserved, zero |
| 32 | 20 each | 16-byte physical separator, then 4-byte right child PageId |

There are `child_count - 1` separators. The internal maximum is `floor((4096 - 32) / 20) + 1 = 204` children; non-root minimum is 102. Every reachable internal page has at least two children, including the root. Initialize prepares an internal header; Write supplies children before the page becomes reachable.

Page helpers interpret guarded bytes and check identity, type/version,
physical/configured capacity, and access bounds. Unused header and RID padding
is ignored on read; writers still zero it and tests audit the encoded format. Each accessed key/RID or child ID is checked when decoded;
validation does not scan every entry or child. Write checks vector dimensions and
capacity, and encodes the supplied sorted entries. Ordering and full structure
are the algorithm's responsibility and are audited by the independent validator.
The entire IndexMeta reserved area is checked only in tests. These changes leave
the physical format unchanged.

## Creation, opening, and ownership

`Create` allocates IndexMeta and an empty leaf root with height 1. `Open` reads
metadata and checks the root's expected type. It does not rebuild the tree or
scan descendants. Root metadata changes only when the tree grows a new root.
Clean databases from the prior format can still be opened; insertion also
supports valid lower fences that are below the right subtree's current minimum.

`BPlusCapacity` defaults to `{254,204}`. Test limits can use leaf maxima 2–254 and
internal maxima 3–204. For maximum M, non-root minimum occupancy is `ceil(M/2)`;
`LeafMin` and `InternalMin` expose this rule. The root leaf may be empty, while an
internal root has at least two children. Capacity is not stored on disk: supply
the same capacity on reopen. The height limit is 32, bounded by the page namespace
and at least two children per internal node.

Use one live noncopyable, nonmoving handle per tree. The tree must outlive its
cursors; its open BufferPool/database must outlive both. Separate handles for the
same metadata page do not share epochs or cached root metadata. A tree retains
metadata, capacity, and an epoch, but no node cache or page guards.

## Search and range initialization

For each separator S between adjacent subtrees:

```text
max(left subtree) < S <= min(right subtree)
```

`Descend` binary searches separators with upper-bound semantics: equality routes
to the right child. Each internal page is released before fetching its child.
For insertion it records only `(page, child_index)` ancestors.
`Contains` then binary searches the target leaf directly in its guarded bytes.
There is no node-vector decoding or linear ordering audit on the lookup path.

`Start` descends at `(key,RID{0,0})` and binary searches the leaf. The iterator
handles an offset at the leaf's end, so initialization needs no second
leaf-advancement loop. `LowerBound` leaves the upper limit open, `EqualRange`
uses identical low/high keys, and `Range` includes both bounds. Reversed bounds
produce an empty cursor. No endpoint arithmetic is used.

## Insertion and splits

1. Descend and save ancestor page IDs and child indices.
2. Decode one leaf into an owned vector and use `std::lower_bound`. Return false
   for an exact duplicate; otherwise insert the pair at the found position.
3. If the leaf fits, write it and finish. Otherwise `SplitLeaf` allocates one right
   page, keeps the original page as left, and divides entries at `size/2`.
   It links left to right and right to the old successor. Copy the first right
   key upward as `SplitResult { separator, right }`.
4. Pop an ancestor, decode it, insert the separator at the saved child index and
   the new right child immediately after that index. If it fits, write it and
   finish. Otherwise `SplitInternal` allocates its right page and divides the
   child vector at `size/2`. Move the boundary separator upward, omitting it from
   both internal pages. Continue with the next ancestor.
5. If the path is exhausted, allocate a new root over the old root (left half)
   and the final right page. `SetRoot` updates persistent and cached metadata.
6. Each successful insertion increments the mutation epoch once.

A leaf overflow of 255 entries splits into 127 and 128 entries. An internal
overflow of 205 children splits into 102 and 103 children. The extra item goes
right. A maximum-4 leaf example, omitting RIDs:

```text
[10,20,30,40] + 25 -> [10,20,25,30,40]
left: [10,20] -> right: [25,30,40] -> old_next
copy up: 25
```

For an internal node with five children:

```text
children:   C0 C1 C2 C3 C4
separators: S0 S1 S2 S3
left:  C0 C1       with S0
move up: S1
right: C2 C3 C4    with S2 S3
```

The distinction between copying a leaf boundary and moving an internal boundary
is explicit in the two split functions. Spans select vector halves without
allocating additional left/right vectors. Pages are allocated only as actual
splits occur; there is no prediction or reservation pass.

## Streaming cursor and resource bounds

`BPlusIterator::Next` checks its captured epoch, fetches the current leaf, copies
one entry, and advances its offset/leaf ID. It follows next links on demand and
compares each logical key to the inclusive upper bound. It returns an owned
`optional<BPlusKey>` with no pin retained. EOF is repeatable; errors propagate.
IndexScan fetches the heap row only after this leaf guard has been released.

Every successful insertion invalidates all prior cursors, including exhausted
ones. Exact duplicate attempts and rejected Database deletes preserve cursors.
The epoch resets on reopen; a cursor cannot outlive its original tree.

At most one tree-owned guard is pinned at a time. Owned scratch consists of one
leaf overflow vector (at most 255 entries), one parent overflow representation
(at most 205 children and 204 separators), and O(height) ancestor IDs/indices.
No table-size or tree-size data structure is retained. One-frame buffer pools
are supported. Search costs O(height * log(node capacity)) key comparisons;
insert additionally moves/serializes O(node capacity) data for each changed
node. Range scans retain constant cursor state and visit linked leaves as needed.

## Failures, persistence, and independent validation

Actual I/O errors propagate and are never treated as successful insertion or
normal EOF. RAII releases guards during exceptions. An allocation or I/O failure
can occur after an earlier split changed pages. Abandon or rebuild that instance;
V1 promises no rollback, retry safety, atomic multi-page mutation, crash recovery,
or reclamation of unreachable allocations. `FlushAll`/`Database::Close` detect
write failures and support clean reopen, without fsync or crash-durability claims.

`tests/bplus_validator.hpp` decodes raw page fields independently of production
search and page helpers. It checks numeric ordering, non-root occupancy, root
exceptions, separator inequalities, uniform depth, single reachability, allocated
IDs, metadata, padding, and a separate cycle-checked linked-leaf walk. Tests keep
corruption fixtures for this validator while production validation stays local.
Randomized set-oracle comparisons, exact duplicate attempts, signed extrema,
small capacities/pools, root growth, reopen, and pin cleanup remain covered.
Database integration compares heap scans, RID-to-tuple maps, index traversal,
row counts, raw metadata, and equality/range results. Indexed deletion rejection
is tested for unchanged persistent bytes and cursor validity.

## Function guide

| Function | Responsibility |
|---|---|
| `Create` | Allocate metadata and an empty root leaf. |
| `Open` | Read metadata, validate root type/capacity, construct a handle. |
| `Descend` | Binary-search internal pages; optionally record ancestor IDs/indices. |
| `LeafLowerBound` | Binary-search guarded leaf entries without decoding a vector. |
| `Contains` | Descend and compare the exact composite pair. |
| `Insert` | Insert in a leaf; propagate splits; create a root if needed. |
| `SplitLeaf` | Allocate right leaf, divide entries, fix links, copy boundary upward. |
| `SplitInternal` | Allocate right internal page, divide children, move boundary upward. |
| `ReadLeaf` / `WriteLeaf` | Decode/encode bounded leaf entries and successor through a short guard. |
| `ReadInternal` / `WriteInternal` | Decode/encode bounded child and separator vectors through a short guard. |
| `AllocatePage` | Allocate one persistent page and release its pin. |
| `SetRoot` | Write root/height metadata and update the cached values. |
| `Start` | Find a leaf and offset for a logical lower bound. |
| `LowerBound` / `EqualRange` / `Range` | Configure unbounded, equal, or inclusive scan bounds. |
| Metadata/capacity/epoch accessors | Return handle state without I/O. |
| `BPlusKey::operator<` | Compare signed key, numeric page ID, then slot ID. |
| `BPlusCapacity::Validate`, `LeafMin`, `InternalMin` | Check allowed capacities and expose occupancy minima. |
| Leaf `Initialize`, `Count`, `Next`, `SetNext` | Initialize page headers; read count/link; write successor. |
| Leaf `Entry`, `WriteEntry`, `Write`, `Validate` | Bounded field access, whole-node serialization, constant-size page checks. |
| Internal `Initialize`, `ChildCount`, `Child`, `Separator` | Initialize headers and access bounded internal fields. |
| Internal `Write`, `Validate` | Serialize children/separators and check page identity/capacity. |
| Metadata `Read`, `Write` | Decode/encode root, height, and key column with validity checks. |
| `CheckRID`, `CheckKey`, `CheckMeta`, `CheckReserved`, `ReadKey`, `WriteKey` | Small local input/field checks and composite-key codecs. |
| Iterator constructor / `Next` | Capture epoch and scan position; yield one owned entry with no retained pin. |
