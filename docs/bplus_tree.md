# Disk-backed B+ tree

The B+ tree implements insertion, exact deletion, lookup, lower bound, and streaming equality/range traversal. Every IndexMeta, leaf, and internal node is a persistent `PageRef{FileKind::Database, id}` accessed through BufferPool and PageGuard. The tree handle stores metadata, capacity configuration, and a mutation epoch. It retains no nodes, page pins, parent pointers, or in-memory map of the tree.

The implementation is in `src/index/bplus_tree.cpp`, with byte-format helpers in `bplus_page.cpp` and cursor advancement in `bplus_iterator.cpp`. Deletion repair lives here; table index ownership and maintenance are coordinated by `src/storage/database.cpp`.

## Keys and duplicates

`BPlusKey { int64_t key; RID rid; }` orders by signed logical key, unsigned numeric RID page ID, then unsigned numeric slot ID. Serialized little-endian bytes are never compared for order. `Insert(key,rid)` returns false for an existing physical pair and true for a new pair. Different RIDs for one logical key are allowed, including duplicates spanning many leaves.

All INT64 values are valid, including both extremes. An invalid RID page ID is rejected. Standalone trees accept other numeric RIDs without fetching heap pages or checking table ownership; Database-owned indexes validate RID ownership through the heap before deletion. `RID{0,0}` is the smallest ordering component.

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

`BPlusLeafPage` and `BPlusInternalPage` interpret guarded PageBytes without owning BufferPool. Validate checks common identity/version, reserved fields, physical/configured count bounds, numeric ordering, and local link/child validity. Indexed access checks bounds. Bounded Write helpers reject overflow and unordered inputs before changing fields. Callers validate before reading individual fields and mark modified guards dirty. `WriteEntry` is a low-level bounded field write; callers preserve ordering. Subtree fences, reachability, and cross-page links are checked by the independent test validator.

## Creation, opening, and capacity

`BPlusTree::Create(pool, key_column, capacity)` allocates IndexMeta first and an empty root leaf second. It stores a valid root with height 1, count 0, and invalid next leaf. Root leaves may contain 0–254 entries without the non-root minimum; internal roots may contain 2–204 children.

Retain `MetaPage()` to reopen a standalone tree. `BPlusTree::Open(pool, meta_page, capacity)` fetches/validates metadata and the root's expected type through BufferPool. It neither scans heap data nor assumes a fixed root page. Descendants are validated when visited; opening is not a full structural audit. IndexMeta is written at creation, root growth, and root contraction.

`BPlusCapacity` defaults to `{254,204}`. Tests may supply `{4,4}` without changing the physical page format. Valid leaf maxima are 2–254 and internal maxima 3–204. For a configured maximum M, non-root minimum is `ceil(M/2)`, exposed by `LeafMin()` and `InternalMin()`. Root exceptions still apply. Capacity is not persisted: tests **must supply the same configuration on reopen**. A different configuration cannot always be detected from page counts. Height is checked in 1–32; the 32-bit page namespace and at least two children per internal node bound the attainable height.

The tree is noncopyable and nonmoving, so cursors can refer to its epoch safely. Use one live handle per tree; it must outlive its cursors, and its BufferPool/database must remain open and outlive both. Separate handles for one metadata page do not share mutation epochs or cached root metadata.

## Stable lower fences and search

For adjacent subtrees separated by physical key S:

```text
max(left subtree) < S <= min(right subtree)
```

A separator is a lower fence for its right subtree. It need not equal the current right minimum. This permits deletion to remove a former right minimum without rewriting every ancestor. Splits create fences from split boundaries; tests also construct a valid fence strictly below the right minimum to verify the weaker contract.

Descent binary searches with composite-key upper-bound semantics:

```text
K < S0              -> child 0
S0 <= K < S1        -> child 1
...
K >= last separator -> last child
```

Equality routes right. `Contains(key,rid)` descends with the full physical key, binary searches one leaf, and compares the exact pair. Local validation examines bounded page contents; lookup does not scan other leaves or the whole tree.

## Streaming traversal and cursor lifetime

`LowerBound(K)` descends at `(K,RID{0,0})`, finds the leaf-local lower bound, and advances to the next leaf if that position is past the leaf's end. It returns an unbounded ascending cursor. `EqualRange(K)` returns an equality cursor. `Range(low,high)` is inclusive: `low <= key <= high`; reversed ranges are empty. No endpoint arithmetic is used, so INT64_MIN/MAX are safe and all duplicate boundary entries are included.

`BPlusIterator::Next()` returns `optional<BPlusKey>`, an owned copy. It checks the captured epoch, fetches and validates the current leaf, copies the entry, advances its leaf/offset state, releases the guard, then returns. It follows next-leaf links on demand. Bounded traversal stops by comparing each logical key with the inclusive upper bound. EOF is repeatable. Errors throw DbError and do not masquerade as EOF.

A cursor holds only a tree reference, leaf PageId, offset, captured epoch, and optional upper bound. It retains no guard, pointer into a page, or page-backed span. LowerBound and every Next return without cursor pins, including EOF and exception paths. Owned RIDs remain usable after eviction and later cursor calls.

Every successful insertion or deletion increments the in-memory epoch once. Existing cursors, including exhausted ones, then throw on Next. Duplicate insertion and absent exact-pair deletion leave the epoch and cursor validity unchanged. Epoch is not persisted and resets on reopen; no cursor from a destroyed tree may be reused.

## Insertion and split propagation

The implementation follows this sequence explicitly:

1. Validate RID and descend. Save ancestor PageIds, chosen child indices, and child counts; release each guard before descending.
2. Binary search the leaf. Return false for an exact duplicate before allocation or modification.
3. Build a bounded leaf vector including the new pair. If it fits, write that leaf, mark dirty, release, and increment the epoch.
4. If full, predict the entire split cascade from saved ancestor counts. Reserve a right leaf, one right internal page per overflowing ancestor, and a new root if propagation reaches above the old root. Release each NewPage guard immediately.
5. Split the leaf, reuse the old page as left, and promote the first right entry. Set right.next to the old next leaf and left.next to the new right page.
6. Insert the separator at the saved parent child index and the new right child immediately after it. If the parent fits, write it and finish.
7. Otherwise split its child vector, promote exactly the boundary separator, remove that separator from both split internal pages, and propagate upward. Reuse the original internal page as left.
8. When propagation passes the old root, write a new two-child root, persist its PageId and incremented height in IndexMeta, then increment the mutation epoch.

Only one leaf overflow vector (at most 255 entries), one current parent overflow representation (at most 205 children), and O(height) IDs/counts are held in memory. No overflow node is written to disk. Parent insertion uses the recorded child index rather than searching by logical key.

For both split kinds, the left receives `floor(total/2)` entries or children and the right receives the remainder. For odd totals the extra item goes right. Production leaf overflow splits 255 entries into 127 and 128; internal overflow splits 205 children into 102 and 103.

### Worked leaf split

With maximum 4, omit RIDs for readability:

```text
[10,20,30,40] + 25 -> [10,20,25,30,40]

original left page: [10,20] -> new right page: [25,30,40] -> old_next
new separator: 25
```

If the original page was the root, the new root has two children and separator 25; height becomes 2. The old root leaf is reused as left.

### Worked internal split

With maximum 4 children, an overflowing node is:

```text
children:   C0 C1 C2 C3 C4
separators: S0 S1 S2 S3

left:  children C0 C1       separators S0
promote: S1
right: children C2 C3 C4    separators S2 S3
```

S1 belongs only in the parent. Production uses the same rule: left C0..C101 with S0..S100, promote S101, right C102..C204 with S102..S203. Tests check the promoted separator is absent from both split children.

Internal split propagation uses the same `ReadInternal(page)` helper as deletion.
It fetches, validates, and returns owned `children` and `separators` vectors with
one spare child slot. Insertion no longer contains a second internal decoder or
manages a parent page guard itself. The fetch count is unchanged. Split writes
use spans over these vectors, avoiding separate left/right copies. The ordinary
leaf insertion path still writes through its existing guard, and lookup/descent
still binary search directly in pages.

## Pins, failures, and persistence

This implementation uses at most **one tree-owned page guard at a time**, including reservation and metadata handling. Structural mutations decode pages to bounded owned scratch before fetching another page. Search and cursor paths read guarded page fields directly without node vectors. The ancestor path and reservations contain only IDs/counts. Tests force eviction with 1–3 frames and verify active pins return to zero after successful and expected-error operations. Multi-level reservations do not accumulate pins.

Preallocation prevents a predictable split-page allocation failure after existing nodes have already changed. It is not transactionality or crash atomicity. Allocation is append-only; unused reserved pages can remain unreachable and are not reclaimed. The focused failure test injects a dirty-eviction write error inside NewPage during reservation, proves existing tree bytes/contents/epoch remain unchanged, and successfully retries. It uses the existing storage/buffer failure hook without adding a separate fault framework.

Unexpected I/O failure after structural modification propagates DbError and may leave uncertain persistent state. Stop using and abandon/rebuild the affected instance; there is no rollback claim, WAL, shadow paging, recovery log, or poisoned-handle framework. Checked BufferPool FlushAll or Database Close provides clean persistence, with no fsync or crash-recovery promise.

Reopen tests cover empty roots, one-entry roots, and multi-level trees, verifying root/height, types, entries, links, ranges, and insertion after reopen. The independent validator in `tests/bplus_validator.hpp` decodes raw fields using the page codecs; it does not call production tree search or page helpers. It checks ordering, occupancy/root exceptions, both stable-fence inequalities, uniform depth, single reachability, allocated IDs, metadata, and a separate cycle-checked leaf-chain traversal against recursive in-order pages and entries. Corruption tests demonstrate rejection of bad fences, repeated children, cycles, broken chains, underfull leaves, duplicate entries, wrong types, and metadata mismatches.

The single-threaded tree supports one INT64 logical key type and one tree per indexed table; see [storage.md](storage.md). IndexScan consumes its owned key/RID cursor and fetches each heap row only after the cursor releases its leaf pin; see [execution.md](execution.md).

## Exact deletion and repair

`Erase(key,rid)` descends with the full physical key and an unpinned ancestor path. It binary searches a bounded owned leaf vector and returns false before any page write if that exact pair is absent. A different RID for the same logical value is unaffected. Successful erase increments the epoch exactly once; all prior cursors then throw on Next, even if exhausted.

Removing a key without underflow writes only that leaf. Stable fences need no minimum propagation: if left ends at 20, fence is 30, and right is `[30,40,50]`, deleting 30 leaves `[40,50]` and the unchanged fence still satisfies `20 < 30 <= 40`. Parent updates occur only during redistribution, parent-entry removal, or root contraction.

Non-root minima are `ceil(maximum/2)`: production leaves 127 of 254 entries; production internals 102 of 204 children; capacity `{4,4}` uses 2 for both. Borrow only from occupancy **strictly greater** than minimum. The deterministic policy at each level is:

1. Borrow from the left sibling if it can lend.
2. Otherwise borrow from the right sibling if it can lend.
3. Otherwise merge into the left sibling if one exists.
4. Otherwise merge the right sibling into the current node.

The left page always survives a merge. An underfull node has minimum minus one items; a nonlending sibling has minimum. Their combined occupancy is `2*ceil(M/2)-1 <= M`, for both even and odd maxima. Internal merges have one fewer separator than total children, including the parent fence. No over-capacity or one-child internal node is serialized.

### Leaf redistribution and merge

Left borrow removes the left leaf's largest pair and prepends it to current. That moved pair becomes the parent fence. Right borrow removes the right leaf's smallest pair and appends it to current; the **remaining right first pair** becomes the parent fence. Keeping the moved pair as the fence would violate the strict left inequality. Both borrow operations preserve next-leaf links.

For a merge, concatenate left entries then right entries and set `left.next = right.next`. Remove the right child's ID and the separator immediately before it from the parent. `RemoveRightChild(left_index)` explicitly erases child `left_index+1` and separator `left_index`; the other child IDs and fences retain their order.

### Internal left rotation: worked example

Omit RIDs, but use physical-key ordering throughout. Assume the child ranges satisfy the displayed stable fences:

```text
left children:     A=[10,11] B=[20,21] C=[30,31]
left separators:  19        29
parent fence:                         39
current children:                        D=[41,50,51]  (underfull)

borrow C from left:
left children:     A         B
left separators:       19
parent fence:                         29  <- old last left separator
current children:                        C          D
current separator:                             39  <- old parent fence
```

The old left separator 29 becomes the parent boundary before moved child C. The old parent fence 39 separates C from the old first current child D. Moving a child alone would lose that boundary. Notice that neither 29 nor 39 equals a current child minimum: the algorithm rotates actual stable fences.

Right rotation is symmetric. For current `[A]`, parent fence 29, and right children `[B,C,D]` with separators `[39,49]`, move B to current, append old parent fence 29 between A and B, remove right's first child and first separator, and promote 39 to the parent. Right remains `[C,D]` with separator 49.

### Internal merge and upward propagation

For adjacent internals, the survivor receives `left.children + right.children` and `left.separators + parent_fence + right.separators`. The parent fence becomes a separator inside the survivor; then the parent loses the right child and that fence.

`Erase` calls `RepairLeaf` once, then iterates up the saved path carrying only an internal node. `RepairInternal` handles each later underflow; no leaf/internal mode flag is needed. A parent at valid occupancy is written and propagation stops. An underfull parent remains in bounded owned scratch and becomes current at the next ancestor. Borrow restores occupancy and stops propagation after its parent is written. The implementation keeps one decoded parent/current set and at most two sibling values, O(height) IDs, and **at most one PageGuard** at a time; sibling checks release their guards before fetching another page. This also works with a one-frame pool.

A left sibling that cannot lend remains in owned scratch while the right sibling is
checked. If neither lends, the merge reuses that decoded left value. With no left
sibling, the right value is merged in its existing scope. Each sibling is read and decoded
at most once for the repair decision; writes still fetch their destinations.
There are no overlapping page guards. This trades
at most one additional bounded sibling vector set for avoiding a second fetch,
validation, and decoding. ReadLeaf reserves its known count; deletion frees a slot before borrowing.
ReadInternal also leaves room for one added child. Both avoid repeated vector
growth during decoding.

### Root contraction and the empty tree

If removal leaves the root with one child, persist that child as root and decrement IndexMeta height. Do not persist the underfull old root. Worked example:

```text
height 3: root R -> [internal L, internal Q]
leaf merge under L -> L underflows
merge L + root fence + Q -> one surviving internal L
root R now has only L
height 2: IndexMeta.root = L; R and Q become unreachable
```

The deterministic sequential test inserts 0 through 10 and deletes 0, 1, 2: the root contracts from height 3 to 2 with surviving fences 6 and 8, then closes/reopens. Larger traces empty a height-7 tree through multiple contractions. When the root is a leaf, occupancy may be zero. The final empty root remains allocated at height 1 with an invalid next link, and insertion reuses that root normally.

### Retirement, validation, and failure limits

After every structural link and root metadata update is finished and guards are released, invalidate the retired pages through `BufferPool::Invalidate`. Invalidation flushes any dirty cached copy before removing its cache mapping. Retirement means structural unreachability; there is no persistent retired-page marker, free list, page-ID reuse, or file truncation. Repeated growth and shrinkage can grow the file indefinitely.

The independent raw-page validator accepts a test-supplied retired-ID set and rejects any retired page reached through children or leaf links. It exports reachable IDs for before/after comparisons, retains stable-fence inequalities, and checks occupancy, depth, unique reachability, cycles, metadata, and exact leaf-chain order. Deterministic fixtures assert actual child/fence rotations and parent-entry removal; randomized tests compare to an independent lexicographic set.

Deletion and Database index maintenance are not atomic under unexpected I/O failure. There is no WAL, rollback, crash recovery, or poisoned-handle mechanism. Propagate DbError and abandon/rebuild an instance whose consistency is uncertain. Clean checked close/reopen is supported; crash recovery is not.

## Complexity and interview checkpoints

For height h and bounded page capacity B, descent visits h structural pages. Searches within a validated page use binary search, but full page validation examines O(B) entries; implementation CPU is therefore bounded by O(hB), not merely O(h log B). Splits/repairs move O(B) entries per level and use O(h) ancestor/reservation IDs plus bounded decoded page scratch. A mutation touches O(h) structural pages with a constant number of sibling accesses per level; cache misses and writeback depend on F.

A range visits the descent path and linked leaves, but this implementation fetches/validates a leaf on each cursor call. Logical distinct leaf pages, repeated BufferPool accesses, and heap RID fetches must be separated. Unclustered table range scans can revisit many heap pages. The append-only retirement policy simplifies identity at the cost of file growth. Stable fences avoid unnecessary ancestor minimum rewrites, while explicit fence rotations preserve correctness during child movement.

The B+ tree tests cover boundary search, duplicate keys, leaf/internal split paths, redistribution, merging, cascading root contraction, reopen, randomized oracle comparison, corruption detection, and independent structural validation. Small capacities are test configuration, not a persisted production table setting.
