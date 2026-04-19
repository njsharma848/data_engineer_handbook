# 13 — Linked List

## 1. When to Use
- Problem explicitly provides `ListNode` / `head` / `next` (or `prev`) pointers.
- Keywords: *"reverse"*, *"detect cycle"*, *"middle node"*, *"merge sorted lists"*, *"remove Nth from end"*, *"reorder"*, *"rotate"*, *"intersection"*, *"partition"*, *"swap nodes"*, *"copy with random pointer"*, *"flatten"*, *"palindrome list"*, *"LRU / LFU cache"*.
- You need **O(1) insert/delete** at arbitrary positions *given a pointer to the node*.
- Memory constraints forbid copying the whole sequence into an array, or the problem demands `O(1)` auxiliary space.
- A problem feels array-ish but forbids random access — you must slide pointers rather than index.
- You are designing a data structure where O(1) insertion/removal at both ends is critical and rebalancing is not needed (LRU cache, MRU list, free-list).

### Signals that it is NOT a linked-list-specific problem
- Random access or binary search is required — copy to an array.
- The problem accepts `O(n)` extra space and index-by-position — array is simpler.
- You need priority ordering — heap, not linked list.

## 2. Core Idea
A linked list is a chain of nodes — each knows only its immediate neighbour(s). You cannot jump, so every technique coordinates a **small fixed number of pointers** moving along the chain:
- **Slow/fast** for cycles and midpoints (LC 141, LC 142, LC 876).
- **Prev/curr/next** for in-place reversal (LC 206, LC 92).
- **Dummy heads** to fold the "is this the new head?" branch into the general case.
- **Gap pointers** for "nth from end" (LC 19).
- **Interleave-then-split** for copying with random pointers (LC 138).

The mental model: "I hold two or three fingers on the list and slide them deliberately, **saving `next` before any rewire**." The hazard is that rewiring a pointer without first capturing the old neighbour loses the tail of the list forever.

Because the data structure is intrinsically sequential, most algorithms are `O(n)` time and `O(1)` space — you make one or a constant number of passes, moving your pointers a constant distance per step.

## 3. Template

### Template A — dummy head for head-uncertain rewiring
```python
class ListNode:
    def __init__(self, val=0, nxt=None):      # GOTCHA: `next` shadows builtin — use `nxt` in __init__ params
        self.val = val
        self.next = nxt                        # attribute is `.next`; param just named differently

def pattern_with_dummy(head, should_drop):
    dummy = ListNode(0, head)                  # dummy points to head; unifies head-change case
    prev = dummy
    while prev.next:                           # `while node:` auto-stops on None (falsy)
        if should_drop(prev.next):
            prev.next = prev.next.next        # unlink — GC reclaims old node since nothing references it
        else:
            prev = prev.next                  # advance (ONLY when not dropping — else you skip the new prev.next)
    return dummy.next                          # GOTCHA: return dummy.next (head may have changed), NOT head
```

### Template B — reverse in place (three-pointer rewire)
```python
def reverse(head):
    prev, curr = None, head                   # tuple-unpacking: simultaneous assignment
    while curr:
        nxt = curr.next                       # GOTCHA: MUST save `next` BEFORE rewiring — else you lose the tail
        curr.next = prev                      # rewire: flip the arrow
        prev = curr                           # advance prev. GOTCHA: order matters — prev=curr BEFORE curr=nxt
        curr = nxt                            # advance curr
    return prev                               # new head (old last node; `curr` exits as None)
```

### Template C — slow/fast pointers (cycle detect, midpoint)
```python
def middle(head):
    """For even-length n, returns the 2nd middle (n//2 + 1 in 1-indexed)."""
    slow = fast = head                          # chained assignment: both point to same node initially
    while fast and fast.next:                   # AND short-circuits: fast.next not accessed if fast is None
        slow = slow.next
        fast = fast.next.next                   # GOTCHA: `fast.next.next` safe only because `fast.next` just checked
    return slow

def has_cycle(head):
    slow = fast = head
    while fast and fast.next:
        slow = slow.next; fast = fast.next.next
        if slow is fast: return True            # GOTCHA: `is` (identity) not `==` (value) — values may collide
    return False

def cycle_start(head):
    """Floyd's: after meeting, restart one at head; both step 1 at a time."""
    slow = fast = head
    while fast and fast.next:
        slow = slow.next; fast = fast.next.next
        if slow is fast:
            ptr = head
            while ptr is not slow:              # identity check — walks until pointers coincide (cycle entrance)
                ptr = ptr.next; slow = slow.next
            return ptr
    return None                                 # no cycle detected
```

### Template D — merge two sorted lists (dummy + merge)
```python
def merge(a, b):
    dummy = ListNode()                         # default args give val=0, next=None
    tail = dummy
    while a and b:                             # loop exits when EITHER is None
        if a.val <= b.val:                     # GOTCHA: `<=` (not `<`) preserves stability — a's equals come first
            tail.next = a; a = a.next
        else:
            tail.next = b; b = b.next
        tail = tail.next
    tail.next = a or b                         # `a or b`: returns first truthy; `None or node` == node. Attaches leftover
    return dummy.next                          # new head (never `head` — dummy is a throwaway)
```

### Template E — gap pointers (nth from end)
```python
def remove_nth_from_end(head, n):
    dummy = ListNode(0, head)                  # dummy handles "remove actual head" case
    lead = lag = dummy
    for _ in range(n + 1):                     # +1 so lag ends one before the victim (not ON it)
        lead = lead.next                       # GOTCHA: if n > length, lead.next raises AttributeError — problem guarantees valid n
    while lead:                                # advance both until lead falls off the end
        lead = lead.next; lag = lag.next
    lag.next = lag.next.next                   # unlink victim
    return dummy.next
```

### Template F — doubly linked list + hashmap (LRU cache)
```python
class Node:
    def __init__(self, key=0, val=0):
        self.key, self.val = key, val          # tuple-unpacking assignment (cleaner than two separate statements)
        self.prev = self.next = None           # chained assignment: both set to same None (safe for immutables)

class LRUCache:
    def __init__(self, cap):
        self.cap = cap
        self.head, self.tail = Node(), Node()  # sentinel nodes — no edge cases for empty list
        self.head.next, self.tail.prev = self.tail, self.head   # simultaneous wire-up via tuple-assignment
        self.map = {}                          # key → Node (so we can unlink in O(1) given a key)

    def _remove(self, node):                   # underscore prefix = "private by convention"
        node.prev.next = node.next             # skip node from prev side
        node.next.prev = node.prev             # skip node from next side

    def _add_front(self, node):
        node.next = self.head.next             # GOTCHA: order matters — set new node's pointers BEFORE rewiring neighbours
        node.prev = self.head
        self.head.next.prev = node
        self.head.next = node

    def get(self, key):
        if key not in self.map: return -1
        node = self.map[key]
        self._remove(node); self._add_front(node)   # move-to-front = mark as recently used
        return node.val

    def put(self, key, val):
        if key in self.map:
            self._remove(self.map[key])        # unlink old node if key exists (will replace, not update in place)
        node = Node(key, val)
        self.map[key] = node
        self._add_front(node)
        if len(self.map) > self.cap:           # evict AFTER insert — cleaner than checking before
            lru = self.tail.prev
            self._remove(lru); del self.map[lru.key]   # `del dict[key]` removes entry in O(1)
```

Key mental tools:
- **Dummy head** is almost always worth it — it unifies "operate on the first node" with the rest. Return `dummy.next`, not `head` (which may have changed).
- **Save `next` first** before rewiring any `.next` pointer — otherwise you lose the rest of the list.
- Always **null-guard `.next.next`** before dereferencing. `fast and fast.next` is the canonical slow/fast loop condition.
- Distinguish **"new list"** (build with dummy, allocate new nodes) vs **"in-place rewire"** (reuse existing nodes, never allocate).
- **Sentinel tails** in doubly-linked lists remove branches on `is_empty` and `at_end`.
- To **convert recursion to iteration**: explicit stack if you need the stack frame; otherwise rewire with prev/curr/next and scan once.
- If the problem allows `O(n)` extra space and you cannot get the algorithm cleanly in `O(1)`, **copy to an array first** — interviewers often accept that as a sane first solution.

## 4. Classic Problems
- **LC 206 — Reverse Linked List** (Easy): three-pointer rewire (Template B).
- **LC 141 / 142 — Linked List Cycle I & II** (Easy/Medium): Floyd's tortoise and hare (Template C).
- **LC 21 — Merge Two Sorted Lists** (Easy): dummy + merge (Template D).
- **LC 19 — Remove Nth Node From End** (Medium): gap of `n` between two pointers (Template E).
- **LC 143 — Reorder List** (Medium): midpoint + reverse second half + merge.
- **LC 146 — LRU Cache** (Medium): doubly linked list + hashmap (Template F).
- **LC 138 — Copy List with Random Pointer** (Medium): interleave-then-split, or hashmap from old node → new node.
- **LC 25 — Reverse Nodes in k-Group** (Hard): reverse a block, reconnect to previous block's tail.

## 5. Worked Example — Reorder List (LC 143)
Problem: given `1 -> 2 -> 3 -> 4 -> 5`, reorder to `1 -> 5 -> 2 -> 4 -> 3`. In general: `L0 → Ln → L1 → Ln-1 → L2 → ...`.

This problem composes three textbook sub-routines. Seeing them all in one solve is the best linked-list-composition drill on LeetCode.

### Step 1. Find the midpoint (Template C)
After the slow/fast walk, `slow` is at the node **just past the split point** (for odd `n`, the true middle; for even, the second middle).

```
slow/fast start:   1 -> 2 -> 3 -> 4 -> 5
  iter 1:          slow=2, fast=3
  iter 2:          slow=3, fast=5
  iter 3:          fast.next is None, stop.
```
So `slow = 3`. First half will be `1 -> 2 -> 3`; second half we reverse is `4 -> 5`. We can split the list at `slow` (set `first_half_tail.next = None`) to prevent the first half from accidentally merging back into the second.

### Step 2. Reverse the second half (Template B)
Treat `slow.next` as the head of the second list. In our example the second half is `4 -> 5`; reversed it becomes `5 -> 4`. After reversal, the first list is `1 -> 2 -> 3 -> None` and the second list is `5 -> 4 -> None`.

Careful: before reversing, cut the first list's tail so the two lists don't share nodes. Set `slow.next = None` **after** capturing the second-half head.

### Step 3. Merge-alternate the two lists
Walk both lists, picking one from the first, one from the second, until one is exhausted. Because the first list is always **≥** the second in length (by construction), the second runs out first.

```python
def reorderList(head):
    if not head or not head.next: return       # GOTCHA: empty/single-node early return; `return` without value = `return None`

    # Step 1: find middle
    slow = fast = head
    while fast.next and fast.next.next:        # different loop condition vs `while fast and fast.next`: ends slow EARLIER (at first-half tail, not second-middle)
        slow = slow.next
        fast = fast.next.next

    # Step 2: reverse second half
    second = slow.next
    slow.next = None                            # cut — GOTCHA: without this, the two halves share tail and reversal creates a cycle
    prev = None
    while second:
        nxt = second.next                       # save-before-rewire
        second.next = prev
        prev = second
        second = nxt
    second = prev                                # head of reversed second half (was the old tail)

    # Step 3: merge alternating (zipper)
    first = head
    while second:                                # second runs out first (it's the shorter half for odd n)
        f_next, s_next = first.next, second.next   # tuple-unpack: save BOTH next pointers before any rewire
        first.next = second
        second.next = f_next
        first = f_next
        second = s_next
```

### Step 4. Trace with `1 -> 2 -> 3 -> 4 -> 5`
After Step 1: `slow = 3` (points to node with value 3), first half tail is node 3.

After Step 2: `slow.next = None`, so first = `1 -> 2 -> 3`, second (reversed) = `5 -> 4`.

Step 3 merge, iteration by iteration (`first`, `second` point at nodes by value):

| iter | `first` | `second` | `f_next` | `s_next` | list after rewires |
|---|---|---|---|---|---|
| 1 | 1 | 5 | 2 | 4 | `1 -> 5 -> 2 -> 3` and second = 4 |
| 2 | 2 | 4 | 3 | None | `1 -> 5 -> 2 -> 4 -> 3` and second = None |
| stop | 3 | None | — | — | — |

Final: `1 -> 5 -> 2 -> 4 -> 3` ✓. Time `O(n)` (three linear passes). Space `O(1)` — no allocations, all in-place rewiring.

### Step 5. What the three sub-routines buy you
- **Midpoint**: lets you split without counting — one pass, no length pre-computation.
- **Reverse**: turns "take from the back" into "take from the front", which is the only cheap operation on a singly-linked list.
- **Merge-alternate**: zipper-merge; the canonical way to interleave two lists in place.

Every complex linked-list problem is a composition of these three moves plus dummy heads. If you can produce them on reflex, you can tackle LC 25 (reverse k-group), LC 143 (reorder), LC 234 (palindrome), LC 148 (merge sort), and LC 92 (reverse between `m` and `n`) from scratch.

## 6. Common Variations

### In-place reversal family
- **Reverse linked list** (LC 206): Template B.
- **Reverse between `m` and `n`** (LC 92): reverse a sublist in place using a dummy head; the head-of-block pointer is the key.
- **Reverse in groups of `k`** (LC 25): reverse a block, reconnect with the tail of the previous block. Careful: last partial block should not be reversed.
- **Swap nodes in pairs** (LC 24): k = 2 special case.
- **Palindrome linked list** (LC 234): reverse second half, compare halves, optionally restore.

### Slow/fast (Floyd's) family
- **Cycle detection** (LC 141): slow+1, fast+2.
- **Cycle entry** (LC 142): after meeting, restart one pointer at head; they meet at the cycle entrance — derivation uses `distance_from_head_to_start + loop_length * k`.
- **Middle of linked list** (LC 876): slow/fast to the middle.
- **Find the duplicate number** (LC 287): Floyd's on `arr[i]` as "next pointer".
- **Happy number** (LC 202): cycle detection on the sum-of-squares sequence.

### Gap / lead-lag pointers
- **Remove Nth from end** (LC 19): gap of `n`, then walk both to the end (Template E).
- **Intersection of two lists** (LC 160): pointers swap to the other list's head on reaching null; they meet at the intersection (or both at null).

### Merge / dummy-head construction
- **Merge two sorted lists** (LC 21): Template D.
- **Merge k sorted lists** (LC 23): heap of list heads, or pairwise merge.
- **Partition list** (LC 86): two dummy lists (less-than, greater-equal), then concatenate.
- **Odd even linked list** (LC 328): two pointers, stitch odd and even sub-chains.

### Copy / clone with auxiliary structure
- **Copy list with random pointer** (LC 138): interleave new nodes with originals; set new node's random; split into two lists.
- **Deep copy arbitrary graph** (LC 133): hashmap `old → new` during DFS/BFS.
- **Flatten multilevel doubly linked list** (LC 430): DFS with a stack; child pointers become next pointers.

### Design / composite structures
- **LRU cache** (LC 146): doubly linked list + hashmap (Template F).
- **LFU cache** (LC 460): two-level structure — frequency buckets, each a doubly linked list.
- **All O(1) data structure** (LC 432): doubly linked list of buckets.

### Sorting a linked list
- **Sort list** (LC 148): merge sort is the natural choice — midpoint + recursive split + merge. `O(n log n)` time, `O(log n)` stack (or iterative bottom-up for `O(1)`).
- **Insertion sort list** (LC 147): classic `O(n^2)` insertion sort.

### Arithmetic on lists
- **Add two numbers** (LC 2, LC 445): digits in reverse or forward order; carry propagation with dummy.
- **Multiply linked lists**: reduce to array then multiply, or simulate long multiplication.

### Edge cases & pitfalls
- **Empty list** (`head is None`) — guard early.
- **Single node** — slow/fast loops should not deref `fast.next.next` without null-check.
- **Two nodes** — off-by-one risks in midpoint and nth-from-end.
- **Cycles in unexpected places** — if cycles are possible, detect first or your loops won't terminate.
- **Losing the tail**: always `nxt = curr.next` **before** rewiring.
- **Memory leaks in GC-free languages**: null out unlinked nodes' `next` pointers in C++.
- **Forgetting to cut** before reversing a sublist → two halves share a tail and you get a cycle.
- **Dummy.next vs head**: when the head changes, return `dummy.next`.
- **Swapping values vs swapping nodes**: if the list has extra fields or is a real data structure (LC 24 requires swapping **nodes**, not values), you must rewire.

## 7. Related Patterns
- **Two Pointers** — slow/fast is a linked-list-specific two-pointer; gap-pointers are another flavour.
- **Recursion** — recursive reversal, merge, and tree-like list operations. Watch for `O(n)` stack depth on long lists.
- **Stack** — push nodes, pop to reverse or to compare (LC 234 palindrome via stack).
- **Hashing** — when you must *detect* an already-visited node without modifying the list (cycle via set), or when random pointers need `old → new` mapping.
- **Merge Sort** — the natural `O(n log n)` in-place sort for linked lists (LC 148).
- **Heap / Priority Queue** — merging `k` sorted lists (LC 23).
- **Arrays** — the "copy to array" fallback is always in your pocket; worth explicitly comparing with the `O(1)` space solution.

**Distinguishing note**: if the problem lets you convert the list to an array, you probably **should** — unless the constraint is explicitly `O(1)` space or the input is too large to copy. Otherwise, the whole point of the pattern is that you cannot index; you can only slide pointers. Every advanced linked-list problem decomposes into **midpoint + reverse + merge + dummy head + slow/fast**. Master those five, and the rest is recombination.
