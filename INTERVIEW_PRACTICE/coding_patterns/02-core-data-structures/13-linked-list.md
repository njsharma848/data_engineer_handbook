# 13 — Linked List

## 1. When to Use
- Problem explicitly provides `ListNode` / `head` / `next` pointers.
- Keywords: *"reverse"*, *"detect cycle"*, *"middle node"*, *"merge sorted lists"*, *"remove Nth from end"*, *"reorder"*, *"rotate"*, *"intersection"*.
- You need **O(1) insert/delete** at arbitrary positions *you already hold a pointer to*.
- Memory constraints forbid copying the whole sequence into an array.
- A problem feels like an array problem but disallows random access.

## 2. Core Idea
A linked list is a chain of nodes — each knows only the next node. You cannot jump, so every technique coordinates a small fixed number of pointers moving along the chain: slow/fast for cycles and midpoints, prev/curr/next for reversal, dummy heads to simplify edge cases. The mental model is "I hold two or three fingers on the list and slide them deliberately."

## 3. Template
```python
class ListNode:
    def __init__(self, val=0, nxt=None):
        self.val = val; self.next = nxt

# 1) Dummy head — avoids branching on "is this the new head?"
def pattern_with_dummy(head):
    dummy = ListNode(0, head)
    prev, curr = dummy, head
    while curr:
        if should_drop(curr):
            prev.next = curr.next
        else:
            prev = curr
        curr = curr.next
    return dummy.next

# 2) Reverse in place
def reverse(head):
    prev, curr = None, head
    while curr:
        nxt = curr.next
        curr.next = prev
        prev = curr
        curr = nxt
    return prev

# 3) Slow / fast pointers — cycle detect, midpoint
def middle(head):
    slow = fast = head
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
    return slow                  # mid (for even length: second mid)

def has_cycle(head):
    slow = fast = head
    while fast and fast.next:
        slow = slow.next; fast = fast.next.next
        if slow is fast: return True
    return False
```
Key mental tools:
- **Dummy head** is almost always worth it — it unifies the "operate on the first node" case with the rest.
- **Save `next` first** before rewiring any `.next` pointer — otherwise you lose the rest of the list.
- Always **null-guard** `.next.next` before dereferencing.
- Distinguish "new list" (build with dummy) vs "in-place rewire" (no new nodes).

## 4. Classic Problems
- **LC 206 — Reverse Linked List** (Easy): three-pointer rewire.
- **LC 141 / 142 — Linked List Cycle I & II** (Easy/Medium): Floyd's tortoise and hare.
- **LC 21 — Merge Two Sorted Lists** (Easy): dummy + merge.
- **LC 19 — Remove Nth Node From End** (Medium): gap of `n` between two pointers.
- **LC 143 — Reorder List** (Medium): midpoint + reverse + merge.

## 5. Worked Example — Reverse Linked List (LC 206)
Problem: given head `1 -> 2 -> 3 -> 4 -> 5 -> None`, return the reversed list.

Approach: three pointers — `prev` (already-reversed prefix), `curr` (node being flipped), `nxt` (saved next before rewire).

```python
def reverseList(head):
    prev, curr = None, head
    while curr:
        nxt = curr.next     # save
        curr.next = prev    # rewire
        prev = curr         # advance prev
        curr = nxt          # advance curr
    return prev
```

Trace (show pointers by the node's value):

| step | `prev` | `curr` | `nxt` saved | after `curr.next = prev` | list state (from `prev`) |
|---|---|---|---|---|---|
| start | None | 1 | — | — | — |
| 1 | None → 1 | 1 → 2 | 2 | 1 → None | `1 -> None` |
| 2 | 1 → 2 | 2 → 3 | 3 | 2 → 1 | `2 -> 1 -> None` |
| 3 | 2 → 3 | 3 → 4 | 4 | 3 → 2 | `3 -> 2 -> 1 -> None` |
| 4 | 3 → 4 | 4 → 5 | 5 | 4 → 3 | `4 -> 3 -> 2 -> 1 -> None` |
| 5 | 4 → 5 | 5 → None | None | 5 → 4 | `5 -> 4 -> 3 -> 2 -> 1 -> None` |
| stop | 5 | None | — | — | return `prev = 5` |

Time `O(n)`, space `O(1)`. The recursive version does the same work but uses `O(n)` stack space.

## 6. Common Variations
- **Reverse between `m` and `n`** (LC 92): reverse a sublist in place using a dummy head.
- **Reverse in groups of `k`** (LC 25): reverse a block, reconnect with the tail of the previous block.
- **Floyd's cycle — find start** (LC 142): after slow/fast meet, restart one pointer at head; they meet at the cycle entrance.
- **Intersection of two lists** (LC 160): two pointers that switch to the other list's head on reaching null.
- **Deep copy with random pointer** (LC 138): interleave new nodes, set randoms, split.
- **LRU Cache** (LC 146): doubly linked list + hashmap for `O(1)` insert/remove/lookup.
- **Edge cases**: empty list (`head is None`), single node, two nodes, cycles, lists that intersect partway.

## 7. Related Patterns
- **Two Pointers** — slow/fast is a linked-list specific two-pointer.
- **Recursion** — recursive reversal, merge, and tree-like list operations.
- **Stack** — push nodes, pop to reverse or to compare (LC 234 palindrome).
- **Hashing** — when you must *detect* an already-visited node without modifying the list (cycle via set).
- **Merge Sort** — the natural `O(n log n)` in-place sort for linked lists (LC 148).

Distinguishing note: if the problem lets you convert the list to an array, you probably **should** — unless the constraint is explicitly `O(1)` space. Otherwise, the whole point of the pattern is that you cannot index; you can only slide pointers.
