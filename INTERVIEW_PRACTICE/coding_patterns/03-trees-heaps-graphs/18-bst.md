# 18 — BST (Binary Search Tree)

## 1. When to Use
- Problem states the input is a **BST** (`left subtree < node.val < right subtree`).
- Keywords: *"kth smallest / largest"*, *"range sum in BST"*, *"successor / predecessor"*, *"validate BST"*, *"insert / delete in BST"*, *"trim BST"*, *"closest value"*.
- You need both **ordered traversal** and **O(log n) search** from the same structure.
- You are asked for operations that would be trivial on a sorted array, but on dynamic data.
- You want a persistent structure that supports "rank" or "count less than" queries (augmented BST).

## 2. Core Idea
A BST encodes a sorted order implicitly: in-order traversal visits keys ascending, and for any query you compare against the current node, then descend into exactly one subtree. When the tree is balanced, search / insert / delete are all `O(log n)`. When it is skewed (e.g. built from sorted input without balancing), operations degrade to `O(n)` — which is why Red-Black and AVL trees exist.

## 3. Template
```python
# 1) Search in a BST
def search(root, target):
    while root:
        if target == root.val: return root
        root = root.left if target < root.val else root.right
    return None

# 2) Insert (returns the new root)
def insert(root, val):
    if root is None: return TreeNode(val)
    if val < root.val:  root.left  = insert(root.left,  val)
    else:               root.right = insert(root.right, val)
    return root

# 3) Validate BST using value bounds
def is_valid(root, lo=float('-inf'), hi=float('inf')):
    if root is None: return True
    if not (lo < root.val < hi): return False
    return (is_valid(root.left,  lo, root.val)
        and is_valid(root.right, root.val, hi))

# 4) In-order iterator — kth smallest
def kth_smallest(root, k):
    stack = []
    node = root
    while stack or node:
        while node:                      # dive left
            stack.append(node); node = node.left
        node = stack.pop()               # visit
        k -= 1
        if k == 0: return node.val
        node = node.right                # dive right
```
Key mental tools:
- **In-order = sorted** — the single most powerful BST fact.
- Validate by passing **bounds down**, not by comparing to immediate children (which misses grandchild violations).
- Successor of `node`: if `node.right` exists, leftmost of `node.right`; else walk up until you come from a left child.
- Deletion: if two children, replace with in-order successor's value, then delete that successor.

## 4. Classic Problems
- **LC 98 — Validate BST** (Medium): bounds-propagating recursion.
- **LC 230 — Kth Smallest Element in a BST** (Medium): in-order iterator.
- **LC 235 — Lowest Common Ancestor of BST** (Easy): descend while both targets are on the same side.
- **LC 108 — Convert Sorted Array to BST** (Easy): pick middle as root, recurse halves.
- **LC 450 — Delete Node in a BST** (Medium): successor swap.

## 5. Worked Example — Kth Smallest Element in a BST (LC 230)
BST used (already a valid BST):
```
        5
       / \
      3   6
     / \
    2   4
   /
  1
```
Goal: find the 3rd smallest value (`k = 3`).

Approach: iterative in-order traversal with an explicit stack — stop when we have visited `k` nodes.

```python
def kthSmallest(root, k):
    stack = []
    node = root
    while stack or node:
        while node:
            stack.append(node); node = node.left
        node = stack.pop()
        k -= 1
        if k == 0: return node.val
        node = node.right
```

Step-by-step (`k` starts at 3; stack holds nodes whose right subtree is unfinished):

| step | action | stack (node vals) | `node` | `k` after visit |
|---|---|---|---|---|
| 1 | dive left from 5 | `[5]` | 3 | — |
| 2 | dive left from 3 | `[5, 3]` | 2 | — |
| 3 | dive left from 2 | `[5, 3, 2]` | 1 | — |
| 4 | dive left from 1 (None) | `[5, 3, 2]` | None | — |
| 5 | pop → visit 1 | `[5, 3, 2]` | None → 1 | 2 |
| 6 | `k != 0`, go to 1.right = None | `[5, 3, 2]` | None | 2 |
| 7 | pop → visit 2 | `[5, 3]` | None → 2 | 1 |
| 8 | `k != 0`, go to 2.right = None | `[5, 3]` | None | 1 |
| 9 | pop → visit 3 | `[5]` | None → 3 | 0 → **return 3** |

Answer = `3`. The walk mirrors the sorted order `1, 2, 3, 4, 5, 6`; we stop at the third. Time `O(h + k)`, space `O(h)` for the stack.

## 6. Common Variations
- **Range sum** (LC 938): prune branches whose subtree range cannot intersect `[L, R]`.
- **Closest value** (LC 270): descend while updating the best `(|val - target|)`.
- **Recover BST** (LC 99): in-order traversal finds the two swapped nodes — the descents.
- **Iterator class** (LC 173): lazy in-order; `hasNext` / `next` in amortised `O(1)`.
- **Augmented BST**: store subtree size in each node → `O(log n)` select/rank (order statistic tree).
- **Balanced tree equivalents**: AVL, Red-Black, Treap, Splay — same operations, balanced in `O(log n)`.
- **Edge cases**: empty tree, duplicates policy (`<` vs `<=`), skewed trees after unsorted inserts.

## 7. Related Patterns
- **Tree DFS** — the *mechanic*; BST adds ordering invariants.
- **Binary Search** — BST's decision procedure is a binary search over tree paths.
- **Sorted Array** — a BST is dynamic version of a sorted array; pick whichever matches update frequency.
- **Heap** — maintains the *largest/smallest* only; BST maintains *all* orderings.
- **Trie** — another tree that indexes by key; trie partitions by character, BST partitions by comparison.

Distinguishing note: if you would use a sorted array for "rank / count / closest" but the data changes online, reach for a BST (or balanced tree). If you only need "min / max / top-k", reach for a heap — it is strictly smaller/faster for that narrower job.
