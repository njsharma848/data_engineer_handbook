# 16 — Tree DFS

## 1. When to Use
- Input is a tree (binary tree, n-ary tree, or parent/child map) and you must visit every node.
- Keywords: *"height"*, *"diameter"*, *"path sum"*, *"subtree"*, *"serialize / deserialize"*, *"lowest common ancestor"*, *"tree DP"*, *"max path sum"*, *"house robber III"*, *"count univalue subtrees"*, *"flatten binary tree"*.
- The answer at a node depends on **aggregates from its subtrees** — post-order combine is the natural shape.
- You need **pre-order** (root first: serialise, path accumulate), **in-order** (BST sorted), or **post-order** (root after children: tree DP) traversal.
- You need to compare two trees structurally (same tree, symmetric, subtree-of).
- You need to **construct** a tree from traversals (LC 105 / 106) — DFS is the natural recursive shape.
- Level order is **not** what you want — that is BFS (file 17).

### Signals this is NOT a Tree DFS problem
- "Level by level", "shortest depth", "first-encountered" — use BFS.
- Tree is a BST and the problem exploits ordering → use BST patterns directly (file 18).
- Multiple independent queries over the same tree — consider LCA preprocessing, Euler tour + sparse table, or HLD.

## 2. Core Idea
Depth-first traversal visits a node, recurses into a child's entire subtree, then backtracks. The resulting call stack hands you exactly the right frame for **tree DP**: each node combines answers from its children (post-order) or distributes state down (pre-order). DFS is the shortest path from "self-similar tree structure" to "O(n) computation".

Three traversal orders — the placement of the "visit" statement relative to the two recursive calls — produce three different data flows:
- **Pre-order** (visit, recurse-left, recurse-right): state flows **down** from root to leaves. Good for: serialisation with null markers, root-to-leaf path accumulation, passing down a running sum.
- **In-order** (recurse-left, visit, recurse-right): in a BST, emits keys in ascending order. Good for: BST validation, k-th smallest, recovering a BST with two swaps.
- **Post-order** (recurse-left, recurse-right, visit): state flows **up** from leaves to root. Good for: heights, sizes, path-through-node problems, tree DP with child aggregates.

The universal DFS contract: each `dfs(node)` call returns a tuple describing the subtree rooted at `node`. What's in the tuple is pattern-specific — height, max path sum ending at this node, `(robbed, not_robbed)` pair, `(is_balanced, height)` pair. Pick the return type deliberately; it is the single most important design decision.

### Flowing state in two directions
Many interesting problems need **both** directions:
- **Down** (argument): ancestor path, running sum, bounds.
- **Up** (return): subtree size, max depth, best subtree answer.

Example: "max path sum through any node" (LC 124). Each call *returns* the best single-legged path ending at this node (flows up), and *updates* a `nonlocal best` using the sum of both legs (the answer through this node). The pattern generalises to diameter, longest zigzag path, longest univalue path.

## 3. Template

### Template A — pre-order / in-order / post-order skeleton
```python
class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

def preorder(node, out):
    if node is None: return
    out.append(node.val)                # visit
    preorder(node.left, out)
    preorder(node.right, out)

def inorder(node, out):
    if node is None: return
    inorder(node.left, out)
    out.append(node.val)                # visit
    inorder(node.right, out)

def postorder(node, out):
    if node is None: return
    postorder(node.left, out)
    postorder(node.right, out)
    out.append(node.val)                # visit
```

### Template B — post-order aggregate (height, sum, count)
```python
def height(node):
    if node is None: return 0
    return 1 + max(height(node.left), height(node.right))

def subtree_sum(node):
    if node is None: return 0
    return node.val + subtree_sum(node.left) + subtree_sum(node.right)
```

### Template C — pre-order with accumulated state
```python
def has_path_sum(node, target):
    if node is None: return False
    if node.left is None and node.right is None:
        return target == node.val
    remaining = target - node.val
    return (has_path_sum(node.left, remaining)
         or has_path_sum(node.right, remaining))
```

### Template D — tree DP with dual-direction state (diameter / max path sum)
```python
def diameter(root):
    best = 0
    def depth(node):
        nonlocal best
        if node is None: return 0
        l = depth(node.left)
        r = depth(node.right)
        best = max(best, l + r)         # path through this node (both legs)
        return 1 + max(l, r)            # single leg reported to parent
    depth(root)
    return best
```

### Template E — tree DP with tuple return (house robber III)
```python
def rob(root):
    def go(node):
        if node is None: return (0, 0)  # (robbed, not_robbed)
        l_rob, l_skip = go(node.left)
        r_rob, r_skip = go(node.right)
        robbed = node.val + l_skip + r_skip
        not_robbed = max(l_rob, l_skip) + max(r_rob, r_skip)
        return (robbed, not_robbed)
    return max(go(root))
```

### Template F — iterative DFS with explicit stack
```python
def preorder_iter(root):
    if not root: return []
    out, stack = [], [root]
    while stack:
        node = stack.pop()
        out.append(node.val)
        if node.right: stack.append(node.right)   # push right first
        if node.left:  stack.append(node.left)    # left popped first
    return out

def inorder_iter(root):
    out, stack = [], []
    node = root
    while stack or node:
        while node:
            stack.append(node); node = node.left
        node = stack.pop()
        out.append(node.val)
        node = node.right
    return out
```

### Template G — two trees in lock-step (same tree, symmetric)
```python
def is_same(a, b):
    if a is None and b is None: return True
    if a is None or b is None: return False
    if a.val != b.val: return False
    return is_same(a.left, b.left) and is_same(a.right, b.right)

def is_symmetric(root):
    def mirror(a, b):
        if a is None and b is None: return True
        if a is None or b is None: return False
        return (a.val == b.val
             and mirror(a.left, b.right)
             and mirror(a.right, b.left))
    return root is None or mirror(root.left, root.right)
```

Key mental tools:
- Decide whether **state flows down** (accumulator in the argument list) or **up** (return value) — many problems need both.
- Post-order is "compute then report"; pre-order is "decide then dive"; in-order is BST-specific.
- For very deep trees (skewed), switch to an **explicit stack** (Template F) to avoid Python's default recursion limit of 1000 — or call `sys.setrecursionlimit(10**6)`.
- The return type of your DFS function is the **most important design decision**. A well-chosen tuple collapses a messy problem to 5 lines.
- `if node is None: return <identity>` — the base case is always "return the identity element for your combine". Identity for sum is 0, for max is `-inf`, for list is `[]`, for "is balanced" is `True`.

## 4. Classic Problems
- **LC 104 — Maximum Depth** (Easy): post-order height (Template B).
- **LC 543 — Diameter of Binary Tree** (Easy): tree DP returning depth, tracking global best (Template D).
- **LC 124 — Binary Tree Maximum Path Sum** (Hard): same shape as diameter, with negative-pruning via `max(0, child)`.
- **LC 236 — Lowest Common Ancestor** (Medium): post-order "which side saw each target".
- **LC 297 — Serialize / Deserialize Binary Tree** (Hard): pre-order with null markers.
- **LC 337 — House Robber III** (Medium): tuple-return DP (Template E).
- **LC 110 — Balanced Binary Tree** (Easy): tuple `(is_balanced, height)` avoids re-computing height.
- **LC 105 — Build Tree from Preorder & Inorder** (Medium): recursive construction.

## 5. Worked Example — Binary Tree Maximum Path Sum (LC 124)
Problem: a path is any sequence of nodes connected by parent-child edges (does not need to pass through the root, and may contain negative values). Return the maximum sum of any path.

Tree:
```
         -10
         /  \
        9   20
            / \
           15  7
```
Expected answer: 42 (path 15 → 20 → 7).

### Step 1. Choose the return type
Each `dfs(node)` should return **the best single-legged path ending at `node`** — i.e. `node.val` plus the better of its two children's single-legged paths (if non-negative). A global `best` tracks the answer across all nodes, considering the **full through-path** `left_gain + node.val + right_gain`.

### Step 2. Prune negative children
If a subtree's best single-legged path is negative, you are better off *not including it* — set the child's contribution to 0 instead. This is the crucial pruning that distinguishes LC 124 from LC 543 (diameter, where edges always contribute positively because we only count edge counts).

### Step 3. Implement
```python
def maxPathSum(root):
    best = float('-inf')
    def gain(node):
        nonlocal best
        if node is None: return 0
        l = max(gain(node.left), 0)      # drop negative children
        r = max(gain(node.right), 0)
        best = max(best, node.val + l + r)
        return node.val + max(l, r)
    gain(root)
    return best
```

### Step 4. Trace post-order on our tree
Post-order visits leaves first, root last.

| node | `gain(left)` raw | `gain(right)` raw | `l` (clipped to ≥0) | `r` | through-path = `node.val + l + r` | `best` after update | returns `node.val + max(l, r)` |
|---|---|---|---|---|---|---|---|
| 9 | 0 | 0 | 0 | 0 | 9 | 9 | 9 |
| 15 | 0 | 0 | 0 | 0 | 15 | 15 | 15 |
| 7 | 0 | 0 | 0 | 0 | 7 | 15 | 7 |
| 20 | 15 | 7 | 15 | 7 | 42 | **42** | 35 (20 + 15) |
| -10 (root) | 9 | 35 | 9 | 35 | 34 | 42 | 25 (-10 + 35) |

Final `best = 42` — the path `15 → 20 → 7` contributing 42. The root `-10` is not on the best path; we avoided it because its *through-path* (34) is worse than 42, and the through-path at its right child (42) was already recorded.

### Step 5. Why the return type matters
If `gain(node)` returned the *entire* best path (both legs), the parent would have no way to extend it — a path cannot "pass through" a node twice. By returning only the single-legged extension, each parent can legally combine at most one child's contribution into its own through-path and then forward a single leg upwards. This is the canonical tree-DP idiom — **the node returns what is *usable* by the parent; the node updates `best` with the full combined answer**.

Time `O(n)`: one post-order visit per node. Space `O(h)` for the recursion stack.

## 6. Common Variations

### Post-order aggregates
- **Maximum depth** (LC 104).
- **Subtree sum / count** (LC 508 most frequent subtree sum): return sum, also accumulate frequencies.
- **Balanced binary tree** (LC 110): tuple `(is_balanced, height)`.
- **Count nodes** (LC 222): for complete binary tree, short-circuit when heights match.
- **Invert binary tree** (LC 226): swap children post-order.

### Pre-order with accumulated state
- **Path sum** (LC 112): subtract `node.val` from remaining target.
- **All root-to-leaf paths** (LC 257): append current value, recurse, append to result at leaf, pop on return.
- **Sum of root-to-leaf numbers** (LC 129): accumulate `current * 10 + node.val`.
- **Binary tree paths** (LC 257): same shape; output list of strings.

### In-order (BST-specific)
- **Validate BST** (LC 98): bounds-propagating DFS.
- **Kth smallest in BST** (LC 230): iterative in-order, stop at k.
- **Convert sorted array to BST** (LC 108): mid element as root, recurse halves.
- **Recover BST** (LC 99): in-order finds the two swapped nodes.
- **Two sum in BST** (LC 653): in-order + hash set, or two iterators.

### Dual-direction tree DP (through-node)
- **Diameter** (LC 543): edge count.
- **Max path sum** (LC 124): value sum with negative pruning.
- **Longest univalue path** (LC 687): count same-value edges.
- **Longest zigzag path** (LC 1372): return `(left_depth, right_depth)`.
- **Binary tree cameras** (LC 968): return state `{HAS_CAMERA, COVERED, NEEDS_COVER}`.

### Lock-step / two-tree DFS
- **Same tree** (LC 100) / **symmetric tree** (LC 101): recurse two nodes in lock-step.
- **Subtree of another tree** (LC 572): for each node, check same-tree against root of candidate.
- **Merge two binary trees** (LC 617): add values, recurse on children.
- **Flip equivalent** (LC 951): two recursive orderings of children.

### Construction from traversals
- **Build from preorder + inorder** (LC 105): root = preorder[0]; split inorder at root's index.
- **Build from inorder + postorder** (LC 106): root = postorder[-1]; split inorder.
- **Serialise / deserialise** (LC 297): pre-order with `null` sentinels.
- **Flatten binary tree to linked list** (LC 114): pre-order with parent pointer.

### LCA and path queries
- **LCA (LC 236)**: post-order returning the found node or null; the first node where both sides are non-null is the LCA.
- **LCA in BST (LC 235)**: descend while both targets are on the same side.
- **Path between two nodes**: root to each, then diff.
- **Binary tree tilt** (LC 563): sum of `|left_sum − right_sum|` over all nodes.

### Iterative DFS
- **Iterative pre / in / post order** (LC 144, 94, 145).
- **Morris traversal**: `O(1)` extra space using in-order threading.
- **Explicit stack with state machine**: useful for interview contexts where recursion is disallowed.

### Edge cases & pitfalls
- **Empty tree** — return identity; don't crash.
- **Single node** — check that your post-order returns the right value for a leaf.
- **Skewed tree** — recursion depth = `n`; raise recursion limit or use iterative.
- **Negative values** (LC 124 specifically) — you need to clip child contributions to zero.
- **Duplicate values** — may break assumptions about uniqueness in BST or LCA.
- **Wrong return type** — the most common debugging failure; write the contract first.
- **Mutating `self.best` vs closure `nonlocal best`** — both work, but inconsistent use causes subtle bugs.
- **Memoisation on trees**: don't memoise on `node.val` (ambiguous); use `id(node)` or tuple keys.

## 7. Related Patterns
- **Recursion** — tree DFS is recursion over a self-similar structure.
- **Tree BFS / Level Order** — the sibling traversal; prefer BFS when the answer is "level-by-level" or "shortest depth".
- **BST Properties** — in-order DFS yields sorted values; exploit for BST-specific questions.
- **Graph DFS** — identical mechanics, plus `visited` to handle cycles.
- **Dynamic Programming on Trees** — tree DP *is* post-order DFS with a richer return tuple.
- **Stack** — the iterative backbone once recursion depth is a concern.
- **Divide and Conquer** — tree DFS is the prototypical D&C: split into subtrees, solve, combine.
- **Backtracking** — when you need to enumerate root-to-leaf configurations, the push-pop on a path list is backtracking.

**Distinguishing note**: if the problem says *"for each node, use info from its subtree"*, that is post-order DFS. If it says *"for each node, know the distance from the root"*, that is pre-order DFS (or BFS level). If it says *"visit nodes in sorted order"* and the tree is a BST, that is in-order DFS. If the answer is the **minimum depth** or something that can be answered layer-by-layer, reach for BFS instead — DFS would work but visit unnecessary deep nodes first.
