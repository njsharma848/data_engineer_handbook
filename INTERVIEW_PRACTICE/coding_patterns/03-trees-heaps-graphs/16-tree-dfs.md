# 16 — Tree DFS

## 1. When to Use
- Input is a tree (binary tree, n-ary tree, or parent/child map) and you must visit every node.
- Keywords: *"height"*, *"diameter"*, *"path sum"*, *"subtree"*, *"serialize / deserialize"*, *"lowest common ancestor"*, *"tree DP"*.
- The answer at a node depends on **aggregates from its subtrees**.
- You need pre-order (root first), in-order (BST sorted), or post-order (root after children) traversal.
- Level order is **not** what you want — that would be BFS (next file).

## 2. Core Idea
Depth-first traversal visits a node, recurses into a child's entire subtree, then backtracks. The resulting call stack hands you exactly the right frame for **tree DP**: each node combines answers from its children (post-order) or distributes state down (pre-order). DFS is the shortest path from "self-similar tree structure" to "O(n) computation".

## 3. Template
```python
class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val; self.left = left; self.right = right

# 1) Post-order aggregate (height / diameter / subtree sum)
def height(node):
    if node is None: return 0
    return 1 + max(height(node.left), height(node.right))

# 2) Pre-order with accumulated state (root-to-leaf path sum)
def has_path_sum(node, target):
    if node is None: return False
    if node.left is None and node.right is None:
        return target == node.val
    t = target - node.val
    return has_path_sum(node.left, t) or has_path_sum(node.right, t)

# 3) In-order (BST → sorted sequence)
def inorder(node, out):
    if node is None: return
    inorder(node.left, out)
    out.append(node.val)
    inorder(node.right, out)

# 4) Tree DP — return multiple values per call
def diameter(root):
    best = 0
    def depth(node):
        nonlocal best
        if node is None: return 0
        l = depth(node.left)
        r = depth(node.right)
        best = max(best, l + r)         # path through this node
        return 1 + max(l, r)            # depth reported to parent
    depth(root)
    return best
```
Key mental tools:
- Decide whether **state flows down** (accumulator in the argument list) or **up** (return value) — many problems need both.
- Post-order is "compute then report"; pre-order is "decide then dive"; in-order is BST-specific.
- For very deep trees (skewed), switch to an **explicit stack** to avoid Python's recursion limit.

## 4. Classic Problems
- **LC 104 — Maximum Depth** (Easy): post-order height.
- **LC 543 — Diameter of Binary Tree** (Easy): tree DP returning depth, tracking global best.
- **LC 124 — Binary Tree Maximum Path Sum** (Hard): same shape as diameter, with negative-pruning via `max(0, child)`.
- **LC 236 — Lowest Common Ancestor** (Medium): post-order "which side saw each target".
- **LC 297 — Serialize / Deserialize Binary Tree** (Hard): pre-order with null markers.

## 5. Worked Example — Diameter of Binary Tree (LC 543)
Problem: the diameter is the length (in edges) of the longest path between any two nodes. Compute it for:
```
        1
       / \
      2   3
     / \
    4   5
```
Observation: the longest path through a given node uses the deepest left chain + deepest right chain. So for each node, report `1 + max(left_depth, right_depth)` to its parent, and update a global `best = left_depth + right_depth`.

```python
def diameterOfBinaryTree(root):
    best = 0
    def depth(node):
        nonlocal best
        if node is None: return 0
        l = depth(node.left)
        r = depth(node.right)
        best = max(best, l + r)
        return 1 + max(l, r)
    depth(root)
    return best
```

Post-order trace (each row is a `depth()` call, computed from the bottom up):

| node | `depth(left)` | `depth(right)` | `l + r` (path through node) | `best` updated | returns `1 + max(l,r)` |
|---|---|---|---|---|---|
| 4 | 0 | 0 | 0 | 0 | 1 |
| 5 | 0 | 0 | 0 | 0 | 1 |
| 2 | 1 | 1 | 2 | **2** | 2 |
| 3 | 0 | 0 | 0 | 2 | 1 |
| 1 (root) | 2 | 1 | 3 | **3** | 3 |

Final `best = 3`. The path is 4 → 2 → 5 (2 edges) vs 4 → 2 → 1 → 3 (3 edges) — the root-containing path wins here. Time `O(n)`, space `O(h)` for the call stack where `h` is tree height.

## 6. Common Variations
- **Path sum / all root-to-leaf paths** (LC 113): push on the way down, pop on the way up.
- **Lowest common ancestor** (LC 236): return the node if either subtree found a target; otherwise bubble up the non-null side.
- **Tree isomorphism / same tree / symmetric** (LC 100, LC 101): compare two nodes in lock-step recursively.
- **Serialise / deserialise** (LC 297): pre-order with `null` sentinels, then split-and-consume in the same order.
- **Tree DP**: "robber" on tree (LC 337), "max path sum" (LC 124), "longest univalue path" (LC 687).
- **Iterative DFS**: explicit stack of `(node, state)` frames to avoid recursion limits.
- **Edge cases**: empty tree, single node, fully skewed left or right (worst-case recursion depth = n).

## 7. Related Patterns
- **Recursion** — tree DFS is recursion over a self-similar structure.
- **Tree BFS / Level Order** — the sibling traversal; prefer BFS when the answer is "level-by-level".
- **BST Properties** — in-order DFS yields sorted values; exploit for BST-specific questions.
- **Graph DFS** — identical mechanics, plus `visited` to handle cycles.
- **Dynamic Programming on Trees** — tree DP *is* post-order DFS with a richer return tuple.
- **Stack** — the iterative backbone once recursion depth is a concern.

Distinguishing note: if the problem says *"for each node, use info from its subtree"*, that is post-order DFS. If it says *"for each node, know the distance from the root"*, that is pre-order DFS (or BFS level). If it says *"visit nodes in sorted order"* and the tree is a BST, that is in-order DFS.
