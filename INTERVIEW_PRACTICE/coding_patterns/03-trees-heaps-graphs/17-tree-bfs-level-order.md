# 17 — Tree BFS / Level Order

## 1. When to Use
- You must visit tree nodes **level by level**, from the root outward.
- Keywords: *"level order"*, *"by depth"*, *"right view"*, *"left view"*, *"bottom-up"*, *"zigzag"*, *"minimum depth"*, *"shortest path in a tree"*, *"serialize by level"*.
- You need the **first occurrence** of something at the shallowest depth.
- You need to report results **grouped per level** (list of lists).
- You only care about the **last node** in each level (LC 199 right side view).

## 2. Core Idea
BFS uses a queue to process all nodes at depth `d` before any at depth `d+1`. Because every node at depth `d` enters the queue before any deeper node, the first time you encounter a state (e.g. target value) you have also found the **shortest** path to it — which is why BFS dominates for minimum-depth and level-grouped questions.

## 3. Template
```python
from collections import deque

# 1) Level order — return list of lists
def level_order(root):
    if root is None: return []
    q = deque([root])
    out = []
    while q:
        layer = []
        for _ in range(len(q)):        # freeze this layer's size
            node = q.popleft()
            layer.append(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
        out.append(layer)
    return out

# 2) Minimum depth — first leaf wins
def min_depth(root):
    if root is None: return 0
    q = deque([(root, 1)])
    while q:
        node, d = q.popleft()
        if node.left is None and node.right is None:
            return d
        if node.left:  q.append((node.left, d + 1))
        if node.right: q.append((node.right, d + 1))

# 3) Right side view — last node of each layer
def right_side_view(root):
    if root is None: return []
    q = deque([root]); out = []
    while q:
        sz = len(q)
        for i in range(sz):
            node = q.popleft()
            if i == sz - 1: out.append(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
    return out
```
Key mental tools:
- Always use `collections.deque` — `list.pop(0)` is `O(n)`.
- **Freeze layer size** with `len(q)` at the top of the `while` loop before you mutate the queue.
- Pair each node with its depth in the tuple when depth matters but you do not want the layer-at-a-time shape.

## 4. Classic Problems
- **LC 102 — Binary Tree Level Order Traversal** (Medium): canonical.
- **LC 107 — Level Order Bottom Up** (Medium): append then reverse (or `deque.appendleft`).
- **LC 103 — Zigzag Level Order** (Medium): alternate left-to-right and right-to-left per layer.
- **LC 199 — Binary Tree Right Side View** (Medium): last node per layer.
- **LC 111 — Minimum Depth of Binary Tree** (Easy): first leaf dequeued.

## 5. Worked Example — Zigzag Level Order Traversal (LC 103)
Problem: return the values level by level, alternating left-to-right for even depths and right-to-left for odd depths.

Tree:
```
        3
       / \
      9  20
         / \
        15  7
```
Approach: standard BFS; toggle a boolean each level and reverse (or `appendleft`) the layer when building.

```python
from collections import deque

def zigzagLevelOrder(root):
    if root is None: return []
    q = deque([root])
    out = []
    left_to_right = True
    while q:
        layer = deque()
        for _ in range(len(q)):
            node = q.popleft()
            if left_to_right:
                layer.append(node.val)
            else:
                layer.appendleft(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
        out.append(list(layer))
        left_to_right = not left_to_right
    return out
```

Step-by-step state:

| iter | frozen size | `left_to_right` | nodes popped in order | `layer` built | q after iter | `out` |
|---|---|---|---|---|---|---|
| 1 | 1 | True | 3 | `[3]` | `[9, 20]` | `[[3]]` |
| 2 | 2 | False | 9 (appendleft → `[9]`), 20 (appendleft → `[20, 9]`) | `[20, 9]` | `[15, 7]` | `[[3],[20,9]]` |
| 3 | 2 | True | 15 (append → `[15]`), 7 (append → `[15, 7]`) | `[15, 7]` | `[]` | `[[3],[20,9],[15,7]]` |

Queue is empty → return `[[3], [20, 9], [15, 7]]`. Time `O(n)`, space `O(w)` where `w` is the widest layer.

## 6. Common Variations
- **Bottom-up level order** (LC 107): build top-down, then reverse the outer list.
- **Connect next right pointers** (LC 116): BFS, chain each layer using the queue or the previously-set `next`.
- **Average / min / max per level** (LC 637): aggregate inside each layer.
- **Vertical order traversal** (LC 314 / 987): BFS with `(node, col)`; bucket by `col`.
- **Multi-source tree BFS**: rare for trees; common on graphs/grids.
- **Depth-first simulation of level order**: recurse with a `depth` argument, append to `out[depth]`.
- **Edge cases**: empty tree, single node, fully skewed tree (each layer has one node).

## 7. Related Patterns
- **Queue / Deque** — the underlying data structure.
- **Tree DFS** — sibling traversal pattern; DFS uses the call stack, BFS uses an explicit queue.
- **Graph BFS** — identical mechanics, plus `visited` for non-tree graphs.
- **Shortest path in unweighted graphs** — BFS generalises from trees to graphs naturally.
- **Bidirectional BFS** — speeds up "minimum steps from A to B" searches on huge graphs.

Distinguishing note: pick BFS when the question talks about **levels**, **depths**, or **first/shortest**. Pick DFS when the question asks for **subtree aggregates** or **paths between any two nodes**. They are peers, not substitutes.
