# 17 — Tree BFS / Level Order

## 1. When to Use
- You must visit tree nodes **level by level**, from the root outward.
- Keywords: *"level order"*, *"by depth"*, *"right view"*, *"left view"*, *"bottom-up"*, *"zigzag"*, *"minimum depth"*, *"shortest path in a tree"*, *"serialize by level"*, *"connect next right pointers"*, *"average / min / max per level"*, *"vertical order"*, *"find largest value in each row"*.
- You need the **first occurrence** of something at the shallowest depth — the minimum-depth property.
- You need to report results **grouped per level** (list of lists of values).
- You only care about the **last (or first) node** in each level (LC 199 right-side view, LC 513 bottom-left).
- You are asked to set per-level pointers (LC 116 / 117 `next` pointer per layer).
- You want to emit values in **bottom-up** order (LC 107) — easiest as top-down + reverse.

### Signals this is NOT a Tree BFS problem
- The answer depends on **subtree aggregates** (height, diameter, subtree sum) → Tree DFS is simpler.
- You need to emit nodes in **in-order / pre-order / post-order** — DFS, not BFS.
- The tree is actually a BST and you exploit ordering — use BST patterns.
- You need to enumerate **paths** (root-to-leaf, any-to-any) — DFS + backtracking.

## 2. Core Idea
BFS uses a queue to process all nodes at depth `d` before any at depth `d+1`. Because every node at depth `d` enters the queue before any deeper node, the **first time** you encounter a state (target value, leaf, goal cell) you have also found the **shortest** path to it — which is why BFS dominates for minimum-depth and level-grouped questions.

The canonical trick is **freezing the layer size** at the top of each outer iteration: `for _ in range(len(q))`. This fixes how many nodes belong to the current level *before* you enqueue their children, letting you emit results layer by layer without tracking depth on every node.

Two useful alternatives to the frozen-size trick:
1. **Depth in the tuple**: enqueue `(node, depth)` and let each item carry its own level — handy when the layer grouping is not needed but the depth is.
2. **Sentinel `None`**: push a `None` marker between levels — emits a natural "end of level" signal, but couples queue machinery with marker handling. Rarely better than frozen-size in Python.

BFS on a tree is a special case of BFS on an unweighted graph: the tree is a DAG with out-degree ≤ 2 and no visited tracking is needed because trees have no cycles or shared parents.

## 3. Template

### Template A — canonical level order (list of lists)
```python
from collections import deque

def level_order(root):
    if root is None: return []
    q = deque([root])
    out = []
    while q:
        layer = []
        for _ in range(len(q)):                # freeze this layer's size
            node = q.popleft()
            layer.append(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
        out.append(layer)
    return out
```

### Template B — minimum depth (first leaf wins)
```python
def min_depth(root):
    if root is None: return 0
    q = deque([(root, 1)])
    while q:
        node, d = q.popleft()
        if node.left is None and node.right is None:
            return d                            # first leaf = shallowest
        if node.left:  q.append((node.left, d + 1))
        if node.right: q.append((node.right, d + 1))
    return 0                                    # unreachable for non-empty tree
```

### Template C — one-value-per-level (right side view, largest per level)
```python
def right_side_view(root):
    if root is None: return []
    q = deque([root])
    out = []
    while q:
        sz = len(q)
        for i in range(sz):
            node = q.popleft()
            if i == sz - 1:                     # last node in this layer
                out.append(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
    return out
```

### Template D — zigzag (alternating left-to-right, right-to-left)
```python
def zigzag(root):
    if root is None: return []
    q = deque([root])
    out = []
    ltr = True
    while q:
        layer = deque()
        for _ in range(len(q)):
            node = q.popleft()
            if ltr: layer.append(node.val)
            else:   layer.appendleft(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
        out.append(list(layer))
        ltr = not ltr
    return out
```

### Template E — connect next right pointers (LC 116)
```python
def connect(root):
    if root is None: return None
    q = deque([root])
    while q:
        sz = len(q)
        for i in range(sz):
            node = q.popleft()
            if i < sz - 1:
                node.next = q[0]                # peek at head of queue
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
    return root
```

### Template F — vertical order (BFS with column bucketing)
```python
from collections import defaultdict

def vertical_order(root):
    if root is None: return []
    cols = defaultdict(list)
    q = deque([(root, 0)])                      # (node, col)
    while q:
        node, c = q.popleft()
        cols[c].append(node.val)
        if node.left:  q.append((node.left,  c - 1))
        if node.right: q.append((node.right, c + 1))
    return [cols[c] for c in sorted(cols)]
```

### Template G — DFS simulation of level order (when recursion is preferred)
```python
def level_order_dfs(root):
    out = []
    def go(node, depth):
        if node is None: return
        if depth == len(out): out.append([])
        out[depth].append(node.val)
        go(node.left, depth + 1)
        go(node.right, depth + 1)
    go(root, 0)
    return out
```

Key mental tools:
- Always use `collections.deque` — `list.pop(0)` is `O(n)` and silently destroys performance on anything beyond tiny inputs.
- **Freeze layer size** with `len(q)` at the top of the `while` loop *before* you mutate the queue. Don't use `len(q)` inside the inner loop.
- Pair each node with its depth in the tuple when depth matters but you do not want the layer-at-a-time shape.
- Queues in Python hold **object references** — enqueueing a node doesn't copy the subtree; it's cheap.
- For trees, **no `visited` set is needed** (no cycles). On general graphs this changes — see file 20.
- When both sides of a layer need to look at each other (LC 116), use **queue peeking (`q[0]`)** instead of a second pass.

## 4. Classic Problems
- **LC 102 — Binary Tree Level Order Traversal** (Medium): canonical Template A.
- **LC 107 — Level Order Bottom Up** (Medium): Template A + reverse (or `deque.appendleft`).
- **LC 103 — Zigzag Level Order** (Medium): Template D.
- **LC 199 — Binary Tree Right Side View** (Medium): Template C — last node per layer.
- **LC 111 — Minimum Depth of Binary Tree** (Easy): Template B — first leaf dequeued.
- **LC 515 — Find Largest Value in Each Tree Row** (Medium): Template A variant, compute max inside the layer loop.
- **LC 116 — Populating Next Right Pointers** (Medium): Template E.
- **LC 314 / 987 — Vertical Order Traversal** (Medium/Hard): Template F, with LC 987 adding tie-break sorting within a column.
- **LC 513 — Find Bottom Left Tree Value** (Medium): BFS right-to-left so the last-dequeued node is the bottom-left.

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
Expected: `[[3], [20, 9], [15, 7]]`.

### Step 1. Why not "BFS then reverse odd layers"?
You *could* do that — BFS gives you each layer in left-to-right order, and you reverse every second one. It works, but it is two passes over each odd layer. Using `deque.appendleft` inside the build builds the zigzagged order on the first pass. The difference is pedagogical more than performance — `O(n)` either way.

### Step 2. The toggle pattern
Keep a boolean `ltr` (left-to-right). For each layer:
- If `ltr`, append nodes to the layer in dequeue order.
- If not `ltr`, `appendleft` so later-dequeued nodes end up earlier in the layer.

Enqueueing children is always `left then right` — the queue order is unchanged; only the **output order for this layer** flips.

### Step 3. Implementation
```python
from collections import deque

def zigzagLevelOrder(root):
    if root is None: return []
    q = deque([root])
    out = []
    ltr = True
    while q:
        layer = deque()
        for _ in range(len(q)):
            node = q.popleft()
            if ltr: layer.append(node.val)
            else:   layer.appendleft(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
        out.append(list(layer))
        ltr = not ltr
    return out
```

### Step 4. Trace

| iter | frozen size | `ltr` | nodes popped (in order) | placement in `layer` | `layer` built | `q` after iter | `out` after iter |
|---|---|---|---|---|---|---|---|
| 1 | 1 | True | 3 | `append(3)` | `[3]` | `[9, 20]` | `[[3]]` |
| 2 | 2 | False | 9, then 20 | `appendleft(9)` → `[9]`; `appendleft(20)` → `[20, 9]` | `[20, 9]` | `[15, 7]` | `[[3], [20, 9]]` |
| 3 | 2 | True | 15, then 7 | `append(15)` → `[15]`; `append(7)` → `[15, 7]` | `[15, 7]` | `[]` | `[[3], [20, 9], [15, 7]]` |

Queue empty → return `[[3], [20, 9], [15, 7]]` ✓.

### Step 5. Complexity
Time `O(n)` — each node is enqueued and dequeued once. Space `O(w)` where `w` is the widest layer (maximum width of the tree). For a complete binary tree, `w ≤ n/2`; for a skewed tree, `w = 1`.

### Step 6. Lesson — BFS primitive + a small switch
Most level-order variants are **BFS Template A with one ingredient added or swapped**:
- Right view: "emit the last node of each layer" — one extra `if`.
- Zigzag: "flip output direction per layer" — one boolean.
- Max/avg/min per level: "aggregate inside the layer loop" — one reducer.
- Connect next: "peek at the next sibling via `q[0]`" — one peek.

This compositional property is why Tree BFS is a single pattern — the skeleton is fixed and you pick an ornament.

## 6. Common Variations

### Per-level grouping
- **Level order** (LC 102): Template A canonical.
- **Bottom-up level order** (LC 107): build top-down, `out.reverse()` at the end — or use `deque.appendleft` to skip the reverse.
- **Zigzag level order** (LC 103): Template D.
- **Level order n-ary** (LC 429): same but `for child in node.children` enqueue loop.

### Per-level aggregates
- **Average of levels** (LC 637): sum within the layer loop, divide by size.
- **Largest per level** (LC 515): max within the layer loop.
- **Find bottom-left value** (LC 513): BFS right-to-left; last popped is the answer. Or track "first of each layer".
- **Smallest value in each row**: symmetric to 515.

### Per-level view (one value per layer)
- **Right side view** (LC 199): last node of each layer (Template C).
- **Left side view**: first node of each layer.
- **Top view / bottom view**: vertical-column first-seen / last-seen.

### Vertical / diagonal / boundary
- **Vertical order traversal** (LC 314): Template F.
- **Vertical order II** (LC 987): add tie-break by row index, then by value.
- **Diagonal traversal of binary tree** (LC 1469 et al.): BFS keyed on `row - col` bucket.
- **Boundary of binary tree** (LC 545): left boundary DFS + leaves DFS + right boundary DFS (composite, not pure BFS).

### Next-pointer layers
- **Populating next right pointers — perfect tree** (LC 116): Template E.
- **Populating next right pointers II — any tree** (LC 117): queue-less `O(1)` space trick using previously-set `next` pointers to walk each layer.

### Minimum-depth / first-encounter
- **Minimum depth of binary tree** (LC 111): Template B; returns on first leaf.
- **Find the leftmost bottom-left** (LC 513): right-to-left BFS.
- **Deepest leaves sum** (LC 1302): sum the final layer.

### Related (tree over a queue)
- **Serialize by level** — BFS serialisation with null markers; useful for debugging.
- **Cousins in binary tree** (LC 993): BFS checking if both targets live in the same layer with different parents.
- **Check completeness of a binary tree** (LC 958): BFS; once a `None` child is seen, all remaining dequeues must be `None`.

### Edge cases & pitfalls
- **Empty tree** — guard `if root is None: return []` at the top.
- **`list.pop(0)` instead of `deque.popleft`** — turns `O(n)` BFS into `O(n^2)`.
- **Computing `len(q)` inside the inner loop** — the queue has already grown; breaks layer boundaries. Always freeze at the top.
- **Marking nodes visited** — unnecessary on a tree (no cycles). On graphs this matters.
- **Tuple-depth vs frozen-size inconsistency** — pick one; mixing both is a common source of bugs.
- **Using recursion for level order** — works (Template G) but loses the "early stop at shortest depth" property; BFS is preferred for min-depth problems.
- **Forgetting to reverse** (LC 107) — the natural order is top-down; bottom-up needs an explicit flip.

## 7. Related Patterns
- **Queue / Deque** — the underlying data structure. BFS is literally "the queue-based tree traversal".
- **Tree DFS** — sibling traversal pattern; DFS uses the call stack, BFS uses an explicit queue. Pick based on whether the problem wants depth-first aggregates or level-by-level output.
- **Graph BFS** — identical mechanics, plus `visited` for non-tree graphs and typically richer state (distances, parents).
- **Shortest path in unweighted graphs** — BFS generalises from trees to graphs naturally.
- **Bidirectional BFS** — speeds up "minimum steps from A to B" searches on huge graphs (word ladder).
- **0-1 BFS** — when edge weights are 0 or 1; deque-based.
- **Dijkstra / A\*** — generalise BFS to weighted graphs (heap-based).

**Distinguishing note**: pick BFS when the question talks about **levels**, **depths**, **first / shortest**, or **per-layer aggregates**. Pick DFS when the question asks for **subtree aggregates** or **paths between any two nodes**. They are peers, not substitutes — most trees admit both solutions, and the *shape of the answer* should drive the choice. If the answer is a **list per level**, that's BFS's home turf; if the answer is a **single global aggregate computed by combining subtrees**, that's DFS's.
