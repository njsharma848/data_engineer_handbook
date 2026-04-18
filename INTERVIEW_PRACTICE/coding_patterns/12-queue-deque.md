# 12 — Queue / Deque

## 1. When to Use
- You need **First In, First Out** processing: BFS, level-order traversal, scheduling, stream processing.
- Keywords: *"level order"*, *"shortest path in unweighted graph"*, *"minimum steps"*, *"nearest"*, *"sliding window maximum"*, *"hot streak of last k"*.
- The algorithm expands in **layers** and you must finish layer `d` before starting layer `d+1`.
- You need cheap operations on both ends (deque): push/pop front *and* back in `O(1)`.
- You want a **sliding monotonic structure**: window max/min, task scheduler cooldowns.

## 2. Core Idea
A queue guarantees that items are processed in arrival order, which is the right invariant for breadth-first exploration: every node at distance `d` is processed before any at distance `d+1`. A deque generalises that to both ends, enabling monotonic-window tricks where stale entries are evicted from the front while new entries are appended to the back.

## 3. Template
```python
from collections import deque

# 1) BFS by layer
def bfs_levels(root, neighbours):
    q = deque([root])
    seen = {root}
    level = 0
    while q:
        for _ in range(len(q)):            # freeze one layer
            node = q.popleft()
            # process node at depth `level`
            for nxt in neighbours(node):
                if nxt not in seen:
                    seen.add(nxt); q.append(nxt)
        level += 1

# 2) Fixed-size sliding window maximum (monotonic deque of indices)
def max_sliding_window(arr, k):
    dq = deque()                           # indices, arr[dq] strictly decreasing
    out = []
    for i, x in enumerate(arr):
        while dq and dq[0] <= i - k:       # evict out-of-window
            dq.popleft()
        while dq and arr[dq[-1]] < x:       # maintain monotonicity
            dq.pop()
        dq.append(i)
        if i >= k - 1:
            out.append(arr[dq[0]])
    return out

# 3) Task scheduler / stream with cooldown — deque of timestamps
def within_last_seconds(times, t, window):
    while times and times[0] <= t - window:
        times.popleft()
    times.append(t)
    return len(times)
```
Key mental tools:
- Always `from collections import deque` — `list.pop(0)` is `O(n)` and kills performance.
- For BFS on a grid, **mark seen on enqueue** (not on dequeue) — otherwise you enqueue duplicates.
- Use the "freeze layer size with `len(q)` at the top of each iteration" trick to emit level-by-level results.

## 4. Classic Problems
- **LC 102 — Binary Tree Level Order Traversal** (Medium): BFS with per-layer grouping.
- **LC 200 — Number of Islands** (Medium): BFS/DFS from each unvisited land cell.
- **LC 994 — Rotting Oranges** (Medium): multi-source BFS from all rotten cells.
- **LC 239 — Sliding Window Maximum** (Hard): monotonic deque.
- **LC 933 — Number of Recent Calls** (Easy): timestamp deque with sliding window eviction.

## 5. Worked Example — Binary Tree Level Order Traversal (LC 102)
Problem: given root of a binary tree, return a list of lists of values at each level.

Tree used:
```
        3
       / \
      9  20
         / \
        15  7
```
Approach: standard BFS with layer freezing.

```python
from collections import deque

def levelOrder(root):
    if not root: return []
    q = deque([root])
    out = []
    while q:
        layer = []
        for _ in range(len(q)):
            node = q.popleft()
            layer.append(node.val)
            if node.left:  q.append(node.left)
            if node.right: q.append(node.right)
        out.append(layer)
    return out
```

Trace (show queue contents as values):

| iter | `len(q)` frozen | layer built | queue before iter | nodes popped / children enqueued | queue after iter | `out` |
|---|---|---|---|---|---|---|
| start | — | — | `[3]` | — | `[3]` | `[]` |
| 1 | 1 | `[3]` | `[3]` | pop 3, enqueue 9, 20 | `[9, 20]` | `[[3]]` |
| 2 | 2 | `[9, 20]` | `[9, 20]` | pop 9 (no kids); pop 20, enqueue 15, 7 | `[15, 7]` | `[[3],[9,20]]` |
| 3 | 2 | `[15, 7]` | `[15, 7]` | pop 15 (no kids); pop 7 (no kids) | `[]` | `[[3],[9,20],[15,7]]` |

Queue empty → return `[[3],[9,20],[15,7]]`. Time `O(n)`, space `O(w)` where `w` is the max layer width.

Every BFS variant (shortest-path-in-grid, 0-1 BFS, multi-source) is a decoration of this same frame.

## 6. Common Variations
- **0-1 BFS**: deque with `appendleft` for edges of weight 0 and `append` for weight 1 — Dijkstra in `O(V+E)` for 0/1 weights.
- **Multi-source BFS**: seed the queue with *all* sources (rotting oranges, walls-and-gates, "01 matrix distance").
- **Bidirectional BFS**: expand frontiers from source and target simultaneously; meet in the middle for huge speedups (word ladder).
- **Monotonic deque**: sliding window max/min, constrained-subsequence-sum DP (LC 1425).
- **Circular queue / ring buffer**: fixed-capacity queue with wrap-around indices.
- **Deques for palindrome checks**: `pop` from both ends.
- **Edge cases**: empty graph, disconnected components, self-loops, revisiting vs infinite loop.

## 7. Related Patterns
- **Stack** — LIFO sibling; swap to a stack to turn BFS into DFS.
- **Graph BFS / Tree BFS** — queues are the substrate of every layered traversal.
- **Heap / Priority Queue** — weighted shortest paths move beyond plain queues.
- **Sliding Window** — monotonic deques power the constant-time window-max variant.
- **Union Find** — alternative for connectivity questions when you do not need distances.

Distinguishing note: if the graph is **unweighted** and you want the **shortest** number of steps, reach for BFS with a queue. If edge weights differ, move to a heap (Dijkstra). If you only care about *reachability* and not distance, a stack-based DFS works just as well.
