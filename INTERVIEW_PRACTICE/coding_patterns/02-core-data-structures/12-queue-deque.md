# 12 — Queue / Deque

## 1. When to Use
- You need **First In, First Out** processing: BFS, level-order traversal, scheduling, stream processing.
- Keywords: *"level order"*, *"shortest path in unweighted graph"*, *"minimum number of steps"*, *"nearest"*, *"sliding window maximum / minimum"*, *"hot streak of last k"*, *"recent calls in the last t seconds"*, *"multi-source BFS"*, *"0-1 BFS"*, *"word ladder"*, *"rotting oranges"*.
- The algorithm expands in **layers** and you must finish layer `d` before starting layer `d+1`.
- You need cheap operations on **both ends**: push/pop front *and* back in `O(1)` → deque.
- You need a **sliding monotonic structure**: window max/min, constrained subsequence DP.
- You are building a **circular buffer** / **ring buffer** / **recent-k** cache where old entries are evicted from the head as new ones arrive at the tail.
- You need **bidirectional search**: BFS from source and target simultaneously to halve the frontier.

### Signals that it is NOT a queue problem
- You need to revisit the most recent element first → stack, not queue.
- Elements have priority beyond arrival order → heap.
- Edge weights are arbitrary non-negative reals → Dijkstra with a heap, not plain BFS.
- The graph is unweighted but you need *all* shortest paths, not just distance → BFS + parent map (queue still works, but record parents).

## 2. Core Idea
A queue guarantees that items are processed in arrival order, which is the right invariant for breadth-first exploration: every node at distance `d` is processed before any at distance `d+1`. This is **why BFS finds shortest paths** in unweighted graphs — the first time you reach a node, it's via the fewest edges. If you used a stack, you'd get DFS and lose that guarantee.

A deque (double-ended queue) generalises to both ends, enabling **monotonic-window tricks** where stale entries are evicted from the front while new entries are appended to the back. The classic example is sliding-window maximum: the deque stores indices whose values are a strictly-decreasing suffix of the window, so the front is always the window's current max.

Three dominant flavours:
1. **Plain BFS queue** (FIFO) — layered graph/grid traversal, shortest path in unweighted graphs.
2. **Monotonic deque** — sliding-window max/min, DP with a convex-hull-trick flavour.
3. **Sliding timestamp deque** — evict entries older than `t - window` from the front; append new timestamps at the back (LC 933, LC 346 moving average).

## 3. Template

### Template A — BFS by layer (unweighted shortest path)
```python
from collections import deque                # GOTCHA: NEVER `list.pop(0)` — O(n); deque.popleft is O(1)

def bfs_shortest(start, target, neighbours):
    if start == target:
        return 0
    q = deque([(start, 0)])                  # deque takes an iterable; tuple-packing avoids parallel lists
    seen = {start}                           # set literal with ONE element; `{start}` NOT `{}` (that's an empty dict!)
    while q:
        node, d = q.popleft()                # tuple-unpack in one step; popleft is O(1) amortised
        for nxt in neighbours(node):
            if nxt == target:
                return d + 1
            if nxt not in seen:              # GOTCHA: mark seen on ENQUEUE (below), not dequeue — prevents duplicates
                seen.add(nxt)
                q.append((nxt, d + 1))
    return -1                                # unreachable
```

### Template B — BFS with frozen layer (emit levels)
```python
def bfs_levels(root, neighbours):
    q = deque([root])
    seen = {root}
    while q:
        size = len(q)                        # GOTCHA: snapshot BEFORE the inner loop — `q` grows as you enqueue children
        for _ in range(size):                # `_` convention for unused loop var
            node = q.popleft()
            visit(node)
            for nxt in neighbours(node):
                if nxt not in seen:
                    seen.add(nxt); q.append(nxt)
        # depth advances implicitly per outer iteration (no explicit depth counter needed)
```

### Template C — Multi-source BFS (all sources distance 0)
```python
def multi_source_bfs(grid, is_source):
    m, n = len(grid), len(grid[0])            # GOTCHA: grid[0] fails on empty grid — guard upstream
    dist = [[-1] * n for _ in range(m)]       # list-comp per row — NEVER `[[-1]*n]*m` (shared rows!)
    q = deque()
    for r in range(m):
        for c in range(n):
            if is_source(grid[r][c]):
                dist[r][c] = 0                 # seed ALL sources with distance 0 — the "multi" in multi-source
                q.append((r, c))
    while q:
        r, c = q.popleft()
        for dr, dc in ((1,0), (-1,0), (0,1), (0,-1)):   # 4-dir; tuple-of-tuples is cheaper than list-of-lists
            nr, nc = r + dr, c + dc
            if 0 <= nr < m and 0 <= nc < n and dist[nr][nc] == -1 and grid[nr][nc] != 'wall':
                # chained compare `0 <= nr < m` is short-circuit AND; bounds check BEFORE index
                dist[nr][nc] = dist[r][c] + 1
                q.append((nr, nc))
    return dist
```

### Template D — Monotonic deque (fixed-size sliding window maximum)
```python
def max_sliding_window(arr, k):
    dq = deque()                             # indices; arr[dq] strictly decreasing. GOTCHA: store INDICES not values (for age-based eviction)
    out = []
    for i, x in enumerate(arr):
        while dq and dq[0] <= i - k:         # `while` (not `if`) — but at most one age-eviction fires per step
            dq.popleft()                     # O(1); list.pop(0) would be O(n)
        while dq and arr[dq[-1]] < x:        # `<` strict; `<=` also works here. dq[-1] peeks back end
            dq.pop()                         # default pops from RIGHT end (unlike popleft)
        dq.append(i)
        if i >= k - 1:                       # only emit AFTER first full window is formed (i.e. i>=k-1 means window covers [i-k+1..i])
            out.append(arr[dq[0]])           # front is the current window max
    return out
```

### Template E — 0-1 BFS (deque as poor-man's Dijkstra)
```python
def zero_one_bfs(start, target, neighbours_with_weights):
    """Edge weights ∈ {0, 1} only. O(V + E) vs Dijkstra's O((V + E) log V)."""
    dq = deque([(start, 0)])
    dist = {start: 0}
    while dq:
        node, d = dq.popleft()
        if node == target:
            return d
        if d > dist[node]:                    # stale entry. GOTCHA: we DON'T remove old entries; skip them lazily
            continue
        for nxt, w in neighbours_with_weights(node):
            nd = d + w
            if nd < dist.get(nxt, float('inf')):   # dict.get(key, default) avoids KeyError
                dist[nxt] = nd
                if w == 0:
                    dq.appendleft((nxt, nd))  # zero-weight: keep on the front (same layer)
                else:
                    dq.append((nxt, nd))      # unit-weight: push to the back (next layer)
    return -1
```

### Template F — Sliding timestamp deque (recent events)
```python
class HitCounter:                              # LC 362
    def __init__(self):
        self.q = deque()                       # timestamps in non-decreasing order. GOTCHA: `self.q` not `q` (instance attr)
    def hit(self, t):
        self.q.append(t)                       # amortised O(1) append
    def getHits(self, t):
        while self.q and self.q[0] <= t - 300:   # evict anything older than 300s window
            self.q.popleft()                    # GOTCHA: t-300 strict vs non-strict depends on inclusive/exclusive window
        return len(self.q)                     # len(deque) is O(1)
```

Key mental tools:
- Always `from collections import deque` — `list.pop(0)` is `O(n)` and kills performance on anything beyond small inputs.
- For BFS on a grid, **mark seen on enqueue** (not on dequeue) — otherwise you enqueue duplicates and distance bookkeeping breaks (`O(V^2)` blowup).
- Use the "freeze layer size with `len(q)` at the top of each iteration" trick to emit level-by-level results without storing depths per node.
- For monotonic deques, **store indices** (not values) so you can evict by window-age (`dq[0] <= i - k`).
- Invariant for sliding-window max: at the end of each iteration, the deque stores a strictly-decreasing suffix of indices within the current window.
- BFS distance is **discrete integers** only (one edge = one step). Anything else needs Dijkstra.

## 4. Classic Problems
- **LC 102 — Binary Tree Level Order Traversal** (Medium): BFS with per-layer freezing (Template B).
- **LC 200 — Number of Islands** (Medium): BFS/DFS from each unvisited land cell; multi-source variant counts connected components.
- **LC 994 — Rotting Oranges** (Medium): multi-source BFS from all rotten cells (Template C).
- **LC 239 — Sliding Window Maximum** (Hard): monotonic deque (Template D).
- **LC 933 — Number of Recent Calls** (Easy): timestamp deque with sliding-window eviction (Template F).
- **LC 127 — Word Ladder** (Hard): BFS on word-transform graph; bidirectional BFS for speedup.
- **LC 1293 — Shortest Path in a Grid with Obstacles Elimination** (Hard): BFS with augmented state `(cell, obstacles_remaining)`.

## 5. Worked Example — Sliding Window Maximum (LC 239)
Problem: given `arr = [1, 3, -1, -3, 5, 3, 6, 7]` and window size `k = 3`, return the max within each sliding window.

Expected: `[3, 3, 5, 5, 6, 7]` (six windows of size 3).

### Step 1. Why naive approaches fail
- **Recompute max per window**: `O(nk)` — too slow when `k ≈ n`.
- **Heap of window values**: `O(n log k)` with lazy deletion, but bookkeeping is fiddly.
- **Monotonic deque**: `O(n)` with a clean invariant. Each index enters and leaves the deque at most once.

### Step 2. The invariant
Let `dq` be a deque of **indices**. At the end of processing index `i`, the invariant is:
- All indices in `dq` are within the current window: `dq[0] >= i - k + 1`.
- The values `arr[dq[0]], arr[dq[1]], ...` are **strictly decreasing**.

Under this invariant, `arr[dq[0]]` is the max of the current window. To restore the invariant when adding index `i`:
1. Evict from the **front** any index that fell out of the window (`dq[0] <= i - k`).
2. Evict from the **back** any index whose value is `< arr[i]` — it can never be the max again (there's a later, at-least-equal value).
3. Append `i`.

### Step 3. Implementation
```python
from collections import deque

def maxSlidingWindow(arr, k):
    dq = deque()
    out = []
    for i, x in enumerate(arr):
        while dq and dq[0] <= i - k:        # step 1: evict stale (outside window) from front
            dq.popleft()
        while dq and arr[dq[-1]] < x:       # step 2: evict dominated (smaller than x) from back
            dq.pop()
        dq.append(i)                         # step 3: append current index
        if i >= k - 1:                       # step 4: emit once first full window is complete
            out.append(arr[dq[0]])           # front is max
    return out
```

### Step 4. Trace with `arr = [1, 3, -1, -3, 5, 3, 6, 7]`, `k = 3`
Show the deque as `[idx(val), idx(val), ...]`.

| `i` | `arr[i]` | evict front (age) | evict back (dominated) | deque after | window starts at | emit? |
|---|---|---|---|---|---|---|
| 0 | 1 | — | — | `[0(1)]` | — | no (`i < k-1`) |
| 1 | 3 | — | pop 0(1) (1 < 3) | `[1(3)]` | — | no |
| 2 | -1 | — | — | `[1(3), 2(-1)]` | 0 | **3** |
| 3 | -3 | — | — | `[1(3), 2(-1), 3(-3)]` | 1 | **3** |
| 4 | 5 | pop 1(3) (`1 <= 4-3 = 1`) | pop 3(-3), pop 2(-1) (both < 5) | `[4(5)]` | 2 | **5** |
| 5 | 3 | — | — | `[4(5), 5(3)]` | 3 | **5** |
| 6 | 6 | — | pop 5(3), pop 4(5) | `[6(6)]` | 4 | **6** |
| 7 | 7 | — | pop 6(6) (6 < 7) | `[7(7)]` | 5 | **7** |

Emitted: `[3, 3, 5, 5, 6, 7]` ✓.

### Step 5. Amortised analysis
Across all iterations, each index is pushed onto the deque once and popped at most once (either from the front due to age, or from the back when dominated). The inner `while` loops can fire many times in one iteration, but the total pops across all iterations is bounded by `n`. Hence the whole scan is `O(n)`. Space: `O(k)` for the deque (the window's indices).

### Step 6. Why a heap is worse
A heap-based solution is `O(n log n)` with lazy deletion. Correct but slower, and the bookkeeping (tag stale entries, pop them lazily) is trickier. The monotonic deque is the textbook choice whenever the aggregate admits a monotone domination relation.

## 6. Common Variations

### Layered BFS (the 80% use case)
- **Binary tree level order** (LC 102, LC 107, LC 199 right-side view): Template B.
- **Number of islands** (LC 200): BFS/DFS, same skeleton.
- **Rotting oranges** (LC 994): multi-source BFS (Template C).
- **Walls and gates** (LC 286): multi-source from all gates.
- **01 matrix** (LC 542): distance from every 0 — multi-source BFS.
- **Shortest path in binary matrix** (LC 1091): BFS with 8-directional moves.
- **Open the lock** (LC 752): BFS over a 4-digit state graph.
- **Word ladder** (LC 127): BFS over a word graph; consider bidirectional BFS.

### Modified BFS (state-augmented)
- **Shortest path with k obstacle eliminations** (LC 1293): state = `(r, c, k_remaining)`.
- **Bus routes** (LC 815): two-level BFS (stops ↔ buses).
- **Minimum genetic mutation** (LC 433): BFS on strings.
- **Jump game IV** (LC 1345): BFS with group-jumps.

### 0-1 BFS and beyond
- **Shortest path in grid with gates and walls** (LC 1368 "Minimum cost to make at least one valid path"): 0-1 BFS (Template E).
- **Escape the grid** / **minimum edges to reverse**: 0-1 BFS on a digraph where reversing an edge costs 1.

### Bidirectional BFS
- **Word ladder** (LC 127): expand the smaller frontier each step.
- **Minimum knight moves** (LC 1197): bidirectional BFS over an infinite board.

### Monotonic deque (sliding window aggregate)
- **Sliding window maximum** (LC 239): Template D.
- **Sliding window minimum**: flip the comparator.
- **Shortest subarray with sum at least k** (LC 862): monotonic deque over prefix sums with negative numbers.
- **Constrained subsequence sum** (LC 1425): DP with a monotonic deque to evict stale transitions.
- **Jump game VI** (LC 1696): same shape.

### Sliding timestamp / rolling window
- **Number of recent calls** (LC 933): Template F.
- **Moving average from data stream** (LC 346).
- **Design hit counter** (LC 362).
- **Logger rate limiter** (LC 359).

### Ring buffer / circular queue
- **Design circular queue** (LC 622): fixed capacity, wrap-around indices.
- **Design circular deque** (LC 641): both ends cheap.

### Edge cases & pitfalls
- **`list.pop(0)`** — `O(n)`, kills BFS performance on large inputs. Use `collections.deque`.
- **Marking seen on dequeue** — allows the same node to be enqueued many times; distance bookkeeping breaks on graphs with high fan-in. Mark on enqueue.
- **Forgetting to evict from the front** in monotonic deque — it silently returns the max over the whole history, not the window.
- **Depth tracking** — either pair `(node, depth)` in the queue, or freeze `len(q)` per layer. Don't mix both.
- **Empty queue / disconnected graph** — return `-1` or a sentinel; don't dereference.
- **Revisiting with a *better* state** — vanilla BFS assumes first-visit = best-visit. In state-augmented BFS, you must allow re-enqueue if the new augmented cost is lower (e.g. LC 1293).
- **Grid boundaries** — always bounds-check before indexing.

## 7. Related Patterns
- **Stack** — LIFO sibling; swap queue ↔ stack to turn BFS into DFS.
- **Graph BFS / Tree BFS** — queues are the substrate of every layered traversal.
- **Heap / Priority Queue** — weighted shortest paths (Dijkstra) move beyond plain queues. Deques with 0-1 weights bridge the gap.
- **Sliding Window** — monotonic deques power the constant-time window-max variant.
- **Union Find** — alternative for connectivity questions when you do not need distances.
- **Dynamic Programming** — many `O(n^2)` DPs over a window reduce to `O(n)` with a monotonic deque (LC 1425, LC 1696).
- **Two Pointers** — same-direction two pointers are a degenerate queue (front = shrink, back = grow).

**Distinguishing note**: if the graph is **unweighted** and you want the **shortest** number of steps, reach for BFS with a queue. If edge weights differ but are all non-negative, move to a heap (Dijkstra). If weights are 0 or 1 only, use 0-1 BFS with a deque. If you only care about *reachability* and not distance, a stack-based DFS works just as well and is often easier to write recursively.
