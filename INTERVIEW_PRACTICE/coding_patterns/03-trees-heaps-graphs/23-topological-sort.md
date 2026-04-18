# 23 — Topological Sort

## 1. When to Use
- You have a **directed acyclic graph (DAG)** and need an order that respects all edges.
- Keywords: *"course schedule"*, *"build order"*, *"task dependencies"*, *"prerequisites"*, *"compilation order"*, *"alien dictionary"*, *"parallel task planning"*.
- You need to detect whether a directed graph has a cycle (Kahn's algorithm does both).
- You want to process nodes such that **every predecessor is processed first** (longest path in DAG, DAG DP).
- You are laying out nodes on a timeline where "before / after" relationships must be honoured.

## 2. Core Idea
In a DAG, at least one node has **in-degree 0** — it depends on nothing. Remove such a node, decrement the in-degrees of its successors, and repeat. The order in which nodes are removed is a topological order. If any node is never removable, the graph contains a cycle. Kahn's algorithm is BFS-based; the DFS variant uses reverse finish-time order.

## 3. Template
```python
from collections import deque, defaultdict

# Kahn's algorithm — BFS-style
def topo_sort(n, edges):
    indeg = [0] * n
    adj = defaultdict(list)
    for u, v in edges:                   # edge u -> v means u must come before v
        adj[u].append(v)
        indeg[v] += 1

    q = deque(i for i in range(n) if indeg[i] == 0)
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in adj[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    if len(order) != n:
        return None                      # cycle detected
    return order

# DFS-based — post-order reversed
def topo_sort_dfs(n, edges):
    adj = defaultdict(list)
    for u, v in edges: adj[u].append(v)
    WHITE, GRAY, BLACK = 0, 1, 2
    color = [WHITE] * n
    order = []

    def visit(u):
        if color[u] == GRAY: raise ValueError("cycle")
        if color[u] == BLACK: return
        color[u] = GRAY
        for v in adj[u]: visit(v)
        color[u] = BLACK
        order.append(u)

    for u in range(n):
        if color[u] == WHITE: visit(u)
    return order[::-1]
```
Key mental tools:
- Choose Kahn's (BFS) when you want the "natural" order and easy cycle detection.
- Choose DFS when you also need finish times or SCCs.
- **Lexicographically smallest topo order** → use a min-heap instead of a deque in Kahn's.
- For "minimum semesters" (LC 1136), count **BFS layers** instead of the order itself.

## 4. Classic Problems
- **LC 207 — Course Schedule** (Medium): can we finish? = DAG check.
- **LC 210 — Course Schedule II** (Medium): return any valid order.
- **LC 269 — Alien Dictionary** (Hard): derive letter order from adjacent word comparisons, then topo sort.
- **LC 310 — Minimum Height Trees** (Medium): topological-style peeling from leaves inward.
- **LC 1136 — Parallel Courses** (Medium): BFS layers give minimum semesters.

## 5. Worked Example — Course Schedule II (LC 210)
Problem: `numCourses = 4`, `prerequisites = [[1,0],[2,0],[3,1],[3,2]]`. Return any order in which all courses can be taken (edges `b -> a` mean "take `b` before `a`"; in the LC input `[a, b]` means `b` is prereq of `a`, i.e. `b -> a`).

Edges as `prereq -> course`:
- `0 -> 1`
- `0 -> 2`
- `1 -> 3`
- `2 -> 3`

Graph:
```
      0
     / \
    v   v
    1   2
     \ /
      v
      3
```
In-degree: `[0, 1, 1, 2]`.

Run Kahn's:

```python
from collections import deque, defaultdict

def findOrder(numCourses, prerequisites):
    indeg = [0] * numCourses
    adj = defaultdict(list)
    for a, b in prerequisites:
        adj[b].append(a)                 # b -> a
        indeg[a] += 1

    q = deque(i for i in range(numCourses) if indeg[i] == 0)
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in adj[u]:
            indeg[v] -= 1
            if indeg[v] == 0: q.append(v)

    return order if len(order) == numCourses else []
```

Trace:

| step | queue before | popped `u` | successors of `u` | `indeg` updates | queue after | `order` |
|---|---|---|---|---|---|---|
| init | `[0]` | — | — | `[0,1,1,2]` | `[0]` | `[]` |
| 1 | `[0]` | 0 | 1, 2 | 1: `1→0`, 2: `1→0` → both zero, enqueue | `[1, 2]` | `[0]` |
| 2 | `[1, 2]` | 1 | 3 | 3: `2→1` | `[2]` | `[0, 1]` |
| 3 | `[2]` | 2 | 3 | 3: `1→0` → enqueue | `[3]` | `[0, 1, 2]` |
| 4 | `[3]` | 3 | — | — | `[]` | `[0, 1, 2, 3]` |

All 4 processed → `order = [0, 1, 2, 3]`. Any order satisfying `0` before `1`/`2`, and `1`/`2` before `3` is valid; `[0, 2, 1, 3]` would also be legal. Time `O(V + E)`, space `O(V + E)`.

If a cycle were present (say an extra edge `3 -> 0`), in-degree of `0` would start at `1`, queue would be empty, and `order` would stay empty → return `[]`.

## 6. Common Variations
- **Cycle detection only**: ignore `order` — just check `len(order) == n`.
- **Lexicographic smallest**: swap `deque` for a `heapq` min-heap.
- **Longest path in a DAG**: topo-order the nodes, then DP by `dp[v] = max(dp[u] + w) for u -> v`.
- **BFS layer-count** (LC 1136 parallel courses): track `semester` by freezing queue length per iteration.
- **Alien dictionary** (LC 269): build edges by pairwise comparing adjacent words — subtle edge cases with prefix order.
- **Offline DAG DP**: topo-sort once, scan in order — avoids recursion on long chains.
- **Edge cases**: graph with no edges (any permutation is valid), duplicate edges, self-loops (immediate cycle).

## 7. Related Patterns
- **Graph BFS** — Kahn's is a BFS whose frontier is "in-degree 0" nodes.
- **Graph DFS** — alternative via reverse post-order.
- **Dynamic Programming on DAGs** — the natural successor step after a topological sort.
- **Union Find** — for undirected connectivity, not directed ordering.
- **Shortest / Longest Path** — topologically ordering a DAG lets you relax edges in `O(V + E)`.

Distinguishing note: any problem with directed **dependency** language ("must do X before Y") and no cycles is a topological-sort problem. If cycles may exist, the topo sort is your *detector* as well as your solution.
