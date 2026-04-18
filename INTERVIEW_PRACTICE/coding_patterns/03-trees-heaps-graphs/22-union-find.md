# 22 — Union Find (Disjoint Set Union)

## 1. When to Use
- You need to track which elements are **in the same group** as edges / merges arrive online.
- Keywords: *"connected components"*, *"redundant edge"*, *"accounts merge"*, *"number of islands II"*, *"dynamic connectivity"*, *"Kruskal's MST"*, *"equations like a/b = k"*.
- The graph **grows** by adding edges (or equivalences) over time — DFS/BFS would re-run on each query.
- You need only the **group identity** of elements, not distances or paths.
- Cycle detection in an **undirected** graph: an edge closes a cycle iff both endpoints already share a root.

## 2. Core Idea
Maintain a forest where each tree represents one group; each node points to a parent, and the root represents the set. `find(x)` walks to the root (with path compression so future calls are `O(1)`), `union(x, y)` links the two roots (by rank/size so trees stay shallow). With both optimisations, `m` operations cost `O(m * α(n))` — effectively linear.

## 3. Template
```python
class DSU:
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank   = [0] * n
        self.size   = [1] * n
        self.components = n

    def find(self, x):
        # Path compression — flatten the tree on the way up
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry: return False           # already connected → cycle edge
        # Union by rank — attach shallower tree under deeper one
        if self.rank[rx] < self.rank[ry]: rx, ry = ry, rx
        self.parent[ry] = rx
        self.size[rx] += self.size[ry]
        if self.rank[rx] == self.rank[ry]: self.rank[rx] += 1
        self.components -= 1
        return True

    def connected(self, x, y):
        return self.find(x) == self.find(y)
```
Key mental tools:
- Path compression alone is usually enough in practice; union-by-rank seals the worst case.
- `union` returning `False` is a first-class signal for "this edge would create a cycle" — very useful.
- Track `components` inline to answer "how many groups?" in `O(1)`.
- For weighted DSU (equations like `a / b = k`), store `rank[x] = ratio to parent` and multiply during `find`.

## 4. Classic Problems
- **LC 547 — Number of Provinces** (Medium): union over adjacency matrix.
- **LC 684 — Redundant Connection** (Medium): first edge whose union returns `False`.
- **LC 721 — Accounts Merge** (Medium): union accounts sharing any email.
- **LC 1319 — Number of Operations to Make Network Connected** (Medium): extra edges vs components − 1.
- **LC 399 — Evaluate Division** (Medium): weighted DSU.

## 5. Worked Example — Redundant Connection (LC 684)
Problem: an undirected tree of `n` nodes had exactly one extra edge added. Find that edge.

Input: `edges = [[1,2], [1,3], [2,3]]`.

Approach: process edges in order, union-ing endpoints. The first edge whose `union` returns `False` connects two already-connected nodes and is the redundant one.

```python
def findRedundantConnection(edges):
    n = len(edges)
    dsu = DSU(n + 1)                 # nodes are 1..n
    for a, b in edges:
        if not dsu.union(a, b):
            return [a, b]
```

Trace (using the template above, showing `parent` only for involved nodes):

| step | edge | `find(a)` | `find(b)` | same? | action | `parent` after |
|---|---|---|---|---|---|---|
| init | — | — | — | — | — | `[0,1,2,3]` |
| 1 | (1,2) | 1 | 2 | no | union → `parent[2]=1`, components 3→2 | `[0,1,1,3]` |
| 2 | (1,3) | 1 | 3 | no | union → `parent[3]=1`, components 2→1 | `[0,1,1,1]` |
| 3 | (2,3) | `find(2)` → parent 1 | `find(3)` → parent 1 | **yes** | `union` returns `False` → edge is redundant | — |

Return `[2, 3]`. Time `O(n * α(n))`, space `O(n)`. The key insight is that the redundant edge is detected the moment you try to unite two endpoints that already share a root.

## 6. Common Variations
- **Count components**: initialise `components = n`, decrement on successful union.
- **Weighted / relation DSU** (LC 399 `a/b`): maintain `weight[x] = ratio(x, parent[x])`; update during `find` with multiplicative compression.
- **Rollback DSU**: support undo by storing union snapshots — used in offline dynamic connectivity.
- **Kruskal's MST**: sort edges by weight, union in order, skip if `union` returns `False`.
- **Generalised grouping keys**: map strings/emails to ints via a dict, then DSU.
- **2D grid components**: flatten `(r, c)` to `r * n + c` as a DSU index (Number of Islands II).
- **Edge cases**: self-loops (same root, returns False), parallel edges (second one is redundant), empty edge list.

## 7. Related Patterns
- **Graph DFS / BFS** — alternatives for **static** component counts; DSU wins online.
- **Kruskal's Algorithm** — MST using DSU as the cycle oracle.
- **Topological Sort** — does not use DSU; directed acyclicity is a different invariant.
- **Hashing** — often the first step before DSU: map arbitrary keys to integer indices.
- **Tarjan's Offline LCA / Offline Range Queries** — DSU with rollback.

Distinguishing note: reach for DSU when the input arrives as a **stream of equivalences** and you must answer "same group?" queries online. Reach for BFS/DFS when the graph is static and you need **distance** or **path** information, not just connectivity.
