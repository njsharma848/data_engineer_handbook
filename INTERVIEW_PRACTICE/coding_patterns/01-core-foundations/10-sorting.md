# 10 — Sorting

## 1. When to Use
- Order matters for a later step: **two pointers**, **binary search**, **greedy**, **grouping identical items**, **interval sweep**.
- Keywords: *"intervals"*, *"meetings"*, *"k closest"*, *"arrange / reorder"*, *"by this key"*, *"merge"*, *"smallest/largest k"*.
- You are told comparisons are valid / keys are orderable.
- A one-time `O(n log n)` sort unlocks a subsequent `O(n)` scan, beating the `O(n^2)` brute force.
- You need a **stable** arrangement (Python's `sorted` / `list.sort` are stable).

## 2. Core Idea
Sorting turns a shapeless input into a structured one so hidden relationships become positional: adjacency, monotonicity, or grouping. Once the data is ordered by the right key, the remaining work is often a linear sweep or a binary search, which is why "sort first, then walk" is one of the most reused skeletons in interview problems.

## 3. Template
```python
# 1) Sort by custom key
items.sort(key=lambda x: (x.priority, -x.age))   # stable, in place

# 2) Sort-then-scan skeleton
def sweep(items):
    items.sort(key=lambda x: x.start)           # dominant preprocessing step
    prev = None
    out = []
    for x in items:
        if prev is not None and overlaps(prev, x):
            prev = merge(prev, x)
        else:
            if prev is not None: out.append(prev)
            prev = x
    if prev is not None: out.append(prev)
    return out

# 3) Counting sort for bounded integer values (O(n + V))
def count_sort(arr, V):
    buckets = [0] * V
    for x in arr: buckets[x] += 1
    out = []
    for v, c in enumerate(buckets):
        out.extend([v] * c)
    return out

# 4) Functools cmp_to_key when the ordering isn't a simple key
from functools import cmp_to_key
def compare(a, b):
    # return <0 if a should come before b, 0 if equal, >0 otherwise
    return (a + b > b + a) - (a + b < b + a)
items.sort(key=cmp_to_key(compare))
```
Key mental tools:
- Python `sort` is Timsort → `O(n log n)` worst case, stable, near-linear on partially sorted input.
- Sort by a **composite key** (tuple) when tie-breaking matters; negate a field for descending order.
- When values are small integers, **counting sort** drops to `O(n + V)`.
- Sorting does not always preserve the original index — save `enumerate(arr)` first if you need it.

## 4. Classic Problems
- **LC 56 — Merge Intervals** (Medium): sort by start, sweep.
- **LC 252 / 253 — Meeting Rooms I/II** (Easy/Medium): sort endpoints or start+heap.
- **LC 179 — Largest Number** (Medium): custom comparator `a+b vs b+a`.
- **LC 75 — Sort Colors** (Medium): Dutch National Flag (3-way partition), counting sort alternative.
- **LC 147 — Insertion Sort List** (Medium): algorithmic, not just "call `.sort()`".

## 5. Worked Example — Merge Intervals (LC 56)
Problem: given `intervals = [[1,3],[2,6],[8,10],[15,18]]`, merge all overlapping intervals.

Approach: sort by `start`. Walk through intervals, merge with the last accepted interval when they overlap.

```python
def merge(intervals):
    intervals.sort(key=lambda iv: iv[0])
    out = []
    for start, end in intervals:
        if out and start <= out[-1][1]:          # overlap with previous
            out[-1][1] = max(out[-1][1], end)
        else:
            out.append([start, end])
    return out
```

Trace (after sort, input is already sorted by start):

| step | interval | `out[-1]` | overlap test | action | `out` |
|---|---|---|---|---|---|
| 1 | `[1,3]` | — | — | append | `[[1,3]]` |
| 2 | `[2,6]` | `[1,3]` | `2 ≤ 3` → yes | extend end to `max(3,6)=6` | `[[1,6]]` |
| 3 | `[8,10]` | `[1,6]` | `8 ≤ 6` → no | append | `[[1,6],[8,10]]` |
| 4 | `[15,18]` | `[8,10]` | `15 ≤ 10` → no | append | `[[1,6],[8,10],[15,18]]` |

Answer `= [[1,6],[8,10],[15,18]]`. Time `O(n log n)` for the sort + `O(n)` for the sweep; space `O(n)` for `out` (or `O(1)` extra if sorting in place and writing into `intervals`).

The whole thing relies on **sorting by start**: without it, an "overlap" pair could be arbitrarily far apart, forcing quadratic work.

## 6. Common Variations
- **Sort by end** instead of start: classic greedy for activity selection (LC 435 non-overlapping intervals).
- **Sweep line / events**: turn intervals into `(time, +1/-1)` events, sort, run a counter — works for max-overlap and busy-time problems.
- **Counting / radix sort**: when keys are bounded integers, sort in `O(n)` linear time.
- **Partial sort**: `heapq.nsmallest(k, arr)` → top-k in `O(n log k)` without sorting the rest.
- **Quickselect**: `k`-th element in `O(n)` average without a full sort.
- **Stability matters**: preserving relative order of equal keys (e.g. sorting students by grade but keeping submission order within grade).
- **Edge cases**: empty list, all-equal keys, already-sorted input, custom comparator with tie-breaks.

## 7. Related Patterns
- **Two Pointers / Sliding Window** — the usual `O(n)` follow-up pass after sorting.
- **Binary Search** — another `O(log n)` follow-up on sorted arrays.
- **Greedy** — many greedy proofs require the input sorted by the greedy key (earliest-deadline, shortest-job, etc.).
- **Heap** — streaming version of sorting when you only need top-`k` or a priority order.
- **Union Find** — Kruskal's MST sorts edges by weight, then unions.
- **Bucket / Counting Sort** — domain-aware alternatives when the value range is small.

Distinguishing note: if a problem smells `O(n^2)` because it compares all pairs, sort first and ask *"does adjacency in sorted order carry the information I need?"* If yes, you just dropped a factor of `n / log n`.
