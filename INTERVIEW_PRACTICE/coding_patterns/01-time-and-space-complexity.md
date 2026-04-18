# 01 — Time & Space Complexity

## 1. When to Use
Apply this lens **before coding** (to set a target) and **after coding** (to verify):
- Input bounds in the prompt: `n <= 10^5`, `n <= 10^9`, `grid[m][n] with m,n <= 1000`.
- Keywords: *"efficient"*, *"large input"*, *"compare approaches"*, *"fastest"*, *"optimize memory"*.
- A working brute force exists and you must justify whether it will pass within time/memory limits.
- You see multiple candidate solutions and need a principled way to pick one.

Rough ceilings per second on a typical judge:

| Input size `n` | Acceptable complexity |
|---|---|
| `n <= 20` | `O(2^n)`, `O(n!)` |
| `n <= 500` | `O(n^3)` |
| `n <= 5_000` | `O(n^2)` |
| `n <= 10^5` | `O(n log n)` |
| `n <= 10^7` | `O(n)` |
| `n <= 10^18` | `O(log n)` or `O(1)` |

## 2. Core Idea
Complexity counts **how the work grows with input**, not wall-clock time. We drop constants and lower-order terms because, at scale, the dominant term swamps everything else. Choosing an algorithm whose growth matches the input size is the difference between a solution that finishes and one that TLEs.

## 3. Template
```python
# Mental template: walk the code, count nested loops + recursion + data-structure ops.

def analyze(arr):
    n = len(arr)

    # O(1) — constant work
    first = arr[0]

    # O(n) — single pass
    total = sum(arr)

    # O(n log n) — sort dominates
    arr.sort()

    # O(n^2) — nested loop
    for i in range(n):
        for j in range(i + 1, n):
            _ = arr[i] + arr[j]

    # Space: O(n) for the copy, O(1) for scalars above.
    copy = arr[:]
    return total, copy
```
Rules of thumb:
- Loops multiply, sequential sections add, recursion = (branches) ^ (depth).
- `sort` is `O(n log n)` time, `O(n)` or `O(log n)` space depending on algorithm.
- Hash map op is amortised `O(1)`; treat worst case as `O(n)` only when collisions matter.

## 4. Classic Problems
- **LC 1 — Two Sum** (Easy): brute `O(n^2)` vs hashed `O(n)`.
- **LC 53 — Maximum Subarray** (Medium): `O(n^3)` → `O(n^2)` → Kadane `O(n)`.
- **LC 74 — Search a 2D Matrix** (Medium): `O(m*n)` vs `O(log(m*n))`.
- **LC 215 — Kth Largest Element** (Medium): sort `O(n log n)` vs heap `O(n log k)` vs quickselect `O(n)` avg.
- **LC 300 — Longest Increasing Subsequence** (Medium): DP `O(n^2)` vs patience sort `O(n log n)`.

## 5. Worked Example — "Two Sum" complexity walkthrough
Problem: given `nums = [2, 7, 11, 15]`, `target = 9`, return indices of the two numbers summing to target.

### Attempt A — brute force
```python
for i in range(n):
    for j in range(i + 1, n):
        if nums[i] + nums[j] == target: return [i, j]
```
Trace for `n = 4`:

| outer `i` | inner `j` range | pair checks |
|---|---|---|
| 0 | 1..3 | 3 |
| 1 | 2..3 | 2 |
| 2 | 3..3 | 1 |
| 3 | — | 0 |

Total checks = `3 + 2 + 1 + 0 = 6 = n(n-1)/2` → **`O(n^2)` time, `O(1)` space.** At `n = 10^5` that is ~5 × 10^9 ops — TLE.

### Attempt B — hashed
```python
seen = {}
for i, x in enumerate(nums):
    if target - x in seen: return [seen[target - x], i]
    seen[x] = i
```
Trace:

| step | `x` | `target - x` | `seen` before | result |
|---|---|---|---|---|
| 0 | 2 | 7 | `{}` | miss, store `{2:0}` |
| 1 | 7 | 2 | `{2:0}` | hit → return `[0, 1]` |

One linear pass, O(1) dict ops → **`O(n)` time, `O(n)` space.** At `n = 10^5` that is ~10^5 ops — fine.

Verdict: trading `O(1)` extra space for `O(n)` auxiliary space drops the growth from quadratic to linear, which is the decision complexity analysis makes obvious.

## 6. Common Variations
- **Amortised vs worst-case**: dynamic-array `append` is `O(1)` amortised, `O(n)` worst case.
- **Expected vs worst-case**: quickselect is `O(n)` expected, `O(n^2)` worst case.
- **Output-sensitive**: finding all pairs that sum to `target` is `Ω(k)` where `k` is the number of pairs — "fast" depends on `k`.
- **Input-shape sensitive**: `O(V + E)` for graphs is linear in the graph, not in `V`.
- **Space tradeoffs**: recursion adds `O(depth)` stack space; iterative may be flat.
- **Bit tricks**: `O(1)` in theory becomes `O(word)` on arbitrary-precision ints.

## 7. Related Patterns
- **Brute Force First** — always benchmark against an `O(n^2)` baseline before optimising.
- **Binary Search** — the standard way to turn `O(n)` into `O(log n)` on sorted/monotonic inputs.
- **Sorting** — a common `O(n log n)` preprocessing step that unlocks `O(n)` scans.
- **Hashing** — the standard way to remove an inner loop by trading space for time.
- **DP vs Backtracking** — memoisation collapses exponential recursion to polynomial time.

Distinguishing note: complexity analysis is not itself a pattern you *apply* — it is the framework you use to **pick** which pattern to apply. Every other file in this series should be read with complexity as the scoreboard.
