# 06 — Two Pointers

## 1. When to Use
- Input is a **sorted array** or becomes sorted after preprocessing.
- You need to find **pairs, triplets, or partitions** that satisfy a condition.
- Keywords: *"pair sum"*, *"closest to target"*, *"reverse in place"*, *"partition"*, *"move zeroes"*, *"remove in place"*, *"merge two sorted"*.
- You would otherwise write an `O(n^2)` pair-loop.
- Two sequences need to be **walked in lock-step** (merging, intersection, palindrome).
- Problem asks for `O(1)` extra space — two pointers beat hashing when memory is constrained.

## 2. Core Idea
Two pointers exploit monotonicity: on a sorted array, moving the left pointer right can only *increase* a pair sum; moving the right pointer left can only *decrease* it. That monotone relationship means each step eliminates an entire row or column of the hypothetical `O(n^2)` search space, collapsing the work to `O(n)`.

## 3. Template
```python
# Variant A — opposite ends, converging (sorted array, pair-sum)
def pair_sum(arr, target):
    lo, hi = 0, len(arr) - 1
    while lo < hi:
        s = arr[lo] + arr[hi]
        if s == target:
            return (lo, hi)
        elif s < target:
            lo += 1           # need larger sum
        else:
            hi -= 1           # need smaller sum
    return None

# Variant B — slow / fast, same direction (in-place filter)
def move_non_zero_forward(arr):
    slow = 0
    for fast in range(len(arr)):
        if arr[fast] != 0:
            arr[slow], arr[fast] = arr[fast], arr[slow]
            slow += 1
    return arr

# Variant C — merge two sorted sequences
def merge(a, b):
    i = j = 0
    out = []
    while i < len(a) and j < len(b):
        if a[i] <= b[j]:
            out.append(a[i]); i += 1
        else:
            out.append(b[j]); j += 1
    out.extend(a[i:]); out.extend(b[j:])
    return out
```
Key mental tools:
- **Invariant**: state precisely what is true of `arr[lo..hi]` at every step.
- **Progress**: every iteration must move at least one pointer, or you loop forever.
- **Termination**: loop ends when `lo >= hi` (converging) or one sequence exhausts (merging).

## 4. Classic Problems
- **LC 167 — Two Sum II (Sorted)** (Medium): classic converging pointers.
- **LC 15 — 3Sum** (Medium): fix one index, two-pointer the rest.
- **LC 11 — Container With Most Water** (Medium): move the shorter wall.
- **LC 42 — Trapping Rain Water** (Hard): left/right max pointers.
- **LC 283 — Move Zeroes** (Easy): slow/fast in-place rewrite.

## 5. Worked Example — Container With Most Water (LC 11)
Problem: given heights `height[i]`, pick two indices `i < j` that maximise `min(h[i], h[j]) * (j - i)`.

Input: `height = [1, 8, 6, 2, 5, 4, 8, 3, 7]`.

Why two pointers: starting from the widest possible base (`lo = 0`, `hi = n-1`) gives the maximum width. Shrinking width is only worthwhile if it buys more height, and the **shorter** wall is the bottleneck — moving the taller wall inward can never help. So: always move the shorter wall.

```python
lo, hi = 0, len(height) - 1
best = 0
while lo < hi:
    area = min(height[lo], height[hi]) * (hi - lo)
    best = max(best, area)
    if height[lo] < height[hi]:
        lo += 1
    else:
        hi -= 1
```

Step-by-step:

| step | `lo` | `hi` | `h[lo]` | `h[hi]` | width | min height | area | `best` | move |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0 | 8 | 1 | 7 | 8 | 1 | 8 | 8 | lo++ (shorter left) |
| 2 | 1 | 8 | 8 | 7 | 7 | 7 | 49 | 49 | hi-- (shorter right) |
| 3 | 1 | 7 | 8 | 3 | 6 | 3 | 18 | 49 | hi-- |
| 4 | 1 | 6 | 8 | 8 | 5 | 8 | 40 | 49 | hi-- (tie, either works) |
| 5 | 1 | 5 | 8 | 4 | 4 | 4 | 16 | 49 | hi-- |
| 6 | 1 | 4 | 8 | 5 | 3 | 5 | 15 | 49 | hi-- |
| 7 | 1 | 3 | 8 | 2 | 2 | 2 | 4 | 49 | hi-- |
| 8 | 1 | 2 | 8 | 6 | 1 | 6 | 6 | 49 | hi-- |
| stop | 1 | 1 | — | — | — | — | — | 49 | lo == hi |

Answer `49`. Time `O(n)`, space `O(1)`. Correctness hinges on the invariant: the optimum involving `h[lo]` cannot be achieved by any `hi' < hi` when `h[lo] < h[hi]`, so discarding `lo` is safe.

## 6. Common Variations
- **3Sum / 4Sum**: fix outer indices, two-pointer the remainder; **skip duplicates** carefully.
- **Closest sum**: track `abs(current - target)` alongside pointers.
- **Bounded window (slow/fast) = Sliding Window** (see next file) — same mechanic, different state.
- **Cycle detection (Floyd's)**: slow/fast with different speeds on a linked list.
- **Partition** (Dutch flag): three pointers for `< pivot`, `== pivot`, `> pivot`.
- **Merge-style**: `k`-way merge generalises to a min-heap (see Heap pattern).
- **Unsorted input**: sort first if the relative order of elements does not matter, else reach for hashing instead.

## 7. Related Patterns
- **Sliding Window** — a same-direction two-pointer whose state is a bounded window.
- **Binary Search** — converges via a single pointer on a sorted index space; two-pointer converges via two.
- **Sorting** — the near-universal preprocessing step that unlocks two pointers.
- **Hashing** — the alternative when input is unsorted and sorting would destroy needed info (e.g. original indices).
- **Linked List** — slow/fast pointers detect cycles, find middle, and reverse.

Distinguishing note: if moving the "wrong" pointer makes the condition **monotonically worse**, you have a two-pointer problem. If the condition depends on a contiguous window's **aggregate**, you have a sliding-window problem.
