# 03 — Arrays

## 1. When to Use
- Input is a **list / vector of comparable items** with order that matters: `nums`, `heights`, `prices`, `intervals`.
- You need **index-based access**, traversal, or in-place rearrangement.
- Keywords: *"rotate"*, *"reverse"*, *"in-place"*, *"kth element"*, *"subarray"*, *"prefix"*, *"index"*.
- Constraints mention contiguous storage, O(1) random access, or ask you to *"do it without extra space"*.
- You know the size up front (or a tight upper bound) — arrays beat linked lists for random access and cache locality.

## 2. Core Idea
An array is a contiguous block of memory offering `O(1)` index access and `O(n)` insert/delete in the middle. Most array patterns exploit that cheap indexing to coordinate multiple positions at once (two pointers, sliding window, prefix sums) or to re-order data so that a hidden structure becomes linear to scan.

## 3. Template
```python
def array_scan(arr):
    n = len(arr)
    if n == 0:
        return None  # always handle empty first

    # --- single pass, constant extra space ---
    running = arr[0]
    for i in range(1, n):
        running = combine(running, arr[i])   # e.g. sum, max, min

    # --- in-place rewrite using a write pointer ---
    write = 0
    for read in range(n):
        if keep(arr[read]):
            arr[write] = arr[read]
            write += 1
    # arr[:write] now holds the filtered result

    # --- reverse in place ---
    lo, hi = 0, n - 1
    while lo < hi:
        arr[lo], arr[hi] = arr[hi], arr[lo]
        lo += 1; hi -= 1

    return running, write, arr
```
Key mental tools:
- **Two indices moving with purpose** (read/write, left/right, slow/fast).
- **Swap, don't shift** — constant work, no `O(n)` pop from the middle.
- **Guard `n == 0` and `n == 1`** up front; most bugs live in empty-array edge cases.

## 4. Classic Problems
- **LC 26 — Remove Duplicates from Sorted Array** (Easy): read/write two pointers.
- **LC 189 — Rotate Array** (Medium): three-reverse trick for `O(1)` space.
- **LC 238 — Product of Array Except Self** (Medium): prefix + suffix passes.
- **LC 31 — Next Permutation** (Medium): in-place index manipulation.
- **LC 41 — First Missing Positive** (Hard): index-as-hash in place, `O(n)` / `O(1)`.

## 5. Worked Example — Remove Duplicates from Sorted Array (LC 26)
Problem: given sorted `nums`, remove duplicates in place so each element appears once. Return the new length `k`; the first `k` slots of `nums` must contain the unique values.

Input: `nums = [1, 1, 2, 2, 2, 3, 4, 4, 5]`.

Two-pointer template: `write` is the next slot for a kept value; `read` scans the whole array.

```python
write = 1  # first element is always kept
for read in range(1, len(nums)):
    if nums[read] != nums[read - 1]:
        nums[write] = nums[read]
        write += 1
return write
```

Step-by-step state:

| step | `read` | `nums[read]` | `nums[read-1]` | new? | `write` before | action | `nums` (prefix up to `write`) |
|---|---|---|---|---|---|---|---|
| init | — | — | — | — | 1 | — | `[1]` |
| 1 | 1 | 1 | 1 | no | 1 | skip | `[1]` |
| 2 | 2 | 2 | 1 | yes | 1 | `nums[1]=2`, `write=2` | `[1, 2]` |
| 3 | 3 | 2 | 2 | no | 2 | skip | `[1, 2]` |
| 4 | 4 | 2 | 2 | no | 2 | skip | `[1, 2]` |
| 5 | 5 | 3 | 2 | yes | 2 | `nums[2]=3`, `write=3` | `[1, 2, 3]` |
| 6 | 6 | 4 | 3 | yes | 3 | `nums[3]=4`, `write=4` | `[1, 2, 3, 4]` |
| 7 | 7 | 4 | 4 | no | 4 | skip | `[1, 2, 3, 4]` |
| 8 | 8 | 5 | 4 | yes | 4 | `nums[4]=5`, `write=5` | `[1, 2, 3, 4, 5]` |

Return `k = 5`. Tail of `nums` (indices 5..8) can hold anything — the problem only inspects the first `k`. Time `O(n)`, space `O(1)`.

## 6. Common Variations
- **Remove duplicates allowing two copies** (LC 80): compare `nums[read]` with `nums[write - 2]`.
- **Move zeroes to the end** (LC 283): same read/write skeleton, keep non-zeros.
- **Rotate by `k`**: `reverse(0..n-1)`, `reverse(0..k-1)`, `reverse(k..n-1)`.
- **Kadane-style running accumulator**: max subarray, max product subarray.
- **Index-as-hash**: when values are bounded by `n`, use the array itself as a frequency table (LC 41, LC 448).
- **Partition in place**: Dutch National Flag for 3-way partitioning (LC 75).
- **Edge cases**: empty, length 1, all duplicates, already sorted descending, negative numbers.

## 7. Related Patterns
- **Strings** — strings are arrays of characters; almost every array technique transfers.
- **Two Pointers / Sliding Window** — the two dominant ways to turn `O(n^2)` array scans into `O(n)`.
- **Prefix Sum** — precompute once to answer range queries in `O(1)`.
- **Sorting** — the usual preprocessing step that unlocks two-pointer, binary search, or greedy.
- **Hashing** — when order does not matter, a set/map often beats an array scan.

Distinguishing note: "arrays" is the **substrate**, not a trick. When a problem is tagged array-only, the real pattern is usually two-pointers, prefix-sum, sorting, or index-as-hash — the array is just the surface you work on.
