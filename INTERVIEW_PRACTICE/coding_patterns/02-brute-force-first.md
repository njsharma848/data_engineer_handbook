# 02 — Brute Force First

## 1. When to Use
- You are **stuck** and do not yet see the trick; any correct answer beats a blank screen.
- The prompt says *"find all…"*, *"count all…"*, *"check every pair"* — the naive definition **is** a valid solution.
- Small constraints (`n <= 20`, `n <= 100`) where the obvious `O(n^2)` / `O(2^n)` already fits.
- You want a **reference oracle** to test a faster solution against.
- Interviewer says *"walk me through your thinking"* — brute force is the natural first step before optimising.

Signal words: *"simplest"*, *"straightforward"*, *"naive"*, *"try all"*, *"enumerate"*.

## 2. Core Idea
The brute force mirrors the problem statement literally: enumerate the search space, check each candidate, return the best. It is almost always correct (just slow), and writing it forces you to nail down inputs, outputs, and edge cases — which is exactly the state you need to be in to spot a speed-up.

## 3. Template
```python
def brute_force(inputs):
    best = None  # or float('inf'), or []
    # 1. Enumerate every candidate the problem mentions
    for candidate in generate_candidates(inputs):
        # 2. Check feasibility / scoring directly from the definition
        if is_valid(candidate, inputs):
            score = evaluate(candidate, inputs)
            # 3. Track the best answer under the problem's comparator
            if best is None or score < best_score:
                best, best_score = candidate, score
    return best

# Common enumeration shapes:
#   - all pairs:         for i in range(n): for j in range(i+1, n): ...
#   - all subarrays:     for i in range(n): for j in range(i, n): ...
#   - all subsets:       for mask in range(1 << n): ...
#   - all permutations:  itertools.permutations(arr)
```

## 4. Classic Problems
- **LC 1 — Two Sum** (Easy): check every pair.
- **LC 53 — Maximum Subarray** (Medium): score every subarray, then optimise to Kadane.
- **LC 78 — Subsets** (Medium): enumerate all `2^n` subsets.
- **LC 46 — Permutations** (Medium): enumerate all `n!` orderings via backtracking.
- **LC 39 — Combination Sum** (Medium): try every combination; then prune.

## 5. Worked Example — Maximum Subarray, brute first
Problem: given `nums = [-2, 1, -3, 4, -1, 2, 1, -5, 4]`, find the contiguous subarray with the largest sum.

### Step 1. Literal brute force, `O(n^3)`
```python
best = float('-inf')
for i in range(n):
    for j in range(i, n):
        s = sum(nums[i:j+1])   # recompute every time
        best = max(best, s)
```
Trace (first few `(i, j)` windows):

| `i` | `j` | `nums[i..j]` | `s` | `best` |
|---|---|---|---|---|
| 0 | 0 | `[-2]` | -2 | -2 |
| 0 | 1 | `[-2, 1]` | -1 | -1 |
| 0 | 2 | `[-2, 1, -3]` | -4 | -1 |
| 3 | 3 | `[4]` | 4 | 4 |
| 3 | 4 | `[4, -1]` | 3 | 4 |
| 3 | 5 | `[4, -1, 2]` | 5 | 5 |
| 3 | 6 | `[4, -1, 2, 1]` | 6 | 6 |

Answer `= 6`, subarray `[4, -1, 2, 1]`. Correct, but `O(n^3)`.

### Step 2. Spot the repeated work
`sum(nums[i:j+1])` recomputes from scratch. If we already know `sum(nums[i:j])`, we can extend by one step. Replace the inner `sum` with a running total:

```python
for i in range(n):
    s = 0
    for j in range(i, n):
        s += nums[j]
        best = max(best, s)
```
Now `O(n^2)`.

### Step 3. Spot the deeper redundancy
Every outer `i` restarts the running sum. But if `s` ever goes negative, starting fresh at `j+1` can only do better. That observation — reached only **because the brute force made the repeated work obvious** — is Kadane's algorithm:

```python
best = cur = nums[0]
for x in nums[1:]:
    cur = max(x, cur + x)
    best = max(best, cur)
```
`O(n)`, answer still `6`. The brute force was the ladder to the insight, not a throwaway.

## 6. Common Variations
- **Generate-and-test with pruning**: start brute, then add `if is_infeasible(partial): continue` — smooths into backtracking.
- **Bitmask brute force**: for `n <= 20`, iterate `mask in range(1 << n)` to enumerate subsets in `O(2^n * n)`.
- **Brute force as oracle**: keep it in your test harness and `assert fast(x) == brute(x)` on random inputs.
- **Randomised brute force**: when the search space is huge, sampling a few candidates can surface structure.
- **Meet-in-the-middle**: brute force both halves separately to turn `O(2^n)` into `O(2^(n/2))`.

## 7. Related Patterns
- **Backtracking** — brute force + pruning on a decision tree.
- **Dynamic Programming** — brute force recursion + memoisation on overlapping subproblems.
- **Sliding Window / Two Pointers** — almost always the `O(n)` upgrade of an `O(n^2)` subarray brute force.
- **Hashing** — the typical way to kill the inner loop in an `O(n^2)` pair-search brute force.
- **Greedy** — when brute force reveals that the locally-best choice is always part of the global optimum.

Distinguishing note: brute force is not a destination, it is a **launchpad**. Its job is to (a) give you a correct answer, (b) expose the redundant work, and (c) anchor test cases for the optimised version.
