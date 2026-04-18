# 24 — Greedy

## 1. When to Use
- The problem asks for an **optimum** (max / min / minimum number of…) and a natural "best next choice" is obvious.
- Keywords: *"minimum number of"*, *"maximum you can"*, *"assign to fit"*, *"intervals / meetings"*, *"schedule"*, *"coins"*, *"activity selection"*, *"jump game"*.
- There is an **exchange argument**: swapping any "better" pair into the greedy solution cannot improve the total.
- Inputs are cheaply sortable by the greedy key (deadline, end time, weight-to-ratio).
- DP would work but its state is overkill — the problem has an **optimal substructure + greedy-choice property**.

## 2. Core Idea
At each step, pick the choice that is locally best according to a fixed rule, commit to it, never revisit. Greedy works when that local choice is guaranteed to be part of some global optimum — usually proved via exchange (swap) or by an inductive invariant. When greedy fails, it is almost always because the local "best" forecloses a strictly better combination, and you need DP or backtracking instead.

## 3. Template
```python
# 1) Sort by greedy key, sweep
def solve(items, key_fn, take):
    items.sort(key=key_fn)
    state = initial_state()
    chosen = []
    for item in items:
        if take(state, item):
            chosen.append(item)
            state = update(state, item)
    return chosen

# 2) Activity selection — earliest-finishing
def max_non_overlapping(intervals):
    intervals.sort(key=lambda x: x[1])   # sort by end
    count, last_end = 0, float('-inf')
    for start, end in intervals:
        if start >= last_end:
            count += 1
            last_end = end
    return count

# 3) Coin change (canonical coins only!) — largest first
def min_coins_canonical(coins, amount):
    coins.sort(reverse=True)
    used = 0
    for c in coins:
        if amount == 0: break
        used += amount // c
        amount %= c
    return used if amount == 0 else -1
```
Key mental tools:
- **Always justify greediness**: write the exchange argument before trusting the algorithm.
- Pick the sort key deliberately: "earliest deadline", "earliest finish", "largest ratio", "most constrained first".
- If greedy looks wrong on a small counterexample, it *is* wrong — switch to DP.
- Combine greedy with a data structure: heap for "best remaining", interval tree for conflicts.

## 4. Classic Problems
- **LC 55 — Jump Game** (Medium): track furthest reachable index.
- **LC 45 — Jump Game II** (Medium): BFS-flavoured greedy expansion of reach.
- **LC 435 — Non-Overlapping Intervals** (Medium): sort by end, keep earliest finishing.
- **LC 253 — Meeting Rooms II** (Medium): greedy + heap of current end times.
- **LC 621 — Task Scheduler** (Medium): most-frequent task first, with cooldown.

## 5. Worked Example — Non-Overlapping Intervals (LC 435)
Problem: given intervals, return the **minimum** number to remove so the rest do not overlap.

Input: `intervals = [[1,2], [2,3], [3,4], [1,3]]`.

Claim: the optimum keeps the **earliest-finishing** compatible intervals. Intuition: finishing earlier leaves the most room for later intervals. Exchange argument: if an optimal solution picks some interval `x` at the same position where the earliest-finishing interval `e` would fit, swapping `x` for `e` does not reduce the count and may improve feasibility downstream.

```python
def eraseOverlapIntervals(intervals):
    intervals.sort(key=lambda iv: iv[1])         # sort by end
    kept, last_end = 0, float('-inf')
    for s, e in intervals:
        if s >= last_end:
            kept += 1
            last_end = e
    return len(intervals) - kept
```

Sort by end: `[[1,2], [2,3], [1,3], [3,4]]` (note `[1,3]` ends at 3, same as `[2,3]` — stable order keeps original).

Trace:

| interval | `s` | `e` | `last_end` before | `s >= last_end`? | `kept` after | `last_end` after |
|---|---|---|---|---|---|---|
| `[1,2]` | 1 | 2 | −∞ | yes | 1 | 2 |
| `[2,3]` | 2 | 3 | 2 | yes | 2 | 3 |
| `[1,3]` | 1 | 3 | 3 | `1 >= 3`? no | 2 | 3 |
| `[3,4]` | 3 | 4 | 3 | yes | 3 | 4 |

`kept = 3`, total = 4, return `4 - 3 = 1`. Remove `[1,3]`. Time `O(n log n)` for the sort, `O(n)` for the sweep. A DP approach exists (`O(n^2)`) but is strictly worse here because the greedy-choice property holds.

## 6. Common Variations
- **Interval scheduling maximisation** = "non-overlapping intervals" with `kept` as the answer.
- **Meeting rooms II** (LC 253): sort starts + min-heap of end times.
- **Jump game** (LC 55): `reach = max(reach, i + nums[i])`; fail if `i > reach`.
- **Gas station** (LC 134): if total gas ≥ total cost, start greedy; reset when deficit.
- **Minimum platforms / arrows to burst balloons** (LC 452): sort by end, shoot at the earliest end; skip balloons already hit.
- **Task Scheduler** (LC 621): max-heap of counts + cooldown queue; elegant variant.
- **Edge cases**: ties (pick deterministically), single interval, all overlap, disjoint inputs.

## 7. Related Patterns
- **Sorting** — greedy algorithms nearly always sort by the greedy key first.
- **Heap / Priority Queue** — when "best remaining" changes each step.
- **Dynamic Programming** — fallback when greedy fails; DP with a `dp[i]` over sorted items is a common pattern.
- **Intervals** — many greedy problems are interval problems in disguise.
- **Two Pointers** — same family (sort + sweep), but two pointers converges; greedy accumulates.

Distinguishing note: whenever you propose a greedy, try to **break it** with a small counterexample. If you cannot, also try to prove correctness with an exchange argument — if both fail, step back to DP or backtracking. A greedy that "feels right" with no proof is how wrong submissions get accepted on easy tests and fail hidden ones.
