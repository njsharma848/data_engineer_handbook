# 25 — Dynamic Programming

## 1. When to Use
- The problem asks for a **count, an optimum (max/min), or feasibility**, not for the configurations themselves.
- Keywords: *"number of ways"*, *"minimum cost"*, *"maximum value"*, *"longest"*, *"edit distance"*, *"can you partition"*, *"knapsack"*, *"how many distinct"*.
- A naive recursion exists but has **overlapping subproblems** (the same call reappears many times).
- The problem has **optimal substructure**: the optimum for `(i, j)` is built from optima for smaller `(i', j')`.
- Greedy fails on a small counterexample — that is your signal to switch to DP.

## 2. Core Idea
Dynamic programming is **brute-force recursion + memoisation** (top-down) or the same recurrence filled **bottom-up** into a table. You turn exponential work into polynomial by computing each subproblem once and reusing it. The art is finding a state representation `(i, j, …)` that is (a) small enough to tabulate, and (b) rich enough to capture everything the transition depends on.

## 3. Template
```python
# 1) Top-down memo (start from recursion, cache results)
from functools import lru_cache

def solve(inputs):
    @lru_cache(maxsize=None)
    def go(state):
        if is_base(state):
            return base_answer(state)
        best = initial_best()
        for choice in choices(state):
            best = combine(best, go(next_state(state, choice)))
        return best
    return go(start_state(inputs))

# 2) Bottom-up — 1D DP (e.g. house robber, climbing stairs)
def rob(nums):
    prev2, prev1 = 0, 0
    for x in nums:
        prev2, prev1 = prev1, max(prev1, prev2 + x)
    return prev1

# 3) Bottom-up — 2D DP (e.g. edit distance, LCS)
def edit_distance(a, b):
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1): dp[i][0] = i
    for j in range(n + 1): dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if a[i-1] == b[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])
    return dp[m][n]

# 4) Knapsack (0/1)
def knapsack01(weights, values, W):
    n = len(weights)
    dp = [0] * (W + 1)
    for i in range(n):
        for w in range(W, weights[i] - 1, -1):      # reverse to avoid reuse
            dp[w] = max(dp[w], dp[w - weights[i]] + values[i])
    return dp[W]
```
Key mental tools (the five-step DP recipe):
1. **Define the state** — what arguments uniquely identify a subproblem?
2. **Base case** — what are the trivially known values?
3. **Transition** — how does the state relate to smaller states?
4. **Order of computation** — topological order over the subproblem DAG (usually nested loops).
5. **Answer** — which state(s) give the final result?

Memoisation vs tabulation: same complexity, different trade-offs. Top-down is easier to write when state transitions are sparse; bottom-up is easier to space-optimise (often to `O(n)` or `O(1)`).

## 4. Classic Problems
- **LC 70 — Climbing Stairs** (Easy): `dp[i] = dp[i-1] + dp[i-2]`.
- **LC 198 — House Robber** (Medium): `dp[i] = max(dp[i-1], dp[i-2] + a[i])`.
- **LC 322 — Coin Change** (Medium): unbounded knapsack for min coins.
- **LC 1143 — Longest Common Subsequence** (Medium): 2D DP over suffixes.
- **LC 72 — Edit Distance** (Hard): 2D DP with three transitions.

## 5. Worked Example — Coin Change (LC 322)
Problem: `coins = [1, 2, 5]`, `amount = 11`. Return the fewest coins summing to `amount`, or `-1` if impossible.

### Step 1. State
`dp[x]` = minimum coins to make exactly `x`. Want `dp[amount]`.

### Step 2. Base
`dp[0] = 0`. Unreachable amounts = sentinel `inf`.

### Step 3. Transition
For each amount `x`, try every coin `c`: `dp[x] = min(dp[x], dp[x - c] + 1)` for all `c <= x`.

### Step 4. Order
Iterate `x` from 1 upward so `dp[x - c]` is already final.

### Step 5. Answer
`dp[amount]` if finite, else `-1`.

```python
def coinChange(coins, amount):
    INF = float('inf')
    dp = [INF] * (amount + 1)
    dp[0] = 0
    for x in range(1, amount + 1):
        for c in coins:
            if c <= x:
                dp[x] = min(dp[x], dp[x - c] + 1)
    return dp[amount] if dp[amount] != INF else -1
```

Trace with `coins = [1, 2, 5]`, `amount = 11` (entries computed left-to-right):

| `x` | transitions considered | `dp[x]` |
|---|---|---|
| 0 | — | 0 |
| 1 | `dp[0]+1=1` | 1 |
| 2 | `dp[1]+1=2`, `dp[0]+1=1` (coin 2) | 1 |
| 3 | `dp[2]+1=2`, `dp[1]+1=2` | 2 |
| 4 | `dp[3]+1=3`, `dp[2]+1=2` | 2 |
| 5 | `dp[4]+1=3`, `dp[3]+1=3`, `dp[0]+1=1` (coin 5) | 1 |
| 6 | `dp[5]+1=2`, `dp[4]+1=3`, `dp[1]+1=2` | 2 |
| 7 | `dp[6]+1=3`, `dp[5]+1=2`, `dp[2]+1=2` | 2 |
| 8 | `dp[7]+1=3`, `dp[6]+1=3`, `dp[3]+1=3` | 3 |
| 9 | `dp[8]+1=4`, `dp[7]+1=3`, `dp[4]+1=3` | 3 |
| 10 | `dp[9]+1=4`, `dp[8]+1=4`, `dp[5]+1=2` | 2 |
| 11 | `dp[10]+1=3`, `dp[9]+1=4`, `dp[6]+1=3` | 3 |

Answer `dp[11] = 3` (e.g. `5 + 5 + 1` or `5 + 2 + 2 + 2 → 4 coins... not that`; `5 + 5 + 1 = 11` with 3 coins is optimal). Time `O(amount * len(coins))`, space `O(amount)`.

Contrast with a **greedy** "largest-first" approach: for `coins = [1, 3, 4]`, `amount = 6`, greedy takes `4 + 1 + 1 = 3 coins`, but optimum is `3 + 3 = 2 coins`. Greedy fails → DP is required.

## 6. Common Variations
- **1D DP**: climbing stairs, house robber, maximum subarray (Kadane), LIS.
- **2D DP**: LCS, edit distance, distinct subsequences, grid paths.
- **Knapsack family**:
  - 0/1 knapsack — each item once; iterate capacity *backwards*.
  - Unbounded knapsack — unlimited copies; iterate capacity *forwards* (coin change).
  - Bounded knapsack — at most `k[i]` copies; binary-expand counts.
- **Interval DP** (matrix-chain, burst balloons, palindrome partitioning): `dp[i][j]` over a length-increasing loop.
- **Tree DP**: post-order DFS returning a tuple of "taken / not taken" per node (LC 337 robber on tree).
- **Bitmask DP** (`n <= 20`): `dp[mask]` where bits encode which items are chosen (LC 847, TSP).
- **Digit DP**: count numbers ≤ `N` with some property, indexed by `(pos, tight, leading_zero, state)`.
- **DP on subsequences vs substrings**: LCS vs longest common substring — different transitions.
- **Space optimisation**: if `dp[i]` depends only on `dp[i-1]` (and `dp[i-2]`), collapse to `O(1)` variables.
- **Edge cases**: impossible target (return sentinel), empty input, overflow for counts (use modulo per problem statement), negative weights (DP may not apply — use shortest-path).

## 7. Related Patterns
- **Recursion** — DP is recursion + memoisation; always derive the recurrence first, then add the cache.
- **Backtracking** — when you must **enumerate** configurations rather than count/optimise. DP replaces backtracking when the subproblems overlap.
- **Greedy** — strictly stronger than DP for the problems where it applies; use it when you can prove the greedy-choice property.
- **Divide and Conquer** — disjoint subproblems, no overlap → no DP needed.
- **Graph Algorithms on DAGs** — shortest / longest path on a DAG is "DP in topological order".
- **Hashing** — the map behind memoisation; for tabulation, an array suffices.
- **Binary Search + DP** — LIS `O(n log n)`, split-array-largest-sum, capacity feasibility.

Distinguishing note: the single best filter is *"does the same subproblem reappear?"* If yes, DP. If each subproblem is unique, D&C. If the local-best always wins, greedy. If you must list configurations, backtracking. DP is the workhorse when those fail — which is often.
