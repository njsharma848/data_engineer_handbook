# 15 — Backtracking

## 1. When to Use
- You must **enumerate or search** all valid configurations: subsets, permutations, combinations, partitions.
- Keywords: *"all possible"*, *"generate every"*, *"n-queens"*, *"sudoku"*, *"maze path"*, *"word search"*, *"valid parentheses of length n"*.
- Small input bounds where an exponential search is acceptable (`n <= 20` for subsets, `n <= 10` for permutations).
- The solution space is a **tree of decisions** — at each node you pick one of a few branches; infeasible branches can be pruned early.
- You need to *return* the configurations, not just count them (counting-only problems sometimes have DP/combinatorial shortcuts).

## 2. Core Idea
Backtracking explores a decision tree depth-first: **choose** a step, **recurse**, then **undo** the step before trying the next. The undo is what makes it backtracking rather than plain DFS — it lets you reuse the same mutable state (a path, a board, a counter) instead of copying it at every node. Early pruning of infeasible partials is what keeps the exponential from being catastrophic.

## 3. Template
```python
def backtrack(path, choices, results):
    # 1) Base case — path represents a complete solution
    if is_goal(path):
        results.append(path.copy())   # copy: path will keep mutating
        return

    # 2) Branch — try each valid next move
    for choice in choices_at(path):
        if not is_valid(path, choice):
            continue                  # prune infeasible branch early
        path.append(choice)           # choose
        backtrack(path, choices, results)  # explore
        path.pop()                    # undo — the key line

# Subsets skeleton (each index in / out)
def subsets(nums):
    res, path = [], []
    def go(i):
        if i == len(nums):
            res.append(path.copy()); return
        go(i + 1)                    # skip
        path.append(nums[i])
        go(i + 1)                    # take
        path.pop()
    go(0)
    return res

# Permutations skeleton (used[] tracks chosen indices)
def permute(nums):
    n = len(nums); res, path = [], []
    used = [False] * n
    def go():
        if len(path) == n:
            res.append(path.copy()); return
        for i in range(n):
            if used[i]: continue
            used[i] = True; path.append(nums[i])
            go()
            path.pop(); used[i] = False
    go()
    return res
```
Key mental tools:
- `results.append(path.copy())` — never append the mutable `path` itself.
- **Every `append` has a matching `pop`.** If it does not, you are not backtracking; you are leaking state.
- **Prune early**: the best speedup is not a faster search, it is a smaller tree.
- For duplicate inputs, sort first and skip `nums[i] == nums[i-1]` when `i > 0` and the prior was not chosen.

## 4. Classic Problems
- **LC 78 — Subsets** (Medium): `2^n` include/exclude.
- **LC 46 — Permutations** (Medium): `n!` orderings.
- **LC 39 — Combination Sum** (Medium): unbounded choice with pruning.
- **LC 51 — N-Queens** (Hard): per-row placement with column/diagonal conflict pruning.
- **LC 79 — Word Search** (Medium): DFS on a grid with visited reset.

## 5. Worked Example — Subsets of `[1, 2, 3]` (LC 78)
Problem: return all subsets of `nums = [1, 2, 3]`.

Approach: at each index `i`, make two decisions — "skip" or "take" — then recurse.

```python
def subsets(nums):
    res, path = [], []
    def go(i):
        if i == len(nums):
            res.append(path.copy()); return
        go(i + 1)                    # skip nums[i]
        path.append(nums[i])
        go(i + 1)                    # take nums[i]
        path.pop()
    go(0)
    return res
```

Decision tree (`i` drilling down, path shown at each node):

```
                           go(0) path=[]
                    /                     \
              skip 1 (go(1) [])           take 1 (go(1) [1])
              /        \                    /          \
        skip 2 []   take 2 [2]        skip 2 [1]     take 2 [1,2]
        /   \        /    \            /    \          /       \
    skip3 take3  skip3  take3       skip3  take3    skip3    take3
     []   [3]    [2]    [2,3]       [1]   [1,3]    [1,2]   [1,2,3]
```

Record on every leaf (step-by-step):

| leaf visited (index path from root) | `path` at leaf | appended to `res` | `res` after |
|---|---|---|---|
| skip, skip, skip | `[]` | `[]` | `[[]]` |
| skip, skip, take | `[3]` | `[3]` | `[[],[3]]` |
| skip, take, skip | `[2]` | `[2]` | `[[],[3],[2]]` |
| skip, take, take | `[2,3]` | `[2,3]` | `[[],[3],[2],[2,3]]` |
| take, skip, skip | `[1]` | `[1]` | `[[],[3],[2],[2,3],[1]]` |
| take, skip, take | `[1,3]` | `[1,3]` | add `[1,3]` |
| take, take, skip | `[1,2]` | `[1,2]` | add `[1,2]` |
| take, take, take | `[1,2,3]` | `[1,2,3]` | add `[1,2,3]` |

Final `res` (8 subsets = `2^3`):
`[[], [3], [2], [2,3], [1], [1,3], [1,2], [1,2,3]]`.

Time `O(n * 2^n)` (each subset costs `O(n)` to copy). Space `O(n)` for recursion depth + `O(n * 2^n)` output. The path-copy on leaf plus the `append/pop` pair around the recursive call are the two idioms to memorise.

## 6. Common Variations
- **Combinations** (fix size `k`): stop recursing when `len(path) == k`.
- **Combination sum with reuse** (LC 39): recurse with same index when choosing to take; move forward when skipping.
- **Subsets / permutations with duplicates** (LC 90, LC 47): sort, skip `nums[i] == nums[i-1]` under the correct condition.
- **Grid backtracking** (LC 79 word search): mark-visited (e.g. replace cell with `#`) and restore on the way out.
- **Constraint-propagation pruning** (LC 37 sudoku, LC 51 n-queens): maintain column/row/box sets; add on enter, remove on leave.
- **Iterative deepening**: bound recursion depth; grow bound until a solution is found.
- **Edge cases**: empty input (one empty subset / one empty permutation), inputs with duplicates, goal reachable at root.

## 7. Related Patterns
- **Recursion** — backtracking is recursion + an *undo* step.
- **DFS** — structurally identical; DFS just omits the explicit undo when state is carried in parameters.
- **Dynamic Programming** — if the decision tree has *overlapping subproblems* (count / optimum, not enumerate), replace backtracking with DP.
- **Brute Force First** — backtracking is brute force with pruning; always note what prunes, otherwise you wrote a slow DFS.
- **Bitmask DP** — for `n <= 20`, a subset enumerated over `range(1 << n)` is often cleaner than recursive backtracking.

Distinguishing note: if the question asks for the **number of** configurations or a best-cost one, consider DP before backtracking. If the question asks to *list* configurations, backtracking is the natural tool.
