# 15 — Backtracking

## 1. When to Use
- You must **enumerate or search** all valid configurations: subsets, permutations, combinations, partitions, colourings, placements.
- Keywords: *"all possible"*, *"generate every"*, *"count the number of ways"* (when the search space has pruning structure), *"n-queens"*, *"sudoku"*, *"maze path"*, *"word search"*, *"valid parentheses of length n"*, *"IP addresses"*, *"palindrome partitioning"*, *"expression operators"*.
- Small input bounds where an exponential search is acceptable: `n <= 20` for subsets (`2^n`), `n <= 10` for permutations (`n!`), `n <= 9` for sudoku / n-queens.
- The solution space is a **tree of decisions** — at each node you pick one of a few branches; infeasible branches can be pruned early.
- You need to *return* the configurations, not just count them (counting-only problems sometimes have DP / combinatorial shortcuts that are faster).
- You need **constraint satisfaction** with **early failure**: the moment a partial assignment violates a constraint, you prune the entire subtree.
- The problem has **no obvious greedy choice** and **no overlapping subproblems** (otherwise DP is better).

### Signals that it is NOT a backtracking problem
- You only need the **count** and the choices are independent → combinatorics (`C(n, k)`, `n!`, product rule).
- You only need the **best** cost and the subproblems overlap → DP.
- The answer is a single yes/no and one feasible witness suffices → greedy or BFS.
- The search space is too large for exponential enumeration (`n > 30`) and you have no pruning → wrong pattern; look for DP / greedy / mathematical structure.

## 2. Core Idea
Backtracking explores a decision tree depth-first: **choose** a step, **recurse**, then **undo** the step before trying the next branch. The undo is what makes it backtracking rather than plain DFS — it lets you reuse the same mutable state (a path, a board, a counter) instead of copying it at every node, which is what keeps the space complexity `O(depth)` instead of `O(tree_size)`.

Three elements define a backtracking algorithm:
1. **State**: a representation of the current partial solution (a `path` list, a board, chosen-indices mask, remaining budget).
2. **Decision**: at each recursion, what are the possible next moves? (add or skip, place a piece, assign a value).
3. **Constraint check / pruning**: before recursing, can this partial extend to a valid complete solution? If not, skip.

The classic shape:
```
def backtrack(state):
    if is_goal(state):
        record(state); return
    for choice in choices(state):
        if not valid(state, choice): continue     # prune
        apply(state, choice)                      # choose
        backtrack(state)                          # explore
        undo(state, choice)                       # un-choose — the key line
```

**Early pruning is the main performance knob.** Naive enumeration is exponential; effective pruning cuts down what's actually explored by orders of magnitude. For n-queens, no pruning means `64! / 56!` board placements — with pruning, only ~40 million for n=12. For sudoku, pruning makes a 10^20-cell search finish instantly.

### Backtracking shapes
- **Include / exclude** at each index: subsets, combinations (`2^n` leaves).
- **Permute**: pick next among unused (`n!` leaves).
- **Place on a grid**: n-queens, sudoku, word search.
- **Parse / partition**: split a string at each valid boundary, recurse on the remainder.
- **Expression enumeration**: at each character, try each operator.
- **Assign values**: graph colouring, constraint propagation.

## 3. Template

### Template A — canonical backtracking (record all solutions)
```python
def backtrack(path, choices, results):
    if is_goal(path):
        results.append(path.copy())            # copy — path keeps mutating
        return
    for choice in choices_at(path):
        if not is_valid(path, choice):         # prune infeasible early
            continue
        path.append(choice)                    # choose
        backtrack(path, choices, results)      # explore
        path.pop()                             # undo
```

### Template B — subsets (include / exclude each index)
```python
def subsets(nums):
    res, path = [], []
    def go(i):
        if i == len(nums):
            res.append(path.copy()); return
        go(i + 1)                              # exclude nums[i]
        path.append(nums[i])
        go(i + 1)                              # include nums[i]
        path.pop()
    go(0)
    return res
```

Alternative "for each start index" shape for LC 78:
```python
def subsets_alt(nums):
    res = []
    def go(start, path):
        res.append(path.copy())                # every node is a valid subset
        for i in range(start, len(nums)):
            path.append(nums[i])
            go(i + 1, path)
            path.pop()
    go(0, [])
    return res
```

### Template C — permutations (used[] tracks chosen indices)
```python
def permute(nums):
    n = len(nums)
    res, path = [], []
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

### Template D — combinations (fix size k, start index to avoid duplicates)
```python
def combinations(n, k):
    res, path = [], []
    def go(start):
        if len(path) == k:
            res.append(path.copy()); return
        for i in range(start, n + 1):
            # Pruning: not enough elements left
            if n - i + 1 < k - len(path): break
            path.append(i)
            go(i + 1)
            path.pop()
    go(1)
    return res
```

### Template E — combination sum with reuse (LC 39)
```python
def combination_sum(candidates, target):
    candidates.sort()
    res, path = [], []
    def go(start, remain):
        if remain == 0:
            res.append(path.copy()); return
        for i in range(start, len(candidates)):
            if candidates[i] > remain: break    # pruning: sorted → all further too large
            path.append(candidates[i])
            go(i, remain - candidates[i])       # reuse: pass `i`, not `i+1`
            path.pop()
    go(0, target)
    return res
```

### Template F — duplicates (sort + skip same-value siblings)
```python
def subsets_with_dup(nums):
    nums.sort()
    res, path = [], []
    def go(start):
        res.append(path.copy())
        for i in range(start, len(nums)):
            if i > start and nums[i] == nums[i - 1]:    # skip duplicate at same level
                continue
            path.append(nums[i])
            go(i + 1)
            path.pop()
    go(0)
    return res
```

The condition `i > start` (not `i > 0`) is subtle: duplicates are skipped **between siblings** (same `start`), not **along the path** (different `start`). This is the lesson that 80% of duplicate-handling bugs boil down to.

### Template G — board / grid placement (n-queens)
```python
def solve_n_queens(n):
    cols, diag1, diag2 = set(), set(), set()
    board, res = [], []
    def go(r):
        if r == n:
            res.append(["." * c + "Q" + "." * (n - c - 1) for c in board])
            return
        for c in range(n):
            if c in cols or (r - c) in diag1 or (r + c) in diag2:
                continue                        # prune: conflict
            board.append(c); cols.add(c); diag1.add(r - c); diag2.add(r + c)
            go(r + 1)
            board.pop(); cols.remove(c); diag1.remove(r - c); diag2.remove(r + c)
    go(0)
    return res
```

### Template H — grid DFS with mark-and-restore (LC 79 word search)
```python
def exist(board, word):
    m, n = len(board), len(board[0])
    def dfs(r, c, k):
        if k == len(word): return True
        if not (0 <= r < m and 0 <= c < n) or board[r][c] != word[k]:
            return False
        ch, board[r][c] = board[r][c], '#'      # mark visited
        found = (dfs(r+1,c,k+1) or dfs(r-1,c,k+1)
              or dfs(r,c+1,k+1) or dfs(r,c-1,k+1))
        board[r][c] = ch                        # restore
        return found
    return any(dfs(r, c, 0) for r in range(m) for c in range(n))
```

Key mental tools:
- `results.append(path.copy())` — **never** append the mutable `path` itself; all entries in `results` would alias the same list.
- **Every `append` has a matching `pop`; every `add` has a matching `remove`.** If it does not, you are not backtracking — you are leaking state.
- **Prune early**: the best speedup is not a faster inner loop, it is a smaller tree. Add constraint checks *before* recursion.
- For **duplicate inputs**, sort first and skip `nums[i] == nums[i-1]` **when `i > start`** (sibling level), not when `i > 0` (path level).
- **Choose the state representation carefully**: sets for O(1) conflict lookup, bitmasks for small alphabets, lists for ordered paths.
- **Return early** on first success when the problem asks for *any* solution (Template H). Don't enumerate exhaustively if you don't need to.
- **Complexity is often exponential** — that's fine for small inputs. Estimate the tree size: branching factor `b`, depth `d` → `O(b^d)` without pruning.

## 4. Classic Problems
- **LC 78 — Subsets** (Medium): `2^n` include/exclude (Template B).
- **LC 90 — Subsets II** (Medium): same with duplicate skipping (Template F).
- **LC 46 — Permutations** (Medium): `n!` orderings (Template C).
- **LC 47 — Permutations II** (Medium): permutations with duplicates — sort + `used[]` + skip.
- **LC 39 — Combination Sum** (Medium): unbounded choice with pruning (Template E).
- **LC 40 — Combination Sum II** (Medium): each candidate once; sort + skip.
- **LC 77 — Combinations** (Medium): fix size `k` (Template D).
- **LC 51 — N-Queens** (Hard): per-row placement with column/diagonal conflict pruning (Template G).
- **LC 37 — Sudoku Solver** (Hard): constraint propagation on rows/cols/boxes.
- **LC 79 — Word Search** (Medium): DFS on a grid with visited reset (Template H).
- **LC 22 — Generate Parentheses** (Medium): implicit state = counts of `(` and `)` placed so far.
- **LC 131 — Palindrome Partitioning** (Medium): split string at each palindromic prefix, recurse on suffix.

## 5. Worked Example — N-Queens (LC 51)
Problem: place `n` queens on an `n × n` board so no two attack each other. Return all distinct solutions. Here `n = 4`.

Why this is a great worked example: it combines a **placement template**, **three separate constraint sets** (column, two diagonal families), and **pruning** that is both necessary and intuitive.

### Step 1. Structure the search
One queen per row (otherwise clearly invalid — two in a row would attack). So we recurse row-by-row; at row `r`, try each column `c ∈ [0, n)`. A placement at `(r, c)` conflicts with a prior queen at `(r', c')` if:
- **Same column**: `c' == c`.
- **Same ↘ diagonal**: `r' - c' == r - c` (top-left to bottom-right).
- **Same ↙ diagonal**: `r' + c' == r + c` (top-right to bottom-left).

Maintain three sets for `O(1)` conflict lookup:
- `cols` — columns already used.
- `diag1` — `r - c` values already used (↘ diagonals).
- `diag2` — `r + c` values already used (↙ diagonals).

### Step 2. Implement
```python
def solveNQueens(n):
    cols, diag1, diag2 = set(), set(), set()
    board = []                                  # board[r] = column chosen in row r
    res = []
    def go(r):
        if r == n:
            res.append(["." * c + "Q" + "." * (n - c - 1) for c in board])
            return
        for c in range(n):
            if c in cols or (r - c) in diag1 or (r + c) in diag2:
                continue
            board.append(c)
            cols.add(c); diag1.add(r - c); diag2.add(r + c)
            go(r + 1)
            board.pop()
            cols.remove(c); diag1.remove(r - c); diag2.remove(r + c)
    go(0)
    return res
```

### Step 3. Trace for `n = 4`
I'll trace the DFS, showing `board` as a list of column choices per row. Pruned branches (conflicts) marked with `✗`. Success marked with `✓`.

Start: `go(0)`. `cols, diag1, diag2 = ∅`.

- Try `r=0, c=0`: place. `board=[0]`, `cols={0}`, `diag1={0}`, `diag2={0}`. Recurse `go(1)`.
  - Try `r=1, c=0`: `c ∈ cols` ✗.
  - Try `r=1, c=1`: `1-1=0 ∈ diag1` ✗.
  - Try `r=1, c=2`: place. `board=[0,2]`, `cols={0,2}`, `diag1={0,-1}`, `diag2={0,3}`. Recurse `go(2)`.
    - Try `c=0`: `cols` ✗. `c=1`: `1+2=3 ∈ diag2` ✗. `c=2`: `cols` ✗. `c=3`: `r-c = -1 ∈ diag1` ✗.
    - All pruned → return. Undo: `board=[0]`, `cols={0}`, `diag1={0}`, `diag2={0}`.
  - Try `r=1, c=3`: place. `board=[0,3]`, `cols={0,3}`, `diag1={0,-2}`, `diag2={0,4}`. Recurse `go(2)`.
    - Try `c=1`: place. `board=[0,3,1]`, `cols={0,3,1}`, `diag1={0,-2,1}`, `diag2={0,4,3}`. Recurse `go(3)`.
      - Try `c=0,1,2,3` — all conflict (work it out: `c=2` → `3-2=1 ∈ diag1` ✗; `c=3` → `cols` ✗; etc.). Prune, undo.
    - Undo; try `c=2,3` — all prune. Undo.
  - No solutions rooted at `c=0`. Undo to top.

- Try `r=0, c=1`: place. `board=[1]`, `cols={1}`, `diag1={-1}`, `diag2={1}`. Recurse `go(1)`.
  - Try `c=3`: place. `board=[1,3]`, `cols={1,3}`, `diag1={-1,-2}`, `diag2={1,4}`. Recurse `go(2)`.
    - Try `c=0`: place. `board=[1,3,0]`, `cols={1,3,0}`, `diag1={-1,-2,2}`, `diag2={1,4,2}`. Recurse `go(3)`.
      - Try `c=2`: `cols` ok; `3-2=1 ∉ diag1`; `3+2=5 ∉ diag2` → place! `go(4)`: `r == n = 4` → record solution. `board=[1,3,0,2]`.
      - `✓` solution: `[".Q..", "...Q", "Q...", "..Q."]`.
      - Undo: pop row 3; try `c=3`: `cols` ✗. Return.
    - Undo row 2; try `c=2`: `1+2=3` — let's check: `diag2 = {1,4}`, ok; `r-c = 0` ok; cols ok. Place. `board=[1,3,2]`. Recurse `go(3)`.
      - Try `c=0,1,2,3`: `0+3=3 ∈ diag2={1,4,5}`? 5 in `diag2`. `c=0` → `r+c=3`, `diag2={1,4,5}` — 3 not there. cols ok. diag1 r-c=3, not in `{-1,-2,1}`. Place `go(4)`? Wait — actually `r=3, c=0`: check conflicts. `board=[1,3,2]` means rows 0,1,2 have queens at cols 1,3,2. New queen at (3,0). Col 0 free; ↘ diag: 0-1=-1, 1-3=-2, 2-2=0, 3-0=3 — no match; ↙ diag: 0+1=1, 1+3=4, 2+2=4 collision between rows 1 and 2! That means my state claim was wrong — let me restate:
      
      Actually `r=2, c=2`: `r+c = 4 ∈ diag2 = {1, 4}` (from row 1, c=3: 1+3=4). **Conflict** ✗. Prune.
    - Continue undo; no more options at row 2. Undo row 1.
  - No more `c` options at row 1 after 3. Undo to top.

- Try `r=0, c=2`: place. `board=[2]`. By symmetry with `c=1`, we find one solution: `board=[2,0,3,1]` → `["..Q.", "Q...", "...Q", ".Q.."]`. `✓`.

- Try `r=0, c=3`: by symmetry with `c=0`, no solutions.

**Final result**: two solutions:
```
.Q..        ..Q.
...Q        Q...
Q...        ...Q
..Q.        .Q..
```

### Step 4. Pruning pays
Without pruning, we'd explore `4^4 = 256` placements. With pruning, we explore ~17 recursive calls. For `n = 12`, the gap is `12^12 ≈ 10^13` vs ~1 million — the difference between "hangs" and "sub-second".

### Step 5. Complexity
Time: `O(n!)` in the worst case (unpruned), but effective pruning makes it far better. Space: `O(n)` for the recursion depth and the three sets.

## 6. Common Variations

### Subsets / powerset
- **Subsets** (LC 78): Template B or alt.
- **Subsets II** (LC 90): with duplicates — Template F.
- **Letter case permutations** (LC 784): like subsets but on letters.
- **Iterator of all subsets** (Gayle McDowell variant): yield instead of append.

### Permutations
- **Permutations** (LC 46): Template C.
- **Permutations II** (LC 47): with duplicates — sort + skip when `i > 0 and nums[i] == nums[i-1] and not used[i-1]`.
- **Next permutation** (LC 31): not backtracking — single-pass array manipulation.
- **Beautiful arrangement** (LC 526): place values 1..n respecting divisibility.
- **Letter combinations of a phone number** (LC 17): k-ary tree of depth `len(digits)`.

### Combinations / sums
- **Combinations** (LC 77): Template D.
- **Combination sum** (LC 39): unbounded reuse — Template E.
- **Combination sum II** (LC 40): each candidate once + duplicates.
- **Combination sum III** (LC 216): fixed-size k, sum target.
- **Target sum** (LC 494): `+` or `−` each number; DP is often better.
- **Partition equal subset sum** (LC 416): DP is the canonical solution; backtracking works for small n.

### Grid / board placement
- **N-Queens** (LC 51): Template G.
- **N-Queens II** (LC 52): count solutions only.
- **Sudoku solver** (LC 37): constraint propagation per cell; fill the most-constrained cell first (minimum remaining values heuristic) for big speedups.
- **Word search** (LC 79): Template H.
- **Word search II** (LC 212): multiple words → Trie + DFS.
- **Robot room cleaner** (LC 489): DFS with backtracking moves.
- **Unique paths III** (LC 980): DFS with "visit every empty cell exactly once".

### String partitioning / parsing
- **Palindrome partitioning** (LC 131): split at each palindromic prefix; often precompute `is_palindrome[i][j]` DP table.
- **Restore IP addresses** (LC 93): four 0–255 segments, no leading zeros.
- **Expression add operators** (LC 282): insert `+`, `−`, `*` to reach target; careful with `*` precedence and leading zeros.
- **Word break II** (LC 140): enumerate all sentences; memoise by suffix.
- **Generate parentheses** (LC 22): implicit state `(open_used, close_used)`.

### Constraint satisfaction
- **Graph colouring** (LC 1042 gardens no adjacent): greedy works due to small degree; general problem is NP-hard.
- **Map colouring / four-colour**: classic CSP.
- **Cryptarithmetic** (SEND + MORE = MONEY): assign digits to letters.
- **Knight's tour**: classic backtracking with Warnsdorff's heuristic for pruning.

### Edge cases & pitfalls
- **Appending `path` instead of `path.copy()`** — all results alias; you get `[[], [], ...]` at the end.
- **Forgetting the undo** — state leaks across branches; you explore nonsense partials.
- **Wrong duplicate-skip condition** — `i > start` (level) vs `i > 0` (path). Mix them up and you either miss solutions or emit duplicates.
- **Not pruning when you could** — backtracking without pruning is brute force; make sure you rejected infeasible partials as early as possible.
- **Using a list as a set** — O(n) `in` membership tests kill performance. Use sets.
- **Mutating input** (Template H marks the board) **and forgetting to restore** — the next DFS branch sees corrupted state.
- **Returning the first solution vs all solutions** — decide up front; the code differs by early return vs full exploration.
- **Recursion depth limit** — for `n = 20` subsets is fine; for deeper search trees, bump `sys.setrecursionlimit`.
- **Counting vs enumerating** — if you only need the count, consider DP or combinatorics; enumerating is wasted work.

## 7. Related Patterns
- **Recursion** — backtracking is recursion + an **undo** step.
- **DFS** — structurally identical; DFS just omits the explicit undo when state is carried in parameters or implicit.
- **Dynamic Programming** — if the decision tree has *overlapping subproblems* (count / optimum, not enumerate), replace backtracking with DP. LC 416, LC 494, LC 139 are all "backtrackable" but better solved with DP.
- **Brute Force First** — backtracking is brute force with pruning; always note what prunes, otherwise you wrote a slow DFS.
- **Bitmask DP** — for `n <= 20`, iterating over `range(1 << n)` or memoising on `(i, mask)` is often cleaner than recursive backtracking (LC 526, LC 698).
- **Greedy** — if a locally optimal choice can be proven globally optimal, greedy beats backtracking (LC 45 jump game II, LC 134 gas station). Try greedy first, fall back to backtracking when you can't.
- **Constraint propagation** — for hard CSPs (sudoku, n-queens) adding forward-checking and MRV heuristics can cut the tree by orders of magnitude.

**Distinguishing note**: if the question asks for the **number of** configurations or a **best-cost** one, consider DP before backtracking. If the question asks to *list* configurations, backtracking is the natural tool. If the question asks to check feasibility, prefer BFS or greedy when available — backtracking is the last resort, not the first. The classic tell is a small input bound (`n <= 20`) combined with "return all" — that's backtracking's home turf.
