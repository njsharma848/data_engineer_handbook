# 14 — Recursion

## 1. When to Use
- Problem has a natural **self-similar structure**: trees, graphs, grids, nested lists, divide-and-conquer.
- Keywords: *"for each subtree"*, *"divide"*, *"conquer"*, *"all possible"*, *"combine the results of…"*.
- A problem can be expressed as a function of itself on a **strictly smaller input**.
- You can describe the answer as *"answer(root) = combine(answer(left), answer(right))"*.
- Backtracking / DFS / tree traversal are needed.

## 2. Core Idea
Recursion solves a problem by **reducing it to smaller instances of itself**, then combining the subproblem results. You need two ingredients: a **base case** that does no further recursion, and a **recursive step** whose input is provably smaller so the call tree terminates. Each recursive call is a frame on the call stack — that stack *is* your data structure.

## 3. Template
```python
# Canonical shape
def solve(instance):
    # 1) Base case — handle the smallest / trivial input
    if is_base(instance):
        return base_answer(instance)

    # 2) Reduce — split into strictly smaller subproblems
    parts = split(instance)

    # 3) Recurse — solve each subproblem
    sub_answers = [solve(p) for p in parts]

    # 4) Combine — merge subproblem results into the answer for `instance`
    return combine(instance, sub_answers)

# Tree example — maximum depth
def max_depth(node):
    if node is None:
        return 0
    return 1 + max(max_depth(node.left), max_depth(node.right))

# Divide and conquer — merge sort
def merge_sort(arr):
    if len(arr) <= 1:
        return arr[:]
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left, right)
```
Key mental tools:
- Write the **base case first** — it is the contract that makes the rest correct.
- State the function's **meaning**: *"given X, return Y"*. Stick to that meaning at every call.
- Assume the recursive call works on the smaller input — do not trace nested frames in your head.
- Python's default recursion limit is 1000; use `sys.setrecursionlimit` or convert to iterative for deep inputs.

## 4. Classic Problems
- **LC 104 — Maximum Depth of Binary Tree** (Easy): textbook tree recursion.
- **LC 21 — Merge Two Sorted Lists** (Easy): recursive merge.
- **LC 50 — Pow(x, n)** (Medium): halve the exponent — `O(log n)`.
- **LC 509 — Fibonacci** (Easy): naive recursion is exponential, memoisation makes it linear.
- **LC 394 — Decode String** (Medium): recursive parser over nested brackets.

## 5. Worked Example — Fibonacci with Memoisation (LC 509)
Problem: compute `F(n)` where `F(0) = 0`, `F(1) = 1`, `F(n) = F(n-1) + F(n-2)`.

### Naive recursion
```python
def fib(n):
    if n < 2: return n
    return fib(n - 1) + fib(n - 2)
```
Call tree for `n = 5`:

```
                fib(5)
             /         \
         fib(4)         fib(3)
         /    \         /    \
     fib(3)  fib(2)  fib(2)  fib(1)
     /   \   /   \   /   \
  fib(2) fib(1) fib(1) fib(0) fib(1) fib(0)
  /  \
fib(1) fib(0)
```
15 calls. Exponential `O(2^n)` — the same subproblems are recomputed repeatedly.

### Add a memo
```python
def fib(n, memo={}):
    if n < 2: return n
    if n in memo: return memo[n]
    memo[n] = fib(n - 1, memo) + fib(n - 2, memo)
    return memo[n]
```
Trace for `n = 5` (top-down fills `memo`):

| call | in memo? | what we compute | `memo` after |
|---|---|---|---|
| `fib(5)` | no | `fib(4) + fib(3)` | — |
| `fib(4)` | no | `fib(3) + fib(2)` | — |
| `fib(3)` | no | `fib(2) + fib(1)` | — |
| `fib(2)` | no | `fib(1) + fib(0) = 1` | `{2:1}` |
| `fib(1)` | — | returns `1` | `{2:1}` |
| `fib(3)` continues | | `fib(2)=1 + 1 = 2` | `{2:1, 3:2}` |
| `fib(4)` continues | | `2 + fib(2)=1 → 3` | `{2:1, 3:2, 4:3}` |
| `fib(5)` continues | | `3 + fib(3)=2 → 5` | `{2:1, 3:2, 4:3, 5:5}` |

Result `5`. Each subproblem is solved once → `O(n)` time and space. This is the fundamental recursion-to-DP pattern: **overlapping subproblems + memo** collapses exponential to polynomial.

## 6. Common Variations
- **Head vs tail recursion**: tail-recursive code is easy to rewrite as iteration; Python does not optimise either.
- **Divide and conquer**: merge sort, quicksort, `pow(x, n)`, maximum subarray (LC 53 D&C solution).
- **Tree recursion**: compute leaf / root aggregates with post-order; root-to-leaf paths with top-down accumulation.
- **Mutual recursion**: `is_even(n) / is_odd(n)`; recursive descent parsers.
- **Memoisation placement**: always check `memo` before doing work; write `memo[key] = result` before returning.
- **Iterative rewrite**: when stack depth is a risk, convert to explicit stack or to DP bottom-up.
- **Edge cases**: empty input, single element, negative inputs, large `n` hitting recursion limit.

## 7. Related Patterns
- **Backtracking** — recursion + "undo the choice" to explore a search tree.
- **Dynamic Programming** — recursion + memo, or the bottom-up iterative dual.
- **DFS** — recursion on graphs/trees; often the natural expression of "visit every child and combine".
- **Divide and Conquer** — recursion where subproblems are disjoint slices of the input.
- **Stack** — the iterative substitute for a recursion's implicit call stack.

Distinguishing note: **recursion** is a technique, not a pattern on its own. You apply it when the problem's structure is self-similar; the actual work lives in the *combine* step, which is where backtracking, DP, D&C, and DFS diverge.
