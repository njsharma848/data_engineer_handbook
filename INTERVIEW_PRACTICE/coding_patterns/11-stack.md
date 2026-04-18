# 11 — Stack

## 1. When to Use
- Input order matters and you need to process the **most recent** item first — **Last In, First Out**.
- Keywords: *"balanced parentheses"*, *"undo"*, *"evaluate expression"*, *"next greater element"*, *"remove adjacent duplicates"*, *"simulate recursion"*, *"decode nested"*.
- You need to **defer** work and revisit items later in reverse order.
- You have a recursive solution and want to convert it iteratively.
- The answer at index `i` depends on the **closest previous index** satisfying a condition (monotonic stack).

## 2. Core Idea
A stack remembers *contextual history* cheaply: push when you enter a new nested frame, pop when you leave it. For "nearest smaller/greater" problems, a **monotonic stack** maintains a strictly increasing (or decreasing) sequence — whenever a new element violates the order, you pop elements and resolve their answer on the way out, giving `O(n)` total because every element is pushed and popped once.

## 3. Template
```python
# 1) Matching / balance (parentheses, brackets, tags)
def is_balanced(s):
    pair = {')': '(', ']': '[', '}': '{'}
    stack = []
    for ch in s:
        if ch in '([{':
            stack.append(ch)
        elif ch in ')]}':
            if not stack or stack.pop() != pair[ch]:
                return False
    return not stack

# 2) Monotonic stack — next greater element to the right
def next_greater(arr):
    n = len(arr)
    out = [-1] * n
    stack = []                     # indices, arr[stack] strictly decreasing
    for i, x in enumerate(arr):
        while stack and arr[stack[-1]] < x:
            out[stack.pop()] = x   # resolved: next greater is arr[i]
        stack.append(i)
    return out

# 3) Expression evaluation (two stacks: numbers and ops)
def eval_rpn(tokens):
    stack = []
    ops = {'+': lambda a, b: a + b,
           '-': lambda a, b: a - b,
           '*': lambda a, b: a * b,
           '/': lambda a, b: int(a / b)}
    for t in tokens:
        if t in ops:
            b = stack.pop(); a = stack.pop()
            stack.append(ops[t](a, b))
        else:
            stack.append(int(t))
    return stack[0]
```
Key mental tools:
- In Python, use a plain `list` with `.append` and `.pop()` — both `O(1)` amortised.
- For monotonic stacks, **store indices** (not values) so you can compute distances as well.
- Decide **strictly** vs **non-strictly** monotone based on the problem's tie-break rule.

## 4. Classic Problems
- **LC 20 — Valid Parentheses** (Easy): pair/match with a stack.
- **LC 739 — Daily Temperatures** (Medium): monotonic decreasing stack.
- **LC 84 — Largest Rectangle in Histogram** (Hard): monotonic stack giving previous-smaller and next-smaller.
- **LC 150 — Evaluate Reverse Polish Notation** (Medium): operand stack.
- **LC 394 — Decode String** (Medium): stack of `(count, partial_string)` frames.

## 5. Worked Example — Daily Temperatures (LC 739)
Problem: given `T = [73, 74, 75, 71, 69, 72, 76, 73]`, return for each day how many days until a warmer temperature; `0` if none.

Observation: for each position, we want the distance to the **next greater** element. A monotonic decreasing stack of indices lets us resolve each element exactly when its answer becomes known.

```python
def dailyTemperatures(T):
    n = len(T)
    out = [0] * n
    stack = []                      # indices with strictly decreasing T values
    for i, t in enumerate(T):
        while stack and T[stack[-1]] < t:
            j = stack.pop()
            out[j] = i - j
        stack.append(i)
    return out
```

Trace (stack stores indices; show `T` values in parentheses for clarity):

| `i` | `T[i]` | stack before | pops resolved (`j → i-j`) | stack after | `out` |
|---|---|---|---|---|---|
| 0 | 73 | `[]` | — | `[0(73)]` | `[0,0,0,0,0,0,0,0]` |
| 1 | 74 | `[0(73)]` | `0 → 1-0 = 1` | `[1(74)]` | `[1,0,0,0,0,0,0,0]` |
| 2 | 75 | `[1(74)]` | `1 → 2-1 = 1` | `[2(75)]` | `[1,1,0,0,0,0,0,0]` |
| 3 | 71 | `[2(75)]` | — (71 < 75) | `[2(75),3(71)]` | unchanged |
| 4 | 69 | `[2(75),3(71)]` | — (69 < 71) | `[2(75),3(71),4(69)]` | unchanged |
| 5 | 72 | `[2,3,4]` | `4 → 5-4=1`, then `3 → 5-3=2` | `[2(75),5(72)]` | `[1,1,0,2,1,0,0,0]` |
| 6 | 76 | `[2(75),5(72)]` | `5 → 6-5=1`, then `2 → 6-2=4` | `[6(76)]` | `[1,1,4,2,1,1,0,0]` |
| 7 | 73 | `[6(76)]` | — (73 < 76) | `[6(76),7(73)]` | unchanged |

End: indices remaining in the stack get `out = 0`. Final `out = [1, 1, 4, 2, 1, 1, 0, 0]`. Every index is pushed once and popped at most once → `O(n)` time, `O(n)` space.

## 6. Common Variations
- **Previous smaller / next smaller** — flip comparison direction.
- **Strictly vs non-strictly** monotonic: "strictly less" sometimes leaves equal elements unresolved — choose based on the problem's equality semantics (LC 84 vs LC 85).
- **Two-pass** for symmetric problems: run the stack left-to-right, then right-to-left; combine at each index (LC 42 trapping rain water has a stack and a two-pointer solution).
- **Stack of frames** (count, accumulated string): LC 394 decode string, LC 224 basic calculator.
- **Nested parsing**: recursive descent ↔ explicit stack; the stack version is a loop you can debug.
- **Edge cases**: empty input, all increasing (stack never pops), all decreasing (stack holds everything), duplicates.

## 7. Related Patterns
- **Recursion** — every recursion has an equivalent explicit-stack iterative form.
- **Queue / Deque** — FIFO sibling; use a deque when both ends must be cheap.
- **Monotonic Deque** — sliding-window max/min, where the stack gains a head as well as a tail.
- **DFS** — iterative DFS uses a stack; the pattern of push-children / pop-node is identical.
- **Greedy** — "remove k digits to make smallest number" uses a monotonic stack to enact a greedy rule.

Distinguishing note: any question asking for the *closest* previous / next element under a comparison is a **monotonic stack** problem. Any question asking about *nesting* or *matching* is a **pair-matching** stack problem. The data structure is the same; the invariant is not.
