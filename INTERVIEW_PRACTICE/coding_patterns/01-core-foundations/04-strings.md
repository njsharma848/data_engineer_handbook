# 04 — Strings

## 1. When to Use
- Input is text: `s`, `word`, `pattern`, `sentence`, `document`.
- Task involves **substrings, palindromes, anagrams, matching, parsing, or transformation**.
- Keywords: *"substring"*, *"subsequence of characters"*, *"contains"*, *"reorder letters"*, *"case-insensitive"*, *"alphanumeric only"*, *"valid parentheses"*, *"decode/encode"*.
- Constraints mention a limited alphabet (`a-z`, ASCII, digits only) — this enables fixed-size counting arrays.
- Problem is stated over characters but behaves like an array problem with extra alphabet structure.

## 2. Core Idea
A string is an array of characters plus an alphabet. Almost every array technique (two pointers, sliding window, prefix sum) applies, and the bounded alphabet unlocks extras: fixed-size frequency vectors, bitmask fingerprints, and O(26) comparisons instead of O(n). Immutability in Python means you build results with `list` / `deque` / `str.join`, never with `+=` in a loop.

## 3. Template
```python
def scan_string(s: str) -> str:
    n = len(s)
    if n == 0:
        return ""

    # 1) Fixed-size counter (lowercase English)
    freq = [0] * 26
    for ch in s:
        freq[ord(ch) - ord('a')] += 1

    # 2) Two-pointer skeleton (palindrome / reverse)
    lo, hi = 0, n - 1
    while lo < hi:
        if not s[lo].isalnum():
            lo += 1
        elif not s[hi].isalnum():
            hi -= 1
        elif s[lo].lower() != s[hi].lower():
            return "not a palindrome"
        else:
            lo += 1; hi -= 1

    # 3) Build output via list + join, never `+=` on str
    parts = []
    for ch in s:
        parts.append(ch.upper() if ch.isalpha() else ch)
    return "".join(parts)
```
Key mental tools:
- `ord(c) - ord('a')` maps lowercase to `0..25`.
- `collections.Counter` for arbitrary alphabets; `[0] * 26` when you know it is English.
- Compare two anagrams by sorted chars *or* by equal frequency vectors.
- A rolling 26-int "signature" supports `O(1)` window updates.

## 4. Classic Problems
- **LC 125 — Valid Palindrome** (Easy): two pointers with alnum skip.
- **LC 242 — Valid Anagram** (Easy): frequency vector.
- **LC 3 — Longest Substring Without Repeating Characters** (Medium): sliding window + last-seen index.
- **LC 438 — Find All Anagrams in a String** (Medium): window of fixed size with 26-int match.
- **LC 5 — Longest Palindromic Substring** (Medium): expand around center.

## 5. Worked Example — Valid Anagram (LC 242)
Problem: return `True` iff `t` is an anagram of `s`.

Input: `s = "anagram"`, `t = "nagaram"`.

Approach: build a 26-int frequency vector for `s`, decrement with `t`, check all zeros.

```python
if len(s) != len(t): return False
freq = [0] * 26
for ch in s: freq[ord(ch) - ord('a')] += 1
for ch in t: freq[ord(ch) - ord('a')] -= 1
return all(v == 0 for v in freq)
```

Trace (only the letters that matter are shown):

| step | action | `a` | `g` | `m` | `n` | `r` |
|---|---|---|---|---|---|---|
| start | — | 0 | 0 | 0 | 0 | 0 |
| s[0]='a' | +1 a | 1 | 0 | 0 | 0 | 0 |
| s[1]='n' | +1 n | 1 | 0 | 0 | 1 | 0 |
| s[2]='a' | +1 a | 2 | 0 | 0 | 1 | 0 |
| s[3]='g' | +1 g | 2 | 1 | 0 | 1 | 0 |
| s[4]='r' | +1 r | 2 | 1 | 0 | 1 | 1 |
| s[5]='a' | +1 a | 3 | 1 | 0 | 1 | 1 |
| s[6]='m' | +1 m | 3 | 1 | 1 | 1 | 1 |
| t[0]='n' | -1 n | 3 | 1 | 1 | 0 | 1 |
| t[1]='a' | -1 a | 2 | 1 | 1 | 0 | 1 |
| t[2]='g' | -1 g | 2 | 0 | 1 | 0 | 1 |
| t[3]='a' | -1 a | 1 | 0 | 1 | 0 | 1 |
| t[4]='r' | -1 r | 1 | 0 | 1 | 0 | 0 |
| t[5]='a' | -1 a | 0 | 0 | 1 | 0 | 0 |
| t[6]='m' | -1 m | 0 | 0 | 0 | 0 | 0 |

All zeros → return `True`. Time `O(n)`, space `O(1)` (26-slot array).

Contrast: sorting both strings works too (`sorted(s) == sorted(t)`) but is `O(n log n)`.

## 6. Common Variations
- **Unicode / arbitrary alphabet**: swap `[0] * 26` for `collections.Counter`.
- **Case / punctuation rules**: always normalise first (`s.lower()`, `ch.isalnum()`), do not sprinkle conversions inside the loop.
- **Rolling window anagrams**: maintain two 26-int arrays, compare each shift in `O(26)`.
- **Palindromes**: two-pointer for *check*, expand-around-center for *find longest*, Manacher for `O(n)`.
- **Pattern matching**: KMP (`O(n + m)`), Z-algorithm, Rabin-Karp rolling hash.
- **String building**: always `"".join(parts)` in Python — `s += ch` in a loop is quadratic.
- **Edge cases**: empty string, single char, all same char, mixed case, non-ASCII.

## 7. Related Patterns
- **Arrays** — strings inherit all array techniques; the alphabet is the bonus.
- **Hashing** — `Counter` and sets for frequency/seen logic over characters or substrings.
- **Sliding Window** — dominant pattern for "longest/shortest substring with constraint".
- **Two Pointers** — palindromes, reversals, filtering alnum.
- **Dynamic Programming** — edit distance, longest common subsequence, palindrome partitioning.
- **Tries** — when you are matching many patterns against one text, or autocompleting prefixes.

Distinguishing note: if the problem asks about **contiguous** slices of characters it is almost always sliding window or two pointers; if it asks about **reorderings** it is almost always a Counter / frequency vector; if it asks about **edit distance** or **subsequences** it is DP.
