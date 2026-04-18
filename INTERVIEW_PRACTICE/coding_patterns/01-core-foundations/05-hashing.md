# 05 — Hashing

## 1. When to Use
- You need **O(1) membership, lookup, or counting**.
- Keywords: *"duplicate"*, *"contains"*, *"seen before"*, *"frequency"*, *"count of"*, *"group by"*, *"complement"*, *"pair that sums to k"*.
- You would otherwise write a nested loop that compares every element to every other.
- You need to **group** items by a key derived from each item (sorted chars, modulo class, canonical form).
- Order does not matter for the answer — sets and dicts intentionally drop order.

## 2. Core Idea
Hashing trades `O(n)` extra space for `O(1)` average-case lookup, removing the inner loop of a naive pair-search. It also gives you a natural *canonicalisation* step: two objects hash to the same bucket iff they share a derived key, which is why anagram grouping, two-sum complements, and "seen-at-index" tricks all reduce to one pass with a dict.

## 3. Template
```python
from collections import defaultdict, Counter

def seen_before(arr):
    seen = set()
    for x in arr:
        if x in seen:
            return True
        seen.add(x)
    return False

def find_complement_pair(arr, target):
    index_of = {}                              # value -> latest index
    for i, x in enumerate(arr):
        need = target - x
        if need in index_of:
            return [index_of[need], i]
        index_of[x] = i
    return []

def group_by_key(items, key_fn):
    buckets = defaultdict(list)                # key -> list of items
    for item in items:
        buckets[key_fn(item)].append(item)
    return list(buckets.values())

def frequency(arr):
    return Counter(arr)                         # Counter is a dict subclass
```
Key mental tools:
- `set` → existence only; `dict` → existence + associated data (index, count, last seen).
- `defaultdict(list)` / `defaultdict(int)` removes boilerplate around missing keys.
- Keys must be **hashable**: use `tuple` for lists, `frozenset` for sets, `(sorted_chars)` for anagrams.
- Worst-case `O(n)` per op on pathological inputs; treat as `O(1)` average unless adversarial.

## 4. Classic Problems
- **LC 1 — Two Sum** (Easy): complement lookup.
- **LC 217 — Contains Duplicate** (Easy): set of seen values.
- **LC 49 — Group Anagrams** (Medium): bucket by sorted-chars key.
- **LC 128 — Longest Consecutive Sequence** (Medium): set membership to start runs only at sequence heads.
- **LC 560 — Subarray Sum Equals K** (Medium): prefix-sum frequency map.

## 5. Worked Example — Group Anagrams (LC 49)
Problem: group strings that are anagrams of each other.

Input: `strs = ["eat", "tea", "tan", "ate", "nat", "bat"]`.

Key idea: two strings are anagrams iff they share the same sorted-character tuple. Use that tuple as the hash key.

```python
from collections import defaultdict

def groupAnagrams(strs):
    buckets = defaultdict(list)
    for s in strs:
        key = tuple(sorted(s))   # canonical form
        buckets[key].append(s)
    return list(buckets.values())
```

Step-by-step `buckets` state:

| step | `s` | `key` | `buckets` after |
|---|---|---|---|
| 1 | `"eat"` | `('a','e','t')` | `{('a','e','t'): ["eat"]}` |
| 2 | `"tea"` | `('a','e','t')` | `{('a','e','t'): ["eat","tea"]}` |
| 3 | `"tan"` | `('a','n','t')` | `{('a','e','t'): ["eat","tea"], ('a','n','t'): ["tan"]}` |
| 4 | `"ate"` | `('a','e','t')` | `{('a','e','t'): ["eat","tea","ate"], ('a','n','t'): ["tan"]}` |
| 5 | `"nat"` | `('a','n','t')` | `{('a','e','t'): ["eat","tea","ate"], ('a','n','t'): ["tan","nat"]}` |
| 6 | `"bat"` | `('a','b','t')` | add third bucket `[("bat")]` |

Return `[["eat","tea","ate"], ["tan","nat"], ["bat"]]`. Time `O(n * k log k)` where `k = max len(s)`; space `O(n * k)` for the buckets.

Optimisation: use a 26-int tuple `tuple(count_vector)` to drop the `log k` factor.

## 6. Common Variations
- **Existence vs count vs index**: choose set, Counter, or dict-of-index based on what downstream logic needs.
- **Rolling hash** (Rabin-Karp): hashing substrings in O(1) per shift for pattern matching.
- **Hash as fingerprint**: 26-int tuple, sorted tuple, or signed frequency delta (LC 76 / LC 438).
- **Prefix-sum + hash**: "number of subarrays with property X" → map prefix value → count.
- **Two-map dance**: LC 290 word-pattern, LC 205 isomorphic strings — two maps enforce bijection.
- **Collision control**: in competitive settings, double hashing (two different moduli) avoids adversarial collisions.
- **Memory trap**: Counters can explode; bound them or evict stale keys (LRU) for streaming data.

## 7. Related Patterns
- **Arrays / Strings** — hashing is the "kill the inner loop" upgrade to brute force on these.
- **Prefix Sum** — partners with a hash map to count subarrays matching a target sum.
- **Sliding Window** — frequency maps are the standard window state.
- **Union Find** — also tracks groups, but for *connectivity* rather than *equality of key*.
- **Trie** — prefix-based grouping of strings; hashing is flat, a trie is hierarchical.
- **Sorting** — an alternative canonicalisation: sort to group, versus hash to group.

Distinguishing note: reach for a **set** the moment you hear "duplicate / seen"; a **dict** the moment you hear "complement / index / last-seen / count"; a **defaultdict(list)** the moment you hear "group by".
