# P2 — FAA registrant → canonical resolution (design + dry-run gate)

**Status: DRAFT. Dry run executed, no edges written. Blocked on helen review.**

## The risk this design is built around

P1 landed 316,308 registrations behind a closed fence. P2 is the step
that makes a **claim about a person**: that the "TRUMP DONALD J" on a
registration is the canonical Donald Trump.

The failure mode is not a crash. It is a confident wrong edge on a
named person — the same shape as the FEC fuzzy-name misattribution
that produced the false $140M Peter Thiel claim. That one was caught
after it was live. So P2's design goal is **not match coverage**; it
is that every edge which lands is one a human would defend, and that
the ones we are unsure about are visibly unsure rather than quietly
included.

## Proposed edge

`Aircraft` is not a graph entity in P1 and P2 does not change that
unilaterally. Two options, needing a decision:

- **(a)** aircraft stays PG-only; the edge is `canonical → aircraft`
  recorded in a join table, still fenced. No `EntityType` change.
- **(b)** aircraft becomes a canonical entity type, and the edge is a
  real `REGISTERS` edge in the graph.

**(a) is recommended for P2.** It gets the analytic value —
"which canonicals own aircraft" — without adding a node kind to the
read gate, the Neo4j projection and the de-anon surface all at once.
(b) can follow once the matching is trusted. Either way every edge
carries a `SourceCitation` to the snapshot sha256, and **0 uncited
edges** stays invariant.

## Matching tiers (deliberately narrow)

Reuses `normalize_name` — the same function that produced
`canonical_name_normalized` — so keys mean the same thing the stored
ones do.

| tier | score | rule |
|---|---|---|
| exact_canonical | 1.00 | normalized registrant == `canonical_name_normalized` |
| exact_alias | 0.90 | normalized registrant == an `entity_aliases.surface_name_normalized` |
| token_set | 0.75 | same token multiset, ≥2 tokens (handles FAA's `LAST FIRST` vs `First Last`) |

**No edit distance, no embeddings, no nickname table.** A tighter net
that reports its own misses is worth more than a wider one whose
false positives have to be found later on live published data. If
coverage turns out to be the problem, widening is a reviewable
follow-up; un-publishing a wrong claim about a person is not
symmetric with that.

## Dry-run results (live DB, 2026-08-29, read-only)

```
aircraft rows                316,308
  with registrant_name       311,467
  distinct registrant names  197,354
rows with >=1 candidate        7,951   (2.6% of named rows)
distinct candidate pairs      10,309   (raw index hits 126,796)

score distribution      1.00  7,143    0.90  671    0.75  137
by tier         exact_canonical 7,143  exact_alias 671  token_set 137

REVIEW FLAGS
  ambiguous AT BEST SCORE        104   <- the real problem (1.3%)
  multi-candidate, resolved    2,239   (settled by tier; see below)
  cross-type registrant          515
  single-token name            1,081
  candidate is suppress          133
  candidate is alias               2

by FAA type_registrant
  3 Corporation 6,652 | 5 Government 600 | 7 LLC 495 | 1 Individual 139
```

## What the numbers say

**Coverage is low and that is the correct outcome.** 2.6% match, and
they are overwhelmingly corporations (6,652 of 7,951) — airlines,
freight, government fleets. Argus's canonical set is built from
political/financial reporting, so it simply does not contain most of
the 197k private registrants. Nothing should be tuned to "improve"
this number.

**The ambiguity is mostly parent-vs-subsidiary, and the score already
resolves it.** My first reading of 2,343 multi-candidate rows was that
the graph held duplicate canonicals needing a dedup pass. Checking the
live rows says otherwise:

| registrant | matches | |
|---|---|---|
| `AMERICAN AIRLINES INC` | `AMERICAN AIRLINES` (carrier) | canonical, **1.00** |
| | `American Airlines Group` (parent) | alias only, 0.90 |
| `UNITED AIRLINES INC` | `UNITED AIRLINES` (carrier) | canonical, **1.00** |
| | `United Airlines Holdings Inc` (parent) | alias only, 0.90 |

These are **two real entities each**, not duplicates — merging them
would be a mistake, and the ontology already has `subsidiary_of` to
relate them. The carrier is what the FAA registrant names, and the
tier ordering picks it. Measured properly — ties **at the best
score** — genuine ambiguity is **104 rows (1.3%)**, not 2,343. Those
104 still need a rule; the rest do not.

**1,081 single-token names.** A bare surname matching a canonical is
the classic false positive. These should be excluded outright, not
scored.

**Only 139 individual registrants match at all** — the population the
fence exists for is almost entirely absent from the graph, which is
reassuring, but 133 candidates land on `suppress` canonicals and 2 on
`alias`. Attaching an aircraft to a suppressed person is a
privacy-surfacing decision and **Tony's call**, not a threshold.

## Proposed gate before any edge is written

1. Exclude single-token and cross-type matches entirely (1,596 rows).
2. Resolve the 104 best-score ties by an explicit rule, or drop them.
   Do **not** dedup carrier-vs-parent; prefer the canonical-tier hit.
3. Hold every `suppress`/`alias` candidate for Tony — no default.
4. Human review of a sample of the ~6,000 surviving corporate matches.
5. Write edges **staged + cited**, publish only after helen validates.

## Operational finding from P1 worth carrying into P2

Postgres `CheckViolation` `DETAIL` echoes the **entire failing row** —
registrant name, street, city, zip. Any P2 path that surfaces a
constraint error into a log, an API response or a report leaks exactly
what the fence withholds. Error handling must strip `DETAIL`.

## Not decided

- (a) vs (b) above.
- Whether an FAA `OTHER NAMES` entry should also be matched (P1 stored
  them; the dry run does not use them).
- Whether co-owned registrations (`type_registrant=4`, 25,845 rows)
  should produce one edge or several.
