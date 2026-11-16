# 06 — Upstream history, issues, forks, alternatives

Upstream: github.com/coinbase/mongobetween. Local master `ea5a87e` (#78, 2024-09-25) equals upstream HEAD, so
nothing has been merged since. The repo is not archived, but maintenance is minimal: there is an open
"is this maintained?" issue (#82), and read-preference PRs went unmerged for years. The README says the proxy was
designed for mongos, and that direct replica-set use "hasn't been battle tested".

## Relevant issues / PRs

| # | State | Topic |
|---|---|---|
| 30 | closed | Atlas 4.4 replica set + Node. First "client metadata … first isMaster" (bool `ismaster:true` not detected, fixed by #41), then "Authentication failed" with credentials/+srv in the app URI. Never resolved by the reporter. 2025 comment: removing SSL from the app→proxy URI fixed it for another user |
| 15 → 20 | merged | isMaster inside OP_MSG intercepted |
| 41 | merged | bool `isMaster` value accepted |
| 16 | open | Drivers mark mongobetween Unknown on upstream errors |
| 22 | closed | Proposal to fake `InterruptedDueToStepDown` (11602) instead of closing the socket |
| 28 | open | Make min/max wire version configurable |
| 32 | merged | maxWire 7 → 8 |
| 31 | open | Honour the client `$readPreference` (Python-tested; someone runs it in a fork) |
| 76 | open | Read preference from the proxy URI |
| 88, 91, 92 | closed | Read-preference variants. #92 (2025-11) parses `$readPreference` with priority txn > cursor > readPref > primary; live in the jitsucom and crowdsecurity forks |
| 79 | open | readPreferenceTags / maxStalenessSeconds question |
| 68 | open | No proxy-side auth; maintainers say use network isolation |
| 61, 80 | open | Custom CA `-cert` flag for upstream TLS (DocumentDB) |
| 45 | merged | Cursor + transaction server pinning |
| 55 | merged | "SDAM v2": metrics/logging only, not routing |
| 69 | merged | Go driver v1.14.0 ("doesn't solve CursorNotFound" with Atlas optimized/LB connection strings) |
| 70 → 73 | merged → reverted | Cursor pinning to connections; reverted because it choked on slow queries |
| 74, 75 | open | Follow-ups to the pinning work |
| 72 | open | DocumentDB not supported |
| 82 | open | Out-of-date deps / security alerts |
| 87 | closed | "DEPRECATED" README banner from a third-party fork; **not** an official deprecation |

No upstream issue mentions PHP or libmongoc.

## Forks with real changes (as of 2026-11)

- **jitsucom** (most developed): #92 read preference, inbound `-proxy-auth user:pass`, command/db/collection
  allowlists, performance work, Go driver v2.
- **Jonoans**: Go driver v2, reported wire version raised to min 9 / max 29.
- **crowdsecurity**: #92 only.
- **TykTechnologies**: read preference (#88 + fix).
- **webflow**: Datadog spans. **mhelmstetter**: metrics/load generator. **mdekkernet**: `-cert`.

These are worth diffing before we implement our own read preference, wire version or auth changes.

## Alternatives

- There is no Atlas-native pooler for a replica set. Atlas optimized connection strings (an LB in front of
  mongos) exist only for sharded clusters.
- You cannot put mongos in front of a plain replica set. The supported route is to convert to a **single-shard
  sharded cluster** (Atlas M30+) and point the apps at mongos. That is officially the shape mongobetween fakes.
- Envoy `mongo_proxy`: observability only, no pooling.
- PHP-specific: ext-mongodb already keeps persistent libmongoc clients per worker. That means one socket per
  member per worker in single-threaded mode, monitoring included. Tuning the FPM worker count is the zero-proxy option.
