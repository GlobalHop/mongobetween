# 08 — Open follow-ups after `feature/rs-connection-pool`

These are the minor findings that per-task reviews deferred (2026-11-16). The final whole-branch review triaged
them. Items already fixed in the final fix wave (`7fa5a71..16c8717`) are listed first. Everything else was judged
"OK to defer" and is kept here as a backlog.

## Fixed in the final fix wave

- Proxy error replies now have a statsd counter: `proxy_error_reply`, tagged with code and command.
- A second signal now hard-exits after 2s. In-flight upstream requests are cancelled on Kill and on Close.
- Inbound auth disabled: `saslStart`, `saslContinue` and `authenticate` get a local code 18 reply. They are no
  longer forwarded upstream.
- Write commands always go to the primary, even if they carry `$readPreference`.
- Before authentication:
  - `ping` is answered locally.
  - `buildInfo` is forwarded as a fixed body built by the proxy.
  - Frames are capped at 64 KiB.
  - `-auth-timeout` closes connections that stay unauthenticated.
  - `-max-connections` limits the number of client connections.
  - Statsd tags are not set from client data.
- TLS on the plain port: a client that starts a TLS handshake is detected and closed. Message bodies are read
  incrementally.
- Docs: the wire-version cap wording and the 11602 transaction-label wording are corrected. Added
  `.dockerignore`, dropped the CI `checks: write` permission, and `make test-unit` now runs with CI unset.

## Still open (raw list from the review ledger)

- Task 2: minor (deferred): CI junit report (gotestsum + action-junit-report) dropped; checks:write unused
- Task 2: minor (deferred): rs_test Disconnect error ignored via `_ =`
- Task 2: minor (deferred): GOMODCACHE mount may fail where cache not owned by uid
- Task 2: minor (deferred): README note — run `make test-docker-down` once after 5.0→8.0 volume change (Task 14 docs)
- Task 2: minor (deferred): no .dockerignore (.git/.superpowers in build context)
- Task 2: minor (deferred): gofmt -l lists pre-existing mongo/command.go, util/statsd_test.go
- Task 2: minor (deferred): golint removed, no revive — lint coverage = v2 defaults; `make lint` target itself not run
- Task 3: minor (deferred): BuildHandshakeResponse initialises cmd := Hello (should be Unknown) — carry to Task 11 (touches same fn)
- Task 3: minor (deferred): no test asserts reply responseTo/header length — carry to Task 11
- Task 3: minor (deferred): -max-wire-version int→int32 no range check/cap; negative = mirror
- Task 3: minor (deferred): empty saslSupportedMechs dropped from fake reply — Task 11 must re-add when auth on
- Task 4: minor (deferred): checkout timeout path not exercised by any test
- Task 4: minor (deferred): WriteException branch in proxy test asserts label only, not code 11602
- Task 4: DONE: errmsg forwarded err.Error() (leaked upstream hostnames); now fixed generic messages, detail stays in logs
- Task 4: minor (deferred): local HandshakeResponse failures mislabelled as 11602 upstream errors
- Task 4: minor (deferred): no statsd counter for proxy error replies
- Task 5: minor (deferred): no test for MoreToCome discard branch (needs fake Expirable conn)
- Task 5: minor (deferred): StripExhaustAllowed no test for nil/short/non-OP_MSG input
- Task 6: minor (deferred): non-zero cursor id with empty ns pinned under "<id>-" silently — consider log/counter
- Task 6: minor (deferred, plan-mandated): legacy OP_KILL_CURSORS ns "" — not routed/evicted (op removed server-side 6.0)
- Task 6: minor (deferred): ok:0 killCursors reply still evicts pin
- Task 6: minor (deferred): killCursors with IDs on different servers → only first ID's server
- Task 6: minor (deferred): cursorNamespaceOfCommand runs on legacy OP_QUERY user filter docs
- Task 6: minor (deferred): real-server ns test never issues getMore; unused a/b collections; legacy case synthetic only
- Task 6: minor (deferred): legacy OP_QUERY case skips killCursors eviction check
- Task 7: minor (deferred): no test for RoundTrip error-path txn pin removal
- Task 7: minor (deferred): no pinnedServer tests for continuing-txn-no-pin+cursor-pin and start+cursorID
- Task 7: minor (deferred): debug log "Using pinned server" lost lsid
- Task 8: minor (deferred): $readPreference on write/DDL command routes to secondary (mongos would force primary) — consider gating by command
- Task 8: minor (deferred): RoundTrip discards ReadPref ok flag — no debug log for invalid $readPreference
- Task 8: minor (deferred): no selectServer priority test; TestGetMoreOperation_CursorPinning name overstates; missing negative staleness/non-string tag/kind-1 tests
- Task 9: minor (deferred): shutdown test may pass without an idle conn accepted; doesn't assert Kill path
- Task 9: minor (deferred): drained only closed in accept(); listen failure → full timeout wait
- Task 9: minor (deferred, ops): no hard-exit path if Kill can't unblock a handler stuck upstream; Mongo Close cancel only after wg.Wait
- Task 9: minor (deferred): validateUpstreamTLS error nondeterministic with multiple non-TLS clusters; no tests for signal handler / cluster tag / CloseUpstreams
- Task 10: minor (deferred): handshake-deadline path untested (plain-client test bounded by client timeouts); no mTLS reject handshake test; config tests only cert-without-key
- Task 10: minor (deferred): duplicate exported LoadServerTLSConfig wrapper; SetDeadline clear error ignored; handshake-failure log at Warn (noisy for scanners)
- Task 11: minor (deferred): no unit tests for authorize() gate (legacy opcodes, OP_QUERY $cmd, authenticate→18, sasl after auth not forwarded)
- Task 11: minor (deferred): pre-auth ping/buildInfo forwarded with lsid/startTransaction fields → txn cache pollution / pool pressure; consider local ping or sanitized forward
- Task 11: minor (deferred): pre-auth hello on non-admin db gets 13
- Task 11: minor (deferred): usernames keep surrounding whitespace
- Task 11: minor (deferred): auth failures logged at Warn with attacker-chosen username, no rate limit
- Task 11: minor (deferred): auth OFF still forwards saslStart/saslContinue/authenticate upstream (pre-existing)
- Task 11: minor (deferred): dead localhost CI-fallback branches left in several tests (cleanup)
- Task 12: minor (deferred): tagged-rp assert.Error accepts any error; default rp passed explicitly; cursor test doesn't prove secondary served; CountDocuments w/o deadline; upstream not disconnected in cleanup
- Task 12: minor (deferred): missing electionId → 40s fail instead of clear message; proxy topology view not asserted directly
- Task 13: minor (deferred): php-smoke --abort-on-container-exit vs completed rs-init fragile across Compose versions; no teardown; temp dir/key/users 0644; tag-only image pins; $majority misnamed; no secondary-used assertion; no timeout guard
- Task 13b: minor (deferred): CloseUpstreams name now also closes Dynamic; Close test doesn't prove polling stops; race capture summary only
- Task 13b: minor (deferred): cancel not called on NewDynamic error path; Close() nil-cancel panic for non-constructor Dynamic; abort test lacks t.Cleanup/srv hang risk on Fatal; 1s bound timing-sensitive under -race
- Task 14: minor (deferred): -max-wire-version "capped at 25" only true in mirror mode; 11602 label wording ignores txn labels; failover/read examples prose-only

## Parked at final re-review

- Final: parked — shutdown-timeout race: request reaching requestContext() just after Kill gets fresh uncancelled ctx and may hang on blackholed upstream until SIGKILL — Ruling: real, low-probability (few-instruction window), second signal os.Exit covers it; defer as follow-up — cost if wrong: one pod waits until terminationGracePeriod SIGKILL during a rare shutdown
