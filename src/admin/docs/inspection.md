<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Apache-2.0
-->

# Connection and worker inspection

The authenticated management API provides `GET /api/v1/threads` and
`GET /api/v1/connections`. Install chimera-admin from [`src/admin`](../README.md), then run:

```sh
chimera-admin --host nas-server inspect dashboard --interactive
chimera-admin inspect dashboard --targets targets.json --interactive
chimera-admin --host nas-server inspect dashboard --watch
chimera-admin --host nas-server inspect connections --service nfs --remote-ip 10.0.0.20
chimera-admin --host nas-server inspect connections --quiet --wide
chimera-admin --host nas-server inspect threads --worker 3 --watch
chimera-admin inspect dashboard --targets targets.json --watch
chimera-admin inspect dashboard --interactive --quiet
```

Single-instance credentials use the existing Bearer/Basic authentication.
`--token-file`, `CHIMERA_ADMIN_TOKEN`, or `--user` with a password prompt are
supported. HTTPS certificate verification stays enabled; use `--ca-file` for
an explicit CA bundle. Global options precede `inspect`.

Inspection uses aligned human-readable tables by default. `--wide` includes
worker IDs, full instance UUIDs, connection ages, message counters and raw
transport identifiers. Ages are formatted for people (for example, `8m 57s`);
JSON keeps the original exact values. `--quiet` hides banners, explanatory notes
and the interactive keyboard footer while keeping data, instance identification,
errors and partial/stale warnings visible. These flags do not alter polling or
JSON/NDJSON payloads.

Administrative `mount`, `export`, `share`, `bucket` and `user` list/get commands
also print aligned tables by default. Add `--json` to preserve the API response
structure for scripting, for example `chimera-admin mount list --json` and
`chimera-admin export get /demo --json`.

The dashboard shows each core worker, including idle workers, followed by accepted
NFS/SMB/S3 transport connections. Remote addresses and ports identify the client
endpoint; local addresses and ports identify the server interface. Worker IDs are
pool indices, not CPU numbers or RPC metric IDs. Connection IDs combine the worker
index with a fresh bind allocation serial. Pair every ID with `server_instance_id`;
reconnects and process restarts create new identities.

NFS versions describe registered RPC programs actually dispatched on that
connection. A supported NFSv4 COMPOUND also records its minor version; NULL alone
does not. Multiple observed versions remain visible. SMB shows the negotiated
dialect and its existing wrapping 32-bit received-message counter, which counts
messages rather than completed commands. S3 reports HTTP framing (`h1`, `h2`, or
unknown), and excludes REST and metrics connections. Initiated RPC callback
connections are excluded. Inproc/Unix endpoints have no invented IP port.

Worker RPC/VFS values are copied from existing histogram instances and role
gauges. They cover all traffic on that worker, including auxiliary RPC services
sharing its context. Connection filters never narrow worker totals. Thread CPU%,
RUNQ%, BUSY%, RPC/s, and RX/TX MB/s use deltas between live samples; a single
snapshot has no rate baseline. BUSY% is the loop-iteration ratio, not a CPU
measure. Poll-mode workers can show high CPU% with low BUSY%. Read and Write
latency comes from the live VFS histograms. Histogram sample rates are not
automatically IOPS or per-client rates. NFS
`per_connection_stats` is null. Large cumulative values and nanosecond timestamps
are decimal strings. Rates use consecutive worker monotonic timestamps; the CLI
resets them after restarts, failures, partial snapshots or counter/context changes.

Connection filters are `worker_id`, `service`, `remote_ip`, `remote_port`,
`local_ip`, `local_port`, and `transport`; different filters are ANDed. IPs must be
numeric IPv4/IPv6. Unknown, duplicate, empty or malformed parameters return 400.
Default services are NFS, SMB and S3; explicitly select `mount`, `nlm`, `nsm` or
`rpcbind` for auxiliary NFS listeners. `/threads` accepts only `worker_id`.

Collection runs asynchronously on each owner worker, including the requesting
worker. There is one collector admission slot across both endpoints, a one-second
deadline, 1,024 total connection rows divided into per-worker quotas, and at most
4,096 scanned entries per worker. Collector storage is bounded by the configured
worker pool and the global row quota. Metadata and bounded result buffers cross threads; live protocol and
transport pointers do not. No JSON allocation or list walk is added to RPC dispatch.

A missing worker or exhausted row/scan budget makes `complete=false`.
`missing_workers`, `truncated`, and `worker_samples` identify the missing coverage.
Narrow filters or select a worker when quotas truncate a listing. These are
independently timestamped samples with no full pagination or retained disconnect
history. HTTP 200 can carry useful partial data; 503 means no selected worker
responded. A stalled collector keeps the admission slot even after an HTTP
cancellation or deadline until every queued task releases; later requests receive
429 with `Retry-After: 1`. This bounds memory during repeated cancellations.

Watch defaults to two seconds, with a one-second minimum and a finite three-second
request timeout. Dashboard requests are sequential. TTY output redraws and
restores the cursor; press `q` or Ctrl-C to exit text watch on a TTY. Pipes
append frames, and `--json --watch` emits NDJSON; those modes exit with Ctrl-C.
`--iterations N` bounds poll attempts. Partial data is marked PARTIAL; failed
refreshes retain STALE data and clear rates. Missing endpoints, invalid input and
authentication failures stop the affected target. Transient failures back off.
Text output separates each instance header from its worker table and connection
details. At narrow terminal widths, endpoints and secondary metadata move into
aligned field/value blocks so complete IP:port values remain visible. Redirected
text has no terminal control sequences.

Interactive dashboard mode uses Python's standard-library curses. It implies
watch with the same interval and independent per-target pollers. Each worker has
an expandable summary containing name, TID, all-traffic connection totals and the
existing RPC/VFS values. Expanded rows show full connection identities, endpoints,
observed application protocol, transport/backend and state. `MATCH` distinguishes
filtered results from `TOTAL(all)`; a partial count is shown as a lower bound.
Worker metrics appear once, and workers with no connections remain visible.

Up/Down select workers, Right/Left expand/collapse, Enter/Space toggle, `a` expands
all, `c` collapses all, PageUp/PageDown scroll the content, and `q` or Ctrl-C exits.
Expansion is local and sends no additional requests. Selection and expansion are
keyed by target, process identity and worker ID. A restarted process resets only
its own state; a removed selected worker moves selection to the nearest surviving
row. Full endpoints wrap across lines when needed. Resizing reflows the display.
Interactive mode requires a TTY for both input and output and rejects `--json` and
`--no-clear`. Terminal modes and cursor visibility are restored after quit,
Ctrl-C and exceptions. `--max-rows` limits text tables only; interactive mode can
scroll every returned row, subject to server collection bounds.

Partial refreshes retain the last complete worker/connection observations when
available, explicitly marking unconfirmed child connections and missing workers.
They do not produce disappearance notices or rates. A failed refresh retains
labelled stale data. Unknown counters display `--`, and initial/reset rates show
warming up. A complete empty result alone can show no matching connections.

For multiple servers, `--targets FILE` loads up to 32 named direct HTTP(S) origins.
Use `--target NAME` to select a subset. Explicit credential environment-variable
names or files belong to each target; paths resolve relative to the targets file.
Each target has one poller/session, independent counters and a deadline. Slow or
failed targets do not block healthy targets. Global connection/authentication
options cannot accompany `--targets`. Configure direct instance URLs rather than
a load-balanced management address.

The supplied [examples](../examples/) are labelled synthetic data. Offline
rendering needs no server or credentials:

See the [OpenAPI schema](../../../docs/openapi.json) for response fields and the
[server inspection guide](../../../docs/inspection.md) for collection behavior and completeness.
