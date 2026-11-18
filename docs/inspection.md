<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Apache-2.0
-->

# Connection and worker inspection API

Chimera exposes read-only, authenticated `GET /api/v1/threads` and
`GET /api/v1/connections` endpoints when the REST service is enabled. Their
request and response schemas are in the [OpenAPI document](openapi.json); the
[API reference](api-reference.md) describes authentication and REST setup.
The [chimera-admin package documentation](../src/admin/README.md) contains CLI usage, examples and client compatibility information.

## Collection and completeness

Collection runs asynchronously on each owner worker, including the requesting
worker. There is one collector admission slot shared by both endpoints, a
one-second deadline, at most 1,024 connection rows divided into per-worker
quotas, and at most 4,096 scanned entries per worker. Collector storage is
bounded by the configured worker pool and global row quota. Metadata and bounded
result buffers cross threads; live protocol and transport pointers do not. JSON
allocation and list walks stay off RPC dispatch.

A missing worker or exhausted row/scan budget makes `complete=false`.
`missing_workers`, `truncated`, and `worker_samples` identify missing coverage.
HTTP 200 can carry useful partial data; HTTP 503 means no selected worker
responded. A stalled collector keeps its admission slot until every queued task
releases, including after request cancellation or deadline; later requests
receive 429 with `Retry-After: 1`.

Each sample is independently timestamped and there is no full pagination or
retained disconnect history. Connection IDs identify observed binds and must be
paired with `server_instance_id`; reconnects and process restarts create new
identities. A missing connection is an observation about the transport list, not
proof that an NFS lease expired.

## Worker and connection data

Worker RPC/VFS values are copied from existing histogram instances and role
gauges. They cover all traffic on that worker, including auxiliary RPC services
sharing its context. Histogram sample rates are not automatically IOPS or
per-client rates. Large cumulative values and nanosecond timestamps are decimal
strings. `per_connection_stats` is currently null for NFS.

NFS application versions describe registered RPC programs observed on that
connection. A supported NFSv4 COMPOUND also records its minor version; NULL
alone does not. Multiple observed versions remain visible. SMB reports the
negotiated dialect and its wrapping 32-bit received-message counter, which
counts messages rather than completed commands. S3 reports HTTP framing (`h1`,
`h2`, or unknown) and excludes REST and metrics connections. Initiated RPC
callback connections are excluded. In-process and Unix endpoints have no
invented IP ports.

Connection filters are `worker_id`, `service`, `remote_ip`, `remote_port`,
`local_ip`, `local_port`, and `transport`; different filters are ANDed. IP
values must be numeric IPv4/IPv6. Unknown, duplicate, empty or malformed
parameters return 400. Default services are NFS, SMB and S3; explicitly select
`mount`, `nlm`, `nsm` or `rpcbind` for auxiliary NFS listeners. `/threads`
accepts only `worker_id`. Add `?live=1` to read worker CPU, scheduler wait and
event-loop iteration counters directly without waking workers. See
[`worker-metrics.md`](worker-metrics.md) for the fields and rate calculations.
The `chimera-admin inspect threads` view reads both the ordinary worker
snapshot and `?live=1`. CPU%, RUNQ%, BUSY%, RPC/s, and RX/TX MB/s are calculated
from two consecutive live samples; a first sample has no rate baseline. BUSY%
is the fraction of event-loop iterations that did work, not CPU utilization.
Poll-mode workers can have high CPU% and low BUSY%. VFS Read/Write latency uses
the existing live log2 histograms. When requesting
`?live=1&sessions=1`, the session list is bounded; its slot-state scan occurs
after releasing the NFS client-table lock.

In the compatibility `nfs4_sessions` array, `highest_slot_in_use` is the
highest slot index that has ever held a request. It is not the current number
of outstanding requests. Use `slots_in_progress` for current in-flight work;
RESYNCING slots count as in progress.

`GET /api/v1/nfs/sessions` is the NFSv4.1+ session endpoint. It accepts
`limit` (1–1,024, default 100), `offset`, and optional decimal `client_id`;
unknown parameters return 400. Results include requested, granted, and server
slot limits; slot-state counts; forechannel request/response size limits; and
creation timestamps. `total` and `truncated` describe the bounded result.
`created_at_monotonic_ns`, `created_at_wall_ns`, and client IDs are decimal
strings. The `nfs4_sessions` array in the live thread response is deprecated;
use the namespaced endpoint for detailed session inspection.

`GET /api/v1/nfs/sessions/{session_id}/slots` returns the bounded slot rows
(`slot`, `state`, `seqid`, and `age_ns` for in-progress slots). The operation
is `null` when the existing request state does not identify it. Unknown session
IDs return 404. Slot rows are capped at 1,024 and report `total`, `truncated`,
and `complete`. The CLI presents this as
`chimera-admin inspect nfs sessions --slots SESSION_ID`.

Session rows include up to 64 `connections`, each with `connection_id`
(`worker:serial`) and `worker_id`, plus the distinct `workers` list.
`connections_truncated` is true if more than 64 distinct bindings were
observed. This list records connections that have bound the session during its
lifetime; a disconnected connection can remain listed until the session is
destroyed.
Session rows also expose `replay_hits`, `seq_misordered`, `bad_slot`,
`delay_errors`, and `slot_in_progress_retries`. These lifetime event counters
are sharded by worker and summed in the inspection reader; the CLI shows them
in the `SESSION EVENTS` table.

`GET /api/v1/nfs/clients` returns the bounded NFSv4 client table. It accepts
`limit` (1–1,024, default 100) and `offset`; unknown or invalid parameters
return 400. Each row includes the decimal `client_id`, `owner_hex`, original
`owner_length` and `owner_truncated`, `state` (`confirmed`, `unconfirmed`,
`expired`, or `courtesy`), `lease_remaining_ms`, `sessions`, `opens`, `locks`,
`delegations`, `minor_version`, and up to eight `remote_addresses` with a
`remote_addresses_truncated` flag. `total`, `offset`, `limit`, `truncated`, and
`complete` describe paging. Owner strings are hex encoded and capped at 64
bytes. State counts are lifecycle gauges; a concurrent create or destroy may
make a snapshot briefly reflect adjacent moments.

Use `chimera-admin inspect nfs clients` to display clients. `--input FILE`,
`--watch`, `--interval`, `--iterations`, `--json`, `--quiet`, `--no-clear`,
`--wide`, `--max-rows`, `--sort`, `--target`, and `--targets FILE` follow the
other inspection views. `--wide` includes owner hex and minor version. These
client and session views cover NFSv4.1 and later; NFSv3 has no sessions or
clients.

During `chimera-admin inspect nfs sessions --watch`, the CLI tracks each
session's peak in-progress slots and reports when `INFL == GRANT` in most
samples. If live VFS Write latency is available, it estimates a request rate as
`GRANT / average Write latency`. Saturation alone does not prove the slot limit
is the bottleneck: a slow backend can also keep all slots occupied, so compare
with a run using a larger negotiated slot count.
The protocol-neutral dashboard adds an NFSv4.1+ sessions panel when NFS is
enabled; NFSv3 itself has no session or client view.
