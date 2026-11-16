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
Use `?live=1&sessions=1` only when an operator needs the NFSv4.1 session and
slot snapshot, because that request takes the NFS client-table lock.
