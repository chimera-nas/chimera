# Worker metrics

`GET /api/v1/threads?live=1` reads a lightweight snapshot without scheduling
work on the Chimera workers. Each worker row includes its worker ID and name,
OS thread ID, cumulative CPU time (`cpu_ns`), cumulative scheduler wait time
(`runq_wait_ns`), and cumulative event-loop iteration counters
(`loop_iters`, `loop_iters_with_work`). The `vfs.operations` array contains the
per-worker cumulative sample count for each VFS operation. Poll at 1 second or
slower and derive rates from differences between consecutive snapshots.
The Prometheus `/metrics` scrape also exports cumulative worker CPU time,
event-loop iterations, RPC accept and traffic counters, active RPC connections,
and VFS operation totals. CPU and busy percentages are calculated from the
previous scrape, so they appear after a second scrape.
RPC rows also expose cumulative receive/transmit messages and bytes, accepts,
closes, decode/reject errors, and per-procedure histogram sample totals. Both RPC
and VFS operation rows include average, approximate p50 and approximate p99
latency in nanoseconds when samples are available. Histogram percentiles are
bucket-boundary estimates.
Each RPC worker row includes listener assignment totals, the current listener
placement policy, and the current connection count. RDMA RPC rows include
cumulative accepted connections by up to 64 remote IPs per worker. The
`rdma_remote_ip_overflow_accepts` counter reports accepts from additional IPs.

An operator can request an NFSv4.1 session snapshot with
`GET /api/v1/threads?live=1&sessions=1`. It includes session IDs, client IDs,
negotiated slot counts, and the highest slot currently in use. This explicit
snapshot takes the NFS client-table lock and reads each session's atomic slot
states; it is excluded from normal live polling.

The owner-thread `/api/v1/connections` snapshot includes per-connection RPC
receive/transmit messages and bytes, completed requests, and in-flight requests
for NFS connections. These values are copied while visiting the connection on
its owner thread; the live `/threads?live=1` path does not read connection
objects.

Calculate event-loop busy percentage as:

```text
100 * delta(loop_iters_with_work) / delta(loop_iters)
```

Calculate worker CPU percentage as `100 * delta(cpu_ns) / elapsed_wall_ns`.
With libevpl poll mode enabled, a worker can spin while idle, so its CPU
percentage can remain close to 100%. The event-loop busy percentage helps
distinguish useful loop work from idle spinning.

Metrics follow a single-writer rule: the worker that owns a counter writes it;
other threads read it using relaxed atomic loads. Counters are cumulative and
are never reset by a reader. Worker metric storage is allocated with inspection
state and remains valid until after the worker pool has stopped.
