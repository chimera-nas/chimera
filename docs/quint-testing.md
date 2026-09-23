<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# Quint functional testing

## Where a check belongs

* Put protocol transitions, authorization, data, namespace and handle lifetime
  expectations in `ext/specs/quint`. The model chooses inputs and computes
  expected results without consulting Chimera.
* Use harness code for wire encoding, identity mapping, fixture configuration,
  event delivery and observations. For example, an NFS reply may omit optional
  attributes; the harness can issue GETATTR and compare it to the model state.
* Keep C tests for wire reference vectors, malformed frames and controlled
  internal faults or races. Label these `functional`/`probe` or `fault_injection`.
  They may remain in the quick tier; the tier and the kind of test are separate.
* Do not remove a probe until its assertions have equivalent live replay
  coverage. Model self-tests alone do not execute the server.

## Named scenarios and random walks

A corpus config can include ordinary seeded batches and named Quint runs:

```json
"scenarios": [
  { "run": "authorizationRegression", "flavor": "stepPermissions" }
]
```

The run executes in that config's model instance, including its capability
bindings, and exports one ITF trace into the same directory as random traces.
Every existing replayer consumes it. `flavor` identifies the coverage group;
it does not replace the run with a random action. A missing or failing run
fails generation. Generation removes obsolete generated traces after success,
so old seeds cannot silently satisfy a coverage gate.

Use named runs for regressions and rare required transitions, alongside random
walks that explore their interactions. Assertions about a model's internal
state need corresponding observable reads, queries or directory listings in
the exported sequence.

The NFS3 authorization run now checks refused SETATTR and UNCHECKED CREATE,
preserved data, mode changes, truncation and extension. The same corpus runs
against direct storage, a local pNFS data server and remote data servers.
Only backing-file fault injection remains in `nfs_pnfs_io_probe.c`.

## Coverage and execution

```sh
ctest --test-dir build/Release -L model_coverage --output-on-failure
ctest --test-dir build/Release -R 'nfs/mbt/pnfs_' --output-on-failure
ctest --test-dir build/Release -R 'smb/mbt/.*_memfs_plain' --output-on-failure
```

Report these separately:

1. Required behavior buckets reached by the generated corpus.
2. Declared traces, traces actually started, completed passes and failures.
3. Harness limitations, platform skips and incomplete traces.
4. Instrumented source coverage, when measured; bucket counts are not line or
   branch coverage percentages.

NFS3, NFS4, NFS auxiliary protocols, DRC and each SMB family have coverage
gates. NFS4 retains its existing baseline rather than accepting missing buckets
by lowering it. SMB base, force-level-2, durable, lease and replay families run
as separate CTests, both plain and signed. A family cannot pass by skipping
every trace. Durable fixtures shut down the server between traces to drain
handles that intentionally outlive connections.

## Pending SMB CREATEs

The lease model records `STATUS_PENDING` without committing the new open.
An explicit ACK or CLOSE transition resolves the break; `LCreateComplete`
then evaluates and records the final result. The wire harness retains the
original request's MessageId/AsyncId and captures its final response separately
from ACK, CLOSE, and ECHO responses. It never supplies an unmodeled ACK.

The generator serializes pending CREATEs and permits ACK/CLOSE actions until
the request can complete. Other families retain atomic CREATE evaluation for
quiet operations and model self-tests. Lease compounds use the existing quiet
compound action; suspending partway through a compound is not modeled yet.

Named lease scenarios cover sharing conflicts and caching breaks, each resolved
by ACK and CLOSE, plus failed lease-key validation without namespace mutation
and shared lease upgrades followed by resizing. Replay has a named duplicate
CreateGuid scenario for an eligible open without a durable grant. Coverage
requires pending requests and successful/refused completions through both
resolution paths. Replayers stop at the first divergent state in each trace.

Lease identity is `(ClientGuid, LeaseKey)` across sessions; the harness maps
symbolic lease keys within each client. The identity probe retains deliberate
cross-client raw-key collisions. Durable and lease fixtures restart the server
between traces, including traces ending with pending requests.

The durable, lease and replay probes remain useful for wire encodings and
operations beyond the current model. Retire individual assertions only once
their behavior has equivalent model and corpus coverage. Streams and FSCTL
effects still need model transitions before their probes can be retired.

This is an incremental migration. The same model/fixture separation should be
applied to the remaining POSIX, FUSE, REST and S3 probes where their assertions
describe ordinary externally visible behavior.
