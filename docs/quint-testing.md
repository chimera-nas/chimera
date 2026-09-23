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

## Remaining work exposed by enabling SMB replay

The durable, lease and replay families now expose failures that were hidden by
whole-family exclusions. These tests deliberately retain normal failure
semantics; there are no expected-failure or skip exemptions.

* Lease CREATE needs explicit pending-request state and completion actions in
  the model, plus asynchronous request routing in the harness. A synchronous
  CREATE currently waits for an ACK that a later model action must send.
  Do not auto-ACK behind the model's back. The old exclusion incorrectly
  treated waiting for a handle-caching lease break as necessarily a server bug.
* Durable create-GUID collision precedence and replay eligibility/break timing
  still need protocol triage. Preserve the first divergent trace/state before
  interpreting downstream mismatches as independent bugs.
* Keep the durable, lease and replay probes until their model families pass.
  Streams and FSCTL effects also still need model transitions before their
  probes can be retired.

This is an incremental migration. The same model/fixture separation should be
applied to the remaining POSIX, FUSE, REST and S3 probes where their assertions
describe ordinary externally visible behavior.
