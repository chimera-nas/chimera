<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Unlicense
-->

# Quint model-based conformance test for the portmap server

This directory is a case study in model-based testing: a formal
[Quint](https://quint-lang.org) model of chimera's portmap/rpcbind server
acts as the test oracle, and the ctest `chimera/server/nfs/portmap_quint`
replays traces generated from that model against a live chimera daemon,
diffing every RPC reply against the reply the model predicted.

## Pieces

- `portmap.qnt` — the model.  Chimera's portmapper is a static registry
  (no `PMAPPROC_SET`/`UNSET`), so each procedure is modeled as a pure
  request → reply function over the `SERVICES` table; the only state is
  the last `(call, reply)` event.  Covers PMAP v2 NULL/GETPORT/DUMP and
  rpcbind v3/v4 GETADDR.  The `repliesExact` invariant and the scenario
  tests pin the semantics chimera deliberately chose: GETPORT/GETADDR
  match the *exact* `(prog, vers, prot)` triple or return 0 — unlike
  classic BSD portmap, which falls back to the port of some other
  registered version of the program on a version mismatch.
- `traces/*.itf.json` — pre-generated random traces (Informal Trace
  Format), checked in so the ctest is hermetic: CI needs only python3,
  not quint or node.
- `portmap_quint_replay.py` — a pure-stdlib ONC RPC client that replays
  a trace's calls over TCP and compares replies.
- `portmap_quint_test_wrapper.sh` — the ctest entry point: boots a
  chimera daemon (memfs backend) in a private network namespace and runs
  the replayer against it.  Exits 77 (skip) where netns/root/python3 are
  unavailable.

## Running

```sh
cd build/Debug && ctest -R portmap_quint --output-on-failure

# Additionally generate + replay a fresh randomized trace (needs quint or npx):
CHIMERA_PORTMAP_QUINT_LIVE=1 ctest -R portmap_quint --output-on-failure
```

Model-only checks (no server involved):

```sh
quint test portmap.qnt
quint run portmap.qnt --invariant=repliesExact
```

## Keeping the model in sync

`SERVICES` in `portmap.qnt` must mirror the table chimera builds in
`nfs_portmap.c` with `external_portmap=false` and the default
`lockmgr_port` (32803) and NSM port (32765) from `server.c`.  If you add
a service, change a port, or change lookup semantics, update the model
accordingly and regenerate the traces:

```sh
./regen_traces.sh
```

A failing replay means the implementation and the model disagree; decide
which one is right before "fixing" either side.  The divergence report
names the trace state, the call arguments, and both replies.
