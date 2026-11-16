<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Apache-2.0
-->

# chimera-admin

Python SDK and command-line interface for the Chimera NAS REST API. The package
provides administrative resource commands and connection/worker inspection,
including live watch, multi-instance polling and an expandable terminal view.

## Install

Install a local checkout for development:

```sh
python3 -m pip install -e '.[dev]'
```

Install the package from this directory with `python3 -m pip install .`. To build a wheel, run `python3 -m pip install build && python3 -m build --wheel`; install the generated wheel from `dist/` when needed.

The installed command is `chimera-admin`. `python3 -m chimera_admin` and the
public `chimera_admin` SDK imports are also supported.

## SDK usage

```python
from chimera_admin import ChimeraAdminClient

with ChimeraAdminClient(host="localhost", port=8080) as client:
    print(client.get_version())
    print(client.get_config()["mounts"])
    print(client.list_users())
```

The SDK retains the existing create, list, get and delete methods for users,
exports, shares, buckets and mounts.

## Use

```sh
# Administrative resources use readable tables by default; --json preserves
# the REST response for scripts.
chimera-admin --host nas-server --port 8080 mount list
chimera-admin --host nas-server export get /demo --json

# One server or named independent instances.
chimera-admin --host nas-server inspect dashboard --watch
chimera-admin inspect dashboard --targets examples/targets.json --interactive

# Offline synthetic examples; no server is contacted.
chimera-admin inspect dashboard --input examples/snapshot.json
chimera-admin inspect dashboard --interactive --input examples/interactive-snapshot.json

# Optional two-instance synthetic watch: run the mock server twice in separate
# terminals, then use the checked-in demo targets.
python3 examples/mock_server.py --port 18080
python3 examples/mock_server.py --port 18081
chimera-admin inspect dashboard --targets examples/fleet-demo.json --watch
```

See [CLI and inspection documentation](docs/inspection.md) for command options,
interactive controls, filtering, quiet/wide output and synthetic examples. The
server REST schema and endpoint behavior are documented in the
[Chimera API reference](../../docs/api-reference.md).

## Compatibility

The SDK preserves the existing Chimera REST response structures and supports
Python 3.8 and newer. Administrative commands use the Chimera REST API v1.
Connection and worker inspection requires the optional `/api/v1/threads` and
`/api/v1/connections` endpoints, introduced with the corresponding server
inspection implementation. Compatibility is exercised against that API
implementation in the Chimera source checkout; there is not yet a separately
published minimum server release for those endpoints.

## Tests

```sh
python3 -m pip install -e '.[dev]'
python3 -m pytest
```

Most SDK, presentation and inspection tests use local fakes or checked-in
synthetic snapshots. Live NFS/SMB tests require a separately configured Chimera
server build and are skipped unless `CHIMERA_SERVER_BINARY` points to the
specific server executable to test. For example:

```sh
CHIMERA_SERVER_BINARY=/path/to/chimera/build/Debug/src/daemon/chimera \
    python3 -m pytest tests/test_api.py tests/test_e2e_nfs_xattr.py tests/test_e2e_smb.py
```

`chimera-admin inspect threads` reads the live worker endpoint. CPU%, RUNQ%,
BUSY%, RPC/s and RX/TX MB/s use deltas between consecutive samples; a one-shot
sample displays `--` for these rates because it has no baseline. BUSY% is the
event-loop iteration ratio, not a CPU measure. Poll-mode workers may show high
CPU% with low BUSY%. Read and Write latency uses the live VFS histograms.

`GET /api/v1/nfs/sessions` provides paged NFSv4.1+ session slot and negotiation
details (`limit`, `offset`, and `client_id`). The legacy `nfs4_sessions` array
in `GET /api/v1/threads?live=1&sessions=1` is deprecated. A full slot table can
mean slot saturation or a slow backend; compare against a client that
negotiates more slots before concluding the workload is slot-bound.
Use `chimera-admin inspect nfs sessions --slots SESSION_ID` to view the slot
states for one session. Session connection IDs and worker associations are not
yet exposed: connection binding stores the session pointer on each connection,
but the session does not retain a list of all its connections.
