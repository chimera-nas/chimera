<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Apache-2.0
-->

# Chimera Admin SDK

Python SDK for the Chimera NAS REST API.

## Installation

```bash
pip install -e src/admin
```

## Usage

```python
from chimera_admin import ChimeraAdminClient

# Create a client
client = ChimeraAdminClient(host="localhost", port=8080)

# Get server version
version_info = client.get_version()
print(f"Server version: {version_info['version']}")

# Using context manager
with ChimeraAdminClient(host="localhost", port=8080) as client:
    print(client.get_version())
```

### User management

```python
with ChimeraAdminClient(host="localhost", port=8080) as client:
    # uid/gid default to 0 when omitted; gids is capped at 64 entries
    client.create_user("alice", uid=1000, gid=1000, gids=[27, 44])

    # A minimal user (uid/gid default to 0)
    client.create_user("svc")

    print(client.list_users())
    print(client.get_user("alice"))

    client.delete_user("svc")
```

## Configuration

The REST API must be enabled in the Chimera server configuration:

```json
{
    "server": {
        "rest_http_port": 8080
    }
}
```

Set `rest_http_port` to 0 (the default) to disable the REST API.

## Running Tests

```bash
cd src/admin
pip install -e ".[dev]"
pytest
```
