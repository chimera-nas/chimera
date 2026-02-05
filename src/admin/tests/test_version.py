# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""Tests for the /version endpoint."""

import pytest

from chimera_admin import ChimeraAdminClient
from chimera_admin.client import ChimeraAdminError


class TestVersion:
    """Tests for the version API."""

    def test_get_version(self, chimera_server):
        """Test that we can retrieve the server version."""
        host, port = chimera_server

        with ChimeraAdminClient(host=host, port=port) as client:
            result = client.get_version()

        assert "version" in result
        assert isinstance(result["version"], str)
        assert len(result["version"]) > 0

    def test_get_version_value(self, chimera_server):
        """Test that the version matches the expected format."""
        host, port = chimera_server

        with ChimeraAdminClient(host=host, port=port) as client:
            result = client.get_version()

        # Version should be in semver format (x.y.z)
        version = result["version"]
        parts = version.split(".")
        assert len(parts) >= 2, f"Version '{version}' should have at least major.minor"

    def test_connection_error(self):
        """Test that connection errors are properly raised."""
        # Try to connect to a port that's not running
        with ChimeraAdminClient(host="127.0.0.1", port=19999, timeout=1) as client:
            with pytest.raises(ChimeraAdminError) as exc_info:
                client.get_version()

        assert "Connection failed" in str(exc_info.value)
