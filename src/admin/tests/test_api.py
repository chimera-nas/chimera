# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Tests for the Chimera Admin REST API."""

import pytest
from chimera_admin import ChimeraAdminClient, ChimeraAdminError


class TestUsersAPI:
    """Test the Users API endpoints."""

    def test_list_users(self, client):
        """Test listing users."""
        users = client.list_users()
        assert isinstance(users, list)

    def test_get_user_not_found(self, client):
        """Test getting a nonexistent user."""
        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_user("nonexistent_user")
        assert exc_info.value.status_code == 404

    def test_create_get_delete_user(self, client):
        """Test creating, fetching, and deleting a user."""
        username = "sdk_test_user"
        try:
            client.create_user(username, uid=4242, gid=4242)

            user = client.get_user(username)
            assert user["username"] == username
            assert user["uid"] == 4242
            assert user["gid"] == 4242
            assert user["pinned"] is True
        finally:
            client.delete_user(username)

        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_user(username)
        assert exc_info.value.status_code == 404

    def test_create_user_defaults(self, client):
        """Test that omitting uid/gid defaults them to 0 on the server."""
        username = "sdk_test_min"
        try:
            client.create_user(username)

            user = client.get_user(username)
            assert user["uid"] == 0
            assert user["gid"] == 0
        finally:
            client.delete_user(username)

    def test_create_user_smbpasswd(self, client):
        """Test creating a user with an SMB password hash."""
        username = "sdk_test_smb"
        try:
            result = client.create_user(username, smbpasswd="aabbccdd")
            assert isinstance(result, dict)

            user = client.get_user(username)
            assert user["username"] == username
        finally:
            client.delete_user(username)

    def test_create_user_too_many_gids(self, client):
        """Test that more than 64 supplementary groups is rejected client-side."""
        with pytest.raises(ValueError):
            client.create_user("sdk_test_gids", gids=list(range(65)))


class TestExportsAPI:
    """Test the NFS Exports API endpoints."""

    def test_list_exports(self, client):
        """Test listing NFS exports."""
        exports = client.list_exports()
        assert isinstance(exports, list)

    def test_get_export_not_found(self, client):
        """Test getting a nonexistent export."""
        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_export("nonexistent_export")
        assert exc_info.value.status_code == 404


class TestSharesAPI:
    """Test the SMB Shares API endpoints."""

    def test_list_shares(self, client):
        """Test listing SMB shares."""
        shares = client.list_shares()
        assert isinstance(shares, list)

    def test_get_share_not_found(self, client):
        """Test getting a nonexistent share."""
        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_share("nonexistent_share")
        assert exc_info.value.status_code == 404


class TestBucketsAPI:
    """Test the S3 Buckets API endpoints."""

    def test_list_buckets(self, client):
        """Test listing S3 buckets."""
        buckets = client.list_buckets()
        assert isinstance(buckets, list)

    def test_get_bucket_not_found(self, client):
        """Test getting a nonexistent bucket."""
        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_bucket("nonexistent_bucket")
        assert exc_info.value.status_code == 404


class TestHTTPS:
    """Test HTTPS API endpoints with self-signed certificate."""

    def test_https_version(self, https_client):
        """Test getting version over HTTPS."""
        version = https_client.get_version()
        assert "version" in version

    def test_https_list_users(self, https_client):
        """Test listing users over HTTPS."""
        users = https_client.list_users()
        assert isinstance(users, list)

    def test_https_list_exports(self, https_client):
        """Test listing exports over HTTPS."""
        exports = https_client.list_exports()
        assert isinstance(exports, list)

    def test_https_list_shares(self, https_client):
        """Test listing shares over HTTPS."""
        shares = https_client.list_shares()
        assert isinstance(shares, list)

    def test_https_list_buckets(self, https_client):
        """Test listing buckets over HTTPS."""
        buckets = https_client.list_buckets()
        assert isinstance(buckets, list)
