# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

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
