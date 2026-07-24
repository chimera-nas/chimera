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

    # Every export name these tests create, for the pre-clean fixture below.
    _TEST_EXPORTS = [
        "sdk_test_export",
        "sdk_test_export_id",
        "sdk_test_export_access",
        "sdk_test_export_anon",
        "sdk_test_export_sec",
        "sdk_test_export_dup",
        "sdk_test_export_dup2",
        "sdk_test_export_bad",
        "sdk_test_export_bad_ac",
        "sdk_test_export_auto1",
        "sdk_test_export_pinned",
        "sdk_test_export_auto2",
        "sdk_test_export_free",
        "sdk_test_export_reuse",
    ]

    @pytest.fixture(autouse=True)
    def _clean_exports(self, client):
        """Best-effort removal of leftovers from a previous killed run.

        The tests pin fixed names and export ids (4000-4004); a run killed
        before its finally-blocks would otherwise leave exports behind and
        turn every later run into 409 conflicts until the server restarts.
        """
        for name in self._TEST_EXPORTS:
            try:
                client.delete_export(name)
            except ChimeraAdminError:
                pass
        yield

    def test_list_exports(self, client):
        """Test listing NFS exports."""
        exports = client.list_exports()
        assert isinstance(exports, list)

    def test_get_export_not_found(self, client):
        """Test getting a nonexistent export."""
        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_export("nonexistent_export")
        assert exc_info.value.status_code == 404

    def test_create_get_delete_export(self, client):
        """Test creating, fetching, and deleting an export."""
        name = "sdk_test_export"
        try:
            client.create_export(name, "/testshare")

            export = client.get_export(name)
            assert export["name"] == name
            assert export["path"] == "/testshare"
            # Auto-assigned export ids start at 1; 0 is reserved.
            assert export["export_id"] >= 1
        finally:
            client.delete_export(name)

        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_export(name)
        assert exc_info.value.status_code == 404

    def test_create_export_id_round_trip(self, client):
        """An explicit export_id is echoed back everywhere."""
        name = "sdk_test_export_id"
        export_id = 4000
        try:
            client.create_export(name, "/testshare", export_id=export_id)

            # get_export echoes the id.
            export = client.get_export(name)
            assert export["export_id"] == export_id

            # list_exports carries it on the matching entry.
            listed = next(
                e for e in client.list_exports() if e["name"] == name)
            assert listed["export_id"] == export_id

            # The running config round-trips it for chimera.json.
            config = client.get_config()
            assert config["exports"][name]["export_id"] == export_id
        finally:
            client.delete_export(name)

    def test_create_export_access_round_trip(self, client):
        """Access control given at create time is echoed back."""
        name = "sdk_test_export_access"
        try:
            client.create_export(
                name, "/testshare", export_id=4003,
                access="ro", squash="none")

            export = client.get_export(name)
            assert export["export_id"] == 4003
            assert export["access"] == "ro"
            assert export["squash"] == "none"

            # The running config round-trips them for chimera.json.
            config = client.get_config()
            assert config["exports"][name]["access"] == "ro"
            assert config["exports"][name]["squash"] == "none"
        finally:
            client.delete_export(name)

    def test_create_export_anon_ids_round_trip(self, client):
        """anonuid/anongid given at create time are echoed back."""
        name = "sdk_test_export_anon"
        try:
            client.create_export(
                name, "/testshare", export_id=4004,
                squash="all", anonuid=1234, anongid=5678)

            export = client.get_export(name)
            assert export["squash"] == "all"
            assert export["anonuid"] == 1234
            assert export["anongid"] == 5678
        finally:
            client.delete_export(name)

    def test_create_export_sec_round_trip(self, client):
        """A sec restriction given at create time is echoed back."""
        name = "sdk_test_export_sec"
        try:
            client.create_export(name, "/testshare", sec=["krb5", "krb5i"])

            export = client.get_export(name)
            assert export["sec"] == ["krb5", "krb5i"]

            # The running config round-trips it for chimera.json.
            config = client.get_config()
            assert config["exports"][name]["sec"] == ["krb5", "krb5i"]
        finally:
            client.delete_export(name)

    def test_create_export_invalid_sec(self, client):
        """Unknown sec flavors are rejected with 400, not widened to any."""
        name = "sdk_test_export_sec"
        with pytest.raises(ChimeraAdminError) as exc_info:
            client.create_export(name, "/testshare", sec=["krb5x"])
        assert exc_info.value.status_code == 400

        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_export(name)
        assert exc_info.value.status_code == 404

    def test_create_export_duplicate_id(self, client):
        """A second export reusing an export_id is rejected with 409."""
        name = "sdk_test_export_dup"
        other = "sdk_test_export_dup2"
        export_id = 4001
        try:
            client.create_export(name, "/testshare", export_id=export_id)

            with pytest.raises(ChimeraAdminError) as exc_info:
                client.create_export(other, "/testshare",
                                     export_id=export_id)
            assert exc_info.value.status_code == 409

            # The rejected export must not have been created.
            with pytest.raises(ChimeraAdminError) as exc_info:
                client.get_export(other)
            assert exc_info.value.status_code == 404
        finally:
            client.delete_export(name)

    def test_create_export_invalid_id(self, client):
        """Out-of-range export ids are rejected with 400."""
        for bad_id in (0, 65536, -5):
            with pytest.raises(ChimeraAdminError) as exc_info:
                client.create_export(
                    "sdk_test_export_bad", "/testshare", export_id=bad_id)
            assert exc_info.value.status_code == 400

        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_export("sdk_test_export_bad")
        assert exc_info.value.status_code == 404

    def test_create_export_invalid_access_control(self, client):
        """Unrecognized access/squash values are rejected with 400 rather
        than silently replaced with the (more permissive) defaults."""
        name = "sdk_test_export_bad_ac"
        for bad_kwargs in (
            {"access": "readonly"},
            {"squash": "rootsquash"},
        ):
            with pytest.raises(ChimeraAdminError) as exc_info:
                client.create_export(name, "/testshare", **bad_kwargs)
            assert exc_info.value.status_code == 400

        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_export(name)
        assert exc_info.value.status_code == 404

    def test_auto_assign_skips_explicit_id(self, client):
        """Auto-assignment never reuses a slot pinned explicitly."""
        auto1 = "sdk_test_export_auto1"
        pinned = "sdk_test_export_pinned"
        auto2 = "sdk_test_export_auto2"
        created = []
        try:
            client.create_export(auto1, "/testshare")
            created.append(auto1)
            base = client.get_export(auto1)["export_id"]
            if base + 2 > 65535:
                pytest.skip("export id space nearly exhausted")

            client.create_export(pinned, "/testshare", export_id=base + 1)
            created.append(pinned)

            client.create_export(auto2, "/testshare")
            created.append(auto2)
            assert client.get_export(auto2)["export_id"] == base + 2
        finally:
            for name in created:
                client.delete_export(name)

    def test_export_id_reusable_after_delete(self, client):
        """Deleting an export frees its id for a new export."""
        export_id = 4002
        client.create_export(
            "sdk_test_export_free", "/testshare", export_id=export_id)
        client.delete_export("sdk_test_export_free")

        name = "sdk_test_export_reuse"
        try:
            client.create_export(name, "/testshare", export_id=export_id)
            assert client.get_export(name)["export_id"] == export_id
        finally:
            client.delete_export(name)


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


class TestMountsAPI:
    """Test the VFS Mounts API endpoints."""

    def test_list_mounts(self, client):
        """Test listing VFS mounts includes the configured mount."""
        mounts = client.list_mounts()
        assert isinstance(mounts, list)
        # The test config defines a "testshare" memfs mount.
        assert any(m["name"] == "testshare" for m in mounts)

    def test_get_mount(self, client):
        """Test fetching the configured mount."""
        mount = client.get_mount("testshare")
        assert mount["name"] == "testshare"
        assert mount["module"] == "memfs"

    def test_get_mount_not_found(self, client):
        """Test getting a nonexistent mount."""
        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_mount("nonexistent_mount")
        assert exc_info.value.status_code == 404

    def test_create_get_delete_mount(self, client):
        """Test creating, fetching, and deleting a mount."""
        name = "sdk_test_mount"
        try:
            client.create_mount(name, module="memfs", path="/")

            mount = client.get_mount(name)
            assert mount["name"] == name
            assert mount["module"] == "memfs"
        finally:
            client.delete_mount(name)

        with pytest.raises(ChimeraAdminError) as exc_info:
            client.get_mount(name)
        assert exc_info.value.status_code == 404

    def test_create_mount_options_round_trip(self, client):
        """Options given at create time are echoed back everywhere."""
        name = "sdk_test_mount_opts"
        options = "ro,foo=bar"
        try:
            client.create_mount(
                name, module="memfs", path="/", options=options)

            # get_mount echoes the options.
            mount = client.get_mount(name)
            assert mount["options"] == options

            # list_mounts carries them on the matching entry.
            listed = next(
                m for m in client.list_mounts() if m["name"] == name)
            assert listed["options"] == options

            # The running config round-trips them for chimera.json.
            config = client.get_config()
            assert config["mounts"][name]["options"] == options
        finally:
            client.delete_mount(name)

    def test_create_mount_invalid_options(self, client):
        """A malformed options string is rejected with HTTP 400."""
        # A leading '=' has an empty key, which the parser rejects.
        with pytest.raises(ChimeraAdminError) as exc_info:
            client._request(
                "POST", "/api/v1/mounts",
                json={
                    "name": "sdk_test_mount_bad",
                    "module": "memfs",
                    "path": "/",
                    "options": "=noKey",
                })
        assert exc_info.value.status_code == 400


class TestConfigAPI:
    """Test the server configuration endpoint."""

    def test_get_config(self, client):
        """Test fetching the running configuration."""
        config = client.get_config()
        assert isinstance(config, dict)
        # The collection sections are always present (empty when unset).
        assert all(
            k in config for k in ("mounts", "exports", "shares", "buckets"))
        # The "users" and "server" sections are intentionally omitted.
        assert "users" not in config
        assert "server" not in config
        # The test config defines a "testshare" memfs mount; the internal
        # "root" mount must not be reported.
        assert "testshare" in config["mounts"]
        assert config["mounts"]["testshare"]["module"] == "memfs"
        assert "root" not in config["mounts"]


# TODO: Re-enable once the self-signed certificate issue is resolved. The
# daemon currently fails to load its auto-generated cert during TLS init
# (ext/libevpl tls), so the HTTPS listener never comes up in this environment.
@pytest.mark.skip(
    reason="HTTPS disabled until the self-signed certificate issue is resolved")
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

    def test_https_list_mounts(self, https_client):
        """Test listing mounts over HTTPS."""
        mounts = https_client.list_mounts()
        assert isinstance(mounts, list)
