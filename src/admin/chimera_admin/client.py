# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Chimera Admin Client - REST API client for Chimera server administration."""

import requests
from typing import Optional
from urllib.parse import urljoin


class ChimeraAdminError(Exception):
    """Base exception for Chimera Admin SDK errors."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class ChimeraAdminClient:
    """Client for interacting with the Chimera REST API.

    Args:
        host: The hostname or IP address of the Chimera server.
        port: The REST API port (default: 8080).
        timeout: Request timeout in seconds (default: 30).
        verify_ssl: Whether to verify SSL certificates (default: True).
        use_https: Whether to use HTTPS (default: False).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        timeout: int = 30,
        verify_ssl: bool = True,
        use_https: bool = False,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.use_https = use_https
        scheme = "https" if use_https else "http"
        self._base_url = f"{scheme}://{host}:{port}"
        self._session = requests.Session()

    @staticmethod
    def _http_error_message(e: requests.exceptions.HTTPError) -> str:
        """Build an error string that keeps the server's explanation.

        The server returns ``{"error": ..., "message": ...}`` bodies whose
        message distinguishes causes that share a status code (e.g. a 409
        for a duplicate export name vs a duplicate export_id). Reporting
        only the requests exception would discard that and leave the user
        guessing which validation failed.
        """
        if e.response is not None:
            try:
                message = e.response.json().get("message")
            except ValueError:
                message = None
            if message:
                return f"HTTP error: {e}: {message}"
        return f"HTTP error: {e}"

    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs,
    ) -> dict:
        """Make an HTTP request to the REST API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            endpoint: API endpoint path (e.g., "/version")
            **kwargs: Additional arguments passed to requests

        Returns:
            Parsed JSON response as a dictionary

        Raises:
            ChimeraAdminError: If the request fails
        """
        url = urljoin(self._base_url, endpoint)
        kwargs.setdefault("timeout", self.timeout)
        kwargs.setdefault("verify", self.verify_ssl)

        try:
            response = self._session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            raise ChimeraAdminError(f"Connection failed: {e}") from e
        except requests.exceptions.Timeout as e:
            raise ChimeraAdminError(f"Request timed out: {e}") from e
        except requests.exceptions.HTTPError as e:
            raise ChimeraAdminError(
                self._http_error_message(e),
                status_code=e.response.status_code
                if e.response is not None
                else None,
            ) from e
        except requests.exceptions.RequestException as e:
            raise ChimeraAdminError(f"Request failed: {e}") from e
        except ValueError as e:
            raise ChimeraAdminError(f"Invalid JSON response: {e}") from e

    def get_version(self) -> dict:
        """Get the Chimera server version.

        Returns:
            Dictionary containing version information, e.g.:
            {"version": "0.1.0"}

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/version")

    def get_openapi(self) -> dict:
        """Get the server's OpenAPI specification.

        Returns:
            The OpenAPI 3.0 document describing the REST API.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/api/openapi.json")

    def get_config(self) -> dict:
        """Get the running server configuration.

        Returns a JSON document compatible with the chimera.json file
        format, reconstructed from live runtime state. The "users" and
        "server" sections are intentionally omitted, and the internal
        "root" mount is excluded.

        Returns:
            Dictionary with "mounts", "exports", "shares", and "buckets"
            sections (each a mapping of resource name to its definition).

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/api/v1/config")

    # Users API

    def list_users(self) -> list:
        """List all builtin users.

        Returns:
            List of user dictionaries.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/api/v1/users")

    def get_user(self, username: str) -> dict:
        """Get a specific user by username.

        Args:
            username: The username to look up.

        Returns:
            User dictionary.

        Raises:
            ChimeraAdminError: If the request fails or user not found
        """
        return self._request("GET", f"/api/v1/users/{username}")

    def create_user(
        self,
        username: str,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        password: Optional[str] = None,
        smbpasswd: Optional[str] = None,
        gids: Optional[list] = None,
    ) -> dict:
        """Create a new builtin user.

        Args:
            username: Username for the new user (required).
            uid: User ID. Optional; the server defaults to 0 when omitted.
            gid: Primary group ID. Optional; the server defaults to 0 when
                omitted.
            password: Optional password.
            smbpasswd: Optional SMB/NTLM password hash.
            gids: Optional list of supplementary group IDs (at most 64).

        Returns:
            Response message.

        Raises:
            ValueError: If more than 64 supplementary group IDs are given.
            ChimeraAdminError: If the request fails (e.g. 400 for missing or
                invalid fields, 500 if the server fails to create the user).
        """
        if gids is not None and len(gids) > 64:
            raise ValueError("at most 64 supplementary groups are allowed")

        data = {"username": username}
        if uid is not None:
            data["uid"] = uid
        if gid is not None:
            data["gid"] = gid
        if password:
            data["password"] = password
        if smbpasswd:
            data["smbpasswd"] = smbpasswd
        if gids is not None:
            data["gids"] = gids
        return self._request("POST", "/api/v1/users", json=data)

    def delete_user(self, username: str) -> None:
        """Delete a user.

        Args:
            username: Username to delete.

        Raises:
            ChimeraAdminError: If the request fails or user not found
        """
        self._request_no_content("DELETE", f"/api/v1/users/{username}")

    # NFS Exports API

    def list_exports(self) -> list:
        """List all NFS exports.

        Returns:
            List of export dictionaries.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/api/v1/exports")

    def get_export(self, name: str) -> dict:
        """Get a specific NFS export by name.

        Args:
            name: The export name to look up.

        Returns:
            Export dictionary.

        Raises:
            ChimeraAdminError: If the request fails or export not found
        """
        return self._request("GET", f"/api/v1/exports/{name}")

    def create_export(
        self,
        name: str,
        path: str,
        export_id: Optional[int] = None,
        access: Optional[str] = None,
        squash: Optional[str] = None,
        anonuid: Optional[int] = None,
        anongid: Optional[int] = None,
        sec: Optional[list] = None,
    ) -> dict:
        """Create a new NFS export.

        Args:
            name: Export name.
            path: VFS path for the export.
            export_id: Optional stable export id (1-65535), embedded in NFS
                file handles; auto-assigned when omitted. Clustered servers
                exporting the same directory must pin the same id so file
                handles stay valid across failover.
            access: Optional access mode, "ro" or "rw" (server default: rw).
            squash: Optional squash policy, "none", "root", or "all"
                (server default: none).
            anonuid: Optional anonymous uid squashed callers are mapped to
                (server default: 65534).
            anongid: Optional anonymous gid squashed callers are mapped to
                (server default: 65534).
            sec: Optional list of allowed RPC security flavors ("sys",
                "krb5", "krb5i", "krb5p"); other flavors are rejected.
                Omitted or empty permits any flavor (the server default).

        Returns:
            Response message.

        Raises:
            ChimeraAdminError: If the request fails (e.g. 400 for an
                out-of-range export_id or an unknown sec flavor, 409 if the
                name or export_id is already in use).
        """
        data = {"name": name, "path": path}
        if export_id is not None:
            data["export_id"] = export_id
        if access is not None:
            data["access"] = access
        if squash is not None:
            data["squash"] = squash
        if anonuid is not None:
            data["anonuid"] = anonuid
        if anongid is not None:
            data["anongid"] = anongid
        if sec is not None:
            data["sec"] = sec
        return self._request("POST", "/api/v1/exports", json=data)

    def delete_export(self, name: str) -> None:
        """Delete an NFS export.

        Args:
            name: Export name to delete.

        Raises:
            ChimeraAdminError: If the request fails or export not found
        """
        self._request_no_content("DELETE", f"/api/v1/exports/{name}")

    # SMB Shares API

    def list_shares(self) -> list:
        """List all SMB shares.

        Returns:
            List of share dictionaries.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/api/v1/shares")

    def get_share(self, name: str) -> dict:
        """Get a specific SMB share by name.

        Args:
            name: The share name to look up.

        Returns:
            Share dictionary.

        Raises:
            ChimeraAdminError: If the request fails or share not found
        """
        return self._request("GET", f"/api/v1/shares/{name}")

    def create_share(self, name: str, path: str) -> dict:
        """Create a new SMB share.

        Args:
            name: Share name.
            path: VFS path for the share.

        Returns:
            Response message.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request(
            "POST", "/api/v1/shares", json={"name": name, "path": path}
        )

    def delete_share(self, name: str) -> None:
        """Delete an SMB share.

        Args:
            name: Share name to delete.

        Raises:
            ChimeraAdminError: If the request fails or share not found
        """
        self._request_no_content("DELETE", f"/api/v1/shares/{name}")

    # S3 Buckets API

    def list_buckets(self) -> list:
        """List all S3 buckets.

        Returns:
            List of bucket dictionaries.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/api/v1/buckets")

    def get_bucket(self, name: str) -> dict:
        """Get a specific S3 bucket by name.

        Args:
            name: The bucket name to look up.

        Returns:
            Bucket dictionary.

        Raises:
            ChimeraAdminError: If the request fails or bucket not found
        """
        return self._request("GET", f"/api/v1/buckets/{name}")

    def create_bucket(self, name: str, path: str) -> dict:
        """Create a new S3 bucket.

        Args:
            name: Bucket name.
            path: VFS path for the bucket.

        Returns:
            Response message.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request(
            "POST", "/api/v1/buckets", json={"name": name, "path": path}
        )

    def delete_bucket(self, name: str) -> None:
        """Delete an S3 bucket.

        Args:
            name: Bucket name to delete.

        Raises:
            ChimeraAdminError: If the request fails or bucket not found
        """
        self._request_no_content("DELETE", f"/api/v1/buckets/{name}")

    # VFS Mounts API

    def list_mounts(self) -> list:
        """List all VFS mounts.

        Returns:
            List of mount dictionaries, each with "name", "module", and
            "path" keys, plus an "options" key for mounts created with
            options.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request("GET", "/api/v1/mounts")

    def get_mount(self, name: str) -> dict:
        """Get a specific VFS mount by name.

        Args:
            name: The mount name to look up. Leading slashes are normalized
                away server-side to match how the mount was registered.

        Returns:
            Mount dictionary with "name", "module", and "path" keys, plus an
            "options" key if the mount was created with options.

        Raises:
            ChimeraAdminError: If the request fails or mount not found
        """
        return self._request("GET", f"/api/v1/mounts/{name}")

    def create_mount(
        self,
        name: str,
        module: str,
        path: str,
        options: Optional[str] = None,
    ) -> dict:
        """Create a new VFS mount.

        Args:
            name: Mount name (the VFS mount path).
            module: VFS module backing the mount (e.g. "linux", "memfs").
            path: Backing path passed to the module.
            options: Optional module-specific options string. When set, it is
                stored on the mount and echoed back by get_mount/list_mounts
                and in the server config.

        Returns:
            Response message.

        Raises:
            ChimeraAdminError: If the request fails (e.g. 400 for missing
                fields or a malformed options string, 500 if the server fails
                to create the mount).
        """
        data = {"name": name, "module": module, "path": path}
        if options is not None:
            data["options"] = options
        return self._request("POST", "/api/v1/mounts", json=data)

    def delete_mount(self, name: str) -> None:
        """Delete a VFS mount.

        Args:
            name: Mount name to delete.

        Raises:
            ChimeraAdminError: If the request fails, the mount is not found
                (404), or the mount is still in use by a share, export, or
                bucket (409).
        """
        self._request_no_content("DELETE", f"/api/v1/mounts/{name}")

    # Named filesystems API

    def create_filesystem(
        self,
        module: str,
        name: str,
        options: Optional[str] = None,
    ) -> dict:
        """Create a named filesystem inside a mkfs-capable VFS module.

        The filesystem is then mountable with a mount path of
        "<name>[/subdir]".

        Args:
            module: VFS module hosting the filesystem (e.g. "memfs",
                "diskfs", "cairn").
            name: Filesystem name.
            options: Optional module-specific options string.

        Returns:
            Response message.

        Raises:
            ChimeraAdminError: If the request fails (e.g. 400 for a module
                without filesystem support, 409 if the name is taken).
        """
        data = {"module": module, "name": name}
        if options is not None:
            data["options"] = options
        return self._request("POST", "/api/v1/filesystems", json=data)

    def delete_filesystem(self, module: str, name: str) -> None:
        """Delete a named filesystem.

        Args:
            module: VFS module hosting the filesystem.
            name: Filesystem name to delete.

        Raises:
            ChimeraAdminError: If the request fails, the filesystem is not
                found (404), or it still has active mounts (409).
        """
        self._request_no_content(
            "DELETE", f"/api/v1/filesystems/{module}/{name}"
        )

    def _request_no_content(
        self,
        method: str,
        endpoint: str,
        **kwargs,
    ) -> None:
        """Make an HTTP request that expects no content (204)."""
        url = urljoin(self._base_url, endpoint)
        kwargs.setdefault("timeout", self.timeout)
        kwargs.setdefault("verify", self.verify_ssl)

        try:
            response = self._session.request(method, url, **kwargs)
            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            raise ChimeraAdminError(f"Connection failed: {e}") from e
        except requests.exceptions.Timeout as e:
            raise ChimeraAdminError(f"Request timed out: {e}") from e
        except requests.exceptions.HTTPError as e:
            raise ChimeraAdminError(
                self._http_error_message(e),
                status_code=e.response.status_code
                if e.response is not None
                else None,
            ) from e
        except requests.exceptions.RequestException as e:
            raise ChimeraAdminError(f"Request failed: {e}") from e

    def close(self):
        """Close the client session."""
        self._session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
