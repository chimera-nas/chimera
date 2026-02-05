# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

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
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        timeout: int = 30,
        verify_ssl: bool = True,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self._base_url = f"http://{host}:{port}"
        self._session = requests.Session()

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
                f"HTTP error: {e}",
                status_code=e.response.status_code if e.response else None,
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
        uid: int,
        gid: int,
        password: Optional[str] = None,
        gids: Optional[list] = None,
    ) -> dict:
        """Create a new builtin user.

        Args:
            username: Username for the new user.
            uid: User ID.
            gid: Primary group ID.
            password: Optional password.
            gids: Optional list of supplementary group IDs.

        Returns:
            Response message.

        Raises:
            ChimeraAdminError: If the request fails
        """
        data = {"username": username, "uid": uid, "gid": gid}
        if password:
            data["password"] = password
        if gids:
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

    def create_export(self, name: str, path: str) -> dict:
        """Create a new NFS export.

        Args:
            name: Export name.
            path: VFS path for the export.

        Returns:
            Response message.

        Raises:
            ChimeraAdminError: If the request fails
        """
        return self._request(
            "POST", "/api/v1/exports", json={"name": name, "path": path}
        )

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
                f"HTTP error: {e}",
                status_code=e.response.status_code if e.response else None,
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
