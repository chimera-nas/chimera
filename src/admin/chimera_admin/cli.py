# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for the Chimera NAS REST API.

Run as a module:

    python3 -m chimera_admin [--host H] [--port P] [--https] <resource> <action> ...

Examples:

    python3 -m chimera_admin version
    python3 -m chimera_admin config
    python3 -m chimera_admin mount list
    python3 -m chimera_admin mount create data --module linux --path /srv/data
    python3 -m chimera_admin user create alice --uid 1000 --gid 1000
"""

import argparse
import json
import sys

from .client import ChimeraAdminClient, ChimeraAdminError


def _client(args) -> ChimeraAdminClient:
    """Build a client from parsed global arguments."""
    return ChimeraAdminClient(
        host=args.host,
        port=args.port,
        timeout=args.timeout,
        verify_ssl=not args.no_verify_ssl,
        use_https=args.https,
    )


def _delete_and_msg(method, ident: str, noun: str) -> str:
    """Run a delete method and return a confirmation message."""
    method(ident)
    return f"{noun} '{ident}' deleted"


def _add_crud_commands(sub, noun: str, label: str) -> None:
    """Add list/get/create/delete commands for a name+path resource.

    Covers exports, shares, and buckets, which share an identical REST
    shape. Client methods are resolved by convention: ``list_<noun>s``,
    ``get_<noun>``, ``create_<noun>``, ``delete_<noun>``.
    """
    plural = noun + "s"
    res = sub.add_parser(noun, help=f"Manage {label}s")
    actions = res.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help=f"List {label}s")
    p.set_defaults(func=lambda c, a: getattr(c, f"list_{plural}")())

    p = actions.add_parser("get", help=f"Get a {label}")
    p.add_argument("name")
    p.set_defaults(func=lambda c, a: getattr(c, f"get_{noun}")(a.name))

    p = actions.add_parser("create", help=f"Create a {label}")
    p.add_argument("name")
    p.add_argument("path", help="VFS path for the resource")
    p.set_defaults(
        func=lambda c, a: getattr(c, f"create_{noun}")(a.name, a.path))

    p = actions.add_parser("delete", help=f"Delete a {label}")
    p.add_argument("name")
    p.set_defaults(
        func=lambda c, a: _delete_and_msg(
            getattr(c, f"delete_{noun}"), a.name, noun))


def _add_user_commands(sub) -> None:
    """Add the ``user`` resource commands."""
    res = sub.add_parser("user", help="Manage builtin users")
    actions = res.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="List users")
    p.set_defaults(func=lambda c, a: c.list_users())

    p = actions.add_parser("get", help="Get a user")
    p.add_argument("username")
    p.set_defaults(func=lambda c, a: c.get_user(a.username))

    p = actions.add_parser("create", help="Create a user")
    p.add_argument("username")
    p.add_argument("--uid", type=int, help="User ID (default: 0)")
    p.add_argument("--gid", type=int, help="Primary group ID (default: 0)")
    p.add_argument("--password", help="Plaintext password")
    p.add_argument("--smbpasswd", help="SMB/NTLM password hash")
    p.add_argument(
        "--supplementary-gid", type=int, action="append", dest="gids",
        metavar="GID", help="Supplementary group ID (repeatable, max 64)")
    p.set_defaults(func=lambda c, a: c.create_user(
        a.username, uid=a.uid, gid=a.gid, password=a.password,
        smbpasswd=a.smbpasswd, gids=a.gids))

    p = actions.add_parser("delete", help="Delete a user")
    p.add_argument("username")
    p.set_defaults(
        func=lambda c, a: _delete_and_msg(
            c.delete_user, a.username, "user"))


def _add_mount_commands(sub) -> None:
    """Add the ``mount`` resource commands."""
    res = sub.add_parser("mount", help="Manage VFS mounts")
    actions = res.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="List VFS mounts")
    p.set_defaults(func=lambda c, a: c.list_mounts())

    p = actions.add_parser("get", help="Get a VFS mount")
    p.add_argument("name")
    p.set_defaults(func=lambda c, a: c.get_mount(a.name))

    p = actions.add_parser("create", help="Create a VFS mount")
    p.add_argument("name")
    p.add_argument(
        "--module", required=True, help="VFS module (e.g. linux, memfs)")
    p.add_argument(
        "--path", required=True, help="Backing path for the module")
    p.add_argument("--options", help="Module-specific options string")
    p.set_defaults(func=lambda c, a: c.create_mount(
        a.name, module=a.module, path=a.path, options=a.options))

    p = actions.add_parser("delete", help="Delete a VFS mount")
    p.add_argument("name")
    p.set_defaults(
        func=lambda c, a: _delete_and_msg(c.delete_mount, a.name, "mount"))


def _build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="python3 -m chimera_admin",
        description="Command-line client for the Chimera NAS REST API.")
    parser.add_argument(
        "--host", default="localhost",
        help="Server hostname or IP (default: localhost)")
    parser.add_argument(
        "--port", type=int, default=8080,
        help="REST API port (default: 8080)")
    parser.add_argument(
        "--https", action="store_true", help="Use HTTPS instead of HTTP")
    parser.add_argument(
        "--no-verify-ssl", action="store_true",
        help="Do not verify TLS certificates")
    parser.add_argument(
        "--timeout", type=int, default=30,
        help="Request timeout in seconds (default: 30)")

    sub = parser.add_subparsers(dest="resource", required=True)

    p = sub.add_parser("version", help="Show server version")
    p.set_defaults(func=lambda c, a: c.get_version())

    p = sub.add_parser(
        "config", help="Show server configuration (chimera.json format)")
    p.set_defaults(func=lambda c, a: c.get_config())

    _add_user_commands(sub)
    _add_crud_commands(sub, "export", "NFS export")
    _add_crud_commands(sub, "share", "SMB share")
    _add_crud_commands(sub, "bucket", "S3 bucket")
    _add_mount_commands(sub)

    return parser


def _emit(result) -> None:
    """Print a handler result: strings as-is, everything else as JSON."""
    if result is None:
        return
    if isinstance(result, str):
        print(result)
    else:
        # Preserve the server's key ordering (e.g. config emits mounts before
        # exports/shares/buckets) rather than alphabetizing it.
        print(json.dumps(result, indent=2, sort_keys=False))


def main(argv=None) -> int:
    """Entry point for ``python3 -m chimera_admin``."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    client = _client(args)
    try:
        result = args.func(client, args)
    except ChimeraAdminError as e:
        msg = str(e)
        if e.status_code is not None:
            msg = f"{msg} (HTTP {e.status_code})"
        print(f"error: {msg}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    finally:
        client.close()

    _emit(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
