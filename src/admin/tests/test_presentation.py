# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Human-readable and JSON output behavior for administrative commands."""

from contextlib import redirect_stdout
import io
import json
import unittest
from unittest.mock import patch

from chimera_admin.cli import main


class FakeClient:
    def list_mounts(self):
        return [{"name": "demo", "module": "memfs", "path": "demo"}]

    def get_export(self, name):
        return {"name": name, "path": "/demo", "export_id": 1, "access": "rw",
                "squash": "none", "anonuid": 65534, "anongid": 65534}

    def list_shares(self):
        return [{"name": "smb", "path": "/demo"}]

    def list_buckets(self):
        return [{"name": "objects", "path": "/demo"}]

    def list_users(self):
        return [{"username": "alice", "uid": 1000, "gid": 100, "gids": [20, 30], "pinned": True}]

    def close(self):
        pass


class AdministrativeOutput(unittest.TestCase):
    def run_cli(self, *args, width=120):
        output = io.StringIO()
        with patch("chimera_admin.cli._client", return_value=FakeClient()), \
                patch("chimera_admin.cli.terminal_width", return_value=width), redirect_stdout(output):
            self.assertEqual(main(list(args)), 0)
        return output.getvalue()

    def test_mount_list_defaults_to_table_and_preserves_relative_path(self):
        self.assertEqual(self.run_cli("mount", "list"), "NAME  MODULE  PATH\ndemo  memfs   demo\n")

    def test_export_get_table_has_stable_columns_and_values(self):
        output = self.run_cli("export", "get", "/demo")
        self.assertIn("NAME   PATH   EXPORT ID  ACCESS  SQUASH  ANON UID  ANON GID", output)
        self.assertRegex(output, r"/demo\s+/demo\s+1\s+rw\s+none\s+65534\s+65534")

    def test_all_resource_lists_use_tables(self):
        for command, header in (("share", "NAME"), ("bucket", "NAME"), ("user", "USERNAME")):
            with self.subTest(command=command):
                self.assertTrue(self.run_cli(command, "list").startswith(header))

    def test_explicit_json_is_the_original_api_value(self):
        for command, args, expected in (
            ("mount", ("list", "--json"), [{"name": "demo", "module": "memfs", "path": "demo"}]),
            ("export", ("get", "/demo", "--json"), FakeClient().get_export("/demo")),
        ):
            with self.subTest(command=command):
                self.assertEqual(json.loads(self.run_cli(command, *args)), expected)

    def test_quiet_hides_non_data_and_narrow_export_uses_details(self):
        self.assertEqual(self.run_cli("mount", "list", "--quiet"), "NAME  MODULE  PATH\ndemo  memfs   demo\n")
        narrow = self.run_cli("export", "get", "/demo", width=80)
        self.assertIn("ANONYMOUS ID MAPPING", narrow)
        self.assertIn("65534", narrow)


if __name__ == "__main__":
    unittest.main()
