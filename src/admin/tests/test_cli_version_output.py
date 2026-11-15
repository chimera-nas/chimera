# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Human and scripting output for the CLI and server version commands."""

from contextlib import redirect_stdout
import io
import json
import unittest
from unittest.mock import patch

from chimera_admin import __version__
from chimera_admin.cli import main


class VersionOutput(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.client = type("Client", (), {
            "get_version": lambda self: {"version": "0.1.0"},
            "close": lambda self: None,
        })()

    def run_cli(self, *args):
        with patch("chimera_admin.cli._client", return_value=self.client), redirect_stdout(self.output):
            self.assertEqual(main(list(args)), 0)

    def test_server_version_is_human_readable_by_default(self):
        self.run_cli("version")
        self.assertEqual(self.output.getvalue(), "Chimera server version: 0.1.0\n")

    def test_server_version_can_return_original_json(self):
        self.run_cli("version", "--json")
        self.assertEqual(json.loads(self.output.getvalue()), {"version": "0.1.0"})

    def test_cli_version_flag_does_not_contact_a_server(self):
        with patch("chimera_admin.cli._client") as client, redirect_stdout(self.output):
            with self.assertRaises(SystemExit) as result:
                main(["-V"])
        self.assertEqual(result.exception.code, 0)
        self.assertEqual(self.output.getvalue(), "chimera-admin {}\n".format(__version__))
        client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
