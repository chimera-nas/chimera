# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Small, dependency-free formatting helpers for human-readable CLI output."""

import json
import shutil
import unicodedata


def value_text(value):
    if value is None:
        return "--"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        if isinstance(value, list) and all(not isinstance(item, (dict, list)) for item in value):
            return ", ".join(value_text(item) for item in value) or "--"
        return json.dumps(value, ensure_ascii=False, separators=(",", ": "))
    return "".join(char if char.isprintable() else "\\x{:02x}".format(ord(char))
                   for char in str(value))


def display_width(value):
    """Return printable terminal width, ignoring ANSI control sequences."""
    return sum(0 if unicodedata.combining(char) else
               2 if unicodedata.east_asian_width(char) in ("W", "F") else 1
               for char in str(value))


def table(headers, rows, numeric=(), width=None, spacing=2):
    """Render left-aligned text and right-aligned numeric columns."""
    rows = [[value_text(item) for item in row] for row in rows]
    widths = [max([display_width(headers[i])] + [display_width(row[i]) for row in rows])
              for i in range(len(headers))]
    sep = " " * spacing
    lines = [sep.join(headers[i] + " " * max(0, widths[i] - display_width(headers[i]))
                      for i in range(len(headers))).rstrip()]
    for row in rows:
        cells = []
        for i, item in enumerate(row):
            padding = " " * max(0, widths[i] - display_width(item))
            cells.append(padding + item if i in numeric else item + padding)
        lines.append(sep.join(cells).rstrip())
    if not rows:
        lines.append("(none)")
    return "\n".join(lines)


def terminal_width(default=120):
    return shutil.get_terminal_size((default, 24)).columns
