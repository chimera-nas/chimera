#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense

"""Keep the VFS executor's operation vocabulary out of production consumers."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEXEMES = re.compile(r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', re.S)


def scrub(source, strings=False):
    def replace(match):
        token = match.group()
        if not strings and not token.startswith("/"):
            return token
        return "".join("\n" if char == "\n" else " " for char in token)

    return LEXEMES.sub(replace, source)


private_headers = ("vfs_internal_procs.h", "vfs_lock_internal.h")
private = {"CHIMERA_VFS_INTERNAL_API"}
for name in private_headers:
    header = scrub((ROOT / "src/vfs" / name).read_text(encoding="utf-8"), strings=True)
    private.update(re.findall(r"\b(chimera_vfs_\w+)\s*\(", header))
symbols = re.compile(r"\b(?:" + "|".join(sorted(private)) + r")\b")
includes = re.compile(
    r'^\s*#\s*include\s*["<][^">\n]*(?:'
    + "|".join(re.escape(name) for name in private_headers) + r')[">]', re.M)
violations = []
for directory in ("src/server", "src/client", "src/posix", "src/rest"):
    for path in sorted((ROOT / directory).rglob("*")):
        if path.suffix not in (".c", ".h") or "tests" in path.parts:
            continue
        source = scrub(path.read_text(encoding="utf-8"))
        for matcher, text in ((includes, source), (symbols, scrub(source, strings=True))):
            for match in matcher.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                violations.append(f"{path.relative_to(ROOT)}:{line}: {match.group().strip()}")

if violations:
    print("Northside VFS API violations (use compounds for filesystem operations):")
    print("\n".join(violations))
    raise SystemExit(1)

print("Northside VFS API boundary passed")
