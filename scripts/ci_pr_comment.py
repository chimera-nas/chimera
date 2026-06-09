#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Post or update a single sticky PR comment via the GitHub REST API.

Usage: ci_pr_comment.py <owner/repo> <pr_number> <body_file>

Uses stdlib urllib only (the self-hosted CI runners have no `gh` CLI).  Auth
token comes from $GH_TOKEN.  The comment is identified/updated by the MARKER on
its first line, so re-runs replace the prior report instead of stacking.
"""
import json
import os
import sys
import urllib.request

MARKER = "<!-- ctest-report -->"


def api(method, url, data=None):
    token = os.environ["GH_TOKEN"]
    req = urllib.request.Request(url, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "chimera-ci-report",
    })
    if data is not None:
        req.data = json.dumps(data).encode()
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as r:
        body = r.read()
    return json.loads(body) if body else None


def main():
    repo, pr, body_file = sys.argv[1], sys.argv[2], sys.argv[3]
    body = open(body_file).read()
    base = f"https://api.github.com/repos/{repo}"

    comments = api("GET", f"{base}/issues/{pr}/comments?per_page=100") or []
    existing = next((c for c in comments
                     if (c.get("body") or "").startswith(MARKER)), None)

    if existing:
        api("PATCH", f"{base}/issues/comments/{existing['id']}", {"body": body})
        print(f"Updated PR comment {existing['id']}")
    else:
        created = api("POST", f"{base}/issues/{pr}/comments", {"body": body})
        print(f"Created PR comment {created['id']}")


if __name__ == "__main__":
    main()
