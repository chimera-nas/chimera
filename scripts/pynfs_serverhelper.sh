#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# pynfs serverhelper for chimera.
#
# pynfs invokes this as:  <script> <base_url> <verb> <args...>
# where <base_url> is passed via testserver.py --serverhelperarg (the chimera
# REST base URL, e.g. http://127.0.0.1:7780) and <verb> <args...> is the
# operation pynfs wants performed out-of-band on the server (DELEG16-20):
#
#   unlink <path>
#   rename <src> <dst>
#   link   <src> <newlink>
#   chmod  <octal-mode> <path>
#
# Each verb is translated into a POST to the chimera /api/v1/debug/fsop debug
# endpoint, which performs the real VFS operation. The VFS core recalls any
# outstanding delegation on the affected file as a side effect, which is what
# the tests verify.

set -u

BASE="$1"; shift
VERB="$1"; shift

case "$VERB" in
    unlink)
        BODY=$(jq -nc --arg p "$1" '{op:"unlink",path:$p}')
        ;;
    rename)
        BODY=$(jq -nc --arg p "$1" --arg q "$2" '{op:"rename",path:$p,path2:$q}')
        ;;
    link)
        BODY=$(jq -nc --arg p "$1" --arg q "$2" '{op:"link",path:$p,path2:$q}')
        ;;
    chmod)
        # pynfs passes the mode as an octal string (e.g. 0777).
        MODE=$(( 8#$1 ))
        BODY=$(jq -nc --argjson m "$MODE" --arg p "$2" '{op:"chmod",mode:$m,path:$p}')
        ;;
    *)
        echo "pynfs_serverhelper: unknown verb '$VERB'" >&2
        exit 2
        ;;
esac

HTTP=$(curl -sS -o /dev/null -w '%{http_code}' \
       -X POST "$BASE/api/v1/debug/fsop" \
       -H 'Content-Type: application/json' \
       -d "$BODY") || {
    echo "pynfs_serverhelper: curl failed for $VERB" >&2
    exit 1
}

if [ "$HTTP" != "200" ]; then
    echo "pynfs_serverhelper: $VERB failed (HTTP $HTTP)" >&2
    exit 1
fi

exit 0
