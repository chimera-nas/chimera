#!/bin/bash
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only
#
# Run the ceph/s3-tests S3 compatibility suite against a freshly started chimera
# daemon. Generates a chimera config (memfs backend, three S3 credentials, a
# runtime bucket root) and a matching s3tests.conf, starts the daemon on
# 127.0.0.1:5000, runs pytest, then tears everything down.
#
# Usage:
#   run_ceph_s3tests.sh --chimera <daemon> [--suite-dir DIR] [--backend memfs]
#                       [pytest args...]
#
# Any trailing arguments are passed straight to pytest, so this can drive the
# whole suite, a single file, a node id, or a -k expression. When no pytest
# target is given it runs the curated allowlist in passing_tests.txt.

set -u

CHIMERA=""
SUITE_DIR="${S3TESTS_DIR:-/opt/s3-tests}"
BACKEND="memfs"
PORT=5000
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PYTEST_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --chimera)   CHIMERA="$2"; shift 2 ;;
        --suite-dir) SUITE_DIR="$2"; shift 2 ;;
        --backend)   BACKEND="$2"; shift 2 ;;
        --port)      PORT="$2"; shift 2 ;;
        *)           PYTEST_ARGS+=("$1"); shift ;;
    esac
done

if [ -z "$CHIMERA" ]; then
    for c in ./src/daemon/chimera ../../../../daemon/chimera; do
        [ -x "$c" ] && CHIMERA="$c" && break
    done
fi

if [ ! -x "$CHIMERA" ]; then
    echo "chimera daemon not found (pass --chimera)" >&2
    exit 1
fi

if [ ! -d "$SUITE_DIR/.venv" ]; then
    echo "ceph s3-tests not installed at $SUITE_DIR (skipping)" >&2
    exit 77   # ctest "skip"
fi

WORK="$(mktemp -d /tmp/ceph_s3tests.XXXXXX)"
cleanup() {
    [ -n "${DPID:-}" ] && kill -9 "$DPID" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$WORK/chimera.conf" <<EOF
{
  "server": { "s3_port": $PORT },
  "s3_access_keys": [
    { "access_key": "mainaccesskey0001", "secret_key": "mainsecretkey0001" },
    { "access_key": "altaccesskey0002",  "secret_key": "altsecretkey0002" },
    { "access_key": "tenantaccesskey03", "secret_key": "tenantsecretkey03" }
  ],
  "mounts": { "share": { "module": "$BACKEND", "path": "/" } },
  "s3_bucket_root": "/share",
  "buckets": {}
}
EOF

cat > "$WORK/s3tests.conf" <<EOF
[DEFAULT]
host = 127.0.0.1
port = $PORT
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = s3t-{random}-
iam name prefix = s3-tests-
iam path prefix = /s3-tests/

[s3 main]
display_name = M. Tester
user_id = testid
email = tester@ceph.com
api_name = default
access_key = mainaccesskey0001
secret_key = mainsecretkey0001

[s3 alt]
display_name = john.doe
email = john.doe@example.com
user_id = altuserid
access_key = altaccesskey0002
secret_key = altsecretkey0002

[s3 tenant]
display_name = testx\$tenanteduser
user_id = tenantuserid
access_key = tenantaccesskey03
secret_key = tenantsecretkey03
email = tenanteduser@example.com
tenant = testx

[iam]
email = s3@example.com
user_id = iamuserid
access_key = iamaccesskey00004
secret_key = iamsecretkey00004
display_name = iamuser

[iam root]
access_key = iamrootkey000005a
secret_key = iamrootsecret0005
user_id = RGW11111111111111111
email = account1@ceph.com

[iam alt root]
access_key = iamaltrootkey0006
secret_key = iamaltrootsecret6
user_id = RGW22222222222222222
email = account2@ceph.com
EOF

"$CHIMERA" -c "$WORK/chimera.conf" > "$WORK/chimera.log" 2>&1 &
DPID=$!

# Wait for the S3 port to accept connections. The /dev/tcp probe runs in a
# subshell so a failed connect can't leak "Connection refused" to our stderr.
for _ in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        break
    fi
    if ! kill -0 "$DPID" 2>/dev/null; then
        echo "chimera daemon exited during startup:" >&2
        cat "$WORK/chimera.log" >&2
        exit 1
    fi
    sleep 0.2
done

# Default target: the curated allowlist of currently-passing tests.
if [ ${#PYTEST_ARGS[@]} -eq 0 ]; then
    ALLOW="$SCRIPT_DIR/passing_tests.txt"
    if [ ! -f "$ALLOW" ]; then
        echo "no pytest target and no allowlist at $ALLOW" >&2
        exit 1
    fi
    mapfile -t NODES < <(grep -vE '^\s*(#|$)' "$ALLOW")
    PYTEST_ARGS=("${NODES[@]}")
fi

cd "$SUITE_DIR"
# AWS_MAX_ATTEMPTS=1 disables botocore's automatic retry/backoff. Without it,
# every request chimera answers with 500 (unimplemented features) is retried
# several times with exponential backoff, making the full-suite run take hours.
S3TEST_CONF="$WORK/s3tests.conf" AWS_MAX_ATTEMPTS=1 .venv/bin/python -m pytest \
    -p no:cacheprovider -q --no-header \
    --timeout=60 --timeout-method=signal \
    "${PYTEST_ARGS[@]}"
exit $?
