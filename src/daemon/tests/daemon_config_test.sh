#!/bin/bash

# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

#
# Daemon configuration validation test
#
# Exercises the export-related startup validation in src/daemon/daemon.c,
# which has no other test coverage:
#   1. Malformed export settings (legacy "options" key, bad access/squash,
#      out-of-range/non-integer/duplicate export_id, bad sec shapes, missing
#      path, bad anonuid, bad nfs_max_exports) must fail startup with a
#      nonzero exit rather than booting with silently-corrected values.
#   2. A valid config must start; explicit export_ids are honored and
#      auto-assignment must not collide with an id pinned by a lexically
#      later entry (the two-pass parse), verified through the REST API.
#   3. A per-export sec restriction round-trips through GET /api/v1/config.
#
# Expected to run under scripts/netns_test_wrapper.sh (isolated network).
#

set -u

if [ $# -lt 1 ]; then
    echo "Usage: $0 <chimera-daemon-binary>"
    exit 1
fi

DAEMON=$1
REST_PORT=18095

TMPDIR=$(mktemp -d)
DAEMON_PID=""

cleanup() {
    if [ -n "${DAEMON_PID}" ]; then
        kill -9 "${DAEMON_PID}" 2>/dev/null
        wait "${DAEMON_PID}" 2>/dev/null
    fi
    rm -rf "${TMPDIR}"
}

trap cleanup EXIT

mkdir -p "${TMPDIR}/state"

PASSED=0
FAILED=0

pass() {
    echo "  PASS: $1"
    PASSED=$((PASSED + 1))
}

fail() {
    echo "  FAIL: $1"
    FAILED=$((FAILED + 1))
}

# Write a config with the given exports object (and optional extra server
# keys) to stdout.
write_config() {
    local exports_json=$1
    local extra_server=${2:-}

    cat <<EOF
{
    "server": {
        "threads": 2,
        "rest_http_port": ${REST_PORT},
        "rest_auth_enabled": false,
        "state_dir": "${TMPDIR}/state"${extra_server}
    },
    "mounts": {
        "data": { "module": "memfs", "path": "/" }
    },
    "exports": ${exports_json}
}
EOF
}

# A bad config must make the daemon exit nonzero on its own, quickly.  A
# timeout kill (rc 124) means the daemon booted despite the bad config,
# which is exactly the silent-fallback failure these checks exist to catch.
expect_startup_failure() {
    local name=$1
    local exports_json=$2
    local extra_server=${3:-}
    local rc

    write_config "${exports_json}" "${extra_server}" > "${TMPDIR}/bad.json"

    timeout 30 "${DAEMON}" -c "${TMPDIR}/bad.json" > /dev/null 2>&1
    rc=$?

    if [ ${rc} -ne 0 ] && [ ${rc} -ne 124 ]; then
        pass "${name}"
    else
        fail "${name} (exit ${rc})"
    fi
}

echo "========================================"
echo "Daemon Config Validation Test"
echo "========================================"

if ! command -v curl > /dev/null 2>&1; then
    echo "ERROR: curl not found in PATH"
    exit 1
fi

echo
echo "  Test: malformed export settings are fatal at startup..."

expect_startup_failure "legacy \"options\" key rejected" \
    '{ "/e": { "path": "/data", "options": "ro" } }'

expect_startup_failure "invalid access value rejected" \
    '{ "/e": { "path": "/data", "access": "readonly" } }'

expect_startup_failure "invalid squash value rejected" \
    '{ "/e": { "path": "/data", "squash": "rootsquash" } }'

# The boolean squash aliases silently ignored on a non-boolean value would
# leave the export unsquashed (the permissive default).
expect_startup_failure "string root_squash rejected" \
    '{ "/e": { "path": "/data", "root_squash": "true" } }'

expect_startup_failure "integer all_squash rejected" \
    '{ "/e": { "path": "/data", "all_squash": 1 } }'

expect_startup_failure "string no_root_squash rejected" \
    '{ "/e": { "path": "/data", "no_root_squash": "yes" } }'

expect_startup_failure "export_id 0 rejected" \
    '{ "/e": { "path": "/data", "export_id": 0 } }'

expect_startup_failure "export_id 65536 rejected" \
    '{ "/e": { "path": "/data", "export_id": 65536 } }'

expect_startup_failure "non-integer export_id rejected" \
    '{ "/e": { "path": "/data", "export_id": "abc" } }'

expect_startup_failure "duplicate export_id rejected" \
    '{ "/e1": { "path": "/data", "export_id": 7 },
       "/e2": { "path": "/data", "export_id": 7 } }'

expect_startup_failure "unknown sec flavor rejected" \
    '{ "/e": { "path": "/data", "sec": ["krb5x"] } }'

expect_startup_failure "non-array sec rejected" \
    '{ "/e": { "path": "/data", "sec": "krb5" } }'

expect_startup_failure "non-string sec entry rejected" \
    '{ "/e": { "path": "/data", "sec": [5] } }'

expect_startup_failure "missing export path rejected" \
    '{ "/e": { } }'

expect_startup_failure "negative anonuid rejected" \
    '{ "/e": { "path": "/data", "anonuid": -1 } }'

expect_startup_failure "nfs_max_exports 0 rejected" \
    '{ "/e": { "path": "/data" } }' \
    ',
        "nfs_max_exports": 0'

echo
echo "  Test: valid config starts; auto-assignment skips pinned ids..."

# The auto entry sorts lexically before the pinned one, so a regression to a
# single ordered pass would hand the auto export id 1 and then fail startup
# on the pinned entry's conflict; the two-pass parse must give the pinned
# entry id 1 and the auto entry id 2.  The sec restriction on the pinned
# entry must round-trip through GET /api/v1/config.  The boolean root_squash
# alias on the auto entry must parse (and serialize as the canonical "root").
write_config '{
    "/a_auto":   { "path": "/data", "root_squash": true },
    "/b_pinned": { "path": "/data", "export_id": 1, "sec": ["krb5"] }
}' > "${TMPDIR}/good.json"

"${DAEMON}" -c "${TMPDIR}/good.json" > "${TMPDIR}/daemon.log" 2>&1 &
DAEMON_PID=$!

BODY=""
for _ in $(seq 1 60); do
    if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
        break
    fi
    BODY=$(curl -s "http://localhost:${REST_PORT}/api/v1/exports" 2>/dev/null)
    if [ -n "${BODY}" ]; then
        break
    fi
    sleep 0.5
done

if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
    fail "daemon stayed up with valid config"
    echo "---- daemon log ----"
    cat "${TMPDIR}/daemon.log"
    echo "--------------------"
    DAEMON_PID=""
elif [ -z "${BODY}" ]; then
    fail "REST API answered on valid config"
else
    pass "daemon started and REST API answered"

    # Flat objects (no nested braces in this config), so split and match
    # per export.
    if echo "${BODY}" | grep -o '{[^}]*}' | grep '"name":"/b_pinned"' |
        grep -q '"export_id":1,'; then
        pass "pinned export got export_id 1"
    else
        fail "pinned export got export_id 1 (body: ${BODY})"
    fi

    if echo "${BODY}" | grep -o '{[^}]*}' | grep '"name":"/a_auto"' |
        grep -q '"export_id":2,'; then
        pass "auto export skipped pinned id, got export_id 2"
    else
        fail "auto export skipped pinned id, got export_id 2 (body: ${BODY})"
    fi

    if echo "${BODY}" | grep -o '{[^}]*}' | grep '"name":"/a_auto"' |
        grep -q '"squash":"root"'; then
        pass "boolean root_squash alias parsed to squash=root"
    else
        fail "boolean root_squash alias parsed to squash=root (body: ${BODY})"
    fi

    if echo "${BODY}" | grep -o '{[^}]*}' | grep '"name":"/b_pinned"' |
        grep -q '"sec":\["krb5"\]'; then
        pass "sec restriction shown in /api/v1/exports"
    else
        fail "sec restriction shown in /api/v1/exports (body: ${BODY})"
    fi

    CONFIG_BODY=$(curl -s "http://localhost:${REST_PORT}/api/v1/config" \
        2>/dev/null)
    if echo "${CONFIG_BODY}" | grep -q '"sec":\["krb5"\]'; then
        pass "sec restriction round-trips through /api/v1/config"
    else
        fail "sec restriction round-trips through /api/v1/config (body: ${CONFIG_BODY})"
    fi
fi

if [ -n "${DAEMON_PID}" ]; then
    kill -TERM "${DAEMON_PID}" 2>/dev/null
    SHUTDOWN_RC=1
    for _ in $(seq 1 60); do
        if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
            wait "${DAEMON_PID}"
            SHUTDOWN_RC=$?
            break
        fi
        sleep 0.5
    done
    if [ ${SHUTDOWN_RC} -eq 0 ]; then
        pass "daemon shut down cleanly on SIGTERM"
    else
        fail "daemon shut down cleanly on SIGTERM (exit ${SHUTDOWN_RC})"
    fi
    DAEMON_PID=""
fi

echo
echo "========================================"
echo "Passed: ${PASSED}"
echo "Failed: ${FAILED}"
echo "========================================"

if [ ${FAILED} -gt 0 ]; then
    echo "Some tests FAILED"
    exit 1
fi

echo "All tests PASSED"
exit 0
