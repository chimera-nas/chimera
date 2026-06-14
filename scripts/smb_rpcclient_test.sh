#!/bin/bash

# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# Independent-client validation of the chimera LSARPC named pipe: starts the
# smbtorture_test harness in serve mode (an in-process chimera SMB server) and
# drives Samba's rpcclient against it, asserting that a real DCE/RPC client can
# bind to \lsarpc and that an implemented operation (LsaGetUserName) decodes.
#
# Usage: smb_rpcclient_test.sh <smbtorture_test_binary> [backend]
# Exit:  0 pass, 1 fail, 77 skip (rpcclient unavailable).

set -u

BIN="${1:?usage: $0 <smbtorture_test_binary> [backend]}"
BACKEND="${2:-memfs}"

# rpcclient is a system binary; never run it under the ASAN LD_PRELOAD.
RPCCLIENT="env -u LD_PRELOAD rpcclient"

if ! command -v rpcclient >/dev/null 2>&1; then
    echo "rpcclient not found in PATH; skipping."
    exit 77
fi

LOG="$(mktemp)"
"$BIN" -s -b "$BACKEND" >"$LOG" 2>&1 &
SRV=$!

cleanup() {
    kill "$SRV" 2>/dev/null
    wait "$SRV" 2>/dev/null
    rm -f "$LOG"
}
trap cleanup EXIT

# Wait for the server to announce readiness (or die).
for _ in $(seq 1 100); do
    grep -q SERVE_READY "$LOG" && break
    kill -0 "$SRV" 2>/dev/null || { echo "server exited early:"; cat "$LOG"; exit 1; }
    sleep 0.1
done

if ! grep -q SERVE_READY "$LOG"; then
    echo "server did not become ready:"; cat "$LOG"; exit 1
fi

rc=0

echo "=== rpcclient getusername (LsaGetUserName, opnum 45) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'getusername' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -q "Account Name: myuser"; then
    echo "PASS: LsaGetUserName decoded"
else
    echo "FAIL: unexpected LsaGetUserName response"; rc=1
fi

echo "=== rpcclient lsaquery (LsaOpenPolicy2 + LsaQueryInfoPolicy, opnums 44/7) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'lsaquery' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -qE "Domain Sid: S-1-5-21-[0-9]+-[0-9]+-[0-9]+"; then
    echo "PASS: LsaQueryInfoPolicy returned the domain name + SID"
else
    echo "FAIL: unexpected LsaQueryInfoPolicy response"; rc=1
fi

echo "=== rpcclient lookupsids S-1-22-1-1001 (LsaLookupSids, opnum 15 -> idmap) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'lookupsids S-1-22-1-1001' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -qi "myuser"; then
    echo "PASS: LsaLookupSids resolved the unix-user SID to its name via the idmap"
else
    echo "FAIL: unexpected LsaLookupSids response"; rc=1
fi

echo "=== rpcclient lookupnames myuser (LsaLookupNames, opnum 14 -> idmap) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'lookupnames myuser' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -q "S-1-22-1-1001"; then
    echo "PASS: LsaLookupNames resolved the name to its unix-user SID via the idmap"
else
    echo "FAIL: unexpected LsaLookupNames response"; rc=1
fi

echo "=== rpcclient netshareenumall 1 (SRVSVC NetShareEnumAll, opnum 15 -> share table) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'netshareenumall 1' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -qw "share" && echo "$OUT" | grep -q "IPC"; then
    echo "PASS: SRVSVC NetShareEnumAll listed the shares + IPC\$ from the share table"
else
    echo "FAIL: unexpected NetShareEnumAll response"; rc=1
fi

echo "=== rpcclient netsharegetinfo share 1 (SRVSVC NetShareGetInfo, opnum 16) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'netsharegetinfo share 1' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -qi "netname: *share"; then
    echo "PASS: SRVSVC NetShareGetInfo returned the share's level-1 info"
else
    echo "FAIL: unexpected NetShareGetInfo response"; rc=1
fi

echo "=== rpcclient srvinfo (SRVSVC NetSrvGetInfo level 101, opnum 21) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'srvinfo' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -qi "platform_id" && echo "$OUT" | grep -qi "chimera"; then
    echo "PASS: SRVSVC NetSrvGetInfo returned the server's level-101 info"
else
    echo "FAIL: unexpected NetSrvGetInfo response"; rc=1
fi

echo "=== rpcclient enumdomains (SAMR Connect + EnumDomains, opnums 0/6) ==="
OUT="$(timeout 20 $RPCCLIENT -U 'myuser%mypassword' 127.0.0.1 -c 'enumdomains' 2>&1)"
echo "$OUT"
if echo "$OUT" | grep -qi "Builtin" && echo "$OUT" | grep -qi "chimera"; then
    echo "PASS: SAMR EnumDomains listed the account domain + Builtin"
else
    echo "FAIL: unexpected EnumDomains response"; rc=1
fi

exit $rc
