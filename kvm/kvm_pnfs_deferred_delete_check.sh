#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Sourced by the pNFS KVM wrappers once the guest workload has succeeded.
#
# A test command carrying DEFERRED_DELETE_EXPECTED must have made the metadata
# server recall a layout because the file was unlinked and only layouts still
# referred to it, and then delete the data server's backing file after that:
# the MDS log shows the recall, then the delete; the metrics endpoint shows an
# "unlinked" recall and no revoked layout.  NO_DEFERRED_DELETE_EXPECTED asserts
# the opposite: no unlink recall and no backing file delete at all.
#
# Usage: pnfs_deferred_delete_check <test_cmd> <mds_log> <netns> <mds_ip>

pnfs_dd_scrape() {
    local netns="$1" ip="$2"
    ip netns exec "$netns" bash -c \
        "exec 3<>/dev/tcp/${ip}/9000 && printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3 && timeout 5 cat <&3" \
        2>/dev/null
}

pnfs_dd_value() {
    local scrape="$1" series="$2"
    printf '%s\n' "$scrape" | grep -F "${series} " | head -1 | awk '{print $2}'
}

pnfs_deferred_delete_check() {
    local cmd="$1" log="$2" netns="$3" ip="$4"
    local i recalled deleted scrape unlinked deadline refused recall_only returned

    if echo "$cmd" | grep -q "NO_DEFERRED_DELETE_EXPECTED"; then
        sleep 1
        if grep -qE "pNFS: file unlinked and no longer open, recalled|pNFS: deleting data-server backing file" "$log"; then
            echo "=== unexpected unlink recall or backing file delete ===" >&2
            grep -E "pNFS:" "$log" | tail -15 >&2
            return 1
        fi
        echo "=== verified: no unlink recall and no backing file delete ==="
        return 0
    fi
    if echo "$cmd" | grep -q "DEFERRED_RECALL_EXPECTED"; then
        recall_only=1
    else
        recall_only=0
        echo "$cmd" | grep -q "DEFERRED_DELETE_EXPECTED" || return 0
    fi

    # The delete happens after the client returns the layout, which can be after
    # the guest's last syscall.  Poll up to 120 s; a good run leaves at once.
    for i in $(seq 1 6000); do
        if [ "$recall_only" = 1 ]; then
            grep -q "pNFS: file fully returned" "$log" && break
        else
            grep -qE "pNFS: deleting data-server backing file|pNFS: data-server backing file .* already gone" "$log" && break
        fi
        sleep 0.02
    done

    recalled=$(grep -n "pNFS: file unlinked and no longer open, recalled layout from" "$log" | head -1 | cut -d: -f1)
    if [ -z "$recalled" ]; then
        echo "=== no CB_LAYOUTRECALL was sent when the file was unlinked ===" >&2
        grep -E "pNFS:" "$log" | tail -15 >&2
        return 1
    fi
    if [ "$recall_only" = 1 ]; then
        returned=$(awk -v r="$recalled" 'NR > r && /pNFS: file fully returned/ { print NR; exit }' "$log")
        if [ -z "$returned" ]; then
            echo "=== the layout was not returned after the recall (recalled=${recalled}) ===" >&2
            grep -E "pNFS:" "$log" | tail -15 >&2
            return 1
        fi
        deleted=$returned
    else
        deleted=$(awk -v r="$recalled" 'NR > r && /pNFS: deleting data-server backing file|pNFS: data-server backing file .* already gone/ { print NR; exit }' "$log")
    fi
    if [ -z "$deleted" ]; then
        echo "=== the backing file was not deleted after the layout recall (recalled=${recalled}) ===" >&2
        grep -E "pNFS:" "$log" | tail -15 >&2
        return 1
    fi

    scrape=$(pnfs_dd_scrape "$netns" "$ip")
    unlinked=$(pnfs_dd_value "$scrape" 'chimera_nfs4_layout_recall{cause="unlinked"}')
    deadline=$(pnfs_dd_value "$scrape" 'chimera_nfs4_layout_revoke{why="deadline"}')
    refused=$(pnfs_dd_value "$scrape" 'chimera_nfs4_layout_revoke{why="refused"}')
    # Values may be rendered as integers or decimals; compare numerically.  A
    # layout revoked at the deadline would mean the client never answered.  A
    # "refused" revoke is not a failure: it counts every answer that ends the
    # layout at once, including NFS4ERR_NOMATCHING_LAYOUT from a client that
    # has nothing left to return.
    if ! awk -v u="${unlinked:-}" -v d="${deadline:-0}" \
        'BEGIN { exit !(u != "" && u + 0 >= 1 && d + 0 == 0) }'; then
        echo "=== layout counters wrong: unlinked recalls=${unlinked:-none} revoked(deadline)=${deadline:-0} revoked(refused)=${refused:-0} ===" >&2
        return 1
    fi

    echo "=== verified: layout recalled at unlink, backing file deleted after (unlinked recalls=${unlinked}, none revoked at the deadline) ==="
    return 0
}
