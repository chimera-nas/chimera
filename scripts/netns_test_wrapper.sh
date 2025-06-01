#!/bin/bash

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <test_command> [args...]"
    echo "Runs a test command in an isolated network namespace"
    exit 1
fi

TEST_NAME="chimera_test_$$_$(date +%s%N)"
NETNS_NAME="netns_${TEST_NAME}"

cleanup() {
    if ip netns list | grep -q "^${NETNS_NAME}"; then
        ip netns delete "${NETNS_NAME}" 2>/dev/null || true
    fi
}

trap cleanup EXIT

ip netns add "${NETNS_NAME}"

ip netns exec "${NETNS_NAME}" ip link set lo up

ip netns exec "${NETNS_NAME}" "$@" 