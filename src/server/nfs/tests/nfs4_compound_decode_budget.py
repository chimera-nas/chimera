#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Decoded arguments must leave response space before any handler runs."""

import argparse
import socket
import struct


def words(*values):
    return struct.pack("!" + "I" * len(values), *values)


def opaque(value):
    return words(len(value)) + value + b"\0" * (-len(value) % 4)


def receive(sock, length):
    result = bytearray()
    while len(result) < length:
        data = sock.recv(length - len(result))
        assert data, "server disconnected while replying to a decoded request"
        result.extend(data)
    return bytes(result)


class Peer:
    def __init__(self, args):
        self.sock = socket.create_connection((args.host, args.port), timeout=10)
        self.xid = 0
        self.minor = args.minor

    def call(self, operations, minor=None, trailer=b""):
        self.xid += 1
        body = opaque(b"decode-budget") + words(self.minor if minor is None else minor,
                                               len(operations)) + b"".join(operations) + trailer
        rpc = words(self.xid, 0, 2, 100003, 4, 1, 0, 0, 0, 0) + body
        self.sock.sendall(words(0x80000000 | len(rpc)) + rpc)
        reply = bytearray()
        while True:
            mark, = struct.unpack("!I", receive(self.sock, 4))
            reply.extend(receive(self.sock, mark & 0x7fffffff))
            if mark & 0x80000000:
                break
        xid, kind, disposition, _, verifier_len = struct.unpack_from("!5I", reply)
        assert (xid, kind, disposition) == (self.xid, 1, 0), "unexpected RPC reply"
        offset = 20 + ((verifier_len + 3) & ~3)
        status, = struct.unpack_from("!I", reply, offset)
        return status, bytes(reply[offset + 4:])

    def healthy(self):
        status, body = self.call([], minor=0)
        assert status == 0 and struct.unpack_from("!I", body)[0] == 0, \
            "server failed a normal COMPOUND after rejecting oversized decoded arguments"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--minor", type=int, default=2)
    parser.add_argument("--export")
    parser.add_argument("--server-log")
    peer = Peer(parser.parse_args())
    try:
        # 124800 bytes of stateids fit the decode arena, but their decoded
        # operation and RPC headers leave less than the 260 reply descriptors.
        stateids = words(55, 7800) + b"\0" * (16 * 7800)  # TEST_STATEID
        write = words(38) + b"\0" * 16 + struct.pack("!Q", 0) + words(2) + opaque(b"data")
        for operations in ([stateids], [write, stateids]):
            for _ in range(4):
                status, _ = peer.call(operations)
                assert status == 5, f"decoded arena exhaustion returned RPC status {status}, expected SYSTEM_ERR"
                peer.healthy()

        # Find the in-memory argarray boundary without assuming C ABI sizes.
        # RENEW stops at its first protocol error, so probing smaller arrays
        # cannot execute a long sequence or mutate filesystem data.
        renew = words(30, 0, 0)
        low, high = 0, 64
        while peer.call([renew] * high)[0] == 0:
            low, high = high, high * 2
            assert high <= 4096, "no bounded decoded operation-array limit"
        while high - low > 1:
            middle = (low + high) // 2
            if peer.call([renew] * middle)[0] == 0:
                low = middle
            else:
                high = middle
        status, _ = peer.call([renew] * high)
        assert status == 5, "argarray admission failed only after exhausting the whole arena"
        peer.healthy()

        # A fully decoded WRITE followed by trailing garbage also owns a
        # cloned buffer, although the call never reaches its frontend handler.
        status, _ = peer.call([write], trailer=words(0))
        assert status == 4, "trailing garbage did not return RPC GARBAGE_ARGS"
        peer.healthy()
        # A decoded WRITE can precede a truncated later operation or an
        # oversized nested array. Neither failure may retain its payload clone.
        for bad in (words(38), words(55, 0xffffffff), words(34)):
            for _ in range(16):
                status, _ = peer.call([write, bad])
                assert status == 4, "partial argument decode did not return GARBAGE_ARGS"
                peer.healthy()
        print("PASS decoded arena admission, partial decode ownership and subsequent normal requests")
    finally:
        peer.sock.close()


if __name__ == "__main__":
    main()
