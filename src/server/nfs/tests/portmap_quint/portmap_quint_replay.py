#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

"""Replay Quint ITF traces against a live chimera portmap server.

Each state of an ITF trace produced from portmap.qnt carries one
(call, reply) event predicted by the model.  This driver re-issues every
call as a real ONC RPC over TCP and diffs the server's reply against the
model's prediction, exiting nonzero on the first divergence.

Usage: portmap_quint_replay.py <host> <trace.itf.json> [more traces...]

Pure stdlib; speaks just enough XDR for portmap v2 (NULL/GETPORT/DUMP)
and rpcbind v3/v4 (GETADDR).
"""

import json
import socket
import struct
import sys

PMAP_PROG = 100000
PMAP_PORT = 111

# Procedure numbers from portmap.x
V2_NULL = 0
V2_GETPORT = 3
V2_DUMP = 4
V34_GETADDR = 3

NETID_STR = {"NetTcp": "tcp", "NetUdp": "udp", "NetTcp6": "tcp6"}


def itf_int(v):
    """Decode an ITF integer, which may be {"#bigint": "n"} or a plain int."""
    if isinstance(v, dict):
        return int(v["#bigint"])
    return int(v)


class XdrError(Exception):
    pass


class Unpacker:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def u32(self):
        if self.pos + 4 > len(self.data):
            raise XdrError("XDR underrun at offset %d" % self.pos)
        (val,) = struct.unpack_from(">I", self.data, self.pos)
        self.pos += 4
        return val

    def string(self):
        length = self.u32()
        padded = (length + 3) & ~3
        if self.pos + padded > len(self.data):
            raise XdrError("XDR string underrun at offset %d" % self.pos)
        val = self.data[self.pos:self.pos + length]
        self.pos += padded
        return val.decode("ascii", errors="replace")

    def done(self):
        if self.pos != len(self.data):
            raise XdrError("%d trailing bytes in reply body"
                           % (len(self.data) - self.pos))


def pack_string(s):
    data = s.encode("ascii")
    return struct.pack(">I", len(data)) + data + b"\0" * (-len(data) % 4)


class RpcClient:
    """Minimal ONC RPC (RFC 5531) client over TCP with record marking."""

    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.xid = 0x43484d00  # arbitrary

    def close(self):
        self.sock.close()

    def _recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise XdrError("connection closed mid-reply")
            buf += chunk
        return buf

    def call(self, vers, proc, args):
        self.xid += 1
        hdr = struct.pack(">IIIIII", self.xid, 0, 2, PMAP_PROG, vers, proc)
        hdr += struct.pack(">IIII", 0, 0, 0, 0)  # AUTH_NONE cred + verf
        record = hdr + args
        self.sock.sendall(struct.pack(">I", 0x80000000 | len(record)) + record)

        body = b""
        while True:
            (mark,) = struct.unpack(">I", self._recv_exact(4))
            body += self._recv_exact(mark & 0x7FFFFFFF)
            if mark & 0x80000000:
                break

        u = Unpacker(body)
        xid = u.u32()
        if xid != self.xid:
            raise XdrError("xid mismatch: sent %#x got %#x" % (self.xid, xid))
        if u.u32() != 1:
            raise XdrError("not a REPLY message")
        if u.u32() != 0:
            raise XdrError("RPC call denied")
        u.u32()  # verf flavor
        verf_len = u.u32()
        u.pos += (verf_len + 3) & ~3
        accept_stat = u.u32()
        if accept_stat != 0:
            raise XdrError("RPC accept_stat %d (vers %d proc %d)"
                           % (accept_stat, vers, proc))
        return u


def uaddr_port(uaddr):
    """Decode the port from a universal address 'a.b.c.d.hi.lo'; '' -> 0."""
    if uaddr == "":
        return 0
    parts = uaddr.split(".")
    if len(parts) < 6:
        raise XdrError("malformed universal address %r" % uaddr)
    return int(parts[-2]) * 256 + int(parts[-1])


def replay_event(rpc, call, reply):
    """Issue one modeled call; return (expected, actual) reply summaries."""
    tag = call["tag"]
    val = call.get("value", {})

    if tag == "NullV2":
        u = rpc.call(2, V2_NULL, b"")
        u.done()
        return ("void", "void")

    if tag == "GetportV2":
        args = struct.pack(">IIII", itf_int(val["prog"]), itf_int(val["vers"]),
                           itf_int(val["prot"]), 0)
        u = rpc.call(2, V2_GETPORT, args)
        port = u.u32()
        u.done()
        return (itf_int(reply["value"]), port)

    if tag == "DumpV2":
        u = rpc.call(2, V2_DUMP, b"")
        actual = []
        while u.u32():  # "value follows" for each pmaplist node
            actual.append((u.u32(), u.u32(), u.u32(), u.u32()))
        u.done()
        expected = [tuple(itf_int(m[k]) for k in ("prog", "vers", "prot", "port"))
                    for m in reply["value"]]
        return (expected, actual)

    if tag in ("GetaddrV3", "GetaddrV4"):
        vers = 3 if tag == "GetaddrV3" else 4
        args = struct.pack(">II", itf_int(val["prog"]), itf_int(val["vers"]))
        args += pack_string(NETID_STR[val["netid"]["tag"]])
        args += pack_string("")  # r_addr
        args += pack_string("")  # r_owner
        u = rpc.call(vers, V34_GETADDR, args)
        uaddr = u.string()
        u.done()
        return (itf_int(reply["value"]), uaddr_port(uaddr))

    raise XdrError("unknown call tag %r in trace" % tag)


def replay_trace(host, path):
    with open(path) as f:
        trace = json.load(f)

    rpc = RpcClient(host, PMAP_PORT)
    try:
        for state in trace["states"]:
            event = state["lastEvent"]
            index = state["#meta"]["index"]
            call = event["call"]
            try:
                expected, actual = replay_event(rpc, call, event["reply"])
            except XdrError as e:
                print("FAIL %s state %d %s: %s"
                      % (path, index, call["tag"], e))
                return False
            if expected != actual:
                print("FAIL %s state %d %s:\n  call:     %s\n"
                      "  expected: %s\n  actual:   %s"
                      % (path, index, call["tag"], json.dumps(call),
                         expected, actual))
                return False
        print("ok %s: %d calls replayed" % (path, len(trace["states"])))
        return True
    finally:
        rpc.close()


def main():
    if len(sys.argv) < 3:
        print("usage: %s <host> <trace.itf.json>..." % sys.argv[0])
        return 2
    host = sys.argv[1]
    ok = True
    for path in sys.argv[2:]:
        ok = replay_trace(host, path) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
