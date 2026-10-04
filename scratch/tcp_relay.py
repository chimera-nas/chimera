# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
"""Scratch: relay one TCP port to another and record both directions.

Usage: tcp_relay.py <listen port> <target host> <target port> <record file>

Each record is: u64 ns timestamp, u32 connection id, u8 direction (0 client to
server, 1 server to client), u32 length, then the bytes.  Runs until killed.
"""
import asyncio
import struct
import sys
import time

listen, host, port, path = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4]
out = open(path, "wb")
conns = [0]


def record(cid, direction, data):
    out.write(struct.pack("<QIBI", time.time_ns(), cid, direction, len(data)) + data)
    out.flush()


async def pump(reader, writer, cid, direction):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            record(cid, direction, data)
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def handle(creader, cwriter):
    conns[0] += 1
    cid = conns[0]
    sreader, swriter = await asyncio.open_connection(host, port)
    await asyncio.gather(pump(creader, swriter, cid, 0), pump(sreader, cwriter, cid, 1))


async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", listen)
    print(f"relaying 127.0.0.1:{listen} -> {host}:{port}, recording {path}", flush=True)
    async with server:
        await server.serve_forever()

asyncio.run(main())
