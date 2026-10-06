#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Real fragmented READ replies must be admitted before a mutating suffix."""

from nfs4_compound_boundaries import *  # noqa: F401,F403
from nfs4_compound_v40_boundaries import Probe40


def vectors(p):
    _, fh, sid = p.create("vector-budget")
    original = bytes(range(256)) * 4
    p.call([op.putfh(fh), op.write(sid, 0, FILE_SYNC4, original)])

    # 256 payload vectors + two prefix flushes + one small payload vector +
    # final framing is exactly the generated adapter's 260-slot capacity.
    # Odd length also proves READ padding shares the next framing descriptor.
    for length in (1024, 1023):
        result = p.call([op.putfh(fh), op.read(sid, 0, length), op.read(sid, 0, 4),
                         op.write(sid, 0, FILE_SYNC4, original[:4])], f"vectors_exact_{length}")
        require(result.resarray[1].data == original[:length] and
                result.resarray[2].data == original[:4], "fitting fragmented reply lost payload bytes")

    # The second fragmented READ must fail in its execution callback. The
    # successful prefix still owns all 256 vectors through accepted finish.
    operations = [op.putfh(fh), op.read(sid, 0, 1024), op.read(sid, 0, 1024),
                  op.write(sid, 0, FILE_SYNC4, b"BAD!")]
    result = p.call(operations, "vectors_stop_write", NFS4ERR_RESOURCE)
    require(len(result.resarray) == 3 and result.resarray[1].data == original and
            result.resarray[-1].resop == OP_READ, "vector admission failed after the WRITE or damaged its prefix")
    require(p.read(fh, sid) == original, "vector exhaustion allowed WRITE")

    # A new VFS span must retain the accepted prefix's output-vector charge.
    # These protocol administration operations intentionally remain separate.
    administrative = op.renew(p.clientid) if p.args.minor == 0 else op.backchannel_ctl(0, [])
    start = 0 if p.args.minor == 0 else 1
    operations = [op.putfh(fh), op.read(sid, 0, 1024), administrative,
                  op.read(sid, 0, 1024), op.write(sid, 0, FILE_SYNC4, b"BAD!")]
    result = p.call(operations, "vectors_across_spans", NFS4ERR_RESOURCE,
                    runs=[(start, 2), (start + 3, 2)])
    require(len(result.resarray) == 4 and result.resarray[1].data == original and
            result.resarray[-1].resop == OP_READ, "an administrative boundary forgot earlier READ vectors")
    require(p.read(fh, sid) == original, "split-span exhaustion allowed WRITE")

    # Empty READs consume framing only and must not retain a stale payload
    # count. Retrying the successful sequence must recreate the same budget.
    result = p.call([op.putfh(fh), op.read(sid, 0, 0), op.read(sid, 0, 1024),
                     op.read(sid, 0, 0), op.getfh()], "vectors_empty_and_data")
    require(result.resarray[1].data == b"" and result.resarray[2].data == original and
            result.resarray[3].data == b"" and result.resarray[4].object == fh,
            "empty READ or retry corrupted the vector budget")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--export", required=True)
    parser.add_argument("--minor", type=int, default=2)
    parser.add_argument("--server-log", required=True)
    args = parser.parse_args()
    p = Probe40(args) if args.minor == 0 else Probe(args)
    try:
        vectors(p)
        p.check_trace()
        text = Path(args.server_log).read_text()
        require("NFS4_VECTOR_FRAGMENT bytes=1024 pieces=256" in text,
                "fixture did not create maximum-fragment READ replies")
        if os.environ.get("CHIMERA_VECTOR_RETRY") == "retry":
            injected = re.findall(r"NFS4_FINISH_RETRY injected [^\n]* id=(\d+)", text)
            accepted = re.findall(r"NFS4_FINISH_RETRY accepted [^\n]* id=(\d+)", text)
            require(len(injected) >= 3 and sorted(injected) == sorted(accepted),
                    "fragmented replies did not reject and accept finish retries")
            require(re.search(r"NFS4_FINISH_RETRY injected [^\n]* execution=[1-9]\d* [^\n]* reads=2", text),
                    "failed READ prefix was not retried with its WRITE suppressed")
    finally:
        p.cleanup()


if __name__ == "__main__":
    main()
