# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""SMB2 MBT corpus coverage gate.

Scans the generated ITF traces and asserts the corpus actually exercises the
behavior buckets the model is meant to cover: every CREATE disposition, every
CreateAction, and the coherency-relevant CREATE statuses (SUCCESS,
SHARING_VIOLATION, OBJECT_NAME_COLLISION, OBJECT_NAME_NOT_FOUND), plus
successful and end-of-file I/O.  Uniform random stepping can silently starve a
matrix corner; this gate turns that starvation red instead of letting a
never-exercised path masquerade as covered (the SMB analogue of the NFS
`coverage` ctest).
"""

import glob
import json
import os
import sys

# NTSTATUS values the corpus must exercise.
ST_SUCCESS = 0x00000000
ST_END_OF_FILE = 0xC0000011
ST_OBJECT_NAME_NOT_FOUND = 0xC0000034
ST_OBJECT_NAME_COLLISION = 0xC0000035
ST_SHARING_VIOLATION = 0xC0000043

DISPOSITIONS = ["DispOpen", "DispCreate", "DispOpenIf", "DispOverwriteIf",
                "DispSupersede"]
# CreateAction: SUPERSEDED=0, OPENED=1, CREATED=2, OVERWRITTEN=3
ACTIONS = [0, 1, 2, 3]


def as_int(o):
    if isinstance(o, dict) and "#bigint" in o:
        return int(o["#bigint"])
    return o


def last_op_key(state):
    for k in state:
        if k.endswith("::lastOp"):
            return k
    return None


def scan(path, buckets):
    with open(path) as f:
        doc = json.load(f)
    states = doc["states"]
    key = last_op_key(states[0])
    for st in states[1:]:
        lo = st.get(key)
        if not lo or lo.get("tag") != "LMsg":
            continue
        v = lo["value"]
        cmds = v["msg"]["cmds"]
        res = v["results"]
        for i in range(min(len(cmds), len(res))):
            c = cmds[i]
            r = res[i]
            ctag = c.get("tag")
            rv = r.get("value", {})
            status = as_int(rv.get("st", 0)) & 0xFFFFFFFF
            if ctag == "CCreate":
                buckets["disp:" + c["value"]["disp"]["tag"]] = True
                buckets["cr_status:0x%08x" % status] = True
                if status == ST_SUCCESS:
                    buckets["action:%d" % as_int(rv.get("act", -1))] = True
            elif ctag == "CRead":
                buckets["read:0x%08x" % status] = True
            elif ctag == "CWrite":
                buckets["write:0x%08x" % status] = True


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: coverage.py <trace-dir>\n")
        return 2
    trace_dir = sys.argv[1]
    traces = sorted(glob.glob(os.path.join(trace_dir, "smb2Base_*.itf.json")))
    if not traces:
        sys.stderr.write("no traces found in %s\n" % trace_dir)
        return 2

    buckets = {}
    for t in traces:
        scan(t, buckets)

    required = []
    required += ["disp:" + d for d in DISPOSITIONS]
    required += ["action:%d" % a for a in ACTIONS]
    required += ["cr_status:0x%08x" % s for s in
                 (ST_SUCCESS, ST_SHARING_VIOLATION, ST_OBJECT_NAME_COLLISION,
                  ST_OBJECT_NAME_NOT_FOUND)]
    required += ["read:0x%08x" % ST_SUCCESS, "read:0x%08x" % ST_END_OF_FILE]
    required += ["write:0x%08x" % ST_SUCCESS]

    missing = [b for b in required if b not in buckets]
    print("SMB2 MBT coverage over %d trace(s):" % len(traces))
    for b in required:
        print("  %-28s %s" % (b, "HIT" if b in buckets else "MISSING"))
    if missing:
        sys.stderr.write("coverage gate: %d bucket(s) never exercised: %s\n"
                         % (len(missing), ", ".join(missing)))
        return 1
    print("all %d coverage buckets exercised" % len(required))
    return 0


if __name__ == "__main__":
    sys.exit(main())
