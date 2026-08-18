#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""Model-coverage report over a corpus of generated ITF traces.

Answers "which interesting behaviors of the model are actually being
exercised" by classifying every trace step into an equivalence-class bucket
(operation x outcome branch, plus a set of interaction predicates) and
aggregating hit counts across the whole corpus.  Buckets that never fire
(STARVED) or fire rarely (LOW) are the coverage holes: either the generator
needs biasing/more seeds, or the scenario needs a targeted `quint verify`
witness (see DEVIATIONS.md / the coverage notes).

This reads the same ITF traces the replayer consumes and needs no server -- it
measures the *model's* generated behavior, independent of chimera.

Usage:
    coverage.py trace1.itf.json trace2.itf.json ...
    coverage.py --glob 'traces/*.itf.json' [--low N] [--expect BUCKETS_FILE]
"""

import argparse
import glob as globmod
import json
import sys


class TraceFormatError(Exception):
    pass


def itf_decode(v):
    """Decode one ITF-encoded Quint value into plain Python data.

    Unknown encodings raise TraceFormatError so a Quint format change is a
    loud failure, never a silently skipped check.  (Formerly shared with
    the retired python replay harness; see git history for replay.py.)
    """
    if isinstance(v, dict):
        special = [k for k in v if k.startswith("#")]
        if special == ["#bigint"]:
            return int(v["#bigint"])
        if special == ["#map"]:
            return {itf_decode(k): itf_decode(val) for k, val in v["#map"]}
        if special == ["#set"]:
            return [itf_decode(x) for x in v["#set"]]
        if special == ["#tup"]:
            return tuple(itf_decode(x) for x in v["#tup"])
        if special:
            raise TraceFormatError(f"unrecognized ITF encoding {special}")
        if set(v.keys()) == {"tag", "value"}:
            return {"tag": v["tag"], "value": itf_decode(v["value"])}
        return {k: itf_decode(val) for k, val in v.items()}
    if isinstance(v, list):
        return [itf_decode(x) for x in v]
    if isinstance(v, (str, bool, int)):
        return v
    raise TraceFormatError(f"unrecognized ITF value {v!r}")


def load_trace(path):
    with open(path) as f:
        raw = json.load(f)
    if "states" not in raw or "vars" not in raw:
        raise TraceFormatError(f"{path}: not an ITF trace")
    states = []
    for st in raw["states"]:
        states.append({k: itf_decode(v) for k, v in st.items()
                       if k != "#meta" and not k.startswith("mbt::")})
    for st in states:
        if "lastOp" not in st or "fs" not in st:
            raise TraceFormatError(f"{path}: state missing lastOp/fs")
    return states

# Every bucket the classifier can emit.  Listing them up front means a bucket
# that is defined-but-never-hit shows up as STARVED rather than silently
# missing -- the whole point of the report.
ALL_BUCKETS = [
    # LOOKUP
    "lookup:ok", "lookup:noent", "lookup:notdir",
    # GETATTR (by target type)
    "getattr:reg", "getattr:dir", "getattr:lnk", "getattr:fifo", "getattr:sock",
    # SETATTR
    "setattr:mode", "setattr:truncate", "setattr:extend", "setattr:size-same",
    "setattr:guard-match", "setattr:guard-stale",
    # ACCESS
    "access:read", "access:execute", "access:all",
    # CREATE
    "create:unchecked-new", "create:unchecked-existing",
    "create:guarded-new", "create:guarded-exist",
    "create:exclusive-new", "create:exclusive-retry-same",
    "create:exclusive-retry-diff",
    # MKDIR / SYMLINK / MKNOD / READLINK
    "mkdir:ok", "mkdir:exist",
    "symlink:ok", "symlink:exist", "readlink:ok",
    "mknod:fifo", "mknod:sock", "mknod:exist",
    # WRITE (disposition x stability)
    "write:overwrite", "write:extend", "write:sparse",
    "write:unstable", "write:datasync", "write:filesync",
    # READ
    "read:full", "read:straddle-eof", "read:past-eof", "read:at-eof",
    # COMMIT
    "commit:ok",
    # REMOVE
    "remove:ok-dies", "remove:ok-drop-hardlink", "remove:isdir", "remove:noent",
    # RMDIR
    "rmdir:ok", "rmdir:notdir", "rmdir:notempty", "rmdir:noent",
    # RENAME
    "rename:move", "rename:noop-self", "rename:noop-hardlink",
    "rename:replace-file", "rename:replace-empty-dir",
    "rename:isdir", "rename:notdir", "rename:notempty",
    "rename:inval-subtree", "rename:noent",
    # LINK
    "link:ok", "link:exist", "link:isdir",
    # READDIR
    "readdir:ok", "readdirplus:ok", "readdir:notdir",
    # constant procs
    "fsstat:ok", "fsinfo:ok", "pathconf:ok",
    # ---- interaction / structural predicates (cross-step) ----
    "x:hardlink-exists",          # some file reached nlink >= 2
    "x:tree-depth>=2", "x:tree-depth>=3", "x:tree-depth>=4",
    "x:rename-moves-dir",         # a directory changed parent
    "x:excl-verifier-cleared",    # exclusive file written, then same-verf retry
    "x:remove-empties-set",       # removed-set grew (object fully unlinked)
    "x:objects>=10",              # namespace filled near the cap
]

OK = 0


def ftype(node):
    return node["ftype"]["tag"]


def tree_depth(fs, fid=0, d=1, seen=None):
    if seen is None:
        seen = set()
    if fid in seen:
        return d
    seen.add(fid)
    m = d
    node = fs.get(fid)
    if node is None:
        return d
    for c in node["ents"].values():
        cn = fs.get(c)
        if cn is not None and cn["ftype"]["tag"] == "TDir":
            m = max(m, tree_depth(fs, c, d + 1, seen))
    return m


FTYPE_BUCKET = {"TReg": "reg", "TDir": "dir", "TLnk": "lnk",
                "TFifo": "fifo", "TSock": "sock"}
ACCESS_BUCKET = {1: "read", 32: "execute", 63: "all"}
STABLE_BUCKET = {0: "unstable", 1: "datasync", 2: "filesync"}


def classify_step(pre, post, op_tag, op, buckets):
    """Emit buckets for one step given pre/post fs snapshots and the lastOp."""
    def hit(b):
        buckets.add(b)

    st = op.get("status")

    if op_tag == "OLookup":
        hit({OK: "lookup:ok", 2: "lookup:noent", 20: "lookup:notdir"}[st])
    elif op_tag == "OGetattr":
        hit("getattr:" + FTYPE_BUCKET[ftype(post[op["obj"]])])
    elif op_tag == "OSetattr":
        if st == 10002:
            hit("setattr:guard-stale")
        else:
            if op["guard"] == 1:
                hit("setattr:guard-match")
            if op["mode"] >= 0 and op["sizeBlocks"] < 0:
                hit("setattr:mode")
            if op["sizeBlocks"] >= 0:
                old = len(pre[op["obj"]]["data"])
                new = op["sizeBlocks"]
                hit("setattr:truncate" if new < old else
                    "setattr:extend" if new > old else "setattr:size-same")
    elif op_tag == "OAccess":
        hit("access:" + ACCESS_BUCKET.get(op["mask"], "all"))
    elif op_tag == "OCreate":
        cm = op["cmode"]["tag"]
        exists = op["name"] in pre[op["dir"]]["ents"]
        if cm == "Exclusive":
            if not exists:
                hit("create:exclusive-new")
            elif st == OK:
                hit("create:exclusive-retry-same")
            else:
                hit("create:exclusive-retry-diff")
        elif cm == "Guarded":
            hit("create:guarded-exist" if st != OK else "create:guarded-new")
        else:
            hit("create:unchecked-existing" if exists else "create:unchecked-new")
    elif op_tag == "OMkdir":
        hit("mkdir:ok" if st == OK else "mkdir:exist")
    elif op_tag == "OSymlink":
        hit("symlink:ok" if st == OK else "symlink:exist")
    elif op_tag == "OReadlink":
        hit("readlink:ok")
    elif op_tag == "OMknod":
        if st != OK:
            hit("mknod:exist")
        else:
            hit("mknod:" + ("fifo" if op["ftype"]["tag"] == "TFifo" else "sock"))
    elif op_tag == "OWrite":
        old = len(pre[op["file"]]["data"])
        off, cnt = op["offset"], op["count"]
        if off > old:
            hit("write:sparse")
        elif off + cnt > old:
            hit("write:extend")
        else:
            hit("write:overwrite")
        hit("write:" + STABLE_BUCKET[op["stable"]])
    elif op_tag == "ORead":
        sz = len(pre[op["file"]]["data"])
        off, cnt = op["offset"], op["count"]
        if off >= sz:
            hit("read:past-eof")
        elif off + cnt > sz:
            hit("read:straddle-eof")
        else:
            hit("read:full")
        if op["eof"] and op["retCount"] > 0:
            hit("read:at-eof")
    elif op_tag == "OCommit":
        hit("commit:ok")
    elif op_tag == "ORemove":
        if st == 21:
            hit("remove:isdir")
        elif st == 2:
            hit("remove:noent")
        else:
            victim = pre[op["dir"]]["ents"][op["name"]]
            hit("remove:ok-dies" if pre[victim]["nlink"] == 1
                else "remove:ok-drop-hardlink")
    elif op_tag == "ORmdir":
        hit({OK: "rmdir:ok", 20: "rmdir:notdir",
             66: "rmdir:notempty", 2: "rmdir:noent"}[st])
    elif op_tag == "ORename":
        classify_rename(pre, op, st, hit)
    elif op_tag == "OLink":
        hit({OK: "link:ok", 17: "link:exist", 21: "link:isdir"}[st])
    elif op_tag == "OReaddir":
        if st == 20:
            hit("readdir:notdir")
        else:
            hit("readdirplus:ok" if op["plus"] else "readdir:ok")
    elif op_tag == "OFsstat":
        hit("fsstat:ok")
    elif op_tag == "OFsinfo":
        hit("fsinfo:ok")
    elif op_tag == "OPathconf":
        hit("pathconf:ok")


def classify_rename(pre, op, st, hit):
    sd, sn, td, tn = op["fromDir"], op["fromName"], op["toDir"], op["toName"]
    src_present = sn in pre[sd]["ents"]
    if st == 2:
        hit("rename:noent")
        return
    if st == 22:
        hit("rename:inval-subtree")
        return
    if st == 21:
        hit("rename:isdir")
        return
    if st == 20:
        hit("rename:notdir")
        return
    if st == 66:
        hit("rename:notempty")
        return
    # OK cases
    src = pre[sd]["ents"].get(sn)
    tgt = pre[td]["ents"].get(tn)
    if (sd == td and sn == tn) or (tgt is not None and tgt == src):
        hit("rename:noop-self" if (sd == td and sn == tn)
            else "rename:noop-hardlink")
    elif tgt is None:
        hit("rename:move")
    elif pre[tgt]["ftype"]["tag"] == "TDir":
        hit("rename:replace-empty-dir")
    else:
        hit("rename:replace-file")


def structural_buckets(pre, post, op_tag, op, buckets):
    """Cross-step predicates that depend on state, not just the op."""
    # Hardlink existence / near-cap namespace / tree depth in the post-state.
    for node in post.values():
        if node["ftype"]["tag"] != "TDir" and node["nlink"] >= 2:
            buckets.add("x:hardlink-exists")
            break
    n = len(post)
    if n >= 10:
        buckets.add("x:objects>=10")
    d = tree_depth(post)
    if d >= 2:
        buckets.add("x:tree-depth>=2")
    if d >= 3:
        buckets.add("x:tree-depth>=3")
    if d >= 4:
        buckets.add("x:tree-depth>=4")
    # A directory changed parent (cross-dir move of a dir).
    if op_tag == "ORename" and op["status"] == OK:
        src = pre[op["fromDir"]]["ents"].get(op["fromName"])
        if (src is not None and pre[src]["ftype"]["tag"] == "TDir"
                and op["fromDir"] != op["toDir"]):
            buckets.add("x:rename-moves-dir")


def analyze(trace_files):
    total = {b: 0 for b in ALL_BUCKETS}
    unknown = {}
    steps = 0
    # excl-verifier-cleared needs per-fid history within a trace.
    for path in trace_files:
        states = load_trace(path)
        written_excl = set()   # fids: exclusively-created then written (mtime bump)
        for i in range(1, len(states)):
            pre = states[i - 1]["fs"]
            post = states[i]["fs"]
            op_tag = states[i]["lastOp"]["tag"]
            op = states[i]["lastOp"]["value"]
            steps += 1
            here = set()
            classify_step(pre, post, op_tag, op, here)
            structural_buckets(pre, post, op_tag, op, here)

            # exclusive-verifier-cleared interaction: a file created exclusively,
            # then written (clears xverf), then an exclusive retry -> EXIST.
            if op_tag == "OCreate" and op["cmode"]["tag"] == "Exclusive" \
                    and op["status"] == OK and op["obj"] != -1:
                if pre.get("__na__") is None:
                    written_excl.discard(op["obj"])
            if op_tag == "OWrite" and op["file"] in post \
                    and post[op["file"]].get("xverf", 0) == 0:
                written_excl.add(op["file"])
            if op_tag == "OCreate" and op["cmode"]["tag"] == "Exclusive" \
                    and op["status"] == 17:
                ex = pre[op["dir"]]["ents"].get(op["name"])
                if ex in written_excl:
                    here.add("x:excl-verifier-cleared")

            if len(states[i]["removed"]) > len(states[i - 1]["removed"]):
                here.add("x:remove-empties-set")

            for b in here:
                if b in total:
                    total[b] += 1
                else:
                    unknown[b] = unknown.get(b, 0) + 1
    return total, unknown, steps


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("traces", nargs="*")
    ap.add_argument("--glob")
    ap.add_argument("--low", type=int, default=5,
                    help="threshold below which a bucket is flagged LOW")
    ap.add_argument("--fail-on-starved", action="store_true",
                    help="exit non-zero if any behavior bucket has zero hits")
    args = ap.parse_args()

    files = list(args.traces)
    if args.glob:
        files += sorted(globmod.glob(args.glob))
    if not files:
        ap.error("no trace files given")

    total, unknown, steps = analyze(files)

    print(f"corpus: {len(files)} traces, {steps} steps\n")
    width = max(len(b) for b in total)
    for b in ALL_BUCKETS:
        c = total[b]
        flag = "  STARVED" if c == 0 else ("  low" if c < args.low else "")
        print(f"  {b:<{width}}  {c:6d}{flag}")

    if unknown:
        print("\nUNCLASSIFIED buckets (classifier/model drift):")
        for b, c in sorted(unknown.items()):
            print(f"  {b}: {c}")

    starved = [b for b in ALL_BUCKETS if total[b] == 0]
    low = [b for b in ALL_BUCKETS if 0 < total[b] < args.low]
    covered = sum(1 for b in ALL_BUCKETS if total[b] > 0)
    print(f"\nsummary: {covered}/{len(ALL_BUCKETS)} buckets covered; "
          f"{len(starved)} starved, {len(low)} low")
    if starved:
        print("STARVED: " + ", ".join(starved))
    if low:
        print("LOW: " + ", ".join(low))

    if unknown:
        # A classifier bucket the ALL_BUCKETS list does not know about means
        # the model grew a behavior the coverage map has not caught up with.
        print("\nFAIL: unclassified buckets present (update ALL_BUCKETS)")
        sys.exit(1)
    if args.fail_on_starved and starved:
        print(f"\nFAIL: {len(starved)} behavior bucket(s) never exercised")
        sys.exit(1)


if __name__ == "__main__":
    main()
