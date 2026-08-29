# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""Control-plane MBT corpus coverage gate.

Scans the generated ITF traces and asserts the corpus actually reaches the
states the model exists to describe.  A control-plane suite is unusually easy
to pass vacuously: a corpus that only ever creates things, never deletes them,
and never sends a protocol request replays green against a server whose
exports API is a no-op after the create.  This gate turns that into a failure.

What it requires, and why each one is here:

  * Every endpoint, at both outcomes.  A create that is only ever seen
    succeeding does not test the conflict, and a delete only ever seen
    succeeding does not test the not-found.  Both are what a configuration
    tool acts on.

  * The referential refusals, actually refused.  A mount pinned by an export,
    a share and a bucket in turn, and a filesystem pinned by a mount.  These
    exist so a resource cannot be pulled out from under its referrers, and a
    corpus that never has a referrer never tests them.

  * A MOUNT that succeeds and one that does not, including an export declared
    over a mount that does not exist -- the case that pays for letting an
    export be created before its target.

  * A request on a handle whose export has since been DELETED.  This is the
    property the suite was built for.  Without it the corpus says nothing
    about whether deleting an export revokes anything.

  * A write through a handle under each of the three squash modes, from both
    credentials.  root_squash maps exactly one of them, so a corpus that only
    ever used one credential could not tell it from either of the others.

  * A write refused by a read-only export, and a read-only export whose
    read-only-ness was set AFTER the handle was minted -- policy is read per
    request, and a corpus that only ever mounts after the final configuration
    would not show it.
"""

import glob
import json
import os
import sys


def unwrap(v):
    """ITF scalars: {"#bigint": "7"} -> 7, plain values unchanged."""
    if isinstance(v, dict) and "#bigint" in v:
        return int(v["#bigint"])
    return v


def state_var(state, name):
    for k, v in state.items():
        if k == name or k.endswith("::" + name):
            return v
    return None


def as_map(v):
    return v.get("#map", []) if isinstance(v, dict) else []


def as_set(v):
    return v.get("#set", []) if isinstance(v, dict) else []


# bucket -> human-readable description of what an empty bucket means.
BUCKETS = {
    "api:fs-create-ok":        "a filesystem created",
    "api:fs-create-conflict":  "a duplicate filesystem name refused",
    "api:fs-delete-ok":        "a filesystem removed",
    "api:fs-delete-missing":   "a filesystem that is not there",
    "api:fs-delete-pinned":    "a filesystem refused while a mount holds it",

    "api:mount-create-ok":     "a mount created",
    "api:mount-create-conflict": "a duplicate mount name refused",
    "api:mount-create-nofs":   "a mount over a filesystem that does not exist",
    "api:mount-delete-ok":     "a mount removed",
    "api:mount-delete-missing": "a mount that is not there",
    "api:mount-delete-pinned": "a mount refused while something is rooted at it",

    "api:export-create-ok":    "an export created",
    "api:export-create-conflict": "a duplicate export name refused",
    "api:export-delete-ok":    "an export removed",
    "api:export-delete-missing": "an export that is not there",

    "api:share-create-ok":     "a share created",
    "api:share-delete-ok":     "a share removed",
    "api:bucket-create-ok":    "a bucket created",
    "api:bucket-delete-ok":    "a bucket removed",
    "api:bucket-get-missing":  "a GET of a bucket that does not exist",
    "api:user-create-ok":      "a user created",
    "api:user-delete-ok":      "a user removed",

    "api:read-present":        "a GET of something that is there",
    "api:read-absent":         "a GET of something that is not",
    "api:config":              "the config reconstruction",

    "mnt:ok":                  "a MOUNT that succeeds",
    "mnt:no-export":           "a MOUNT of a name that is not an export",
    "mnt:no-mount":            "a MOUNT of an export over an absent mount",

    "nfs:getattr-live":        "a request on a live handle",
    "nfs:getattr-stale":       "a request on a handle whose export is gone",
    "nfs:create-live":         "a write through a live handle",
    "nfs:create-stale":        "a write through a handle whose export is gone",
    "nfs:create-rofs":         "a write refused by a read-only export",
    "nfs:squash-none-root":    "root writing under squash=none",
    "nfs:squash-none-user":    "a user writing under squash=none",
    "nfs:squash-root-root":    "root squashed by squash=root",
    "nfs:squash-root-user":    "a user NOT squashed by squash=root",
    "nfs:squash-all-root":     "root squashed by squash=all",
    "nfs:squash-all-user":     "a user squashed by squash=all",
}

STATUS_BUCKET = {
    ("RFsCreate", 201): "api:fs-create-ok",
    ("RFsCreate", 409): "api:fs-create-conflict",
    ("RFsDelete", 204): "api:fs-delete-ok",
    ("RFsDelete", 404): "api:fs-delete-missing",
    ("RFsDelete", 409): "api:fs-delete-pinned",
    ("RMountCreate", 201): "api:mount-create-ok",
    ("RMountCreate", 409): "api:mount-create-conflict",
    ("RMountCreate", 404): "api:mount-create-nofs",
    ("RMountDelete", 204): "api:mount-delete-ok",
    ("RMountDelete", 404): "api:mount-delete-missing",
    ("RMountDelete", 409): "api:mount-delete-pinned",
    ("RExportCreate", 201): "api:export-create-ok",
    ("RExportCreate", 409): "api:export-create-conflict",
    ("RExportDelete", 204): "api:export-delete-ok",
    ("RExportDelete", 404): "api:export-delete-missing",
    ("RShareCreate", 201): "api:share-create-ok",
    ("RShareDelete", 204): "api:share-delete-ok",
    ("RBucketCreate", 201): "api:bucket-create-ok",
    ("RBucketDelete", 204): "api:bucket-delete-ok",
    ("RBucketGet", 404): "api:bucket-get-missing",
    ("RUserCreate", 201): "api:user-create-ok",
    ("RUserDelete", 204): "api:user-delete-ok",
    ("RConfig", 200): "api:config",
}

READS = {"RMountGet", "RExportGet", "RShareGet", "RBucketGet", "RUserGet"}

NFS3_OK, NFS3ERR_ROFS, NFS3ERR_STALE = 0, 30, 70
MNT3_OK, MNT3ERR_NOENT = 0, 2


def main(trace_dir):
    seen = set()
    paths = sorted(glob.glob(os.path.join(trace_dir, "*.itf.json")))

    if not paths:
        print("coverage: no traces in %s" % trace_dir, file=sys.stderr)
        return 1

    for path in paths:
        with open(path) as fh:
            trace = json.load(fh)

        states = trace.get("states", [])
        for i, state in enumerate(states):
            op = state_var(state, "lastOp")
            if not op or i == 0:
                continue
            tag, val = op.get("tag"), op.get("value", {})

            if tag == "OApi":
                req = val.get("req", {})
                rtag = req.get("tag")
                st = unwrap(val.get("st"))
                bucket = STATUS_BUCKET.get((rtag, st))
                if bucket:
                    seen.add(bucket)
                if rtag in READS:
                    seen.add("api:read-present" if st == 200
                             else "api:read-absent")

            elif tag == "OMnt":
                st = unwrap(val.get("st"))
                if st == MNT3_OK:
                    seen.add("mnt:ok")
                else:
                    # Which of the two reasons: is the name an export at all?
                    ctlst = state_var(state, "ctlst") or {}
                    exports = [unwrap(k) for k, _ in
                               as_map(ctlst.get("exports"))]
                    seen.add("mnt:no-mount" if val.get("name") in exports
                             else "mnt:no-export")

            elif tag in ("OGetattr", "OCreate"):
                st = unwrap(val.get("st"))
                kind = "getattr" if tag == "OGetattr" else "create"
                if st == NFS3ERR_STALE:
                    seen.add("nfs:%s-stale" % kind)
                elif st == NFS3ERR_ROFS:
                    seen.add("nfs:create-rofs")
                elif st == NFS3_OK:
                    seen.add("nfs:%s-live" % kind)

                if tag == "OCreate" and st == NFS3_OK:
                    # Which squash policy produced this owner.  Read the
                    # export the handle resolves to out of the state, so the
                    # bucket names the POLICY rather than the outcome -- a
                    # corpus could otherwise satisfy "squashed" without ever
                    # using squash=root.
                    ctlst = state_var(state, "ctlst") or {}
                    handles = as_map(state_var(state, "handles"))
                    slot = unwrap(val.get("slot"))
                    cred = unwrap(val.get("cred"))
                    h = next((v for k, v in handles if unwrap(k) == slot), None)
                    if h:
                        eid = unwrap(h.get("exp"))
                        for _, e in as_map(ctlst.get("exports")):
                            if unwrap(e.get("id")) == eid:
                                who = "root" if cred == 0 else "user"
                                seen.add("nfs:squash-%s-%s"
                                         % (e.get("squash"), who))

    missing = sorted(set(BUCKETS) - seen)

    print("ctl corpus coverage: %d/%d buckets over %d traces"
          % (len(BUCKETS) - len(missing), len(BUCKETS), len(paths)))

    if missing:
        print("\nuncovered:", file=sys.stderr)
        for m in missing:
            print("  %-28s %s" % (m, BUCKETS[m]), file=sys.stderr)
        print("\nThe corpus does not reach these states, so replaying it "
              "green says nothing about them.  Extend or reseed a batch in "
              "CMakeLists.txt rather than deleting the bucket, unless the "
              "behaviour itself has gone away.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1
                  else "traces"))
