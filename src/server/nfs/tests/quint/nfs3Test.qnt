// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Deterministic self-checks for the NFS3 model, executed with `quint test`.
// These validate the model's own transition logic without a server, so a
// model regression is caught before it can invalidate generated traces.

module nfs3Test {
    import nfs3.* from "./nfs3"

    run createThenLookupTest =
        init
            .then(applyCreate(ROOT, "a", Unchecked, 420, ROOT_CRED))
            .expect(lastOp == OCreate({ dir: ROOT, name: "a", cmode: Unchecked,
                                        mode: 420, verf: -1, status: OK, obj: 1 , cred: ROOT_CRED }))
            .then(applyLookup(ROOT, "a"))
            .expect(lastOp == OLookup({ dir: ROOT, name: "a", status: OK, child: 1 }))
            .then(applyLookup(ROOT, "b"))
            .expect(lastOp == OLookup({ dir: ROOT, name: "b", status: ERR_NOENT, child: -1 }))
            .then(applyLookup(1, "a"))
            .expect(lastOp == OLookup({ dir: 1, name: "a", status: ERR_NOTDIR, child: -1 }))

    run guardedCreateTest =
        init
            .then(applyCreate(ROOT, "a", Guarded, 420, ROOT_CRED))
            .then(applyCreate(ROOT, "a", Guarded, 493, ROOT_CRED))
            .expect(lastOp == OCreate({ dir: ROOT, name: "a", cmode: Guarded,
                                        mode: 493, verf: -1, status: ERR_EXIST, obj: -1 , cred: ROOT_CRED }))
            // UNCHECKED over the existing file: same object, mode untouched.
            .then(applyCreate(ROOT, "a", Unchecked, 493, ROOT_CRED))
            .expect(lastOp == OCreate({ dir: ROOT, name: "a", cmode: Unchecked,
                                        mode: 493, verf: -1, status: OK, obj: 1 , cred: ROOT_CRED }))
            .expect(fs.get(1).mode == 420)

    // RFC 1813 3.3.8: exclusive create is idempotent for the same verifier
    // (retry returns the same object), and NFS3ERR_EXIST for a different
    // verifier.  chimera returns EXIST even on the same-verifier retry --
    // that divergence is deviation `exclusive-create-retry-exist`, checked
    // against the live server by the harness; the *model* is RFC-correct.
    run exclusiveCreateTest =
        init
            .then(applyCreateExcl(ROOT, "x", 1))
            .expect(lastOp == OCreate({ dir: ROOT, name: "x", cmode: Exclusive,
                                        mode: -1, verf: 1, status: OK, obj: 1 , cred: ROOT_CRED }))
            .expect(fs.get(1).mode == 420 and fs.get(1).xverf == 1)
            // same verifier -> idempotent success, same object
            .then(applyCreateExcl(ROOT, "x", 1))
            .expect(lastOp == OCreate({ dir: ROOT, name: "x", cmode: Exclusive,
                                        mode: -1, verf: 1, status: OK, obj: 1 , cred: ROOT_CRED }))
            // different verifier -> EXIST
            .then(applyCreateExcl(ROOT, "x", 2))
            .expect(lastOp == OCreate({ dir: ROOT, name: "x", cmode: Exclusive,
                                        mode: -1, verf: 2, status: ERR_EXIST, obj: -1 , cred: ROOT_CRED }))

    // A WRITE (mtime change) invalidates the verifier, so even a same-verifier
    // CREATE afterwards is EXIST; a mode-only SETATTR does not.
    run exclusiveVerifierClearedTest =
        init
            .then(applyCreateExcl(ROOT, "y", 1))
            .then(applySetattr(1, 493, -1, 0))          // mode-only: verifier intact
            .then(applyCreateExcl(ROOT, "y", 1))
            .expect(lastOp == OCreate({ dir: ROOT, name: "y", cmode: Exclusive,
                                        mode: -1, verf: 1, status: OK, obj: 1 , cred: ROOT_CRED }))
            .then(applyWrite(1, 0, 1, 1, 2, ROOT_CRED))            // WRITE: clears verifier
            .expect(fs.get(1).xverf == 0)
            .then(applyCreateExcl(ROOT, "y", 1))
            .expect(lastOp == OCreate({ dir: ROOT, name: "y", cmode: Exclusive,
                                        mode: -1, verf: 1, status: ERR_EXIST, obj: -1 , cred: ROOT_CRED }))

    run writeReadTest =
        init
            .then(applyCreate(ROOT, "a", Unchecked, 420, ROOT_CRED))
            // Sparse write: blocks 2-3 := 1, blocks 0-1 zero-filled.
            .then(applyWrite(1, 2, 2, 1, 0, ROOT_CRED))
            .expect(lastOp == OWrite({ file: 1, offset: 2, count: 2, pat: 1, stable: 0,
                                       status: OK, newSizeBlocks: 4 , cred: ROOT_CRED }))
            .expect(fs.get(1).data == [0, 0, 1, 1])
            .then(applyWrite(1, 1, 1, 3, 2, ROOT_CRED))
            .expect(fs.get(1).data == [0, 3, 1, 1])
            .then(applyRead(1, 1, 2, ROOT_CRED))
            .expect(lastOp == ORead({ file: 1, offset: 1, count: 2, status: OK,
                                      retCount: 2, eof: false, blocks: [3, 1] , cred: ROOT_CRED }))
            // Read reaching exactly EOF sets eof.
            .then(applyRead(1, 2, 2, ROOT_CRED))
            .expect(lastOp == ORead({ file: 1, offset: 2, count: 2, status: OK,
                                      retCount: 2, eof: true, blocks: [1, 1] , cred: ROOT_CRED }))
            // Short read crossing EOF.
            .then(applyRead(1, 3, 2, ROOT_CRED))
            .expect(lastOp == ORead({ file: 1, offset: 3, count: 2, status: OK,
                                      retCount: 1, eof: true, blocks: [1] , cred: ROOT_CRED }))
            // Read entirely beyond EOF.
            .then(applyRead(1, 5, 1, ROOT_CRED))
            .expect(lastOp == ORead({ file: 1, offset: 5, count: 1, status: OK,
                                      retCount: 0, eof: true, blocks: [] , cred: ROOT_CRED }))
            .then(applyCommit(1))
            .expect(lastOp == OCommit({ file: 1, status: OK }))

    run setattrTest =
        init
            .then(applyCreate(ROOT, "a", Unchecked, 420, ROOT_CRED))
            .then(applyWrite(1, 0, 2, 2, 2, ROOT_CRED))
            // Truncate to one block.
            .then(applySetattr(1, -1, 1, 0))
            .expect(fs.get(1).data == [2])
            // Extend to three blocks: zero-filled.
            .then(applySetattr(1, -1, 3, 1))
            .expect(fs.get(1).data == [2, 0, 0])
            // Stale guard: NOT_SYNC and no change.
            .then(applySetattr(1, 493, 0, 2))
            .expect(lastOp == OSetattr({ obj: 1, mode: 493, sizeBlocks: 0,
                                         guard: 2, status: ERR_NOT_SYNC }))
            .expect(fs.get(1).mode == 420 and fs.get(1).data == [2, 0, 0])
            .then(applySetattr(1, 493, -1, 0))
            .expect(fs.get(1).mode == 493)

    // REMOVE and RMDIR are both type-checked (chimera enforces the type via
    // the CHIMERA_VFS_REMOVE_ISNOTDIR / _ISDIR flags): REMOVE of a directory
    // is NFS3ERR_ISDIR, RMDIR of a non-directory is NFS3ERR_NOTDIR, and
    // neither mutates state on a mismatch.
    run removeRmdirTypeCheckTest =
        init
            .then(applyMkdir(ROOT, "d", 493))
            .then(applyCreate(ROOT, "f", Unchecked, 420, ROOT_CRED))
            // REMOVE of a directory -> ISDIR, no change.
            .then(applyRemove(ROOT, "d"))
            .expect(lastOp == ORemove({ dir: ROOT, name: "d", status: ERR_ISDIR }))
            .expect(fs.keys() == Set(ROOT, 1, 2))
            // RMDIR of a file -> NOTDIR, no change.
            .then(applyRmdir(ROOT, "f"))
            .expect(lastOp == ORmdir({ dir: ROOT, name: "f", status: ERR_NOTDIR }))
            .expect(fs.keys() == Set(ROOT, 1, 2))
            // Correct types succeed.
            .then(applyRemove(ROOT, "f"))
            .expect(lastOp == ORemove({ dir: ROOT, name: "f", status: OK }))
            .then(applyRmdir(ROOT, "d"))
            .expect(lastOp == ORmdir({ dir: ROOT, name: "d", status: OK }))
            .expect(fs.keys() == Set(ROOT) and removed == Set(1, 2))
            // Immediately after a real removal, GETATTR on the dead handle
            // returns NOENT (the reliable case; see the staleGetattr note in
            // nfs3.qnt for why longer traces cannot assert this).
            .then(applyStaleGetattr(1))
            .expect(lastOp == OStaleGetattr({ obj: 1, status: ERR_NOENT }))

    run mkdirRmdirTest =
        init
            .then(applyMkdir(ROOT, "d", 493))
            .then(applyMkdir(ROOT, "d", 420))
            .expect(lastOp == OMkdir({ dir: ROOT, name: "d", mode: 420,
                                       status: ERR_EXIST, obj: -1 }))
            .then(applyCreate(1, "a", Unchecked, 420, ROOT_CRED))
            .then(applyRmdir(ROOT, "d"))
            .expect(lastOp == ORmdir({ dir: ROOT, name: "d", status: ERR_NOTEMPTY }))
            .then(applyRemove(1, "a"))
            .then(applyRmdir(ROOT, "d"))
            .expect(lastOp == ORmdir({ dir: ROOT, name: "d", status: OK }))
            .expect(fs.keys() == Set(ROOT))

    run renameMatrixTest =
        init
            .then(applyMkdir(ROOT, "a", 493))          // fid 1: dir a
            .then(applyCreate(ROOT, "b", Unchecked, 420, ROOT_CRED))   // fid 2: file b
            .then(applyMkdir(ROOT, "c", 493))          // fid 3: dir c
            // file over dir -> ISDIR
            .then(applyRename(ROOT, "b", ROOT, "a"))
            .expect(lastOp == ORename({ fromDir: ROOT, fromName: "b",
                                        toDir: ROOT, toName: "a", status: ERR_ISDIR }))
            // dir over file -> NOTDIR
            .then(applyRename(ROOT, "a", ROOT, "b"))
            .expect(lastOp == ORename({ fromDir: ROOT, fromName: "a",
                                        toDir: ROOT, toName: "b", status: ERR_NOTDIR }))
            // dir into own subtree -> INVAL
            .then(applyRename(ROOT, "a", 1, "d"))
            .expect(lastOp == ORename({ fromDir: ROOT, fromName: "a",
                                        toDir: 1, toName: "d", status: ERR_INVAL }))
            // self-rename -> OK no-op
            .then(applyRename(ROOT, "a", ROOT, "a"))
            .expect(lastOp == ORename({ fromDir: ROOT, fromName: "a",
                                        toDir: ROOT, toName: "a", status: OK }))
            .expect(fs.get(ROOT).ents.keys() == Set("a", "b", "c"))
            // dir over empty dir -> OK, target dies
            .then(applyRename(ROOT, "a", ROOT, "c"))
            .expect(lastOp == ORename({ fromDir: ROOT, fromName: "a",
                                        toDir: ROOT, toName: "c", status: OK }))
            .expect(fs.get(ROOT).ents.keys() == Set("b", "c") and
                    fs.get(ROOT).ents.get("c") == 1 and removed == Set(3))
            // file over file -> OK, target dies
            .then(applyCreate(ROOT, "d", Unchecked, 420, ROOT_CRED))    // fid 4
            .then(applyRename(ROOT, "d", ROOT, "b"))
            .expect(fs.get(ROOT).ents.get("b") == 4 and removed == Set(2, 3))
            // nonexistent source -> NOENT
            .then(applyRename(ROOT, "zz", ROOT, "b"))
            .expect(lastOp == ORename({ fromDir: ROOT, fromName: "zz",
                                        toDir: ROOT, toName: "b", status: ERR_NOENT }))

    run linkTest =
        init
            .then(applyCreate(ROOT, "a", Unchecked, 420, ROOT_CRED))
            .then(applyMkdir(ROOT, "d", 493))
            .then(applyLink(1, 2, "x"))
            .expect(lastOp == OLink({ obj: 1, dir: 2, name: "x", status: OK }))
            .expect(fs.get(1).nlink == 2)
            // hardlink-pair rename: no-op, both names remain
            .then(applyRename(ROOT, "a", 2, "x"))
            .expect(lastOp == ORename({ fromDir: ROOT, fromName: "a",
                                        toDir: 2, toName: "x", status: OK }))
            .expect(fs.get(ROOT).ents.keys().contains("a") and
                    fs.get(2).ents.keys().contains("x"))
            // link to existing name -> EXIST
            .then(applyLink(1, 2, "x"))
            .expect(lastOp == OLink({ obj: 1, dir: 2, name: "x", status: ERR_EXIST }))
            // link a directory -> ISDIR
            .then(applyLink(2, ROOT, "y"))
            .expect(lastOp == OLink({ obj: 2, dir: ROOT, name: "y", status: ERR_ISDIR }))
            // removing one name keeps the object with nlink 1
            .then(applyRemove(ROOT, "a"))
            .expect(fs.get(1).nlink == 1 and removed == Set())
            // removing the last name kills it
            .then(applyRemove(2, "x"))
            .expect(removed == Set(1))

    run symlinkTest =
        init
            .then(applySymlink(ROOT, "s", "a/b/target"))
            .expect(lastOp == OSymlink({ dir: ROOT, name: "s", target: "a/b/target",
                                         status: OK, obj: 1 }))
            .expect(fs.get(1).mode == 493)   // chimera: mode forced 0755
            .then(applyReadlink(1))
            .expect(lastOp == OReadlink({ obj: 1, status: OK, target: "a/b/target" }))
            .then(applySymlink(ROOT, "s", "x"))
            .expect(lastOp == OSymlink({ dir: ROOT, name: "s", target: "x",
                                         status: ERR_EXIST, obj: -1 }))

    run readdirTest =
        init
            .then(applyCreate(ROOT, "a", Unchecked, 420, ROOT_CRED))
            .then(applyMkdir(ROOT, "b", 493))
            .then(applyReaddir(ROOT, false))
            .expect(lastOp == OReaddir({ dir: ROOT, plus: false, status: OK,
                                         names: Set("a", "b") }))
            .then(applyReaddir(1, true))
            .expect(lastOp == OReaddir({ dir: 1, plus: true, status: ERR_NOTDIR,
                                         names: Set() }))

    run emptyFileReadTest =
        init
            .then(applyCreate(ROOT, "a", Unchecked, 420, ROOT_CRED))
            .then(applyRead(1, 0, 1, ROOT_CRED))
            .expect(lastOp == ORead({ file: 1, offset: 0, count: 1, status: OK,
                                      retCount: 0, eof: true, blocks: [] , cred: ROOT_CRED }))

    // A long random walk preserves the structural invariants.
    run randomWalkTest =
        init.then(200.reps(_ => all { inv, step }))
}
