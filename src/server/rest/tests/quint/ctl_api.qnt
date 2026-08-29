// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/**
 * ctl_api.qnt -- what chimera's administrative API is, as a state machine over
 * resources rather than as a list of endpoints.
 *
 * The control plane has five collections (filesystems, mounts, NFS exports,
 * SMB shares, S3 buckets) plus users, and almost everything it must get right
 * is one of three things:
 *
 *   1. A collection behaves like a collection.  After a create the resource is
 *      readable and listed; after a delete it is neither.  A second create of
 *      the same name is a conflict, not a silent overwrite -- an overwrite
 *      would change an export's access mode or a mount's target under a
 *      running client.  A second delete is a not-found, not a success.
 *
 *   2. References are honoured.  A mount is the target of exports, shares and
 *      buckets; a filesystem is the target of mounts.  Removing something out
 *      from under its referrers would leave an export pointing at nothing, so
 *      it must be refused while a referrer exists.  This is the property that
 *      cannot be checked one endpoint at a time, which is why the model holds
 *      all the collections at once.
 *
 *   3. The reconstruction agrees.  GET /api/v1/config rebuilds a chimera.json
 *      from live state, and it is the only place two independent renderings of
 *      the same facts exist.  The model requires them to agree exactly, which
 *      is what catches a field added to one and forgotten in the other.
 *
 * Everything here is a pure function of the state, so `apply` is both the
 * specification and the oracle: the replay harness issues the request against
 * a live chimera and compares its HTTP status against what `apply` says, then
 * compares its listings against the state `apply` produced.
 */
module ctlApi {

    // ==================================================================
    // HTTP status codes
    // ==================================================================

    pure val OK          = 200
    pure val CREATED     = 201
    pure val NO_CONTENT  = 204
    pure val BAD_REQUEST = 400
    pure val NOT_FOUND   = 404
    pure val CONFLICT    = 409

    // ==================================================================
    // Resources
    // ==================================================================

    /**
     * An NFS export.
     *
     * `id` is the stable export identity, not a serial number: it is stamped
     * into every filehandle minted under the export (nfs_fh_wrap.h), so it is
     * what a later request presents to say which export its handle belongs to.
     * That makes it part of the protocol surface, not an implementation
     * detail, and it is why the model pins one per export name rather than
     * letting the server assign them -- see ctl.qnt's EXPORTS for the rest of
     * that argument.
     *
     * `ro` and `squash` are the two access policies an export carries.  Both
     * are consulted per request rather than baked into the handle, so a
     * change to either takes effect on handles that already exist.
     */
    type Export = { path: str, id: int, ro: bool, squash: str }

    pure val SQ_NONE = "none"
    pure val SQ_ROOT = "root"
    pure val SQ_ALL  = "all"

    /**
     * A mount: which filesystem is attached at this name, and which INSTANCE
     * of it.
     *
     * The instance number has no counterpart in the REST API -- it is never
     * sent or returned -- but it is what a filehandle's inner VFS handle is
     * scoped to, so unmounting and remounting is not a no-op from a client's
     * point of view even when the same filesystem comes back.  Modelling it
     * lets `handleLive` below state that in one place instead of leaving it
     * implicit.
     */
    type Mount = { fs: str, inst: int }

    /**
     * The whole administrative state.
     *
     * Filesystems are named within one module (the model uses a single
     * mkfs-capable backend; a second would multiply the state space without
     * asking a new question, since the module is only ever part of a key).
     * Shares and buckets map a name to the mount they are rooted at, which is
     * all the control plane knows about them.
     */
    type Ctl = {
        fss: Set[str],
        mounts: str -> Mount,
        exports: str -> Export,
        shares: str -> str,
        buckets: str -> str,
        users: Set[str],
        nextInst: int,
    }

    // ==================================================================
    // Requests
    // ==================================================================

    /**
     * One REST call.
     *
     * The reads (get/list/config) are here alongside the writes because they
     * are where a write is observed: a create that returns 201 and then does
     * not appear in the listing has not created anything, and only a model
     * that can express both can say so.
     */
    type Req =
        | RFsCreate(str)
        | RFsDelete(str)
        | RMountCreate({ name: str, fs: str })
        | RMountDelete(str)
        | RMountGet(str)
        | RMountList
        | RExportCreate({ name: str, ro: bool, squash: str })
        | RExportDelete(str)
        | RExportGet(str)
        | RExportList
        | RShareCreate(str)
        | RShareDelete(str)
        | RShareGet(str)
        | RShareList
        | RBucketCreate(str)
        | RBucketDelete(str)
        | RBucketGet(str)
        | RBucketList
        | RUserCreate(str)
        | RUserDelete(str)
        | RUserGet(str)
        | RUserList
        | RConfig

    /// A request's outcome: the HTTP status, and the state it leaves behind.
    type ApiRes = { st: int, c: Ctl }

    // ==================================================================
    // Helpers
    // ==================================================================

    /// A map without one key.  Quint has no map-remove, and the idiom is
    /// verbose enough to be worth a name.
    pure def mapDrop(m: str -> a, k: str): str -> a =
        m.keys().exclude(Set(k)).mapBy(x => m.get(x))

    /// Every mount name some export, share or bucket is rooted at.
    pure def referencedMounts(c: Ctl): Set[str] =
        c.exports.keys().map(n => c.exports.get(n).path)
            .union(c.shares.keys().map(n => c.shares.get(n)))
            .union(c.buckets.keys().map(n => c.buckets.get(n)))

    /// Every filesystem name some mount is attached to.
    pure def referencedFss(c: Ctl): Set[str] =
        c.mounts.keys().map(n => c.mounts.get(n).fs)

    /// The export a filehandle's stamped id names, if any still does.
    pure def exportById(c: Ctl, id: int): Set[str] =
        c.exports.keys().filter(n => c.exports.get(n).id == id)

    // ==================================================================
    // The API
    // ==================================================================

    /**
     * Create a named filesystem.
     *
     * A name already taken is a conflict rather than a silent reuse: two
     * mounts of "the same" filesystem must mean the same bytes, and a create
     * that quietly returned the existing one would make a caller believe it
     * had a fresh namespace.
     */
    pure def fsCreate(c: Ctl, n: str): ApiRes =
        if (c.fss.contains(n)) { st: CONFLICT, c: c }
        else { st: CREATED, c: { ...c, fss: c.fss.union(Set(n)) } }

    /**
     * Remove a named filesystem.
     *
     * Refused while a mount is attached to it.  Not merely tidiness: the mount
     * is the path an export resolves through, so destroying the filesystem
     * under it would turn every handle in that namespace into a dangling
     * reference with no administrative action having said so.
     */
    pure def fsDelete(c: Ctl, n: str): ApiRes =
        if (not(c.fss.contains(n))) { st: NOT_FOUND, c: c }
        else if (referencedFss(c).contains(n)) { st: CONFLICT, c: c }
        else { st: NO_CONTENT, c: { ...c, fss: c.fss.exclude(Set(n)) } }

    /**
     * Attach a filesystem at a mount name.
     *
     * A target that does not exist is NOT_FOUND -- the same answer the
     * filesystems endpoint gives for a module it does not have.  It is worth
     * being explicit that this is a specification and not a description: a
     * server that answered 500 here would be reporting its own failure for
     * what is entirely the caller's error, and a caller cannot tell "your
     * filesystem name is wrong" from "the server is broken" in order to
     * retry.
     */
    pure def mountCreate(c: Ctl, n: str, fs: str): ApiRes =
        if (c.mounts.keys().contains(n)) { st: CONFLICT, c: c }
        else if (not(c.fss.contains(fs))) { st: NOT_FOUND, c: c }
        else { st: CREATED,
               c: { ...c,
                    mounts: c.mounts.put(n, { fs: fs, inst: c.nextInst }),
                    nextInst: c.nextInst + 1 } }

    /// Detach a mount.  Refused while an export, share or bucket is rooted at
    /// it, for the same reason fsDelete is refused while mounted.
    pure def mountDelete(c: Ctl, n: str): ApiRes =
        if (not(c.mounts.keys().contains(n))) { st: NOT_FOUND, c: c }
        else if (referencedMounts(c).contains(n)) { st: CONFLICT, c: c }
        else { st: NO_CONTENT, c: { ...c, mounts: c.mounts.mapDrop(n) } }

    /**
     * Create an NFS export.
     *
     * The path is NOT required to resolve.  That is a deliberate choice rather
     * than an oversight: chimera.json declares mounts and exports in one
     * document with no ordering between them, and an API that refused an
     * export whose mount had not been created yet would make the REST surface
     * unable to express a configuration the file can.  The consequence is
     * carried where it belongs -- an export over an absent mount is created,
     * listed, and simply not mountable (see ctl.qnt's `mnt`).
     */
    pure def exportCreate(c: Ctl, n: str, e: Export): ApiRes =
        if (c.exports.keys().contains(n)) { st: CONFLICT, c: c }
        else if (exportById(c, e.id).size() > 0) { st: CONFLICT, c: c }
        else { st: CREATED, c: { ...c, exports: c.exports.put(n, e) } }

    /**
     * Delete an NFS export.
     *
     * Nothing pins an export, so this always succeeds when it exists -- but it
     * is the single most consequential call in the API, because it is the only
     * one that revokes access.  What it must do to the filehandles already
     * minted under it is stated in ctl.qnt (`handleLive`), not here.
     */
    pure def exportDelete(c: Ctl, n: str): ApiRes =
        if (not(c.exports.keys().contains(n))) { st: NOT_FOUND, c: c }
        else { st: NO_CONTENT, c: { ...c, exports: c.exports.mapDrop(n) } }

    pure def shareCreate(c: Ctl, n: str, path: str): ApiRes =
        if (c.shares.keys().contains(n)) { st: CONFLICT, c: c }
        else { st: CREATED, c: { ...c, shares: c.shares.put(n, path) } }

    pure def shareDelete(c: Ctl, n: str): ApiRes =
        if (not(c.shares.keys().contains(n))) { st: NOT_FOUND, c: c }
        else { st: NO_CONTENT, c: { ...c, shares: c.shares.mapDrop(n) } }

    pure def bucketCreate(c: Ctl, n: str, path: str): ApiRes =
        if (c.buckets.keys().contains(n)) { st: CONFLICT, c: c }
        else { st: CREATED, c: { ...c, buckets: c.buckets.put(n, path) } }

    pure def bucketDelete(c: Ctl, n: str): ApiRes =
        if (not(c.buckets.keys().contains(n))) { st: NOT_FOUND, c: c }
        else { st: NO_CONTENT, c: { ...c, buckets: c.buckets.mapDrop(n) } }

    pure def userCreate(c: Ctl, n: str): ApiRes =
        if (c.users.contains(n)) { st: CONFLICT, c: c }
        else { st: CREATED, c: { ...c, users: c.users.union(Set(n)) } }

    pure def userDelete(c: Ctl, n: str): ApiRes =
        if (not(c.users.contains(n))) { st: NOT_FOUND, c: c }
        else { st: NO_CONTENT, c: { ...c, users: c.users.exclude(Set(n)) } }

    /// A read of one resource: 200 when it exists, 404 when it does not.
    pure def getStatus(present: bool): int =
        if (present) OK else NOT_FOUND
}
