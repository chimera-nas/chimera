// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Chimera VFS module SDK: the module contract.
 *
 * A backend module is a struct chimera_vfs_module of callbacks plus a
 * unique file-handle magic (vfs_fh_magic.h) and a capability mask.
 * In-tree modules are linked into chimera; out-of-tree modules are
 * built as shared objects exporting a `struct chimera_vfs_module
 * vfs_<name>` symbol and loaded via the share configuration's
 * module_path.
 */

#include <stdint.h>

#include "vfs_fh_magic.h"

struct evpl;
struct prometheus_metrics;
struct chimera_vfs_request;

/* Version of the module-facing SDK contract (this header, vfs_request.h,
 * and the types they include).  Bumped on any incompatible change;
 * chimera_vfs_register() refuses a module built against a different
 * version, so a stale out-of-tree binary fails loudly at load time
 * instead of corrupting memory. */
#define CHIMERA_VFS_SDK_VERSION             1

/* If set, module requires open handles for path operations
 * such as mkdir, remove, open_at, etc.  Equivalent to POSIX open
 * with O_PATH flag.
 *
 * If not set, VFS may create synthetic open handles that
 * only contain the file handle w/o an explicit open callout
 * to the module for stateless operation (ie NFS3).
 */
#define CHIMERA_VFS_CAP_OPEN_PATH_REQUIRED  (1U << 0)

/* If set, module requires open handles for file operations
 * and for setattr on directories.
 *
 * If not set, VFS may create synthetic open handles that
 * only contain the file handle w/o an explicit open callout
 * to the module for stateless operation (ie NFS3).
 */
#define CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED  (1U << 1)

/* If set, dispatch function is synchronous/blocking
 * and chimera will delegate VFS requests to a separate
 * threadpool.  This is useful for modules that perform
 * blocking operations such as I/O.
 *
 * If not set, VFS requests will be dispatched from the
 * main threadpool and the dispatch function is expected
 * to return quickly.
 */
#define CHIMERA_VFS_CAP_BLOCKING            (1U << 2)

/* If set, module supports chimera_vfs_create_unlinked()
 * Used primarily for S3 PUT.
 */
#define CHIMERA_VFS_CAP_CREATE_UNLINKED     (1U << 3)

/* If set, module supports filesystem operations (directories, files, etc.)
 * All current backends should declare this capability.
 */
#define CHIMERA_VFS_CAP_FS                  (1U << 4)

/* If set, module supports key-value operations
 * (put_key, get_key, delete_key, search_keys)
 */
#define CHIMERA_VFS_CAP_KV                  (1U << 5)

/* If set, module supports FH-relative operations (lookup_at, mkdir_at, etc.)
 * All current backends support this.
 */
#define CHIMERA_VFS_CAP_FS_RELATIVE_OP      (1U << 6)

/* If set, module supports path-based operations (open, mkdir, etc.)
 * Path-based operations take a full path relative to the mount point.
 * If a module does not support path ops, the VFS core will resolve
 * path components one at a time using FH-relative operations.
 *
 * A module that sets CAP_FS_PATH_OP and does NOT set CAP_FS_RELATIVE_OP is
 * "path-only" (see chimera_vfs_module_is_path_only): it has no persistent file
 * handles.  Metadata ops are dispatched as a single path-relative op against the
 * mount root; an open file's handle carries an OPAQUE per-open token (not a
 * path, not re-openable via open_fh -- only the mount-root fh is re-openable),
 * and read/write/getattr/setattr/commit/readdir/close operate through the open
 * handle's vfs_private.  This mirrors the Linux cifs client.  A path-only mount
 * therefore cannot be re-exported by a handle-based consumer (the NFS server):
 * open_fh of a child token returns ESTALE.
 */
#define CHIMERA_VFS_CAP_FS_PATH_OP          (1U << 7)

/* If set, module supports byte-range file locking via chimera_vfs_lock(). */
#define CHIMERA_VFS_CAP_FS_LOCK             (1U << 8)

/* If set, module supports reverse path lookup: given a directory FH,
 * resolve (parent_fh, name_in_parent).  Enables precise subtree
 * change notifications via CHIMERA_VFS_OP_GETPARENT. */
#define CHIMERA_VFS_CAP_RPL                 (1U << 9)

/* If set, module supports server-side byte-range copy between two
 * open handles served by the same module (chimera_vfs_copy_range). */
#define CHIMERA_VFS_CAP_COPY_RANGE          (1U << 10)

/* If set, module supports reflink/COW share of a byte range between
 * two open handles served by the same module (chimera_vfs_clone_range).
 * Modules backed by filesystems without reflink support should leave
 * this unset; the VFS layer will surface ENOTSUP. */
#define CHIMERA_VFS_CAP_CLONE_RANGE         (1U << 11)

/* If set, module supports zero-copy move of a byte range between two
 * open handles served by the same module (chimera_vfs_move_range): block
 * references are transferred from source to destination and the source
 * range becomes a hole. Not exposed over NFS; intended for server-internal
 * use (e.g. S3 multipart-upload completion). */
#define CHIMERA_VFS_CAP_MOVE_RANGE          (1U << 12)

/* If set, the module can persist an opaque "handle-state" record atomically
 * with an open/create (see struct chimera_vfs_handle_state below): the record
 * is committed in the same transaction as the open, so a crash cannot leave
 * the file created without its record (or vice versa).  Used by the SMB server
 * to persist SMB3 persistent-handle records.  Backends without this cap ignore
 * any handle_state passed on an open. */
#define CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE (1U << 13)

/* Opaque key/value record the caller asks the backend to persist atomically
 * as part of an open/create.  The VFS layer never interprets the bytes; for
 * the SMB server the key is "smbdh\0"+CreateGuid and the value is a serialized
 * persistent-handle record.  Stored in the backend's KV namespace, so it can
 * later be enumerated with chimera_vfs_search_keys and removed with
 * chimera_vfs_delete_key (clear-on-close / reap need not be atomic). */
struct chimera_vfs_handle_state {
    const void *key;
    uint32_t    key_len;
    const void *value;
    uint32_t    value_len;
};

/* If set, module supports extended attributes via
 * chimera_vfs_get_xattr / set_xattr / list_xattrs / remove_xattr.
 * Surfaced over NFSv4.2 (RFC 8276). Modules that leave this unset
 * cause the VFS layer to return ENOTSUP. */
#define CHIMERA_VFS_CAP_XATTR                 (1U << 18)

/* setxattr_option4 values (RFC 8276 §8) passed to chimera_vfs_set_xattr().
 * Kept numerically identical to the on-the-wire NFSv4.2 enum. */
#define CHIMERA_VFS_XATTR_EITHER              0 /* create or replace */
#define CHIMERA_VFS_XATTR_CREATE              1 /* must not already exist */
#define CHIMERA_VFS_XATTR_REPLACE             2 /* must already exist */

/* Module persists the opaque CHIMERA_VFS_ATTR_PNFS_LAYOUT attribute, so the NFS
 * server can store per-file pNFS layout state on it and hand out pNFS layouts.
 * This is the "orchestrated" model: the module is a passive vessel and the NFS
 * server produces the layout (creating data-server backing files itself). */
#define CHIMERA_VFS_CAP_LAYOUT                (1U << 14)

/* Module SOURCES the layout itself: it already knows where a file's data
 * physically lives and synthesizes a protocol-neutral layout via
 * CHIMERA_VFS_OP_GET_LAYOUT.  The NFS server is the consumer (it only encodes
 * what the module returns) and does NO orchestration.  Mutually exclusive in
 * effect with CHIMERA_VFS_CAP_LAYOUT for a given file.  Exactly one of the
 * class bits below should accompany it. */
#define CHIMERA_VFS_CAP_LAYOUT_SOURCE         (1U << 15)
#define CHIMERA_VFS_CAP_LAYOUT_CLASS_FLEX     (1U << 16) /* produces flex-files (RFC 8435)  */
#define CHIMERA_VFS_CAP_LAYOUT_CLASS_BLOCK    (1U << 17) /* produces block volume (RFC 5663)*/
#define CHIMERA_VFS_CAP_LAYOUT_CLASS_SCSI     (1U << 19) /* produces SCSI volume (RFC 8154) */

/* If set, the backend provides the memory for READ data itself (e.g. memfs
 * returns refs to its in-memory SHARED block iovecs; the nfs proxy returns the
 * buffers handed up by its upstream RPC reply).  If UNSET, the VFS core
 * allocates the read buffers on the connection thread BEFORE dispatch and
 * places them in request->read.iov (request->read.buffers_provided != 0); the
 * backend MUST read into those buffers and must NOT allocate its own.  Keeping
 * read-buffer ownership on the connection thread is what makes the delegation
 * (worker-thread) read path safe without SHARED iovecs -- the buffers are
 * allocated and released on the same (connection) thread.  See
 * chimera_vfs_read_owned() / chimera_vfs_read_complete(). */
#define CHIMERA_VFS_CAP_READ_PROVIDES_BUFFERS (1U << 20)

/* If set, the module stores the canonical Windows/NFSv4 ACL (via va_acl)
 * losslessly.  If unset, the module is mode-only: the VFS translates ACLs to
 * and from UNIX mode bits on its behalf.  There is no POSIX.1e capability by
 * design -- Chimera carries a single ACL model (see vfs_acl.h). */
#define CHIMERA_VFS_CAP_ACL_NATIVE            (1U << 23)

/* If set, the module delegates discretionary access control to a real
 * underlying enforcer (e.g. the host kernel, via the seteuid/setegid
 * impersonation in chimera_setup_credential).  The central VFS access gate
 * (chimera_vfs_gate) is then a no-op for this module, since enforcing in the
 * engine on top of the kernel would double-evaluate and -- on a mode-only
 * backend whose ACL is mode-derived -- could only ever agree with it anyway.
 *
 * Modules WITHOUT this bit (memfs, cairn, ...) have no native DAC, so the VFS
 * engine is their sole authorization point and the gate enforces the canonical
 * ACL for them.  Note this is orthogonal to CAP_ACL_NATIVE: "stores the ACL"
 * and "enforces the ACL" are different properties (memfs/cairn store but do not
 * enforce; linux/io_uring enforce in-kernel but do not store the rich ACL). */
#define CHIMERA_VFS_CAP_DELEGATES_DAC         (1U << 21)

/* If set, the module supports named streams (SMB Alternate Data Streams) on
 * regular files via chimera_vfs_open_stream / list_streams / remove_stream.
 * A named stream is an independent data fork addressed by name; it shares the
 * base file's metadata (mode/owner/timestamps/ACL) but has its own size and
 * content.  Modules that leave this unset cause the VFS layer to return
 * ENOTSUP.  Currently only memfs advertises it. */
#define CHIMERA_VFS_CAP_NAMED_STREAMS         (1U << 22)

/* If set, the module supplies a native change attribute: a monotonically
 * increasing per-object version counter returned via va_change /
 * CHIMERA_VFS_ATTR_CHANGE, bumped on every data or metadata mutation.  This
 * lets the NFS server return the counter as fattr4_change and report
 * change_attr_type NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR.  Modules that leave this
 * unset have change derived from ctime (change_attr_type TIME_METADATA). */
#define CHIMERA_VFS_CAP_CHANGE                (1U << 24)

/* If set, the module manages named filesystems via CHIMERA_VFS_OP_MKFS /
 * CHIMERA_VFS_OP_RMFS.  Filesystems are created by name, mounted with a
 * module path of "<name>[/subpath]" (the leading path component selects the
 * filesystem), and removed only while no mount references them (RMFS returns
 * CHIMERA_VFS_EBUSY otherwise).  Modules without this bit interpret the whole
 * module path themselves (e.g. as a host path for passthrough backends). */
#define CHIMERA_VFS_CAP_MKFS                  (1UL << 25)

/* Backend lease arbitration (the claim-core projection boundary).
 *
 * A backend declaring CHIMERA_VFS_CAP_LEASE is the lease ARBITER for its
 * files: the claim core routes cross-node visibility through it instead of
 * deciding purely node-locally.  The wire is kindless -- masks, never
 * protocol constructs -- and carries exactly two claim shapes:
 *
 *   AGGREGATE: one revocable per-node token per file covering the union of
 *     the node's local holders -- rev_used (R|W: data the node reads/writes
 *     or caches) plus bind_deny (R|W|D: the union of binding share-deny
 *     bits).  Held lazily (escalate-or-reuse): the hot I/O path never
 *     round-trips; only the first escalating acquire does.  The backend
 *     may grant a subset, and may later RECALL via the recall callback
 *     captured at acquire time; the node drains its local holders and
 *     releases (the release IS the recall ack).
 *
 *   RANGE: one binding, non-recallable record per byte-range lock, keyed by
 *     the cluster-stable owner identity (never a node id -- POSIX same-owner
 *     coalescing must survive a client landing on two nodes).  Confirmed
 *     before the local grant completes; all-or-nothing.
 *
 * A backend that sets this bit must implement BOTH lease ops and invoke the
 * recall callback from whatever context it likes (the core marshals).  There
 * is no partial mode. */
#define CHIMERA_VFS_CAP_LEASE                 (1U << 26)

struct chimera_vfs_module {
    /* Required
     * Set to CHIMERA_VFS_SDK_VERSION.  Checked at registration so a module
     * built against an incompatible SDK is rejected instead of loaded.
     */
    uint32_t    sdk_version;

    /* Required
     * Short name for the module to be used in creating shares
     */
    const char *name;

    /* Required
     * Set to CHIMERA_FS_FH_MAGIC value reserved in vfs_fh_magic.h
     */
    uint8_t     fh_magic;

    /* Required
     * Bitwise OR of CHIMERA_VFS_CAP_* above
     */
    uint64_t    capabilities;

    /* Optional
     * Called once at initialization to setup global state
     * Return a pointer to global state structure
     * Receives module-specific configuration JSON data as an argument.
     */
    void      * (*init)(
        const char                *cfgdata,
        struct prometheus_metrics *metrics);

    /* Optional
     * Called once at destruction to clean up global state
     * returned from the init function
     */
    void        (*destroy)(
        void *);

    /* Optional
     * Called once per thread at initialization to setup per-thread state
     * Receives global state pointer as an argument
     * Return a pointer to per-thread state structure
     */
    void      * (*thread_init)(
        struct evpl *evpl,
        void        *private_data);

    /* Optional
     * Called once per thread at destruction to clean up per-thread state
     * Receives per-thread state pointer as an argument
     */
    void        (*thread_destroy)(
        void *);

    /* Required
     * Called to dispatch a request to the module
     * Receives request and per-thread state pointer as an argument
     *
     * Module shuold call request->complete(request) when the
     * request processing is completed.
     *
     * If dispatch logic is blocking, set the blocking flag to 1 above.
     *
     * If blocking flag is unset, requests will be dispatched from
     * chimera's main threadpool, ie the same threadpool that is
     * pumping network traffic.  In this case the dispatch function is
     * expected to quickly complete and then asynchronously make the
     * complete callback later after any underlying slow operations
     * such as I/O have been asynchronously completed.
     *
     * If blocking flag is set, requests will be dispatched from a
     * separate dedicated pool of threads which will expect to process
     * only one request at a time.  The thread handoff adds overhead,
     * but nonetheless this scheme avoids stalling the main network
     * threads due to blocking inside VFS modules.
     *
     * Implementing VFS modules in a non-blocking manner is recommended
     * where feasible.
     */
    void (*dispatch)(
        struct chimera_vfs_request *request,
        void                       *private_data);
};

/* A module is "path-only" when it supports path-relative ops but has no
 * persistent file handles (no FH-relative ops).  See CAP_FS_PATH_OP. */
static inline int
chimera_vfs_module_is_path_only(const struct chimera_vfs_module *module)
{
    return (module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) &&
           !(module->capabilities & CHIMERA_VFS_CAP_FS_RELATIVE_OP);
} /* chimera_vfs_module_is_path_only */
