// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <ctype.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_mount_table.h"
#include "sdk/vfs_access.h"
#include "sdk/vfs_acl.h"
#include "common/misc.h"
#include "common/macros.h"

/* -------------------------------------------------------------------------
 * SMB-style directory wildcard matching (MS-FSA 2.1.4.4), applied here in the
 * VFS core so the backends stay oblivious to filtering -- they always readdir
 * every entry and the core drops the non-matching ones before they reach the
 * caller's per-entry callback.  Only SMB QUERY_DIRECTORY passes a pattern;
 * every other caller passes NULL and gets the unfiltered scan.
 *
 * Five metacharacters, case-insensitive (ASCII), modeled on Samba's
 * ms_fnmatch_core: '*' (zero+ of any char), '?' (exactly one), and the DOS
 * variants '<' (DOS_STAR -- zero+ but stops at the last '.'), '>' (DOS_QM --
 * one char, with '.'/end-of-name handled specially) and '"' (DOS_DOT -- a '.'
 * or end-of-name).  A client downconverts trailing '*'/'?'/'.' to these for
 * 8.3 compatibility, and the MS-FSA suite exercises them directly.
 * ------------------------------------------------------------------------- */

static void
chimera_vfs_readdir_complete(
    struct chimera_vfs_request *request);

/* The remaining expression matches end-of-name iff every metacharacter left can
 * match zero characters (MS-FSA null_match). */
static int
chimera_vfs_match_null(const char *p)
{
    for (; *p; p++) {
        if (*p != '?' && *p != '"' && *p != '>') {
            return 0;
        }
    }
    return 1;
} /* chimera_vfs_match_null */

/* Recursive matcher over NUL-terminated strings; ldot points at the last '.'
 * in the name (or NULL).  Returns 0 on match, -1 otherwise. */
static int
chimera_vfs_match_core(
    const char *p,
    const char *n,
    const char *ldot)
{
    char c;

    while ((c = *p++)) {
        switch (c) {
            case '*':
                while (*n) {
                    if (chimera_vfs_match_core(p, n, ldot) == 0) {
                        return 0;
                    }
                    n++;
                }
                break;
            case '<':
                if (!*n && !*p) {
                    return 0;
                }
                while (*n) {
                    if (chimera_vfs_match_core(p, n, ldot) == 0) {
                        return 0;
                    }
                    if (n == ldot) {
                        if (chimera_vfs_match_core(p, n + 1, ldot) == 0) {
                            return 0;
                        }
                        return -1;
                    }
                    n++;
                }
                break;
            case '?':
                if (!*n) {
                    return -1;
                }
                n++;
                break;
            case '>':
                if (n[0] == '.') {
                    if (!n[1] && chimera_vfs_match_null(p)) {
                        return 0;
                    }
                    break;
                }
                if (!*n) {
                    return chimera_vfs_match_null(p) ? 0 : -1;
                }
                n++;
                break;
            case '"':
                if (*n == 0 && chimera_vfs_match_null(p)) {
                    return 0;
                }
                if (*n != '.') {
                    return -1;
                }
                n++;
                break;
            default:
                if (toupper((unsigned char) c) != toupper((unsigned char) *n)) {
                    return -1;
                }
                n++;
        } /* switch */
    }

    return *n ? -1 : 0;
} /* chimera_vfs_match_core */

/* True iff `name` matches the SMB wildcard `pattern`.  A NULL/empty pattern or
 * the universal "*" matches everything (the common no-filter fast path). */
SYMBOL_EXPORT int
chimera_vfs_dirent_match(
    const char *name,
    int         namelen,
    const char *pattern,
    int         patternlen)
{
    char        nbuf[256], pbuf[256];
    const char *ldot;

    if (!pattern || patternlen == 0) {
        return 1;
    }
    if (patternlen == 1 && pattern[0] == '*') {
        return 1;
    }

    if (namelen >= (int) sizeof(nbuf)) {
        namelen = sizeof(nbuf) - 1;
    }
    if (patternlen >= (int) sizeof(pbuf)) {
        patternlen = sizeof(pbuf) - 1;
    }
    memcpy(nbuf, name, namelen);
    nbuf[namelen] = '\0';
    memcpy(pbuf, pattern, patternlen);
    pbuf[patternlen] = '\0';

    ldot = strrchr(nbuf, '.');

    return chimera_vfs_match_core(pbuf, nbuf, ldot) == 0;
} /* chimera_vfs_dirent_match */

/* Per-entry interposer: drop entries that do not match the readdir's pattern,
 * present an entry that is a mount point as the root of what is mounted
 * there -- its handle and attributes, as a lookup of the name would return --
 * and, for a caller that may not search the directory, reduce every entry to
 * its name and inum, forwarding the rest to the path's real callback. */
static int
chimera_vfs_readdir_filter_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_vfs_request     *request = arg;
    struct chimera_vfs_open_handle *dir     = request->readdir.handle;
    struct chimera_vfs_attrs        cross;
    struct chimera_vfs_attrs        withheld;
    uint8_t                         cross_fh[CHIMERA_VFS_FH_SIZE + 16];
    int                             cross_fh_len;

    if (request->readdir.match_pattern &&
        !chimera_vfs_dirent_match(name, namelen,
                                  request->readdir.match_pattern,
                                  request->readdir.match_pattern_len)) {
        return 0;
    }

    if (chimera_vfs_mount_table_cover_root(request->thread->vfs->mount_table,
                                           dir->fh, dir->fh_len, name, namelen,
                                           cross_fh, &cross_fh_len, &cross) == 0) {
        cross.va_req_mask = attrs->va_req_mask;
        memcpy(cross.va_fh, cross_fh, cross_fh_len);
        cross.va_fh_len    = cross_fh_len;
        cross.va_set_mask |= CHIMERA_VFS_ATTR_FH;
        if (cross.va_set_mask & CHIMERA_VFS_ATTR_INUM) {
            inum = cross.va_ino;
        }
        attrs = &cross;
    }

    /* Stat-ing or reaching an entry needs search permission on the
     * directory, so without it the caller gets what read permission gives,
     * as getdents(2) does: the name, the inode number and the file type (in
     * va_mode's S_IFMT bits, with ATTR_MODE left unset).  nfsd sends no more
     * when its per-entry lookup is refused.  It still returns "." and ".."
     * whole, composing their handles without a lookup; they are withheld
     * here too, since LOOKUP refuses them without search.  The mark lets a
     * protocol tell the withheld from the absent. */
    if (request->readdir.withhold) {
        memset(&withheld, 0, sizeof(withheld));
        withheld.va_req_mask = attrs->va_req_mask;
        withheld.va_set_mask = CHIMERA_VFS_ATTR_WITHHELD |
            (attrs->va_set_mask & CHIMERA_VFS_ATTR_INUM);
        withheld.va_ino = attrs->va_ino;
        if (attrs->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
            withheld.va_mode = attrs->va_mode & S_IFMT;
        }
        attrs = &withheld;
    }

    return request->readdir.inner_callback(inum, cookie, name, namelen, attrs,
                                           request->readdir.inner_arg);
} /* chimera_vfs_readdir_filter_callback */

/* Non-blocking filter path completion: the filter interpose pointed
 * proto_private_data at the VFS request so the per-entry callback could reach
 * the pattern; restore the caller's private_data before the final completion,
 * which is passed proto_private_data.  (The blocking path's bounce_complete
 * already restores orig_private_data itself.) */
static void
chimera_vfs_readdir_filter_complete(struct chimera_vfs_request *request)
{
    request->proto_private_data = request->readdir.orig_private_data;
    chimera_vfs_readdir_complete(request);
} /* chimera_vfs_readdir_filter_complete */

static int
chimera_vfs_readdir_bounce_result_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_vfs_request       *request = arg;
    struct chimera_vfs_readdir_entry *entry;
    int                               entry_size;
    char                             *entry_data;

    entry_size = (sizeof(*entry) + namelen + 7) & ~7;

    /* Check if we have enough space in the bounce buffer */
    if (request->readdir.bounce_offset + entry_size > request->readdir.bounce_iov.length) {
        return -1;
    }

    /* Pack the entry into the bounce buffer */
    entry_data = (char *) request->readdir.bounce_iov.data + request->readdir.bounce_offset;
    entry      = (struct chimera_vfs_readdir_entry *) entry_data;

    entry->inum    = inum;
    entry->cookie  = cookie;
    entry->namelen = namelen;
    entry->attrs   = *attrs;
    memcpy(entry_data + sizeof(*entry), name, namelen);

    request->readdir.bounce_offset += entry_size;

    return 0;
} /* chimera_vfs_readdir_bounce_result_callback */

static void
chimera_vfs_readdir_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_readdir_complete_t complete = request->proto_callback;

    chimera_vfs_complete(request);

    complete(request->status,
             request->readdir.handle,
             request->readdir.r_cookie,
             request->readdir.r_verifier,
             request->readdir.r_eof,
             &request->readdir.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_readdir_complete */ /* chimera_vfs_readdir_complete */



static void
chimera_vfs_bounce_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_readdir_entry *entry;
    char                             *data_ptr;
    char                             *data_end;
    int                               rc = 0;

    request->proto_private_data = request->readdir.orig_private_data;

    data_ptr = request->readdir.bounce_iov.data;
    data_end = data_ptr + request->readdir.bounce_offset;

    while (data_ptr < data_end && rc == 0) {
        entry = (struct chimera_vfs_readdir_entry *) data_ptr;

        rc = request->readdir.orig_callback(
            entry->inum,
            entry->cookie,
            data_ptr + sizeof(*entry),
            entry->namelen,
            &entry->attrs,
            request->proto_private_data);

        if (rc != 0) {
            /* Application aborted the scan */
            request->readdir.r_eof    = 0;
            request->readdir.r_cookie = entry->cookie;
            break;
        }

        data_ptr += (sizeof(*entry) + entry->namelen + 7) & ~7;
    }

    evpl_iovec_release(request->thread->evpl, &request->readdir.bounce_iov);

    chimera_vfs_readdir_complete(request);
} /* chimera_vfs_bounce_complete */


static void
chimera_vfs_readdir_dispatch(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        flags,
    const char                     *match_pattern,
    int                             match_pattern_len,
    int                             withhold,
    chimera_vfs_readdir_callback_t  callback,
    chimera_vfs_readdir_complete_t  complete,
    void                           *private_data)
{
    struct chimera_vfs_request *request;
    struct chimera_vfs_module  *module;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        complete(CHIMERA_VFS_PTR_ERR(request), handle, 0, 0, 0, NULL, private_data);
        return;
    }

    module = request->module;

    /* A withheld entry keeps only its inode number and file type (see
     * chimera_vfs_readdir_filter_callback), so don't have the backend stat
     * each child and build its handle only for the filter to drop them. */
    if (withhold) {
        attr_mask &= CHIMERA_VFS_ATTR_INUM | CHIMERA_VFS_ATTR_MODE;
    }

    request->opcode                         = CHIMERA_VFS_OP_READDIR;
    request->readdir.handle                 = handle;
    request->readdir.attr_mask              = attr_mask;
    request->readdir.cookie                 = cookie;
    request->readdir.verifier               = verifier;
    request->readdir.flags                  = flags;
    request->readdir.callback               = callback;
    request->readdir.r_dir_attr.va_req_mask = dir_attr_mask;
    request->readdir.r_dir_attr.va_set_mask = 0;
    request->readdir.r_verifier             = 0;
    request->proto_callback                 = complete;
    request->proto_private_data             = private_data;

    request->readdir.bounce_offset = 0;
    request->readdir.orig_callback = NULL;

    /* If this module is blocking then we need to bounce the results into the original thread
     * before making the caller provided result callback.  This only applies when the request
     * will actually be dispatched to a delegation thread; if the sync delegation pool is
     * disabled and there is no async pool, the blocking module runs inline on this thread and
     * its result callback can safely target the original buffers directly.
     */

    if ((module->capabilities & CHIMERA_VFS_CAP_BLOCKING) &&
        (thread->vfs->num_sync_delegation_threads > 0 ||
         thread->vfs->num_async_delegation_threads > 0)) {

        evpl_iovec_alloc(thread->evpl, 64 * 1024, 8, 1, 0, &request->readdir.bounce_iov);

        request->readdir.orig_callback     = callback;
        request->readdir.orig_private_data = private_data;

        request->readdir.callback   = chimera_vfs_readdir_bounce_result_callback;
        request->proto_private_data = request;

        request->complete = chimera_vfs_bounce_complete;

    } else {
        request->complete = chimera_vfs_readdir_complete;
    }

    /* When a wildcard is supplied, something is mounted below "/" (so an
     * entry may be a mount point), or the entries' attributes are withheld,
     * interpose the filter over whichever per-entry callback the path above
     * established (the caller's directly, or the bounce collector): the
     * backend keeps emitting every entry, and the filter drops the
     * non-matching ones, presents mount points and strips withheld entries
     * before they reach it.  Skip the pattern for the universal "*" (and
     * NULL), which matches everything. */
    request->readdir.match_pattern = NULL;
    if (match_pattern && match_pattern_len > 0 &&
        !(match_pattern_len == 1 && match_pattern[0] == '*')) {
        request->readdir.match_pattern     = match_pattern;
        request->readdir.match_pattern_len = match_pattern_len;
    }
    request->readdir.withhold = withhold;
    if (request->readdir.match_pattern || withhold ||
        chimera_atomic_load_n(&thread->vfs->mount_table->num_covers,
                              CHIMERA_MEMORY_RELAXED) > 0) {
        request->readdir.inner_callback = request->readdir.callback;
        request->readdir.inner_arg      = request->proto_private_data;
        request->readdir.callback       = chimera_vfs_readdir_filter_callback;
        /* The blocking path already saves orig_private_data and restores it in
         * bounce_complete; the non-blocking path's completion goes straight to
         * chimera_vfs_readdir_complete, so wrap it to restore the caller's
         * private_data (the completion is passed proto_private_data, which we
         * are about to repoint at the VFS request for the filter). */
        if (request->complete == chimera_vfs_readdir_complete) {
            request->readdir.orig_private_data = request->proto_private_data;
            request->complete                  = chimera_vfs_readdir_filter_complete;
        }
        request->proto_private_data = request;
    }

    chimera_vfs_dispatch(request);
} /* chimera_vfs_readdir_dispatch */

/* Carries the readdir arguments across the getattr+ACL fetch that authorizes
 * an enumeration whose read was not bound at open, or decides whether
 * the caller may search the directory. */
struct chimera_vfs_readdir_gate {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *handle;
    int                             bound;
    uint64_t                        attr_mask;
    uint64_t                        dir_attr_mask;
    uint64_t                        cookie;
    uint64_t                        verifier;
    uint32_t                        flags;
    const char                     *match_pattern;
    int                             match_pattern_len;
    chimera_vfs_readdir_callback_t  callback;
    chimera_vfs_readdir_complete_t  complete;
    void                           *private_data;
};

_Static_assert(sizeof(struct chimera_vfs_readdir_gate) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "readdir gate context outgrew the request gate scratch area");

static void
chimera_vfs_readdir_gate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_readdir_gate *gate     = private_data;
    int                              withhold = 0;
    uint32_t                         granted;

    if (error_code != CHIMERA_VFS_OK) {
        gate->complete(error_code, gate->handle, 0, 0, 0, NULL,
                       gate->private_data);
        chimera_vfs_gate_scratch_free(gate->thread, gate);
        return;
    }

    /* Enumerating a non-directory is the backend's ENOTDIR, which outranks
     * any access error, as nfsd's fh_verify(S_IFDIR) does.  Only a directory
     * is judged here. */
    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) || S_ISDIR(attr->va_mode)) {
        /* The full grant, not a yes/no: it also says whether the caller may
         * search (EXECUTE) the directory, which decides whether its entries'
         * attributes and handles may be returned with the names. */
        granted = chimera_vfs_access_check(attr, gate->cred,
                                           CHIMERA_ACE_MASK_ALL);

        /* A bound stream's read was settled at open; only search is
         * judged here, against the directory as it is now. */
        if (!gate->bound && !(granted & CHIMERA_ACE_READ_DATA)) {
            gate->complete(CHIMERA_VFS_EACCES, gate->handle, 0, 0, 0, NULL,
                           gate->private_data);
            chimera_vfs_gate_scratch_free(gate->thread, gate);
            return;
        }

        withhold = (gate->attr_mask & ~CHIMERA_VFS_ATTR_INUM) &&
            !(granted & CHIMERA_ACE_EXECUTE);
    }

    chimera_vfs_readdir_dispatch(gate->thread, gate->cred, gate->handle,
                                 gate->attr_mask, gate->dir_attr_mask,
                                 gate->cookie, gate->verifier, gate->flags,
                                 gate->match_pattern, gate->match_pattern_len,
                                 withhold, gate->callback, gate->complete,
                                 gate->private_data);
    chimera_vfs_gate_scratch_free(gate->thread, gate);
} /* chimera_vfs_readdir_gate_complete */

/*
 * Enumerating a directory requires read (LIST_DIRECTORY) on it, as opendir(3)
 * does.  The engine checks it here, failing closed, for every caller (NFSv3,
 * NFSv4, FUSE, the POSIX client, find): the backends rely on it, and the
 * passthroughs enumerate through a descriptor opened privileged by handle,
 * which the kernel never checks against the caller.
 *
 * POSIX binds a directory stream's right to list at opendir(), so a later
 * chmod must not break an open stream.  A caller holding such a stream --
 * FUSE after OPENDIR, the POSIX client on a directory it opened for reading
 * -- checked read when it opened it, and says so with
 * CHIMERA_VFS_READDIR_READ_BOUND.  Every other enumeration -- every NFS
 * READDIR, since NFS has no directory open, and find -- is judged against the
 * directory's current mode and ACL on every call.  The grant an open stamps
 * on the handle is deliberately not consulted: the open cache shares one
 * handle among all opens of an identity, so another protocol's open (FUSE,
 * or an NFSv4 exclusive OPEN that re-opens an existing name) would otherwise
 * exempt the stateless callers.
 *
 * Search (EXECUTE) is never bound: POSIX checks it when a child is stat'ed
 * or opened.  A caller that asks for more than the entries' names and inode
 * numbers, and may read the directory but not search it, gets the entries
 * with their attributes and handles withheld (CHIMERA_VFS_ATTR_WITHHELD), so
 * READDIRPLUS cannot hand out what LOOKUP refuses.  So a bound stream still
 * pays a getattr on every call that asks for any entry attribute, the file
 * type included, as FUSE's READDIR always does.  FUSE needs the check on a
 * no_default_permissions mount, where the kernel leaves search to the
 * lookups it sends and would serve a later stat from the entries READDIRPLUS
 * cached instead.
 *
 * SMB (AUTH_ATTR) is exempt: it binds access at CREATE, and QUERY_DIRECTORY
 * checks the open's granted access for FILE_LIST_DIRECTORY itself.
 */
SYMBOL_EXPORT void
chimera_vfs_readdir(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        flags,
    const char                     *match_pattern,
    int                             match_pattern_len,
    chimera_vfs_readdir_callback_t  callback,
    chimera_vfs_readdir_complete_t  complete,
    void                           *private_data)
{
    struct chimera_vfs_readdir_gate *gate;
    int                              bound;

    /* The caller's binding is the VFS's to honour, never the backend's. */
    bound  = !!(flags & CHIMERA_VFS_READDIR_READ_BOUND);
    flags &= ~CHIMERA_VFS_READDIR_READ_BOUND;

    /* gate_needed_dac, not gate_needed: it keeps the DELEGATES_DAC
     * passthroughs in, whose privileged descriptor the kernel never checks
     * against the caller. */
    if (cred->flavor != CHIMERA_VFS_AUTH_ATTR &&
        chimera_vfs_gate_needed_dac(handle->vfs_module->capabilities, cred)) {
        if (!bound || (attr_mask & ~CHIMERA_VFS_ATTR_INUM)) {
            gate = chimera_vfs_gate_scratch_alloc(thread);

            gate->thread            = thread;
            gate->cred              = cred;
            gate->handle            = handle;
            gate->bound             = bound;
            gate->attr_mask         = attr_mask;
            gate->dir_attr_mask     = dir_attr_mask;
            gate->cookie            = cookie;
            gate->verifier          = verifier;
            gate->flags             = flags;
            gate->match_pattern     = match_pattern;
            gate->match_pattern_len = match_pattern_len;
            gate->callback          = callback;
            gate->complete          = complete;
            gate->private_data      = private_data;

            chimera_vfs_getattr(thread, cred, handle,
                                CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                                chimera_vfs_readdir_gate_complete, gate);
            return;
        }
    }

    chimera_vfs_readdir_dispatch(thread, cred, handle, attr_mask,
                                 dir_attr_mask, cookie, verifier, flags,
                                 match_pattern, match_pattern_len, 0, callback,
                                 complete, private_data);
} /* chimera_vfs_readdir */
