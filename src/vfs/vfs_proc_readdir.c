// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <ctype.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
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
 * forwarding the rest to the path's real callback. */
static int
chimera_vfs_readdir_filter_callback(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    if (!chimera_vfs_dirent_match(name, namelen,
                                  request->readdir.match_pattern,
                                  request->readdir.match_pattern_len)) {
        return 0;
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
    struct chimera_vfs_request *request;
    struct chimera_vfs_module  *module;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        complete(CHIMERA_VFS_PTR_ERR(request), handle, 0, 0, 0, NULL, private_data);
        return;
    }

    module = request->module;

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

    /* When a wildcard is supplied, interpose the filter over whichever per-entry
     * callback the path above established (the caller's directly, or the bounce
     * collector): the backend keeps emitting every entry, and the filter drops
     * the non-matching ones before they reach it.  Skip the interposition for
     * the universal "*" (and NULL), which match everything. */
    request->readdir.match_pattern = NULL;
    if (match_pattern && match_pattern_len > 0 &&
        !(match_pattern_len == 1 && match_pattern[0] == '*')) {
        request->readdir.match_pattern     = match_pattern;
        request->readdir.match_pattern_len = match_pattern_len;
        request->readdir.inner_callback    = request->readdir.callback;
        request->readdir.inner_arg         = request->proto_private_data;
        request->readdir.callback          = chimera_vfs_readdir_filter_callback;
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
} /* chimera_vfs_readdir */ /* chimera_vfs_readdir */
