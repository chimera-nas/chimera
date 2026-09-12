// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

static void
chimera_vfs_lookup_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

static void
chimera_vfs_lookup_pathonly_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

/* Length of the path prefix preceding the first "." or ".." component (with
 * the separating slash stripped): -1 when the path has neither, 0 when the
 * path begins with one.
 *
 * Both need the same treatment for the same reason: a path-only backend
 * collapses them LEXICALLY, which loses the requirement that whatever precedes
 * them be a directory.  "b/.." and "b/." are each ENOTDIR when b is a file, and
 * a backend that simply deletes the component answers about b instead. */
static inline int
chimera_vfs_dot_prefix_len(
    const char *p,
    int         len)
{
    int i = 0;

    while (i < len) {
        int start = i;

        while (i < len && p[i] != '/') {
            i++;
        }
        if ((i - start == 1 && p[start] == '.') ||
            (i - start == 2 && p[start] == '.' && p[start + 1] == '.')) {
            int end = start;

            while (end > 0 && p[end - 1] == '/') {
                end--;
            }
            return end;
        }
        while (i < len && p[i] == '/') {
            i++;
        }
    }
    return -1;
} /* chimera_vfs_dot_prefix_len */

/* Completion of the "."/".."-prefix resolution (follows symlinks) for a
 * path-only mount.  A path-only backend collapses both lexically, so "b/.." or
 * "b/." with a dangling or non-directory "b" would wrongly succeed; resolving
 * the prefix first gives POSIX's ENOENT (dangling chain) or ENOTDIR (non-dir),
 * and only a real directory lets the whole path (with its "."/".." collapsed)
 * resolve.  The
 * prefix lookup is a whole-path resolution against the mount root, so DAC stays
 * server-delegated -- no per-component EACCES. */
static void
chimera_vfs_lookup_pathonly_dotdot_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request     *lp_request = private_data;
    struct chimera_vfs_thread      *thread     = lp_request->thread;
    struct chimera_vfs_open_handle *oh         = lp_request->lookup.handle;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(thread, oh);
        lp_request->lookup.callback(error_code, NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISDIR(attr->va_mode)) {
        chimera_vfs_release(thread, oh);
        lp_request->lookup.callback(CHIMERA_VFS_ENOTDIR, NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    chimera_vfs_lookup_at(
        thread,
        lp_request->cred,
        oh,
        lp_request->lookup.pathc,
        strlen(lp_request->lookup.pathc),
        lp_request->lookup.attr_mask | CHIMERA_VFS_ATTR_MODE,
        0,
        chimera_vfs_lookup_pathonly_complete,
        lp_request);
} /* chimera_vfs_lookup_pathonly_dotdot_complete */

static inline void
chimera_vfs_lookup_open_dispatch(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    const char                 *component;
    int                         componentlen;
    int                         final;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    lp_request->lookup.handle = oh;

    /* Save parent fh for potential relative symlink resolution */
    memcpy(lp_request->lookup.parent_fh, oh->fh, oh->fh_len);
    lp_request->lookup.parent_fh_len = oh->fh_len;

    /* Crossed into a path-only mount: it has no child fhs to walk, so hand the
     * whole remaining path to it in one lookup and return the final attrs. */
    if (chimera_vfs_module_is_path_only(oh->vfs_module)) {
        const char *remaining  = lp_request->lookup.pathc;
        int         remlen     = strlen(remaining);
        int         prefix_len = chimera_vfs_dot_prefix_len(remaining, remlen);

        /* A "." or ".." after a real component collapses lexically in the
         * backend, so resolve the component(s) before it first (following
         * symlinks, as POSIX does for a non-final component): a dangling chain
         * or missing prefix is ENOENT, a non-directory prefix is ENOTDIR, and
         * only a real directory prefix lets the whole path resolve. */
        if (prefix_len > 0) {
            chimera_vfs_lookup(
                thread,
                lp_request->cred,
                oh->fh,
                oh->fh_len,
                remaining,
                prefix_len,
                CHIMERA_VFS_ATTR_MODE,
                CHIMERA_VFS_LOOKUP_FOLLOW,
                chimera_vfs_lookup_pathonly_dotdot_complete,
                lp_request);
            return;
        }

        chimera_vfs_lookup_at(
            thread,
            lp_request->cred,
            oh,
            remaining,
            remlen,
            lp_request->lookup.attr_mask | CHIMERA_VFS_ATTR_MODE,
            0,
            chimera_vfs_lookup_pathonly_complete,
            lp_request);
        return;
    }

    component = lp_request->lookup.pathc;

    while (*lp_request->lookup.pathc != '/' && *lp_request->lookup.pathc != '\0') {
        lp_request->lookup.pathc++;
    }

    componentlen = lp_request->lookup.pathc - component;

    while (*lp_request->lookup.pathc == '/' && *lp_request->lookup.pathc != '\0') {
        lp_request->lookup.pathc++;
    }

    final = (*lp_request->lookup.pathc == '\0');

    /* Always request mode so we can detect symlinks */
    chimera_vfs_lookup_at(
        thread,
        lp_request->cred,
        oh,
        component,
        componentlen,
        (final ? lp_request->lookup.attr_mask : CHIMERA_VFS_ATTR_FH) | CHIMERA_VFS_ATTR_MODE,
        0,
        chimera_vfs_lookup_complete,
        lp_request);

} /* chimera_vfs_lookup_open_dispatch */

static void
chimera_vfs_lookup_symlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    char                       *target;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    lp_request->lookup.handle = oh;

    /* Use the path buffer after the current path for symlink target storage */
    target = lp_request->lookup.path + strlen(lp_request->lookup.path) + 1;

    chimera_vfs_readlink(
        thread,
        lp_request->cred,
        oh,
        target,
        CHIMERA_VFS_PATH_MAX,
        0,
        chimera_vfs_lookup_readlink_complete,
        lp_request);
} /* chimera_vfs_lookup_symlink_open_complete */

static void
chimera_vfs_lookup_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    char                       *target;
    char                       *new_path;
    int                         remaining_len;
    int                         new_pathlen;
    const uint8_t              *start_fh;
    int                         start_fh_len;

    chimera_vfs_release(thread, lp_request->lookup.handle);
    lp_request->lookup.handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    /* Target is stored after the original path */
    target                = lp_request->lookup.path + strlen(lp_request->lookup.path) + 1;
    target[target_length] = '\0';

    /* Calculate remaining path after symlink */
    remaining_len = strlen(lp_request->lookup.pathc);

    /* Build new path: symlink target + remaining path */
    if (target[0] == '/') {
        /* Absolute symlink - restart from root */
        start_fh     = lp_request->fh;
        start_fh_len = lp_request->fh_len;

        /* Skip leading slashes in target */
        while (*target == '/') {
            target++;
            target_length--;
        }
    } else {
        /* Relative symlink - continue from parent directory */
        start_fh     = lp_request->lookup.parent_fh;
        start_fh_len = lp_request->lookup.parent_fh_len;
    }

    /* Construct new path in the plugin_data buffer.  After the first
     * followed link the current path (and the readlink target stored just
     * past it) already live in plugin_data, so assemble the spliced path in
     * a scratch buffer before copying it back to avoid overlapping the
     * sources with the destination. */
    char scratch[CHIMERA_VFS_PATH_MAX];

    new_path = lp_request->plugin_data;

    if (remaining_len > 0) {
        /* Target + "/" + remaining */
        new_pathlen = target_length + 1 + remaining_len;
        if (new_pathlen >= CHIMERA_VFS_PATH_MAX) {
            lp_request->lookup.callback(CHIMERA_VFS_ENAMETOOLONG,
                                        NULL,
                                        lp_request->lookup.private_data);
            chimera_vfs_request_free(thread, lp_request);
            return;
        }
        memcpy(scratch, target, target_length);
        scratch[target_length] = '/';
        memcpy(scratch + target_length + 1, lp_request->lookup.pathc, remaining_len);
    } else {
        /* Just the target */
        new_pathlen = target_length;
        memcpy(scratch, target, target_length);
    }

    memcpy(new_path, scratch, new_pathlen);
    new_path[new_pathlen] = '\0';

    /* Reset path pointer */
    lp_request->lookup.path    = new_path;
    lp_request->lookup.pathc   = new_path;
    lp_request->lookup.pathlen = new_pathlen;

    /* Continue walking from start point */
    chimera_vfs_open_fh(thread,
                        lp_request->cred,
                        start_fh,
                        start_fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_lookup_open_dispatch,
                        lp_request);
} /* chimera_vfs_lookup_readlink_complete */

static void
chimera_vfs_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    int                         final      = (*lp_request->lookup.pathc == '\0');
    unsigned int                open_flags;
    int                         follow_symlink;

    chimera_vfs_release(thread, lp_request->lookup.handle);
    lp_request->lookup.handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code,
                                    NULL,
                                    lp_request->lookup.private_data);

        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    /* Check if this is a symlink that needs to be followed */
    follow_symlink = 0;
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(attr->va_mode)) {
        if (!final) {
            /* Always follow symlinks in path components */
            follow_symlink = 1;
        } else if (lp_request->lookup.flags & CHIMERA_VFS_LOOKUP_FOLLOW) {
            /* Follow final symlink if requested */
            follow_symlink = 1;
        }
    }

    if (follow_symlink) {
        /* Check for symlink loop */
        lp_request->lookup.symlink_count++;
        if (lp_request->lookup.symlink_count > CHIMERA_VFS_SYMLOOP_MAX) {
            lp_request->lookup.callback(CHIMERA_VFS_ELOOP,
                                        NULL,
                                        lp_request->lookup.private_data);
            chimera_vfs_request_free(thread, lp_request);
            return;
        }

        /* Open the symlink to read its target */
        memcpy(lp_request->lookup.next_fh, attr->va_fh, attr->va_fh_len);
        chimera_vfs_open_fh(thread,
                            lp_request->cred,
                            lp_request->lookup.next_fh,
                            attr->va_fh_len,
                            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
                            chimera_vfs_lookup_symlink_open_complete,
                            lp_request);
        return;
    }

    if (final) {
        lp_request->lookup.callback(CHIMERA_VFS_OK,
                                    attr,
                                    lp_request->lookup.private_data);

        chimera_vfs_request_free(thread, lp_request);

    } else {

        open_flags = CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED;

        if (!final) {
            open_flags |= CHIMERA_VFS_OPEN_DIRECTORY;
        }

        memcpy(lp_request->lookup.next_fh, attr->va_fh, attr->va_fh_len);
        chimera_vfs_open_fh(thread,
                            lp_request->cred,
                            lp_request->lookup.next_fh,
                            attr->va_fh_len,
                            open_flags,
                            chimera_vfs_lookup_open_dispatch,
                            lp_request);
    }
} /* chimera_vfs_lookup_complete */

/*
 * Path-only fast path: a path-only mount (no FH-relative ops) resolves the whole
 * path in a single lookup_at against the mount root, with the full path as the
 * "component".  No per-component traversal, no open_fh on intermediates.
 *
 * The component-walk path follows a final symlink (chimera_vfs_lookup_complete)
 * component by component, but the path-only mount hands the whole path over at
 * once and gets back the raw final entry -- so when that entry is a symlink and
 * the caller asked to follow (stat, not lstat), the follow is done here: read
 * the link and re-resolve the whole path with the target spliced in.  Relative
 * targets resolve lexically against the link's directory (a real CIFS mount does
 * the same); a self-referential link exhausts CHIMERA_VFS_SYMLOOP_MAX and
 * surfaces ELOOP rather than being reported as a plain success.
 */
static void chimera_vfs_lookup_pathonly_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

static void
chimera_vfs_lookup_pathonly_symlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    char                       *target;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code, NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    lp_request->lookup.handle = oh;

    /* Read the link target into the buffer just past the current path. */
    target = lp_request->lookup.path + strlen(lp_request->lookup.path) + 1;

    chimera_vfs_readlink(thread, lp_request->cred, oh, target,
                         CHIMERA_VFS_PATH_MAX, 0,
                         chimera_vfs_lookup_pathonly_readlink_complete,
                         lp_request);
} /* chimera_vfs_lookup_pathonly_symlink_open_complete */

static void
chimera_vfs_lookup_pathonly_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *lp_request = private_data;
    struct chimera_vfs_thread  *thread     = lp_request->thread;
    char                       *target;
    char                        scratch[CHIMERA_VFS_PATH_MAX];
    int                         nlen;

    (void) attr;

    chimera_vfs_release(thread, lp_request->lookup.handle);
    lp_request->lookup.handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        lp_request->lookup.callback(error_code, NULL,
                                    lp_request->lookup.private_data);
        chimera_vfs_request_free(thread, lp_request);
        return;
    }

    target                = lp_request->lookup.path + strlen(lp_request->lookup.path) + 1;
    target[target_length] = '\0';

    {
        const char *orig = lp_request->lookup.path;
        int         olen = strlen(orig);
        int         comp_end, suffix_len, i;

        /* A trailing '/' (POSIX dir-forcing) is a suffix ON the symlink
         * component, not part of its name.  The path-only mount stopped on the
         * last NON-empty component, so splice the target in place of THAT and
         * keep the trailing slashes after it -- otherwise "x/b/" (b -> "a")
         * would splice to "x/b/a", re-hit b every hop, and loop to ELOOP where
         * the answer is the dangling target's ENOENT. */
        comp_end = olen;
        while (comp_end > 0 && orig[comp_end - 1] == '/') {
            comp_end--;
        }
        suffix_len = olen - comp_end;

        if (target[0] == '/') {
            /* Absolute target: a path in the VFS NAMESPACE, not in whatever
             * the caller happened to resolve against -- so the retry restarts
             * at the namespace root rather than at lp_request->fh, which is
             * only the same thing when the lookup began there.  A lookup based
             * on a mount root (every *at call, and any path-only mount reached
             * through one) would otherwise re-resolve "/test/d" INSIDE the
             * share as "test/d" and answer ENOENT for a target that resolves
             * perfectly well one level up.
             *
             * The rebase sticks: once the path is namespace-absolute, the
             * relative splices that follow it are lexical on that path and
             * must keep resolving from the same root. */
            lp_request->fh_len = sizeof(lp_request->fh);
            chimera_vfs_get_root_fh(lp_request->fh, &lp_request->fh_len);

            while (*target == '/') {
                target++;
                target_length--;
            }
            nlen = target_length + suffix_len;
            if (nlen >= CHIMERA_VFS_PATH_MAX) {
                goto too_long;
            }
            memcpy(scratch, target, target_length);
            memcpy(scratch + target_length, orig + comp_end, suffix_len);
        } else {
            /* Relative target: splice onto the link's lexical directory (the
             * component before the last non-empty one). */
            int dlen = 0;

            for (i = comp_end - 1; i >= 0; i--) {
                if (orig[i] == '/') {
                    dlen = i;
                    break;
                }
            }

            if (dlen > 0) {
                nlen = dlen + 1 + target_length + suffix_len;
                if (nlen >= CHIMERA_VFS_PATH_MAX) {
                    goto too_long;
                }
                memcpy(scratch, orig, dlen);
                scratch[dlen] = '/';
                memcpy(scratch + dlen + 1, target, target_length);
                memcpy(scratch + dlen + 1 + target_length, orig + comp_end,
                       suffix_len);
            } else {
                nlen = target_length + suffix_len;
                if (nlen >= CHIMERA_VFS_PATH_MAX) {
                    goto too_long;
                }
                memcpy(scratch, target, target_length);
                memcpy(scratch + target_length, orig + comp_end, suffix_len);
            }
        }
    }

    memcpy(lp_request->lookup.path, scratch, nlen);
    lp_request->lookup.path[nlen] = '\0';
    lp_request->lookup.pathc      = lp_request->lookup.path;
    lp_request->lookup.pathlen    = nlen;

    /* Re-resolve the spliced path from the top (the mount is crossed again). */
    chimera_vfs_open_fh(thread, lp_request->cred, lp_request->fh,
                        lp_request->fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_lookup_open_dispatch, lp_request);
    return;

 too_long:
    lp_request->lookup.callback(CHIMERA_VFS_ENAMETOOLONG, NULL,
                                lp_request->lookup.private_data);
    chimera_vfs_request_free(thread, lp_request);
} /* chimera_vfs_lookup_pathonly_readlink_complete */

static void
chimera_vfs_lookup_pathonly_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_request   *lp_request = private_data;
    struct chimera_vfs_thread    *thread     = lp_request->thread;
    chimera_vfs_lookup_callback_t callback   = lp_request->lookup.callback;
    void                         *priv       = lp_request->lookup.private_data;

    (void) dir_attr;

    /* Follow a final symbolic link if the caller asked to (stat, open) -- the
    * path-only mount returns the raw link entry, so the follow lives here. */
    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && S_ISLNK(attr->va_mode) &&
        (lp_request->lookup.flags & CHIMERA_VFS_LOOKUP_FOLLOW)) {

        if (++lp_request->lookup.symlink_count > CHIMERA_VFS_SYMLOOP_MAX) {
            chimera_vfs_release(thread, lp_request->lookup.handle);
            callback(CHIMERA_VFS_ELOOP, NULL, priv);
            chimera_vfs_request_free(thread, lp_request);
            return;
        }

        memcpy(lp_request->lookup.next_fh, attr->va_fh, attr->va_fh_len);
        chimera_vfs_release(thread, lp_request->lookup.handle);
        lp_request->lookup.handle = NULL;

        chimera_vfs_open_fh(thread, lp_request->cred, lp_request->lookup.next_fh,
                            attr->va_fh_len,
                            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                            CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_vfs_lookup_pathonly_symlink_open_complete,
                            lp_request);
        return;
    }

    chimera_vfs_release(thread, lp_request->lookup.handle);
    chimera_vfs_request_free(thread, lp_request);

    callback(error_code, error_code == CHIMERA_VFS_OK ? attr : NULL, priv);
} /* chimera_vfs_lookup_pathonly_complete */

SYMBOL_EXPORT void
chimera_vfs_lookup(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    uint64_t                       attr_mask,
    uint32_t                       flags,
    chimera_vfs_lookup_callback_t  callback,
    void                          *private_data)
{
    struct chimera_vfs_request *lp_request;

    while (pathlen > 0 && *path == '/') {
        path++;
        pathlen--;
    }

    /* POSIX: a pathname longer than {PATH_MAX} (including the terminating null,
     * so pathlen must be < CHIMERA_VFS_PATH_MAX) fails with ENAMETOOLONG before
     * any lookup is attempted -- the whole string is the caller's, so nothing
     * has to be resolved to judge it.
     *
     * A single component longer than {NAME_MAX} is NOT judged here.  Pathname
     * resolution (XBD 4.13) reaches a component only through the directories
     * before it, so an over-long component behind a directory the caller cannot
     * search owes EACCES, not ENAMETOOLONG; scanning the whole path up front
     * answered the length first and got that backwards for every op that
     * resolves a path (stat, chmod, chown, truncate, open).  The walk below
     * hands each component to chimera_vfs_lookup_at() as it reaches it, and
     * that is where the bound -- and the search check that outranks it -- now
     * live. */
    if (pathlen >= CHIMERA_VFS_PATH_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, private_data);
        return;
    }

    if (pathlen == 0) {
        struct chimera_vfs_attrs attr;

        attr.va_req_mask = attr_mask;
        attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
        memcpy(attr.va_fh, fh, fhlen);
        attr.va_fh_len = fhlen;
        callback(CHIMERA_VFS_OK,
                 &attr,
                 private_data);
        return;
    }

    lp_request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(lp_request)) {
        callback(CHIMERA_VFS_PTR_ERR(lp_request), NULL, private_data);
        return;
    }

    lp_request->lookup.path          = lp_request->plugin_data;
    lp_request->lookup.pathlen       = pathlen;
    lp_request->lookup.pathc         = lp_request->lookup.path;
    lp_request->lookup.handle        = NULL;
    lp_request->lookup.attr_mask     = attr_mask;
    lp_request->lookup.flags         = flags;
    lp_request->lookup.symlink_count = 0;
    lp_request->lookup.private_data  = private_data;
    lp_request->lookup.callback      = callback;
    lp_request->lookup.parent_fh_len = 0;

    memcpy(lp_request->lookup.path, path, pathlen);

    lp_request->lookup.path[pathlen] = '\0';

    chimera_vfs_open_fh(thread,
                        cred,
                        fh,
                        fhlen,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_lookup_open_dispatch,
                        lp_request);

} /* chimera_vfs_lookup */
