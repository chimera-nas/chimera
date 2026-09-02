// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#include "smb_internal.h"
#include "vfs/sdk/vfs_attrs.h"
#include "evpl/evpl.h"
#include "common/misc.h"

/*
 * File operations for the SMB2 client -- a PATH-ONLY VFS backend.
 *
 * Metadata ops (lookup/mkdir/remove/open) address files by the full
 * mount-relative path carried in request->X.name and do a transient SMB2 CREATE
 * on that path.  An open file's VFS handle carries an opaque token fh
 * [mount_id][server_index][FileId]; ops on an open handle
 * (read/write/getattr/setattr/commit/close) recover the server + FileId from the
 * handle's vfs_private (struct chimera_smb_client_open), never from a path.
 */

/* Shared op helpers (smb_send_create/close, parse, attrs, op-state struct, the
* network-open-info / create-result structs, and smb_handle_open_state) are
* declared in smb_internal.h so smb_io.c and smb_namespace.c can reuse them. */

/* ---- path table (id <-> full mount-relative path) ---------------------- */

/*
 * SMB2 FileIds are per-open and cannot be re-opened after CLOSE, so an
 * open-token fh stores a stable id (XXH3 of the object's full mount-relative
 * path) instead.  This table maps that id back to the path so open_fh can
 * re-CREATE any object by path -- the missing piece that made every non-root
 * fh answer ESTALE.  Keyed by id; the id is a pure function of the path, so a
 * (vanishingly unlikely) collision of two distinct paths just keeps the most
 * recent -- open_fh only ever has the id to work from.
 */

uint64_t
chimera_smb_path_intern(
    struct chimera_smb_client_server *server,
    const char                       *path,
    int                               path_len)
{
    uint64_t             id     = XXH3_64bits(path, (size_t) path_len);
    unsigned             bucket = (unsigned) (id % CHIMERA_SMB_PATH_BUCKETS);
    struct smb_path_ent *ent;

    pthread_mutex_lock(&server->path_lock);

    for (ent = server->path_buckets[bucket]; ent; ent = ent->next) {
        if (ent->id == id) {
            /* Same id: refresh the path in case a collision remapped it. */
            if (ent->path_len != path_len ||
                memcmp(ent->path, path, (size_t) path_len) != 0) {
                char *dup = malloc((size_t) path_len + 1);
                if (dup) {
                    memcpy(dup, path, (size_t) path_len);
                    dup[path_len] = '\0';
                    free(ent->path);
                    ent->path     = dup;
                    ent->path_len = path_len;
                }
            }
            pthread_mutex_unlock(&server->path_lock);
            return id;
        }
    }

    ent           = calloc(1, sizeof(*ent));
    ent->id       = id;
    ent->path_len = path_len;
    ent->path     = malloc((size_t) path_len + 1);
    memcpy(ent->path, path, (size_t) path_len);
    ent->path[path_len]          = '\0';
    ent->next                    = server->path_buckets[bucket];
    server->path_buckets[bucket] = ent;

    pthread_mutex_unlock(&server->path_lock);
    return id;
} /* chimera_smb_path_intern */

const char *
chimera_smb_path_resolve(
    struct chimera_smb_client_server *server,
    uint64_t                          id,
    int                              *out_len)
{
    unsigned             bucket = (unsigned) (id % CHIMERA_SMB_PATH_BUCKETS);
    struct smb_path_ent *ent;
    const char          *path = NULL;

    pthread_mutex_lock(&server->path_lock);
    for (ent = server->path_buckets[bucket]; ent; ent = ent->next) {
        if (ent->id == id) {
            path = ent->path;
            if (out_len) {
                *out_len = ent->path_len;
            }
            break;
        }
    }
    pthread_mutex_unlock(&server->path_lock);
    return path;
} /* chimera_smb_path_resolve */

void
chimera_smb_path_table_clear(struct chimera_smb_client_server *server)
{
    int i;

    pthread_mutex_lock(&server->path_lock);
    for (i = 0; i < CHIMERA_SMB_PATH_BUCKETS; i++) {
        struct smb_path_ent *ent = server->path_buckets[i];
        while (ent) {
            struct smb_path_ent *next = ent->next;
            free(ent->path);
            free(ent);
            ent = next;
        }
        server->path_buckets[i] = NULL;
    }
    pthread_mutex_unlock(&server->path_lock);
} /* chimera_smb_path_table_clear */

/*
 * Build the full mount-relative path for a `name` given relative to the parent
 * handle in request->fh.
 *
 * Most ops arrive rebased against the mount root (request->fh is the root fh),
 * where `name` already IS the full path.  But an *at op issued against a real
 * directory descriptor (openat/unlinkat/utimensat with a dirfd) hands the
 * backend that directory's open-token handle as the parent and only the leaf as
 * `name`; a path-only backend must prefix the directory's path itself, since
 * SMB2 addresses every CREATE from the share root, not from an open FileId.
 * The parent's path is the one interned when it was opened (SD5's path table).
 *
 * Returns the length written (NUL-terminated), or -1 if it does not fit or the
 * parent handle's path is unknown.
 */
static int
smb_at_full_path(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *name,
    int                             namelen,
    char                           *out,
    int                             out_max)
{
    const char *ppath;
    int         plen = 0, n;

    if (chimera_smb_fh_is_root(request->fh_len)) {
        if (namelen >= out_max) {
            return -1;
        }
        memcpy(out, name, namelen);
        out[namelen] = '\0';
        return namelen;
    }

    ppath = chimera_smb_path_resolve(conn->server,
                                     chimera_smb_fh_path_id(request->fh), &plen);
    if (!ppath) {
        return -1;
    }

    /* parent + '/' + name, except an empty parent (the share root itself, whose
     * open-token path is "") joins to just the name. */
    n = plen + (plen ? 1 : 0) + namelen;
    if (n >= out_max) {
        return -1;
    }
    n = 0;
    memcpy(out, ppath, plen);
    n += plen;
    if (plen) {
        out[n++] = '/';
    }
    memcpy(out + n, name, namelen);
    n     += namelen;
    out[n] = '\0';
    return n;
} /* smb_at_full_path */

/* ---- small helpers ----------------------------------------------------- */

size_t
smb_utf16le_encode(
    const char *s,
    int         len,
    uint8_t    *out)
{
    int i;

    /* A negative length must not reach the cast below: the loop would write
     * nothing while (size_t) len * 2 wrapped to an enormous value, and every
     * caller uses the return as the length of the buffer it just filled --
     * appending that to a cursor would read far past the end of it. */
    if (len <= 0) {
        return 0;
    }

    for (i = 0; i < len; i++) {
        /* SMB path separators are backslashes. */
        uint8_t c = (uint8_t) (s[i] == '/' ? '\\' : s[i]);
        out[i * 2]     = c;
        out[i * 2 + 1] = 0;
    }
    return (size_t) len * 2;
} /* smb_utf16le_encode */

void
smb_parse_open_info(
    struct evpl_iovec_cursor *body,
    struct smb_open_info     *r)
{
    uint32_t reserved;

    evpl_iovec_cursor_get_uint64(body, &r->crttime);
    evpl_iovec_cursor_get_uint64(body, &r->atime);
    evpl_iovec_cursor_get_uint64(body, &r->mtime);
    evpl_iovec_cursor_get_uint64(body, &r->ctime);
    evpl_iovec_cursor_get_uint64(body, &r->alloc_size);
    evpl_iovec_cursor_get_uint64(body, &r->end_of_file);
    evpl_iovec_cursor_get_uint32(body, &r->file_attributes);
    evpl_iovec_cursor_get_uint32(body, &reserved);
} /* smb_parse_open_info */

void
smb_parse_create_reply(
    struct evpl_iovec_cursor *body,
    struct smb_create_result *r)
{
    uint16_t structsize;
    uint8_t  oplock, flags;

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint8(body, &oplock);
    evpl_iovec_cursor_get_uint8(body, &flags);
    evpl_iovec_cursor_get_uint32(body, &r->create_action);
    smb_parse_open_info(body, &r->info);
    evpl_iovec_cursor_get_uint64(body, &r->file_id.pid);
    evpl_iovec_cursor_get_uint64(body, &r->file_id.vid);

    (void) structsize;
    (void) oplock;
    (void) flags;
} /* smb_parse_create_reply */

/* Map SMB attrs into a chimera_vfs_attrs.  SMB exposes no POSIX owner; report
 * the requesting credential as the owner (correct for a caller inspecting files
 * it created), and a caller-supplied stable inode number. */
void
smb_apply_attrs(
    const struct chimera_vfs_request *request,
    struct chimera_vfs_attrs         *attr,
    const struct smb_open_info       *info,
    uint64_t                          ino)
{
    smb_fill_attrs_from_network_open(attr, info->crttime, info->atime,
                                     info->mtime, info->ctime, info->alloc_size,
                                     info->end_of_file, info->file_attributes);

    attr->va_ino       = ino | 1;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_INUM;

    if (request->cred) {
        attr->va_uid       = request->cred->uid;
        attr->va_gid       = request->cred->gid;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    }
} /* smb_apply_attrs */

/* Send an SMB2 CREATE on `path`, optionally carrying `ctx` create contexts (a
 * pre-built, already-chained context blob) after the name. */
void
smb_send_create_ex(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    const uint8_t                  *ctx,
    uint32_t                        ctx_len,
    chimera_smb_client_reply_cb     reply_cb,
    void                           *reply_arg)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  name16[2 * CHIMERA_SMB_PATH_MAX];
    size_t                   name16_len;
    uint32_t                 name_end, ctx_off = 0, pad = 0;

    if (path_len < 0 || path_len > CHIMERA_SMB_PATH_MAX) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    name16_len = smb_utf16le_encode(path, path_len, name16);

    /* Offsets are relative to the SMB2 header start: NameOffset = header(64) +
     * fixed body(56).  Create contexts (if any) follow the name, 8-byte aligned. */
    if (ctx_len > 0) {
        name_end = sizeof(struct smb2_header) + 56 + (uint32_t) name16_len;
        ctx_off  = (name_end + 7) & ~7u;
        pad      = ctx_off - name_end;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_CREATE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_CREATE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, 0);                             /* SecurityFlags */
    evpl_iovec_cursor_append_uint8(&cursor, ctx_len ? SMB2_OPLOCK_LEVEL_LEASE : 0); /* RequestedOplockLevel */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_IMPERSONATION_IMPERSONATION);
    evpl_iovec_cursor_append_uint64(&cursor, 0);                            /* SmbCreateFlags */
    evpl_iovec_cursor_append_uint64(&cursor, 0);                            /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, desired_access);
    evpl_iovec_cursor_append_uint32(&cursor, 0);                            /* FileAttributes */
    evpl_iovec_cursor_append_uint32(&cursor, share_access);
    evpl_iovec_cursor_append_uint32(&cursor, disposition);
    evpl_iovec_cursor_append_uint32(&cursor, options);
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 56); /* NameOffset */
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) name16_len);        /* NameLength */
    evpl_iovec_cursor_append_uint32(&cursor, ctx_off);                      /* CreateContextsOffset */
    evpl_iovec_cursor_append_uint32(&cursor, ctx_len);                      /* CreateContextsLength */
    if (name16_len > 0) {
        evpl_iovec_cursor_append_blob(&cursor, name16, name16_len);
    }

    if (ctx_len > 0) {
        /* Unaligned: append_blob would re-align the cursor and shift these past
         * the CreateContextsOffset we declared above (the name already left the
         * cursor at name_end). */
        static uint8_t zero[8] = { 0 };
        if (pad > 0) {
            evpl_iovec_cursor_append_blob_unaligned(&cursor, zero, pad);
        }
        evpl_iovec_cursor_append_blob_unaligned(&cursor, (uint8_t *) ctx, ctx_len);
    }

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request, reply_cb, reply_arg);
} /* smb_send_create_ex */

/* Send an SMB2 CREATE on `path` (full mount-relative path; "" for the root). */
void
smb_send_create(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    chimera_smb_client_reply_cb     reply_cb)
{
    smb_send_create_ex(conn, request, path, path_len, desired_access,
                       share_access, disposition, options, NULL, 0, reply_cb,
                       request);
} /* smb_send_create */

/* ---- symbolic-link following (client-side resolution) ------------------ */

/*
 * A CREATE whose final path component resolves to a symbolic link the caller
 * wants to follow comes back STATUS_STOPPED_ON_SYMLINK carrying a
 * SymbolicLinkErrorResponse (MS-SMB2 2.2.2.2.1): the server read the link and
 * handed us its target, expecting us -- the client -- to splice the target into
 * the path and retry.  That is how a real SMB client (and cifs.ko) follows a
 * server-side symlink; the proxy did neither and simply reported ELOOP.
 *
 * The chimera server stops only on a final-component link (it follows
 * intermediates during the walk), so UnparsedPathLength is always 0 and the
 * rewrite is: replace the last component of the requested path with the target
 * (relative), or use the target from the share root (absolute).  A chain of
 * links loops here until SMB_SYMLOOP_MAX, then surfaces the ELOOP.
 */
#define SMB_SYMLOOP_MAX 40

struct smb_follow_ctx {
    struct chimera_vfs_request *request;
    chimera_smb_client_reply_cb real_cb;
    uint32_t                    desired_access;
    uint32_t                    share_access;
    uint32_t                    disposition;
    uint32_t                    options;
    int                         hops;
    int                         path_len;
    int                         cctx_len;
    char                        path[CHIMERA_SMB_PATH_MAX + 1];
    uint8_t                     cctx[CHIMERA_SMB_LEASE_CTX_SIZE];
};

/* Parse a SymbolicLinkErrorResponse body into the UTF-8 target (with '\'->'/')
 * and its relative flag.  Returns 0 on success, -1 if the body is not a
 * well-formed 'SYML' response we can act on. */
static int
smb_parse_symlink_error(
    struct evpl_iovec_cursor *body,
    char                     *out_target,
    int                       out_max,
    int                      *out_relative)
{
    uint16_t structsize, reserved16, reparse_data_len, unparsed_len;
    uint16_t sub_off, sub_len, print_off, print_len;
    uint32_t byte_count, sym_link_len, sym_tag, reparse_tag, flags;
    int      i, n;
    uint8_t  name16[2 * CHIMERA_SMB_PATH_MAX];

    /* SMB2 ERROR Response header. */
    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &reserved16);   /* ErrCtxCount + Reserved */
    evpl_iovec_cursor_get_uint32(body, &byte_count);
    if (structsize != SMB2_ERROR_REPLY_SIZE || byte_count < 4) {
        return -1;
    }

    /* SymbolicLinkErrorResponse. */
    evpl_iovec_cursor_get_uint32(body, &sym_link_len);
    evpl_iovec_cursor_get_uint32(body, &sym_tag);
    evpl_iovec_cursor_get_uint32(body, &reparse_tag);
    evpl_iovec_cursor_get_uint16(body, &reparse_data_len);
    evpl_iovec_cursor_get_uint16(body, &unparsed_len);
    evpl_iovec_cursor_get_uint16(body, &sub_off);
    evpl_iovec_cursor_get_uint16(body, &sub_len);
    evpl_iovec_cursor_get_uint16(body, &print_off);
    evpl_iovec_cursor_get_uint16(body, &print_len);
    evpl_iovec_cursor_get_uint32(body, &flags);

    (void) reparse_data_len;
    (void) print_off;
    (void) print_len;

    if (sym_tag != SMB2_SYMLINK_ERROR_TAG ||
        reparse_tag != SMB2_IO_REPARSE_TAG_SYMLINK ||
        unparsed_len != 0) {
        /* Not a link error we can resolve (or an intermediate-component stop we
         * do not splice); let the caller see the raw ELOOP. */
        return -1;
    }

    /* PathBuffer begins here; SubstituteNameOffset is relative to it. */
    if (sub_off > 0) {
        evpl_iovec_cursor_skip(body, sub_off);
    }
    if (sub_len == 0 || (int) sub_len > (int) sizeof(name16)) {
        return -1;
    }
    if (evpl_iovec_cursor_get_blob(body, name16, sub_len) < 0) {
        return -1;
    }

    /* Decode UTF-16LE (ASCII subset, as the whole client round-trips) and flip
     * SMB's '\' separators back to '/'. */
    n = sub_len / 2;
    if (n >= out_max) {
        n = out_max - 1;
    }
    for (i = 0; i < n; i++) {
        char c = (char) name16[i * 2];
        out_target[i] = (c == '\\') ? '/' : c;
    }
    out_target[n] = '\0';

    *out_relative = !(flags & SMB2_SYMLINK_FLAG_ABSOLUTE) &&
        (flags & SMB2_SYMLINK_FLAG_RELATIVE);
    return n;
} /* smb_parse_symlink_error */

/* Splice `target` (length tlen) into `path` in place of its final component
 * (relative) or from the share root (absolute).  Returns the new length, or -1
 * if it would not fit. */
static int
smb_splice_symlink_target(
    char       *path,
    int         path_len,
    const char *target,
    int         tlen,
    int         relative)
{
    int base;

    if (!relative) {
        while (tlen > 0 && target[0] == '/') {
            target++;
            tlen--;
        }
        if (tlen > CHIMERA_SMB_PATH_MAX) {
            return -1;
        }
        memmove(path, target, tlen);
        return tlen;
    }

    /* base = start of the final component (index just past the last '/'). */
    base = path_len;
    while (base > 0 && path[base - 1] != '/') {
        base--;
    }
    if (base + tlen > CHIMERA_SMB_PATH_MAX) {
        return -1;
    }
    memmove(path + base, target, tlen);
    return base + tlen;
} /* smb_splice_symlink_target */

static void
smb_create_follow_shim(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct smb_follow_ctx      *fc      = arg;
    struct chimera_vfs_request *request = fc->request;

    if (status == SMB2_STATUS_STOPPED_ON_SYMLINK && fc->hops > 0) {
        char target[CHIMERA_SMB_PATH_MAX + 1];
        int  relative = 0;
        int  tlen     = smb_parse_symlink_error(body, target, sizeof(target),
                                                &relative);
        int  newlen;

        if (tlen > 0 &&
            (newlen = smb_splice_symlink_target(fc->path, fc->path_len, target,
                                                tlen, relative)) >= 0) {
            fc->path_len = newlen;
            fc->hops--;
            smb_send_create_ex(conn, request, fc->path, fc->path_len,
                               fc->desired_access, fc->share_access,
                               fc->disposition, fc->options,
                               fc->cctx_len ? fc->cctx : NULL,
                               (uint32_t) fc->cctx_len,
                               smb_create_follow_shim, fc);
            return;
        }
        /* Could not parse or splice the target: fall through and let the caller
         * see the ELOOP the raw status maps to. */
    }

    fc->real_cb(conn, status, hdr, body, body_len, request);
    free(fc);
} /* smb_create_follow_shim */

/* Like smb_send_create_ex, but transparently follows a final-component symlink
 * the server stops on, retrying on the resolved path (up to SMB_SYMLOOP_MAX). */
static void
smb_send_create_follow(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    const uint8_t                  *cctx,
    uint32_t                        cctx_len,
    chimera_smb_client_reply_cb     reply_cb)
{
    struct smb_follow_ctx *fc;

    if (path_len < 0 || path_len > CHIMERA_SMB_PATH_MAX ||
        cctx_len > sizeof(fc->cctx)) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    fc                 = calloc(1, sizeof(*fc));
    fc->request        = request;
    fc->real_cb        = reply_cb;
    fc->desired_access = desired_access;
    fc->share_access   = share_access;
    fc->disposition    = disposition;
    fc->options        = options;
    fc->hops           = SMB_SYMLOOP_MAX;
    fc->path_len       = path_len;
    memcpy(fc->path, path, path_len);
    fc->path[path_len] = '\0';
    fc->cctx_len       = (int) cctx_len;
    if (cctx_len) {
        memcpy(fc->cctx, cctx, cctx_len);
    }

    smb_send_create_ex(conn, request, fc->path, fc->path_len, desired_access,
                       share_access, disposition, options,
                       cctx_len ? fc->cctx : NULL, cctx_len,
                       smb_create_follow_shim, fc);
} /* smb_send_create_follow */

/* Build an RqLs (lease request v1) create context into `buf` (>= 56 bytes);
 * returns its length.  Header(16) + name "RqLs"(4) + pad(4) + data(32). */
uint32_t
smb_build_lease_ctx(
    uint8_t       *buf,
    const uint8_t *lease_key,
    uint32_t       lease_state)
{
    memset(buf, 0, CHIMERA_SMB_LEASE_CTX_SIZE);

    /* Context header. */
    smb_wire_set_le16(buf + 4, 16);      /* NameOffset (from context start) */
    smb_wire_set_le16(buf + 6, 4);       /* NameLength */
    smb_wire_set_le16(buf + 10, 24);     /* DataOffset */
    smb_wire_set_le32(buf + 12, 32);     /* DataLength (RqLs v1) */
    memcpy(buf + 16, "RqLs", 4);

    /* RqLs v1 data: LeaseKey(16), LeaseState(4), LeaseFlags(4), Duration(8). */
    memcpy(buf + 24, lease_key, 16);
    smb_wire_set_le32(buf + 40, lease_state);

    return CHIMERA_SMB_LEASE_CTX_SIZE;
} /* smb_build_lease_ctx */

void
smb_send_close(
    struct chimera_smb_client_conn          *conn,
    struct chimera_vfs_request              *request,
    const struct chimera_smb_client_file_id *file_id,
    chimera_smb_client_reply_cb              reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;

    chimera_smb_client_pdu_begin(conn, SMB2_CLOSE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_CLOSE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Flags */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Reserved */
    evpl_iovec_cursor_append_uint64(&cursor, file_id->pid);
    evpl_iovec_cursor_append_uint64(&cursor, file_id->vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request, reply_cb, request);
} /* smb_send_close */

/* ---- CLOSE (VFS op) ---------------------------------------------------- */

static void
chimera_smb_close_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_close_reply */

void
chimera_smb_client_close(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open   *open_state =
        (struct chimera_smb_client_open *) request->close.vfs_private;
    struct chimera_smb_client_file_id file_id = open_state->file_id;

    free(open_state);

    smb_send_close(conn, request, &file_id, chimera_smb_close_reply);
} /* chimera_smb_client_close */

/* ---- getattr (handle-based QUERY_INFO) --------------------------------- */

#define CHIMERA_SMB_STATFS_ATTRS \
        (CHIMERA_VFS_ATTR_SPACE_TOTAL | CHIMERA_VFS_ATTR_SPACE_FREE | \
         CHIMERA_VFS_ATTR_SPACE_AVAIL)

/* statfs is funnelled through getattr (the client opens a handle and requests
 * the SPACE_* attrs); answer it with QUERY_INFO FILESYSTEM /
 * FileFsFullSizeInformation instead of the per-file network-open info. */
static void
chimera_smb_statfs_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;
    uint16_t                    structsize, out_offset;
    uint32_t                    out_length, sectors_per_unit, bytes_per_sector;
    uint64_t                    total_units, caller_avail_units, actual_avail_units;
    uint64_t                    bytes_per_unit;
    int                         consumed;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &out_offset);
    evpl_iovec_cursor_get_uint32(body, &out_length);

    consumed = evpl_iovec_cursor_consumed(body);
    if (out_offset > consumed) {
        evpl_iovec_cursor_skip(body, out_offset - consumed);
    }

    /* FileFsFullSizeInformation (MS-FSCC 2.5.4): TotalAllocationUnits(8),
     * CallerAvailableAllocationUnits(8), ActualAvailableAllocationUnits(8),
     * SectorsPerAllocationUnit(4), BytesPerSector(4). */
    evpl_iovec_cursor_get_uint64(body, &total_units);
    evpl_iovec_cursor_get_uint64(body, &caller_avail_units);
    evpl_iovec_cursor_get_uint64(body, &actual_avail_units);
    evpl_iovec_cursor_get_uint32(body, &sectors_per_unit);
    evpl_iovec_cursor_get_uint32(body, &bytes_per_sector);

    (void) structsize;
    (void) out_length;

    bytes_per_unit = (uint64_t) sectors_per_unit * bytes_per_sector;

    request->getattr.r_attr.va_fs_space_total = total_units * bytes_per_unit;
    request->getattr.r_attr.va_fs_space_avail = caller_avail_units * bytes_per_unit;
    request->getattr.r_attr.va_fs_space_free  = actual_avail_units * bytes_per_unit;
    request->getattr.r_attr.va_set_mask      |= CHIMERA_SMB_STATFS_ATTRS;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_statfs_reply */

static void
chimera_smb_client_statfs(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    struct chimera_smb_client_open *open_state)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;

    /* QUERY_INFO, FILESYSTEM / FileFsFullSizeInformation, on the open FileId. */
    chimera_smb_client_pdu_begin(conn, SMB2_QUERY_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_QUERY_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILESYSTEM);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_FS_FULL_SIZE_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, 32);            /* OutputBufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* InputBufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* InputBufferLength */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* AdditionalInformation */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Flags */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_statfs_reply, request);
} /* chimera_smb_client_statfs */

static void
chimera_smb_getattr_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request     *request    = arg;
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->getattr.handle);
    struct smb_open_info            info;
    uint16_t                        structsize, out_offset;
    uint32_t                        out_length;
    int                             consumed;

    (void) conn;
    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &out_offset);
    evpl_iovec_cursor_get_uint32(body, &out_length);

    consumed = evpl_iovec_cursor_consumed(body);
    if (out_offset > consumed) {
        evpl_iovec_cursor_skip(body, out_offset - consumed);
    }

    smb_parse_open_info(body, &info);

    smb_apply_attrs(request, &request->getattr.r_attr, &info,
                    open_state->file_id.pid);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_getattr_reply */

void
chimera_smb_client_getattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->getattr.handle);
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;

    if (!open_state) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* A statfs request (SPACE_* attrs) goes to the filesystem-info query. */
    if (request->getattr.r_attr.va_req_mask & CHIMERA_SMB_STATFS_ATTRS) {
        chimera_smb_client_statfs(conn, request, open_state);
        return;
    }

    /* SMB2 QUERY_INFO, FILE / FileNetworkOpenInformation, on the open FileId. */
    chimera_smb_client_pdu_begin(conn, SMB2_QUERY_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_QUERY_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_NETWORK_OPEN_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_FILE_NETWORK_OPEN_INFO_SIZE); /* OutputBufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* InputBufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* InputBufferLength */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* AdditionalInformation */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Flags */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_getattr_reply, request);
} /* chimera_smb_client_getattr */

/* ---- lookup_at (full path, transient open) ----------------------------- */

static void
chimera_smb_lookup_close_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) status;

    request->complete(request);
} /* chimera_smb_lookup_close_reply */

static void
chimera_smb_lookup_create_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    struct smb_create_result     r;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    /* Return a re-openable child fh: the path id of the component resolved
     * against the parent handle.  chimera_vfs_lookup and every *at op built on
     * it (utimensat, faccessat, ...) open that fh next, so a zero-length fh --
     * the old path-only shortcut -- made them all ESTALE.  open_fh re-CREATEs
     * the interned path, exactly as for an open_at handle. */
    char     fullpath[CHIMERA_SMB_PATH_MAX + 1];
    int      fullpath_len = smb_at_full_path(conn, request,
                                             request->lookup_at.component,
                                             request->lookup_at.component_len,
                                             fullpath, sizeof(fullpath));
    uint64_t path_id;

    if (fullpath_len < 0) {
        path_id = XXH3_64bits(request->lookup_at.component,
                              request->lookup_at.component_len);
        request->lookup_at.r_attr.va_fh_len = 0;
    } else {
        path_id                             = chimera_smb_path_intern(conn->server, fullpath, fullpath_len);
        request->lookup_at.r_attr.va_fh_len =
            chimera_smb_encode_open_fh(request->fh, path_id,
                                       request->lookup_at.r_attr.va_fh);
        request->lookup_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
    }

    smb_apply_attrs(request, &request->lookup_at.r_attr, &r.info, path_id);

    request->status = CHIMERA_VFS_OK;
    state->file_id  = r.file_id;

    smb_send_close(conn, request, &state->file_id, chimera_smb_lookup_close_reply);
} /* chimera_smb_lookup_create_reply */

void
chimera_smb_client_lookup_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    char path[CHIMERA_SMB_PATH_MAX + 1];
    int  path_len;

    path_len = smb_at_full_path(conn, request, request->lookup_at.component,
                                request->lookup_at.component_len,
                                path, sizeof(path));
    if (path_len < 0) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    smb_send_create(conn, request, path, path_len,
                    SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, 0,
                    chimera_smb_lookup_create_reply);
} /* chimera_smb_client_lookup_at */

/* ---- open_at / open_fh (persistent open) ------------------------------- */

static void
chimera_smb_open_at_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request     *request = arg;
    struct chimera_smb_client_open *open_state;
    struct smb_create_result        r;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    open_state               = calloc(1, sizeof(*open_state));
    open_state->file_id      = r.file_id;
    open_state->server_index = (uint8_t) conn->server->index;
    open_state->is_directory = (r.info.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) != 0;

    /* Intern the full mount-relative path (the leaf resolved against the parent
     * handle) so open_fh can re-CREATE this object after its handle is evicted;
     * the path id is the handle's fh identity. */
    char     fullpath[CHIMERA_SMB_PATH_MAX + 1];
    int      fullpath_len = smb_at_full_path(conn, request,
                                             request->open_at.name,
                                             request->open_at.namelen,
                                             fullpath, sizeof(fullpath));
    uint64_t path_id;

    if (fullpath_len < 0) {
        /* Parent path unknown: fall back to the leaf so the open still yields a
         * usable (if not re-derivable) handle. */
        path_id = chimera_smb_path_intern(conn->server, request->open_at.name,
                                          request->open_at.namelen);
    } else {
        path_id = chimera_smb_path_intern(conn->server, fullpath, fullpath_len);
    }

    smb_apply_attrs(request, &request->open_at.r_attr, &r.info, path_id);

    /* The handle's identity is the open token built from the path id. */
    request->open_at.r_attr.va_fh_len = chimera_smb_encode_open_fh(
        request->fh, path_id, request->open_at.r_attr.va_fh);
    request->open_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;

    request->open_at.r_vfs_private = (uint64_t) (uintptr_t) open_state;
    request->open_at.r_created     = (r.create_action == SMB2_CREATE_ACTION_CREATED);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_open_at_reply */

void
chimera_smb_client_open_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    uint32_t       disposition;
    uint32_t       desired_access;
    /* A directory open must request FILE_DIRECTORY_FILE; only a non-directory
     * open may set FILE_NON_DIRECTORY_FILE.  The server now enforces the option
     * against the target type (FILE_IS_A_DIRECTORY otherwise), so a directory
     * open that left NON_DIRECTORY_FILE set would be refused. */
    uint32_t       options = (request->open_at.flags & CHIMERA_VFS_OPEN_DIRECTORY)
                             ? SMB2_FILE_DIRECTORY_FILE : SMB2_FILE_NON_DIRECTORY_FILE;
    uint8_t        lease_ctx[CHIMERA_SMB_LEASE_CTX_SIZE];
    uint8_t        lease_key[16];
    const uint8_t *ctx     = NULL;
    uint32_t       ctx_len = 0;
    char           path[CHIMERA_SMB_PATH_MAX + 1];
    int            path_len;

    /* Resolve the leaf against the parent handle (a dirfd-relative open); for a
     * root-relative open this is just the name. */
    path_len = smb_at_full_path(conn, request, request->open_at.name,
                                request->open_at.namelen, path, sizeof(path));
    if (path_len < 0) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {
        disposition = (request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE)
                      ? SMB2_FILE_CREATE : SMB2_FILE_OPEN_IF;
    } else {
        disposition = SMB2_FILE_OPEN;
    }

    /*
     * NOFOLLOW: open the symlink (reparse point) itself rather than following
     * it to its target, so callers like readlink see the link node.
     */
    if (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
        options |= SMB2_FILE_OPEN_REPARSE_POINT;
    }

    desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
        SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE;

    /* Request a read+handle-caching lease on file opens.  HANDLE caching is the
     * prerequisite the server requires before it will grant a durable handle
     * (the next increment), and the client acks any later break.  Directory
     * opens skip it. */
    if (!(request->open_at.flags & CHIMERA_VFS_OPEN_DIRECTORY)) {
        /* Build the key in uint64 units and copy it in, rather than writing
         * through a uint64_t alias of the byte array: the array has no
         * guaranteed alignment for a 64-bit store and the aliasing is
         * undefined behaviour.  It also leaves lease_key visibly initialized,
         * which the type pun did not -- the analyzer reported the bytes as
         * undefined here and again where the finished context is appended to
         * the send cursor. */
        uint64_t k[2] = { chimera_rand64(), chimera_rand64() };

        memcpy(lease_key, k, sizeof(lease_key));
        ctx_len = smb_build_lease_ctx(lease_ctx, lease_key,
                                      SMB2_LEASE_READ_CACHING | SMB2_LEASE_HANDLE_CACHING);
        ctx = lease_ctx;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
        /* Open the link node itself (FILE_OPEN_REPARSE_POINT set above); the
         * server does not stop on it, so no client-side resolution is wanted. */
        smb_send_create_ex(conn, request,
                           path, path_len,
                           desired_access,
                           SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                           disposition, options, ctx, ctx_len,
                           chimera_smb_open_at_reply, request);
    } else {
        /* Follow a final-component symlink the server stops on, resolving it
         * client-side and retrying on the target. */
        smb_send_create_follow(conn, request,
                               path, path_len,
                               desired_access,
                               SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                               disposition, options, ctx, ctx_len,
                               chimera_smb_open_at_reply);
    }
} /* chimera_smb_client_open_at */

static void
chimera_smb_open_fh_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request     *request = arg;
    struct chimera_smb_client_open *open_state;
    struct smb_create_result        r;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    open_state               = calloc(1, sizeof(*open_state));
    open_state->file_id      = r.file_id;
    open_state->server_index = (uint8_t) conn->server->index;
    open_state->is_directory = (r.info.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) != 0;

    request->open_fh.r_vfs_private = (uint64_t) (uintptr_t) open_state;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_open_fh_reply */

void
chimera_smb_client_open_fh(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    const char *path;
    int         path_len;
    uint32_t    options;
    uint32_t    desired_access;

    options = (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY)
              ? SMB2_FILE_DIRECTORY_FILE : SMB2_FILE_NON_DIRECTORY_FILE;

    if (chimera_smb_fh_is_root(request->fh_len)) {
        /* The share root is the empty path; read access suffices for it. */
        path           = "";
        path_len       = 0;
        desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_READ_ATTRIBUTES |
            SMB2_FILE_LIST_DIRECTORY;
    } else {
        /* Re-CREATE any other object from the path its id was interned under
         * (at open_at time).  A never-seen id means the object was never
         * opened through this mount -- there is nothing to re-derive. */
        path = chimera_smb_path_resolve(conn->server,
                                        chimera_smb_fh_path_id(request->fh),
                                        &path_len);
        if (!path) {
            request->status = CHIMERA_VFS_ESTALE;
            request->complete(request);
            return;
        }
        /* Match open_at's broad grant so the re-opened handle serves reads,
         * writes and metadata ops alike (the mount authenticates as one
         * identity, so the server admits whatever that identity may do). */
        desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
            SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE;
    }

    smb_send_create(conn, request, path, path_len, desired_access,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, options,
                    chimera_smb_open_fh_reply);
} /* chimera_smb_client_open_fh */

/* ---- mkdir_at (full path, transient open) ------------------------------ */

static void
chimera_smb_mkdir_close_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) status;

    request->complete(request);
} /* chimera_smb_mkdir_close_reply */

static void
chimera_smb_mkdir_create_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    struct smb_create_result     r;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    /* Transient open of a directory: return attrs, no persistent fh. */
    smb_apply_attrs(request, &request->mkdir_at.r_attr, &r.info,
                    XXH3_64bits(request->mkdir_at.name, request->mkdir_at.name_len));

    request->status = CHIMERA_VFS_OK;
    state->file_id  = r.file_id;

    smb_send_close(conn, request, &state->file_id, chimera_smb_mkdir_close_reply);
} /* chimera_smb_mkdir_create_reply */

void
chimera_smb_client_mkdir_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    smb_send_create(conn, request,
                    request->mkdir_at.name, request->mkdir_at.name_len,
                    SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_CREATE, SMB2_FILE_DIRECTORY_FILE,
                    chimera_smb_mkdir_create_reply);
} /* chimera_smb_client_mkdir_at */

/* ---- remove_at (full path, delete-on-close) ---------------------------- */

static void
chimera_smb_remove_close_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_remove_close_reply */

static void
chimera_smb_remove_create_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    struct smb_create_result     r;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);
    state->file_id = r.file_id;

    /* FILE_DELETE_ON_CLOSE was set on the open, so CLOSE removes the file. */
    smb_send_close(conn, request, &state->file_id, chimera_smb_remove_close_reply);
} /* chimera_smb_remove_create_reply */

void
chimera_smb_client_remove_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    /* POSIX unlink/rmdir removes the named entry itself; a final-component
     * symlink must be deleted as the link, never followed (which would ELOOP or
     * hit the target).  FILE_OPEN_REPARSE_POINT opens the link node so
     * DELETE_ON_CLOSE removes it. */
    smb_send_create(conn, request,
                    request->remove_at.name, request->remove_at.namelen,
                    SMB2_DELETE | SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN,
                    SMB2_FILE_DELETE_ON_CLOSE | SMB2_FILE_OPEN_REPARSE_POINT,
                    chimera_smb_remove_create_reply);
} /* chimera_smb_client_remove_at */

/* ---- setattr (SET_INFO on the open handle) ----------------------------- */

#define CHIMERA_SMB_SETATTR_TIMES \
        (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME | \
         CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_BTIME)

/* Final SET_INFO reply (size-only, times-only, or the times leg of size+times). */
static void
chimera_smb_setattr_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_setattr_reply */

/* SET_INFO FileBasicInformation: set the requested timestamps (a zero FILETIME
 * means "leave unchanged"; FileAttributes 0 likewise). */
static void
chimera_smb_setattr_send_basic(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->setattr.handle);
    struct chimera_vfs_attrs       *set_attr   = request->setattr.set_attr;
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;
    uint64_t                        crttime, atime, mtime, ctime;

    crttime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_BTIME) ?
        smb_timespec_to_filetime(&set_attr->va_btime) : 0;
    atime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) ?
        smb_timespec_to_filetime(&set_attr->va_atime) : 0;
    mtime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) ?
        smb_timespec_to_filetime(&set_attr->va_mtime) : 0;
    ctime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_CTIME) ?
        smb_timespec_to_filetime(&set_attr->va_ctime) : 0;

    chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_BASIC_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, 40);                            /* BufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 32); /* BufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* AdditionalInformation */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
    /* FILE_BASIC_INFORMATION: Creation/LastAccess/LastWrite/ChangeTime + attrs. */
    evpl_iovec_cursor_append_uint64(&cursor, crttime);
    evpl_iovec_cursor_append_uint64(&cursor, atime);
    evpl_iovec_cursor_append_uint64(&cursor, mtime);
    evpl_iovec_cursor_append_uint64(&cursor, ctime);
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* FileAttributes (no change) */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* Reserved */

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_setattr_reply, request);
} /* chimera_smb_setattr_send_basic */

/* After the size leg: chain the times leg if any timestamps were also set. */
static void
chimera_smb_setattr_size_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) hdr;
    (void) body;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    if (request->setattr.set_attr->va_set_mask & CHIMERA_SMB_SETATTR_TIMES) {
        chimera_smb_setattr_send_basic(conn, request);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_setattr_size_reply */

void
chimera_smb_client_setattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->setattr.handle);
    struct chimera_vfs_attrs       *set_attr   = request->setattr.set_attr;
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;

    if (!open_state) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    /* SMB can set size (FileEndOfFileInformation) and timestamps (FileBasic-
     * Information).  POSIX mode/owner have no SMB2 equivalent against this
     * server, so those bits are accepted but not applied. */
    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        /* Set size first, then chain the times leg from its reply. */
        chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &hdr);

        evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
        evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
        evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_ENDOFFILE_INFO);
        evpl_iovec_cursor_append_uint32(&cursor, 8);                             /* BufferLength */
        evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 32); /* BufferOffset */
        evpl_iovec_cursor_append_uint16(&cursor, 0);                             /* Reserved */
        evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* AdditionalInformation */
        evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
        evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
        evpl_iovec_cursor_append_uint64(&cursor, set_attr->va_size);

        chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                      chimera_smb_setattr_size_reply, request);
        return;
    }

    if (set_attr->va_set_mask & CHIMERA_SMB_SETATTR_TIMES) {
        chimera_smb_setattr_send_basic(conn, request);
        return;
    }

    /* Nothing SMB can apply (e.g. mode/owner only) -- accept as a no-op. */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_client_setattr */

/* ---- commit (FLUSH) ---------------------------------------------------- */

static void
chimera_smb_commit_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_commit_reply */

void
chimera_smb_client_commit(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->commit.handle);
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;

    if (!open_state) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_FLUSH, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_FLUSH_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Reserved1 */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Reserved2 */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_commit_reply, request);
} /* chimera_smb_client_commit */

/* read / write / readdir live in smb_io.c; rename/symlink/mknod in
 * smb_namespace.c -- they reuse the shared helpers declared in smb_internal.h. */
