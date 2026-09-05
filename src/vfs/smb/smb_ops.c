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

#ifndef SMB2_0_IOCTL_IS_FSCTL
#define SMB2_0_IOCTL_IS_FSCTL 0x00000001
#endif /* ifndef SMB2_0_IOCTL_IS_FSCTL */

/* A single stable st_dev for every object served through this proxy mount:
 * POSIX requires all files on one filesystem to share st_dev, and the model's
 * identity check requires it stable across ops.  The value is arbitrary -- the
 * model checks consistency, not the number. */
#define CHIMERA_SMB_ST_DEV    ((uint64_t) 0x00736d62)  /* 'smb' */

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

/* Collapse "." , ".." and empty ("//") components of a mount-relative path in
 * place, the way an SMB client must before sending -- the wire path is a
 * Windows-syntax name and the server rejects a literal ".." as
 * OBJECT_PATH_SYNTAX_BAD (which the VFS surfaces as EIO).  A ".." that would
 * escape the share root is dropped (the root's parent is itself).  Lexical
 * resolution matches a real CIFS mount; it differs from POSIX only when a
 * non-final component is a symlink, which the model rarely exercises.  Returns
 * the new length. */
static int
smb_path_normalize(
    char *path,
    int   len)
{
    int comps[CHIMERA_SMB_PATH_MAX / 2 + 1]; /* start offset of each kept comp */
    int ncomp = 0, i = 0, out = 0;

    while (i < len) {
        int start = i, clen;

        while (i < len && path[i] != '/') {
            i++;
        }
        clen = i - start;
        i++;                                 /* skip the '/' (or past the end) */

        if (clen == 0 || (clen == 1 && path[start] == '.')) {
            continue;                        /* "" or "." -- drop */
        }
        if (clen == 2 && path[start] == '.' && path[start + 1] == '.') {
            if (ncomp > 0) {
                out = comps[--ncomp];        /* pop the previous component */
            }
            continue;                        /* ".." at root -- drop */
        }
        if (out > 0) {
            path[out++] = '/';
        }
        comps[ncomp++] = out;
        memmove(path + out, path + start, clen);
        out += clen;
    }

    path[out] = '\0';
    return out;
} /* smb_path_normalize */

int
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
        return smb_path_normalize(out, namelen);
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
    return smb_path_normalize(out, n);
} /* smb_at_full_path */

/*
 * Give a create-and-forget namespace op (symlink/mkdir/mknod) a re-openable
 * child fh, the same path-id fh open_at/lookup_at return.  The VFS name-cache
 * insert reads r_attr.va_fh_len unconditionally, so leaving it as free-list
 * garbage crashes the engine (negative memcpy) and caching a WRONG child fh
 * mis-resolves the name later; interning the child's full path here makes the
 * cached entry correct.  Safe to call before the create -- the fh is a pure
 * function of the path, and the core only reads r_attr on success.
 */
void
chimera_smb_set_child_fh(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *name,
    int                             namelen,
    int                             nofollow,
    struct chimera_vfs_attrs       *r_attr)
{
    char     fullpath[CHIMERA_SMB_PATH_MAX + 1];
    int      len;
    uint64_t id;

    r_attr->va_set_mask = 0;

    len = smb_at_full_path(conn, request, name, namelen, fullpath,
                           sizeof(fullpath));
    if (len < 0) {
        r_attr->va_fh_len = 0;
        return;
    }

    id                  = chimera_smb_path_intern(conn->server, fullpath, len);
    r_attr->va_fh_len   = chimera_smb_encode_open_fh(request->fh, id, nofollow, r_attr->va_fh);
    r_attr->va_ino      = id | 1;
    r_attr->va_set_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_INUM;
} /* chimera_smb_set_child_fh */

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

/* ---- real POSIX attrs (modefromsid security descriptor + FileAllInfo) ---- */

/* Match S-1-5-88-<kind>-<value> at `buf` and extract <value>.  Mirrors the
 * server's parse_unix_sid (smb_proc_security.c): revision 1, 3 sub-authorities,
 * NT authority 5, first sub-authority 88, second == kind, third == value. */
static int
smb_parse_unix_sid(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t       kind,
    uint32_t      *value)
{
    if (len < 20 || buf[0] != 1 || buf[1] != 3 || buf[7] != 5) {
        return -1;
    }
    if (smb_wire_le32(buf + 8) != 88 || smb_wire_le32(buf + 12) != kind) {
        return -1;
    }
    *value = smb_wire_le32(buf + 16);
    return 0;
} /* smb_parse_unix_sid */

/* Parse the "modefromsid" NT security descriptor the server emits (owner SID ->
 * uid, group SID -> gid, a DACL ACE's S-1-5-88-3 SID -> mode bits).  Sets the
 * va_* fields + mask bits it can find.  Mirror of the server's write side in
 * smb_proc_security.c. */
static void
smb_sd_to_attrs(
    const uint8_t            *sd,
    uint32_t                  len,
    struct chimera_vfs_attrs *attr,
    uint32_t                 *r_perms,
    int                      *r_have_mode)
{
    uint32_t off_owner, off_group, off_dacl, value;

    *r_have_mode = 0;
    if (len < 20) {
        return;
    }

    off_owner = smb_wire_le32(sd + 4);
    off_group = smb_wire_le32(sd + 8);
    off_dacl  = smb_wire_le32(sd + 16);

    if (off_owner && off_owner + 20 <= len &&
        smb_parse_unix_sid(sd + off_owner, len - off_owner, 1, &value) == 0) {
        attr->va_uid       = value;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
    }
    if (off_group && off_group + 20 <= len &&
        smb_parse_unix_sid(sd + off_group, len - off_group, 2, &value) == 0) {
        attr->va_gid       = value;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
    }
    if (off_dacl && off_dacl + 8 <= len) {
        const uint8_t *acl       = sd + off_dacl;
        uint16_t       acl_size  = smb_wire_le16(acl + 2);
        uint16_t       ace_count = smb_wire_le16(acl + 4);
        uint32_t       pos       = 8;
        uint16_t       i;

        for (i = 0; i < ace_count && pos + 8 <= acl_size &&
             off_dacl + pos + 8 <= len; i++) {
            uint16_t ace_size   = smb_wire_le16(acl + pos + 2);
            uint32_t sid_offset = pos + 8;   /* ACE hdr(4) + access mask(4) */

            if (off_dacl + sid_offset + 20 <= len &&
                smb_parse_unix_sid(acl + sid_offset, len - off_dacl - sid_offset,
                                   3, &value) == 0) {
                *r_perms     = value;   /* full mode: type bits included */
                *r_have_mode = 1;
                break;
            }
            if (ace_size == 0) {
                break;
            }
            pos += ace_size;
        }
    }
} /* smb_sd_to_attrs */

/* Parse the fixed head of a FileAllInformation buffer (MS-FSCC 2.4.7) into
 * attr: times, size, link count, and the POSIX file TYPE derived from the
 * Windows attribute word (directory / reparse -> symlink / regular).  The
 * permission bits are a type-appropriate default until the security leg
 * overwrites them, so a server that cannot answer the SECURITY query still
 * yields the same attrs the synthesized path always did. */
static void
smb_all_info_to_attrs(
    const uint8_t            *b,
    int                       len,
    struct chimera_vfs_attrs *attr)
{
    uint32_t file_attributes;
    uint32_t mode_type, def_perm;

    if (len < 72) {
        return;
    }

    smb_filetime_to_timespec(smb_wire_le64(b + 0), &attr->va_btime);
    smb_filetime_to_timespec(smb_wire_le64(b + 8), &attr->va_atime);
    smb_filetime_to_timespec(smb_wire_le64(b + 16), &attr->va_mtime);
    smb_filetime_to_timespec(smb_wire_le64(b + 24), &attr->va_ctime);

    file_attributes     = smb_wire_le32(b + 32);
    attr->va_space_used = smb_wire_le64(b + 40);
    attr->va_size       = smb_wire_le64(b + 48);
    attr->va_nlink      = smb_wire_le32(b + 56);
    /* Report a stable POSIX identity.  The inode number is the server's
     * IndexNumber (b+64) -- one value per object, shared by every hard link and
     * preserved across renames, unlike the path-derived id used for the FH
     * (which changes with the name).  st_dev is a single per-mount constant so
     * every object on this filesystem shares it, as POSIX requires.  Together
     * they give a (dev,ino) pair that is consistent across ops. */
    if (len >= 72) {
        uint64_t index_number = smb_wire_le64(b + 64);
        if (index_number) {
            attr->va_ino       = index_number;
            attr->va_dev       = CHIMERA_SMB_ST_DEV;
            attr->va_set_mask |= CHIMERA_VFS_ATTR_INUM | CHIMERA_VFS_ATTR_DEV;
        }
    }

    if (file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) {
        mode_type     = S_IFDIR;
        def_perm      = 0755;
        attr->va_size = 0;          /* a directory carries no data stream */
    } else if (file_attributes & SMB2_FILE_ATTRIBUTE_REPARSE_POINT) {
        mode_type = S_IFLNK;
        def_perm  = 0777;
    } else {
        mode_type = S_IFREG;
        def_perm  = 0644;
    }
    attr->va_mode = mode_type | def_perm;

    attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_NLINK |
        CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_SPACE_USED |
        CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME |
        CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_BTIME;
} /* smb_all_info_to_attrs */

/* Skip a QUERY_INFO reply's fixed header to the start of its output buffer and
 * return the buffer length (0 on a malformed reply). */
static uint32_t
smb_query_info_buffer(struct evpl_iovec_cursor *body)
{
    uint16_t structsize, out_offset;
    uint32_t out_length;
    int      consumed;

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &out_offset);
    evpl_iovec_cursor_get_uint32(body, &out_length);
    (void) structsize;

    consumed = evpl_iovec_cursor_consumed(body);
    if ((int) out_offset > consumed) {
        evpl_iovec_cursor_skip(body, (int) out_offset - consumed);
    }
    return out_length;
} /* smb_query_info_buffer */

/* ---- attr-enrich chain: FileAllInformation then SECURITY on a FileId ----- */

/* Second leg: the modefromsid security descriptor -> real uid/gid and the
 * permission bits, merged onto the type FileAllInformation already set.  Then
 * the op's terminal action runs (complete, or close+complete). */
static void
smb_attr_enrich_security_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    uint8_t                      sd[512];
    uint32_t                     sd_len, perms = 0;
    int                          have_mode = 0;

    (void) hdr;
    (void) body_len;

    /* A security query the server cannot answer leaves the type/nlink/times the
     * first leg gathered; report those rather than fail the whole op. */
    if (status == SMB2_STATUS_SUCCESS) {
        sd_len = smb_query_info_buffer(body);
        if (sd_len > sizeof(sd)) {
            sd_len = sizeof(sd);
        }
        if (sd_len && evpl_iovec_cursor_get_blob(body, sd, sd_len) >= 0) {
            smb_sd_to_attrs(sd, sd_len, state->enrich_attr, &perms, &have_mode);
            if (have_mode) {
                /* modefromsid carries the POSIX type: a device/FIFO/socket node
                 * is a reparse point on the wire (FileAttributes reads back as a
                 * symlink) but a real special file here, so its S-1-5-88-3 type
                 * bits win over the FileAttributes-derived type.  Regular files,
                 * dirs and symlinks agree on both, so this is a no-op for them. */
                if (perms & S_IFMT) {
                    state->enrich_attr->va_mode = perms;
                } else {
                    state->enrich_attr->va_mode =
                        (state->enrich_attr->va_mode & S_IFMT) | (perms & 07777);
                }
            }
        }
    }

    state->enrich_done(conn, request);
} /* smb_attr_enrich_security_reply */

/* Fire the QUERY_INFO SECURITY (owner|group|dacl) leg on the enrich FileId. */
static void
smb_attr_enrich_send_security(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_op_state *state = request->plugin_data;
    struct evpl_iovec            iov;
    struct evpl_iovec_cursor     cursor;
    struct smb2_header          *hdr;

    chimera_smb_client_pdu_begin(conn, SMB2_QUERY_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_QUERY_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_SECURITY);
    evpl_iovec_cursor_append_uint8(&cursor, 0);             /* FileInfoClass */
    evpl_iovec_cursor_append_uint32(&cursor, 512);          /* OutputBufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, 0);            /* InputBufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);            /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);            /* InputBufferLength */
    evpl_iovec_cursor_append_uint32(&cursor, 0x7);          /* OWNER|GROUP|DACL */
    evpl_iovec_cursor_append_uint32(&cursor, 0);            /* Flags */
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  smb_attr_enrich_security_reply, request);
} /* smb_attr_enrich_send_security */

/* First leg: FileAllInformation -> type, link count, size, times.  Chains the
 * security leg for owner/group/mode. */
static void
smb_attr_enrich_allinfo_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    uint8_t                      buf[512];
    uint32_t                     out_length;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    out_length = smb_query_info_buffer(body);
    if (out_length > sizeof(buf)) {
        out_length = sizeof(buf);
    }
    if (out_length && evpl_iovec_cursor_get_blob(body, buf, out_length) >= 0) {
        smb_all_info_to_attrs(buf, (int) out_length, state->enrich_attr);
    }

    smb_attr_enrich_send_security(conn, request);
} /* smb_attr_enrich_allinfo_reply */

/* Start the AllInfo -> SECURITY enrich chain on `fid`, merging the real attrs
 * into `attr`, then running `done`.  Caller has already set any synthesized
 * fallback (fh, ino) on `attr`. */
static void
smb_attr_enrich_begin(
    struct chimera_smb_client_conn          *conn,
    struct chimera_vfs_request              *request,
    const struct chimera_smb_client_file_id *fid,
    struct chimera_vfs_attrs                *attr,
    void (                                  *done )(
        struct chimera_smb_client_conn *,
        struct chimera_vfs_request *))
{
    struct chimera_smb_op_state *state = request->plugin_data;
    struct evpl_iovec            iov;
    struct evpl_iovec_cursor     cursor;
    struct smb2_header          *hdr;

    state->file_id     = *fid;
    state->enrich_attr = attr;
    state->enrich_done = done;

    chimera_smb_client_pdu_begin(conn, SMB2_QUERY_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_QUERY_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_ALL_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, 512);          /* OutputBufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, 0);            /* InputBufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);            /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);            /* InputBufferLength */
    evpl_iovec_cursor_append_uint32(&cursor, 0);            /* AdditionalInformation */
    evpl_iovec_cursor_append_uint32(&cursor, 0);            /* Flags */
    evpl_iovec_cursor_append_uint64(&cursor, fid->pid);
    evpl_iovec_cursor_append_uint64(&cursor, fid->vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  smb_attr_enrich_allinfo_reply, request);
} /* smb_attr_enrich_begin */

/* ---- writing real POSIX attrs (modefromsid security descriptor) --------- */

/* Write an S-1-5-88-<kind>-<value> SID (20 bytes).  Mirror of the server's
 * write_unix_sid (smb_proc_security.c). */
static void
smb_write_unix_sid(
    uint8_t *buf,
    uint32_t kind,
    uint32_t value)
{
    buf[0] = 1;                                   /* revision            */
    buf[1] = 3;                                   /* sub-authority count */
    buf[2] = 0; buf[3] = 0; buf[4] = 0;
    buf[5] = 0; buf[6] = 0; buf[7] = 5;           /* NT authority        */
    smb_wire_set_le32(buf + 8, 88);
    smb_wire_set_le32(buf + 12, kind);
    smb_wire_set_le32(buf + 16, value);
} /* smb_write_unix_sid */

/* Build a self-relative "modefromsid" NT security descriptor carrying whichever
 * of owner / group / mode is set in `set_attr`, and the AdditionalInformation
 * flags that tell the server which to apply.  Returns the descriptor length.
 * `out` must have room for 20 + 3*20 + 36 = 116 bytes. */
static uint32_t
smb_build_modefromsid_sd(
    const struct chimera_vfs_attrs *set_attr,
    uint8_t                        *out,
    uint32_t                       *r_addl_info)
{
    int      want_uid  = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_UID) != 0;
    int      want_gid  = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_GID) != 0;
    int      want_mode = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) != 0;
    uint32_t off       = 20;                        /* after the 20-byte header */
    uint32_t off_owner = 0, off_group = 0, off_dacl = 0;
    uint16_t control = 0x8000;                      /* SE_SELF_RELATIVE */
    uint32_t addl    = 0;

    if (want_uid) {
        off_owner = off;
        smb_write_unix_sid(out + off, 1, (uint32_t) set_attr->va_uid);
        off  += 20;
        addl |= 0x1;                                /* OWNER_SECURITY_INFORMATION */
    }
    if (want_gid) {
        off_group = off;
        smb_write_unix_sid(out + off, 2, (uint32_t) set_attr->va_gid);
        off  += 20;
        addl |= 0x2;                                /* GROUP_SECURITY_INFORMATION */
    }
    if (want_mode) {
        off_dacl = off;
        control |= 0x0004;                          /* SE_DACL_PRESENT */
        /* ACL header (8) + one ALLOW ACE (4 hdr + 4 mask + 20 SID = 28). */
        out[off]     = 2;                           /* AclRevision */
        out[off + 1] = 0;
        smb_wire_set_le16(out + off + 2, 36);       /* AclSize = 8 + 28 */
        smb_wire_set_le16(out + off + 4, 1);        /* AceCount */
        smb_wire_set_le16(out + off + 6, 0);        /* Sbz2 */
        out[off + 8] = 0;                           /* AceType = ACCESS_ALLOWED */
        out[off + 9] = 0;                           /* AceFlags */
        smb_wire_set_le16(out + off + 10, 28);       /* AceSize */
        smb_wire_set_le32(out + off + 12, 0);       /* AccessMask (unused) */
        smb_write_unix_sid(out + off + 16, 3,
                           (uint32_t) (set_attr->va_mode & 07777));
        off  += 8 + 28;
        addl |= 0x4;                                /* DACL_SECURITY_INFORMATION */
    }

    /* Security-descriptor header (MS-DTYP 2.4.6, self-relative). */
    out[0] = 1;                                     /* Revision */
    out[1] = 0;                                     /* Sbz1     */
    smb_wire_set_le16(out + 2, control);
    smb_wire_set_le32(out + 4, off_owner);
    smb_wire_set_le32(out + 8, off_group);
    smb_wire_set_le32(out + 12, 0);                 /* OffsetSacl */
    smb_wire_set_le32(out + 16, off_dacl);

    *r_addl_info = addl;
    return off;
} /* smb_build_modefromsid_sd */

/* True when `set_attr` carries any POSIX owner/group/mode the server can apply
 * through a modefromsid security descriptor. */
static inline int
smb_set_attr_has_posix_perm(const struct chimera_vfs_attrs *set_attr)
{
    return set_attr && (set_attr->va_set_mask &
                        (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID |
                         CHIMERA_VFS_ATTR_MODE)) != 0;
} /* smb_set_attr_has_posix_perm */

/* The POSIX owner/group/mode a freshly created object must carry.  The mount
 * session is a single identity (root), so the server would otherwise leave the
 * object owned by root; POSIX makes a new object owned by the creator's
 * effective uid/gid, so stamp that (plus the requested mode) -- what the model
 * created it as.  Returns 1 if there is anything to stamp. */
int
smb_build_create_owner_attrs(
    const struct chimera_vfs_request *request,
    const struct chimera_vfs_attrs   *set_attr,
    struct chimera_vfs_attrs         *out)
{
    memset(out, 0, sizeof(*out));

    if (request->cred) {
        out->va_uid       = request->cred->uid;
        out->va_gid       = request->cred->gid;
        out->va_set_mask |= CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    }
    if (set_attr && (set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        out->va_mode      = set_attr->va_mode;
        out->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    return out->va_set_mask != 0;
} /* smb_build_create_owner_attrs */

/* Send an SMB2 SET_INFO SECURITY on `fid` carrying the modefromsid descriptor
 * built from `set_attr`, then invoke `reply_cb`.  The caller must have opened
 * `fid` with WRITE_OWNER (for owner/group) and/or WRITE_DAC (for mode). */
void
smb_send_set_security(
    struct chimera_smb_client_conn          *conn,
    struct chimera_vfs_request              *request,
    const struct chimera_smb_client_file_id *fid,
    const struct chimera_vfs_attrs          *set_attr,
    chimera_smb_client_reply_cb              reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  sd[128];
    uint32_t                 sd_len, addl = 0;

    sd_len = smb_build_modefromsid_sd(set_attr, sd, &addl);

    chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_SECURITY);
    evpl_iovec_cursor_append_uint8(&cursor, 0);             /* FileInfoClass */
    evpl_iovec_cursor_append_uint32(&cursor, sd_len);       /* BufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 32); /* BufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);            /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, addl);         /* AdditionalInformation */
    evpl_iovec_cursor_append_uint64(&cursor, fid->pid);
    evpl_iovec_cursor_append_uint64(&cursor, fid->vid);
    evpl_iovec_cursor_append_blob(&cursor, sd, sd_len);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request, reply_cb,
                                  request);
} /* smb_send_set_security */

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
    int                      *out_relative,
    int                      *out_unparsed)
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
        reparse_tag != SMB2_IO_REPARSE_TAG_SYMLINK) {
        /* Not a link error we can resolve; let the caller see the raw ELOOP. */
        return -1;
    }

    /* UnparsedPathLength is the byte count (UTF-16) of the path suffix AFTER
     * the symlink the server stopped on -- non-zero for an intermediate (mid-
     * path) symlink, which must be followed and the suffix preserved, not
     * surfaced as ELOOP. */
    *out_unparsed = unparsed_len / 2;

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
    int         relative,
    int         unparsed)
{
    int  base, parsed_len, suffix_len, newlen;
    char scratch[CHIMERA_SMB_PATH_MAX + 1];

    /* The server stopped on a symlink after resolving path[0..parsed_len); the
     * remaining path[parsed_len..path_len) is the unparsed suffix (the tail
     * after an intermediate symlink, or a trailing '/' after a final one).  The
     * symlink is the last component of the parsed portion.  Splice the target
     * in place of THAT component and keep the suffix, so "x/a/z" (a -> "c")
     * resolves to "x/c/z" and "x/b/" (b -> "c") to "x/c/" -- not "x/a/z"'s "z"
     * component or "x/b/"'s empty one, which would re-hit the same link every
     * hop and loop to ELOOP where the answer is the dangling target's ENOENT. */
    if (unparsed < 0 || unparsed > path_len) {
        unparsed = 0;
    }
    parsed_len = path_len - unparsed;
    suffix_len = unparsed;

    if (!relative) {
        /* Absolute target: resolve from the share root (drop the whole prefix
         * up to and including the symlink), keeping the suffix. */
        while (tlen > 0 && target[0] == '/') {
            target++;
            tlen--;
        }
        base = 0;
    } else {
        /* Relative target: replace the symlink component in place. */
        base = parsed_len;
        while (base > 0 && path[base - 1] != '/') {
            base--;
        }
    }

    newlen = base + tlen + suffix_len;
    if (newlen > CHIMERA_SMB_PATH_MAX) {
        return -1;
    }

    /* new = path[0..base) + target + path[parsed_len..path_len) (the suffix).
     * Assemble in scratch: the suffix overlaps the destination, so an in-place
     * memmove would be error-prone. */
    memcpy(scratch, path, base);
    memcpy(scratch + base, target, tlen);
    memcpy(scratch + base + tlen, path + parsed_len, suffix_len);
    memcpy(path, scratch, newlen);
    return newlen;
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
        int  unparsed = 0;
        int  tlen     = smb_parse_symlink_error(body, target, sizeof(target),
                                                &relative, &unparsed);
        int  newlen;

        /* A FINAL-component symlink (unparsed==0) with an ABSOLUTE target cannot
         * be resolved here: the stored target is a client-namespace path (the
         * mount prefix baked in, e.g. "/test/b"), and this backend resolves only
         * within the share (root == the mount point), so splicing it would climb
         * to "/test/test/b" -> ENOENT.  Hand the link node back to the core
         * instead -- re-open the reparse point itself (metadata only; op_complete
         * reads the link and re-resolves the absolute target from the vfs-
         * namespace root, crossing the mount correctly).  A relative or mid-path
         * (unparsed>0) target stays share-local and is spliced below. */
        if (tlen > 0 && !relative && unparsed == 0) {
            smb_send_create_ex(conn, request, fc->path, fc->path_len,
                               SMB2_FILE_READ_ATTRIBUTES | SMB2_DELETE |
                               SMB2_READ_CONTROL | SMB2_WRITE_DACL | SMB2_WRITE_OWNER,
                               fc->share_access, fc->disposition,
                               fc->options | SMB2_FILE_OPEN_REPARSE_POINT,
                               fc->cctx_len ? fc->cctx : NULL,
                               (uint32_t) fc->cctx_len,
                               fc->real_cb, request);
            free(fc);
            return;
        }

        if (tlen > 0 &&
            (newlen = smb_splice_symlink_target(fc->path, fc->path_len, target,
                                                tlen, relative, unparsed)) >= 0) {
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

/* getattr terminal: the enrich chain has merged the real attrs; report. */
static void
chimera_smb_getattr_enrich_done(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_getattr_enrich_done */

void
chimera_smb_client_getattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->getattr.handle);

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

    /* Synthesized fallbacks (kept if the security leg cannot answer): a stable
     * per-handle inode id, and the calling credential as owner. */
    request->getattr.r_attr.va_ino       = open_state->file_id.pid | 1;
    request->getattr.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_INUM;
    if (request->cred) {
        request->getattr.r_attr.va_uid       = request->cred->uid;
        request->getattr.r_attr.va_gid       = request->cred->gid;
        request->getattr.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_UID |
            CHIMERA_VFS_ATTR_GID;
    }

    /* FileAllInformation (type, link count, size, times) then the modefromsid
     * security descriptor (owner/group/mode) -- the real POSIX attrs. */
    smb_attr_enrich_begin(conn, request, &open_state->file_id,
                          &request->getattr.r_attr,
                          chimera_smb_getattr_enrich_done);
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

/* lookup terminal: close the transient handle (its reply completes the op). */
static void
chimera_smb_lookup_enrich_done(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_op_state *state = request->plugin_data;

    smb_send_close(conn, request, &state->file_id,
                   chimera_smb_lookup_close_reply);
} /* chimera_smb_lookup_enrich_done */

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
        /* Set the fh's nofollow bit only for a reparse point (a symlink or a
         * special file): its fh must re-open the node itself, not follow it.  A
         * directory or a regular file is never followed, so its fh must NOT
         * carry the bit -- open_fh would otherwise re-CREATE it with
         * OPEN_REPARSE_POINT and a directory then reads back as a non-directory,
         * breaking a create dispatched against it as a resolved parent. */
        int nofollow =
            (r.info.file_attributes & SMB2_FILE_ATTRIBUTE_REPARSE_POINT) ? 1 : 0;

        path_id                             = chimera_smb_path_intern(conn->server, fullpath, fullpath_len);
        request->lookup_at.r_attr.va_fh_len =
            chimera_smb_encode_open_fh(request->fh, path_id, nofollow,
                                       request->lookup_at.r_attr.va_fh);
        request->lookup_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
    }

    /* Synthesized fallback (fh + ino + calling-cred owner); the enrich chain
     * then overwrites type/nlink/size/times and owner/mode with the real attrs
     * before the transient handle is closed. */
    smb_apply_attrs(request, &request->lookup_at.r_attr, &r.info, path_id);

    request->status = CHIMERA_VFS_OK;
    (void) state;

    smb_attr_enrich_begin(conn, request, &r.file_id, &request->lookup_at.r_attr,
                          chimera_smb_lookup_enrich_done);
} /* chimera_smb_lookup_create_reply */

void
chimera_smb_client_lookup_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    char path[CHIMERA_SMB_PATH_MAX + 1];
    int  path_len;

    /* Resolving a name under a non-directory parent handle is ENOTDIR (POSIX),
     * even when that parent's name was unlinked while it stayed open: the open
     * token keeps the inode's type, whereas re-resolving the parent's now-stale
     * path would answer ENOENT.  A path-only backend cannot infer this from the
     * wire (the server reports OBJECT_PATH_NOT_FOUND -> ENOENT for a file in the
     * path prefix), so the open state's recorded type decides it here. */
    if (smb_parent_is_nondir(request->lookup_at.handle)) {
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    path_len = smb_at_full_path(conn, request, request->lookup_at.component,
                                request->lookup_at.component_len,
                                path, sizeof(path));
    if (path_len < 0) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* Resolve to the raw entry (like an NFS LOOKUP): open the reparse point so a
     * FINAL-component symlink comes back as the link node itself rather than
     * STOPPED_ON_SYMLINK, and the VFS core decides whether to follow it.  But an
     * INTERMEDIATE symlink (the whole mount-relative path is handed to a path-
     * only backend at once, so "a/b" where "a" is a link stops on "a") must be
     * followed here -- POSIX always follows mid-path symlinks -- rather than
     * surfaced as ELOOP.  The follow shim does exactly that: REPARSE_POINT means
     * a final link never trips it, while a mid-path stop (UnparsedPathLength>0)
     * is spliced and retried. */
    smb_send_create_follow(conn, request, path, path_len,
                           SMB2_FILE_READ_ATTRIBUTES,
                           SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                           SMB2_FILE_OPEN, SMB2_FILE_OPEN_REPARSE_POINT,
                           NULL, 0,
                           chimera_smb_lookup_create_reply);
} /* chimera_smb_client_lookup_at */

/* ---- open_at / open_fh (persistent open) ------------------------------- */

/* open_at terminal after a create's owner/mode stamp: the object now carries
 * the real POSIX attrs; report the open regardless of the stamp's status. */
static void
chimera_smb_open_at_secured_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) status;
    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_open_at_secured_reply */

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
        request->fh, path_id,
        (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) ? 1 : 0,
        request->open_at.r_attr.va_fh);
    request->open_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;

    request->open_at.r_vfs_private = (uint64_t) (uintptr_t) open_state;
    request->open_at.r_created     = (r.create_action == SMB2_CREATE_ACTION_CREATED);

    /* A fresh create is born owned by the mount session (one identity for the
     * whole mount); stamp the caller's real POSIX owner/mode onto it with a
     * modefromsid SET_SECURITY, exactly as cifs.ko does after a create, so the
     * object matches what the model created under that uid.  The open requested
     * WRITE_OWNER|WRITE_DAC for this.  A failed stamp still yields a usable
     * handle -- complete OK rather than lose the create. */
    if (request->open_at.r_created) {
        struct chimera_vfs_attrs owner;

        if (smb_build_create_owner_attrs(request, request->open_at.set_attr,
                                         &owner)) {
            smb_send_set_security(conn, request, &open_state->file_id, &owner,
                                  chimera_smb_open_at_secured_reply);
            return;
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_open_at_reply */

void
chimera_smb_client_open_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    uint32_t disposition;
    uint32_t desired_access;
    /* A directory open must request FILE_DIRECTORY_FILE; only a non-directory
     * open may set FILE_NON_DIRECTORY_FILE.  The server now enforces the option
     * against the target type (FILE_IS_A_DIRECTORY otherwise), so a directory
     * open that left NON_DIRECTORY_FILE set would be refused. */
    /* Constrain the target type only when the caller committed to one: an
     * explicit directory open is DIRECTORY_FILE; a write-intent file open is
     * NON_DIRECTORY_FILE (opening a directory for writing is EISDIR).  A PATH /
     * INFERRED open (chmod/chown/stat by path, which do not know the type), or a
     * read-only open, constrains neither: POSIX open(dir, O_RDONLY) succeeds and
     * hands back a directory fd, so only a write-intent open turns a directory
     * into EISDIR. */
    uint32_t       options =
        (request->open_at.flags & CHIMERA_VFS_OPEN_DIRECTORY) ? SMB2_FILE_DIRECTORY_FILE :
        (request->open_at.flags & (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                                   CHIMERA_VFS_OPEN_READ_ONLY)) ? 0 :
        SMB2_FILE_NON_DIRECTORY_FILE;
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

    /* POSIX O_CREAT|O_EXCL over an existing symlink is EEXIST regardless of the
     * link's target: the exclusive create must see the link node itself, never
     * follow it (which would create the target, or stop on the link as ELOOP).
     * OPEN_REPARSE_POINT makes an existing symlink collide
     * (OBJECT_NAME_COLLISION -> EEXIST); it is inert for a fresh name or a
     * regular-file collision. */
    if (disposition == SMB2_FILE_CREATE) {
        options |= SMB2_FILE_OPEN_REPARSE_POINT;
    }

    /* WRITE_OWNER|WRITE_DAC|READ_CONTROL: the rights the server enforces for a
     * modefromsid SET_SECURITY, so this handle can stamp the caller's POSIX
     * owner/mode -- at create time here, or later via fchmod/chown/setattr on
     * the same handle.  The mount session is root, which the server grants. */
    desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
        SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE |
        SMB2_READ_CONTROL | SMB2_WRITE_DACL | SMB2_WRITE_OWNER;

    /* Opening the link node itself for METADATA (lstat/readlink/lchown, all
     * O_PATH-style) must not ask for data access -- a symlink reparse point has
     * no data stream and the server refuses READ/WRITE_DATA on it.  Gate this on
     * OPEN_PATH: a data-intent O_NOFOLLOW open (open(O_NOFOLLOW|O_RDWR) on a
     * regular file) is NOT metadata-only -- it needs its read/write grant, and
     * O_NOFOLLOW only means "fail if the leaf is a symlink" (which the reparse
     * open then surfaces as the server refusing data on a link node). */
    if ((request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
        (request->open_at.flags & CHIMERA_VFS_OPEN_PATH)) {
        desired_access = SMB2_FILE_READ_ATTRIBUTES | SMB2_DELETE |
            SMB2_READ_CONTROL | SMB2_WRITE_DACL | SMB2_WRITE_OWNER;
    }

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

    options =
        (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY) ? SMB2_FILE_DIRECTORY_FILE :
        (request->open_fh.flags & (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED)) ? 0 :
        SMB2_FILE_NON_DIRECTORY_FILE;

    /* A nofollow fh names a symlink itself (from a lookup or a NOFOLLOW open);
     * re-CREATE its path as the reparse point so the open lands on the link,
     * not whatever it points at -- how the core reads a link's target and how a
     * self-referential link is caught as ELOOP rather than silently followed. */
    if (!chimera_smb_fh_is_root(request->fh_len) &&
        (chimera_smb_fh_is_nofollow(request->fh) ||
         (request->open_fh.flags & CHIMERA_VFS_OPEN_NOFOLLOW))) {
        options |= SMB2_FILE_OPEN_REPARSE_POINT;
    }

    if (chimera_smb_fh_is_root(request->fh_len)) {
        /* The share root is the empty path.  It also needs the SD rights so a
         * chmod/chown of the export root (the model's fsInit normalization)
         * lands rather than failing ACCESS_DENIED. */
        path     = "";
        path_len = 0;
        /* WRITE_DATA (ADD_FILE on a directory) so a POSIX fsync of a directory
         * handle -- which the SMB server, like Samba, refuses on a handle
         * without write access -- is permitted; the mount already adds children
         * at the root, so the right is not new. */
        desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
            SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_LIST_DIRECTORY |
            SMB2_READ_CONTROL | SMB2_WRITE_DACL | SMB2_WRITE_OWNER;
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
         * writes and metadata ops alike, including the WRITE_OWNER|WRITE_DAC a
         * later chmod/chown needs (the mount authenticates as one identity, so
         * the server admits whatever that identity may do). */
        desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
            SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE |
            SMB2_READ_CONTROL | SMB2_WRITE_DACL | SMB2_WRITE_OWNER;
    }

    /* An explicit NOFOLLOW open targets a symlink node to read its target (or
     * lstat it): request metadata-only access so the server satisfies it with an
     * O_PATH-style handle.  A symlink has no data stream, so asking for
     * READ/WRITE_DATA would make the backend follow the link (ELOOP) instead of
     * opening the node.  Gate this on the explicit flag, not on the fh's
     * nofollow bit: every looked-up fh carries that bit, and a plain metadata
     * op (chmod/chown) on a regular file's lookup fh still needs its full
     * WRITE_DACL|WRITE_OWNER grant. */
    if (request->open_fh.flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
        desired_access = SMB2_FILE_READ_ATTRIBUTES | SMB2_READ_CONTROL;
    }

    /* Creating a symlink or device node opens its parent directory O_PATH (as a
     * dirfd for the *_at op); that directory handle is only a dirfd -- it never
     * deletes or renames anything (remove_at/rename_at/rmdir each re-open their
     * target with their own DELETE-access CREATE).  Its broad grant's DELETE bit
     * is therefore unused, and worse: the cached parent handle's lingering
     * DELETE reservation is read by a later rename INTO the directory as a
     * conflicting deleter, failing it SHARING_VIOLATION (EAGAIN).  Drop DELETE
     * for O_PATH directory handles; keep it for file handles and keep
     * WRITE_OWNER|WRITE_DACL throughout so chmod/chown via the handle lands. */
    if ((request->open_fh.flags & CHIMERA_VFS_OPEN_PATH) &&
        (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY)) {
        desired_access &= ~(uint32_t) SMB2_DELETE;
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

/* After the new directory's owner/mode stamp: close the transient handle. */
static void
chimera_smb_mkdir_secured_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;

    (void) status;
    (void) hdr;
    (void) body;
    (void) body_len;

    smb_send_close(conn, request, &state->file_id, chimera_smb_mkdir_close_reply);
} /* chimera_smb_mkdir_secured_reply */

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

    /* Stamp the caller's real POSIX owner/mode onto the new directory before
     * closing (the create opened with WRITE_OWNER|WRITE_DAC). */
    {
        struct chimera_vfs_attrs owner;

        if (smb_build_create_owner_attrs(request, request->mkdir_at.set_attr,
                                         &owner)) {
            smb_send_set_security(conn, request, &state->file_id, &owner,
                                  chimera_smb_mkdir_secured_reply);
            return;
        }
    }

    smb_send_close(conn, request, &state->file_id, chimera_smb_mkdir_close_reply);
} /* chimera_smb_mkdir_create_reply */

void
chimera_smb_client_mkdir_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    char fullpath[CHIMERA_SMB_PATH_MAX + 1];
    int  fullpath_len;

    /* A dirfd-relative create whose parent is a non-directory is ENOTDIR, even
     * when the parent's name was unlinked (the open fd keeps the inode alive):
     * the parent's interned path is then stale, so path resolution would give a
     * misleading ENOENT.  The open state records the type directly. */
    if (smb_parent_is_nondir(request->mkdir_at.handle)) {
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    /* Give the new directory a re-openable child fh for the VFS name cache. */
    chimera_smb_set_child_fh(conn, request, request->mkdir_at.name,
                             request->mkdir_at.name_len, 0,
                             &request->mkdir_at.r_attr);

    /* Resolve the leaf against the parent handle (a dirfd-relative create);
     * for a root-relative create this is just the name. */
    fullpath_len = smb_at_full_path(conn, request, request->mkdir_at.name,
                                    request->mkdir_at.name_len, fullpath,
                                    sizeof(fullpath));
    if (fullpath_len < 0) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* OPEN_REPARSE_POINT: an mkdir whose name is an existing symlink is EEXIST
     * (POSIX), not a follow into the target -- the create must collide with the
     * link node rather than stop on it (ELOOP).  Inert for a fresh name. */
    smb_send_create(conn, request,
                    fullpath, fullpath_len,
                    SMB2_FILE_READ_ATTRIBUTES | SMB2_READ_CONTROL |
                    SMB2_WRITE_DACL | SMB2_WRITE_OWNER,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_CREATE,
                    SMB2_FILE_DIRECTORY_FILE | SMB2_FILE_OPEN_REPARSE_POINT,
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
    /* A dirfd-relative unlink/rmdir whose parent is a non-directory is ENOTDIR
    * (even when the parent's name was unlinked while its fd stayed open -- its
    * interned path is then stale and would resolve to a misleading ENOENT). */
    if (smb_parent_is_nondir(request->remove_at.handle)) {
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    /* POSIX unlink/rmdir removes the named entry itself; a final-component
     * symlink must be deleted as the link, never followed (which would ELOOP or
     * hit the target).  FILE_OPEN_REPARSE_POINT opens the link node so
     * DELETE_ON_CLOSE removes it. */
    smb_send_create_follow(conn, request,
                           request->remove_at.name, request->remove_at.namelen,
                           SMB2_DELETE | SMB2_FILE_READ_ATTRIBUTES,
                           SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                           SMB2_FILE_OPEN,
                           SMB2_FILE_DELETE_ON_CLOSE | SMB2_FILE_OPEN_REPARSE_POINT,
                           NULL, 0,
                           chimera_smb_remove_create_reply);
} /* chimera_smb_client_remove_at */

/* ---- setattr (SET_INFO on the open handle) ----------------------------- */

#define CHIMERA_SMB_SETATTR_TIMES \
        (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME | \
         CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_BTIME)

/* Terminal SET_INFO reply for the modefromsid security leg (owner/group/mode). */
static void
chimera_smb_setattr_security_reply(
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
} /* chimera_smb_setattr_security_reply */

/* After the size/times legs: apply POSIX owner/group/mode with a modefromsid
 * SET_SECURITY if any was requested, else finish.  The handle carries
 * WRITE_OWNER|WRITE_DAC (every regular open requests them). */
static void
chimera_smb_setattr_data_done(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->setattr.handle);

    if (open_state && smb_set_attr_has_posix_perm(request->setattr.set_attr)) {
        smb_send_set_security(conn, request, &open_state->file_id,
                              request->setattr.set_attr,
                              chimera_smb_setattr_security_reply);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_setattr_data_done */

/* Reply for the times (FileBasicInformation) leg: chain the security leg. */
static void
chimera_smb_setattr_basic_reply(
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

    chimera_smb_setattr_data_done(conn, request);
} /* chimera_smb_setattr_basic_reply */

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
                                  chimera_smb_setattr_basic_reply, request);
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

    chimera_smb_setattr_data_done(conn, request);
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

    /* SMB sets size (FileEndOfFileInformation), timestamps (FileBasic-
     * Information), and POSIX owner/group/mode (a modefromsid SET_SECURITY --
     * chained last, after any size/times leg). */
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

    /* Owner/group/mode only (or nothing): straight to the security leg. */
    chimera_smb_setattr_data_done(conn, request);
} /* chimera_smb_client_setattr */

/* ---- allocate (fallocate) --------------------------------------------- */

/* Completion for the EndOfFile grow (ALLOCATE) or the SET_ZERO_DATA punch
 * (DEALLOCATE): map status and finish. */
static void
chimera_smb_allocate_reply(
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

    request->status = (status == SMB2_STATUS_SUCCESS)
                      ? CHIMERA_VFS_OK
                      : chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_allocate_reply */

/* The file's current size is now in r_post_attr (queried via the enrich chain).
 * fallocate(mode 0) grows the file to offset+length when that exceeds the
 * current EOF and is otherwise a no-op (a non-sparse backend has no holes to
 * fill); it never shrinks. */
static void
chimera_smb_allocate_query_done(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_op_state *state  = request->plugin_data;
    uint64_t                     target = request->allocate.offset +
        request->allocate.length;
    struct evpl_iovec            iov;
    struct evpl_iovec_cursor     cursor;
    struct smb2_header          *hdr;

    if (target <= request->allocate.r_post_attr.va_size) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Reflect the grown size in the post-attrs the enrich query populated. */
    request->allocate.r_post_attr.va_size = target;

    chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_ENDOFFILE_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, 8);                              /* BufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 32); /* BufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                              /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                              /* AdditionalInformation */
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.vid);
    evpl_iovec_cursor_append_uint64(&cursor, target);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_allocate_reply, request);
} /* chimera_smb_allocate_query_done */

/* posix_fallocate/fallocate(2).  ALLOCATE (flags 0) reserves space for
 * [offset, offset+length) and grows the file to offset+length if it is
 * currently shorter -- realized as a size query followed by a conditional
 * FileEndOfFileInformation set.  DEALLOCATE (punch hole, keep size) is an
 * FSCTL_SET_ZERO_DATA over the range, which the server maps to a backend
 * deallocate. */
void
chimera_smb_client_allocate(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state =
        smb_handle_open_state(request->allocate.handle);
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;
    uint32_t                        input_offset;

    if (!open_state) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    if (request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
        chimera_smb_client_pdu_begin(conn, SMB2_IOCTL, &iov, &cursor, &hdr);

        input_offset = sizeof(struct smb2_header) + 56;

        evpl_iovec_cursor_append_uint16(&cursor, SMB2_IOCTL_REQUEST_SIZE);
        evpl_iovec_cursor_append_uint32(&cursor, SMB2_FSCTL_SET_ZERO_DATA);
        evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
        evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
        evpl_iovec_cursor_append_uint32(&cursor, input_offset);
        evpl_iovec_cursor_append_uint32(&cursor, 16);                    /* InputCount */
        evpl_iovec_cursor_append_uint32(&cursor, 0);                     /* MaxInputResponse */
        evpl_iovec_cursor_append_uint32(&cursor, 0);                     /* OutputOffset */
        evpl_iovec_cursor_append_uint32(&cursor, 0);                     /* OutputCount */
        evpl_iovec_cursor_append_uint32(&cursor, 0);                     /* MaxOutputResponse */
        evpl_iovec_cursor_append_uint32(&cursor, SMB2_0_IOCTL_IS_FSCTL); /* Flags */
        evpl_iovec_cursor_append_uint32(&cursor, 0);                     /* Reserved2 */
        /* FILE_ZERO_DATA_INFORMATION: FileOffset, BeyondFinalZero. */
        evpl_iovec_cursor_append_uint64(&cursor, request->allocate.offset);
        evpl_iovec_cursor_append_uint64(&cursor, request->allocate.offset +
                                        request->allocate.length);

        chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                      chimera_smb_allocate_reply, request);
        return;
    }

    /* ALLOCATE: query the current size, then grow if needed. */
    smb_attr_enrich_begin(conn, request, &open_state->file_id,
                          &request->allocate.r_post_attr,
                          chimera_smb_allocate_query_done);
} /* chimera_smb_client_allocate */

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
