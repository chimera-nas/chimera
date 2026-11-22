/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Generic-access CREATE of objects whose access comes from a POSIX mode.
 *
 * An object made over NFS or S3, or seeded by the server, has only a POSIX
 * mode.  The ACL the engine derives from it (chimera_acl_from_mode) is in
 * POSIX terms: its entries grant no SYNCHRONIZE, and FILE_WRITE_ATTRIBUTES to
 * the owner only.  Every NT generic right expands to a mask that includes
 * SYNCHRONIZE, and FILE_GENERIC_WRITE includes FILE_WRITE_ATTRIBUTES, so
 * judged as it stands that ACL refuses a GENERIC_READ open -- what the Linux
 * cifs client sends for O_RDONLY -- to every authenticated user except root,
 * and a GENERIC_WRITE open (O_WRONLY, O_RDWR) to every writer but the owner.
 *
 * The SMB server therefore presents such an ACL in Windows terms: r, w and x
 * become FILE_GENERIC_READ, FILE_GENERIC_WRITE and FILE_GENERIC_EXECUTE.
 * The CREATE check, the access reported for the handle and the security
 * descriptor a client reads all use that one descriptor.  A stored entry of
 * another shape is judged as it stands, as Windows judges it.
 *
 * No SMB client can make a mode-only object: a CREATE stamps the default
 * DACL, which grants full control.  So this probe makes them through the VFS
 * as root, as an NFS client with root access would, and then opens
 * them over SMB as an ordinary account.  The negative cases keep the mode
 * authoritative: a generic open must still be refused when the mode bits deny
 * the access.  A few objects carry a stored ACL instead: entries in Windows
 * terms, judged as they stand; the entries a mode gives, stored with no
 * control flags; and a default DACL rewritten by a chmod, which turns its
 * owner, group and everyone entries into mode-derived ones.  Mode-derived
 * entries are recognised by their shape.
 *
 * A handle granted FILE_WRITE_ATTRIBUTES, where the descriptor grants it, may
 * set explicit times and DOS attributes, so a group writer's GENERIC_WRITE
 * handle may.  An overwriting open is checked for, and granted, that right
 * whatever it asked for, so it may stamp the file's attributes as it empties
 * it.  A new named stream of another user's file is created without an
 * access check (named streams are on for that row), and its handle may not.
 *
 * memfs hands the check an ACL synthesized from the mode.  A cairn leg checks
 * that a named stream's open takes its access from the file's stored ACL.
 *
 * Run with "passthrough", the probe exercises the backends that hand the check
 * the mode alone.  On linux it opens the caller's own objects, where the owner
 * is granted what it asks for on Windows owner rules, and reads back the
 * Windows-terms DACL linux synthesizes.  On io_uring it opens other users'
 * objects, which are judged against the mode in Windows terms as on memfs.
 * That mode runs only on Linux, as root, and reports a skip otherwise.
 */

#include "smb2_mbt_common.h"

#include <limits.h>
#include <sys/stat.h>

#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_acl.h"

static int failures = 0;

/* The exact masks the server reports, in Windows terms.  An exact match also
 * shows the view hands a reader or writer nothing beyond its generic rights
 * (no WRITE_DAC, WRITE_OWNER, DELETE, FILE_EXECUTE or FILE_DELETE_CHILD).  A
 * MAXIMUM_ALLOWED grant may also hold DELETE through the parent directory
 * (parent_delete). */
#define MASK_GENERIC_R    0x00120089u /* FILE_GENERIC_READ */
#define MASK_GENERIC_RW   0x0012019fu /* FILE_GENERIC_READ | FILE_GENERIC_WRITE */
#define MASK_OWNER_RW_ACE 0x001e019fu /* the owner entry of rw-: also WRITE_DAC,
                                       * WRITE_OWNER */
#define MASK_OWNER_RW     0x001f019fu /* what that owner holds: also DELETE */
#define MASK_DELETE       0x00010000u
#define MASK_DELETE_CHILD 0x00000040u

#define CHECK(cond, ...)                             \
        do {                                         \
            if (cond) {                              \
                printf("ok   - " __VA_ARGS__);       \
                printf("\n");                        \
            } else {                                 \
                printf("FAIL - " __VA_ARGS__);       \
                printf("\n");                        \
                failures++;                          \
            }                                        \
        } while (0)

#define SEED_FS           "fs0"
#define SEED_MOUNT        "__modeacl_seed"

/* ---- seeding objects through the VFS ------------------------------------ */

enum seed_acl {
    SEED_ACL_NONE,      /* mode only */
    SEED_ACL_EVERYONE,  /* one EVERYONE@ allow entry, acl_mask */
    SEED_ACL_USER,      /* one allow entry for SMB2W_UID, acl_mask */
    SEED_ACL_MODE,      /* the mode's own entries, stored with no control
                         * flags, as a store that keeps none hands them back */
    SEED_ACL_DEFAULT,   /* the default DACL: owner and SYSTEM full control */
    SEED_ACL_INHERIT,   /* SMB2W_UID: full control of a directory, and acl_mask
                         * inherited by the files made in it */
    SEED_ACL_DENY_USER, /* a deny entry for SMB2W_UID with acl_mask, then
                         * EVERYONE@ full control */
};

struct seed_obj {
    const char *name;
    int         dir;    /* a directory, else a regular file */
    uint32_t    perm;   /* permission bits */
    uint32_t    uid;
    uint32_t    gid;
    int         acl;    /* enum seed_acl */
    uint32_t    acl_mask;
    uint32_t    chmod;  /* permission bits set after the create, when nonzero */
};

struct seed_ctx {
    struct chimera_vfs_thread      *thread;
    struct chimera_vfs_open_handle *root_oh;
    struct chimera_vfs_open_handle *chmod_oh;
    /* uid 0: the objects belong to other uids, and the server's own identity
     * is not root everywhere (on Windows it is the anonymous uid). */
    struct chimera_vfs_cred         cred;
    /* Outlives seed_next's frame: a passthrough backend runs the create on a
     * delegation thread and reads it after seed_next has returned. */
    struct chimera_vfs_attrs        set_attr;
    uint64_t                        acl_buf[(sizeof(struct chimera_acl) +
                                             5 * sizeof(struct chimera_ace) + 7) / 8];
    const struct seed_obj          *objs;
    int                             nobjs;
    int                             next;
    enum chimera_vfs_error status;
    int                             done;
};

static void seed_next(
    struct seed_ctx *ctx);

static void
seed_umount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct seed_ctx *ctx = private_data;

    (void) thread;
    (void) status;
    ctx->done = 1;
} /* seed_umount_cb */

static void
seed_finish(
    struct seed_ctx       *ctx,
    enum chimera_vfs_error status)
{
    ctx->status = status;

    if (ctx->root_oh) {
        chimera_vfs_release(ctx->thread, ctx->root_oh);
        ctx->root_oh = NULL;
    }

    chimera_vfs_umount(ctx->thread, chimera_vfs_get_server_cred(),
                       SEED_MOUNT, seed_umount_cb, ctx);
} /* seed_finish */

static void
seed_chmod_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct seed_ctx *ctx = private_data;

    (void) pre_attr;
    (void) set_attr;
    (void) post_attr;

    chimera_vfs_release(ctx->thread, ctx->chmod_oh);
    ctx->chmod_oh = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        seed_finish(ctx, error_code);
        return;
    }
    seed_next(ctx);
} /* seed_chmod_cb */

static void
seed_file_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct seed_ctx *ctx = private_data;

    (void) set_attr;
    (void) attr;
    (void) dir_pre_attr;
    (void) dir_post_attr;

    if (error_code != CHIMERA_VFS_OK) {
        if (oh) {
            chimera_vfs_release(ctx->thread, oh);
        }
        seed_finish(ctx, error_code);
        return;
    }

    /* A chmod of an object with a stored ACL rewrites its owner, group and
     * everyone entries from the new mode (chimera_acl_chmod). */
    if (ctx->objs[ctx->next - 1].chmod) {
        struct chimera_vfs_attrs *sa = &ctx->set_attr;

        memset(sa, 0, sizeof(*sa));
        sa->va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sa->va_mode     = ctx->objs[ctx->next - 1].chmod;
        ctx->chmod_oh   = oh;
        chimera_vfs_setattr(ctx->thread, &ctx->cred, oh, sa,
                            0, 0, seed_chmod_cb, ctx);
        return;
    }

    if (oh) {
        chimera_vfs_release(ctx->thread, oh);
    }
    seed_next(ctx);
} /* seed_file_cb */

static void
seed_dir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct seed_ctx *ctx = private_data;

    (void) set_attr;
    (void) attr;
    (void) dir_pre_attr;
    (void) dir_post_attr;

    if (error_code != CHIMERA_VFS_OK) {
        seed_finish(ctx, error_code);
        return;
    }
    seed_next(ctx);
} /* seed_dir_cb */

static void
seed_next(struct seed_ctx *ctx)
{
    const struct seed_obj    *o;
    struct chimera_vfs_attrs *set_attr = &ctx->set_attr;

    if (ctx->next == ctx->nobjs) {
        seed_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    o = &ctx->objs[ctx->next++];

    memset(set_attr, 0, sizeof(*set_attr));
    set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    set_attr->va_mode = (o->dir ? S_IFDIR : S_IFREG) | o->perm;
    set_attr->va_uid  = o->uid;
    set_attr->va_gid  = o->gid;

    if (o->acl != SEED_ACL_NONE) {
        struct chimera_acl *acl = (struct chimera_acl *) ctx->acl_buf;

        if (o->acl == SEED_ACL_DEFAULT) {
            chimera_acl_default_acl(set_attr->va_mode, acl, 2);
        } else if (o->acl == SEED_ACL_MODE) {
            chimera_acl_from_mode(o->perm, acl, 5);
            acl->ctrl_flags = 0;
        } else if (o->acl == SEED_ACL_INHERIT) {
            memset(acl, 0, chimera_acl_size(2));
            acl->num_aces = 2;
            for (int i = 0; i < 2; i++) {
                acl->aces[i].type     = CHIMERA_ACE_ALLOWED;
                acl->aces[i].who.type = CHIMERA_PRINCIPAL_USER;
                acl->aces[i].who.id   = SMB2W_UID;
            }
            acl->aces[0].access_mask = CHIMERA_ACE_MASK_ALL;
            acl->aces[1].access_mask = o->acl_mask;
            acl->aces[1].flags       = CHIMERA_ACE_FLAG_FILE_INHERIT |
                CHIMERA_ACE_FLAG_INHERIT_ONLY;
        } else if (o->acl == SEED_ACL_DENY_USER) {
            memset(acl, 0, chimera_acl_size(2));
            acl->num_aces            = 2;
            acl->aces[0].type        = CHIMERA_ACE_DENIED;
            acl->aces[0].access_mask = o->acl_mask;
            acl->aces[0].who.type    = CHIMERA_PRINCIPAL_USER;
            acl->aces[0].who.id      = SMB2W_UID;
            acl->aces[1].type        = CHIMERA_ACE_ALLOWED;
            acl->aces[1].access_mask = CHIMERA_ACE_MASK_ALL;
            acl->aces[1].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
            acl->aces[1].who.special = CHIMERA_WHO_EVERYONE;
        } else {
            memset(acl, 0, chimera_acl_size(1));
            acl->num_aces            = 1;
            acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
            acl->aces[0].access_mask = o->acl_mask;
            if (o->acl == SEED_ACL_USER) {
                acl->aces[0].who.type = CHIMERA_PRINCIPAL_USER;
                acl->aces[0].who.id   = SMB2W_UID;
            } else {
                acl->aces[0].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
                acl->aces[0].who.special = CHIMERA_WHO_EVERYONE;
            }
        }
        set_attr->va_acl       = acl;
        set_attr->va_set_mask |= CHIMERA_VFS_ATTR_ACL;
    }

    if (o->dir) {
        chimera_vfs_mkdir_at(ctx->thread, &ctx->cred,
                             ctx->root_oh, o->name, strlen(o->name), set_attr,
                             CHIMERA_VFS_ATTR_FH, 0, 0, seed_dir_cb, ctx);
    } else {
        chimera_vfs_open_at(ctx->thread, &ctx->cred,
                            ctx->root_oh, o->name, strlen(o->name),
                            CHIMERA_VFS_OPEN_CREATE, set_attr,
                            CHIMERA_VFS_ATTR_FH, 0, 0, seed_file_cb, ctx);
    }
} /* seed_next */

static void
seed_root_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct seed_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        seed_finish(ctx, error_code);
        return;
    }
    ctx->root_oh = oh;
    seed_next(ctx);
} /* seed_root_cb */

static void
seed_mounted_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct seed_ctx          *ctx = private_data;
    struct chimera_vfs_mount *m;

    if (status != CHIMERA_VFS_OK) {
        ctx->status = status;
        ctx->done   = 1;
        return;
    }

    m = chimera_vfs_mount_table_find_exact(thread->vfs->mount_table,
                                           SEED_MOUNT, strlen(SEED_MOUNT));
    if (!m) {
        seed_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    chimera_vfs_open_fh(thread, &ctx->cred,
                        m->root_fh, m->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        seed_root_cb, ctx);
} /* seed_mounted_cb */

/* Create `objs` in the root of `module_path` on `module` (the share's root),
 * as root, with the given mode, ownership and stored ACL. */
static void
seed_objects(
    struct smb2_env       *env,
    const char            *module,
    const char            *module_path,
    const struct seed_obj *objs,
    int                    nobjs)
{
    struct evpl    *evpl;
    struct seed_ctx ctx;

    memset(&ctx, 0, sizeof(ctx));

    evpl       = evpl_create(NULL);
    ctx.thread = chimera_vfs_thread_init(evpl, chimera_server_get_vfs(env->server));
    ctx.objs   = objs;
    ctx.nobjs  = nobjs;
    ctx.status = CHIMERA_VFS_OK;
    chimera_vfs_cred_init_unix(&ctx.cred, 0, 0, 0, NULL);

    chimera_vfs_mount_detached(ctx.thread, chimera_vfs_get_server_cred(),
                               SEED_MOUNT, module, module_path, NULL,
                               seed_mounted_cb, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(ctx.thread);
    evpl_destroy(evpl);

    if (ctx.status != CHIMERA_VFS_OK) {
        fprintf(stderr, "seeding mode-only objects failed: %d\n", ctx.status);
        smb2c_exit(1);
    }
} /* seed_objects */

/* ---- the probes ---------------------------------------------------------- */

/* The objects the probe opens.  The SMB account is SMB2W_UID/SMB2W_GID
 * (1000/1000), so the root-owned ones are evaluated against their "other"
 * bits and the SMB2W_UID-owned ones against their owner bits. */
/* *INDENT-OFF* */
/* uncrustify 0.78.1 does not converge on these aligned tables, so they are guarded. */
static const struct seed_obj objs[] = {
    /* name             dir  perm  uid        gid       */
    { "root_0644.bin",  0,   0644, 0,         0         },
    { "root_0600.bin",  0,   0600, 0,         0         },
    { "mine_0644.bin",  0,   0644, SMB2W_UID, SMB2W_GID },
    { "mine_0444.bin",  0,   0444, SMB2W_UID, SMB2W_GID },
    { "root_dir",       1,   0755, 0,         0         },
    { "grp_0664.bin",   0,   0664, 0,         SMB2W_GID },
    { "grp_0640.bin",   0,   0640, 0,         SMB2W_GID },
    { "root_0666.bin",  0,   0666, 0,         0         },
    { "grp_0646.bin",   0,   0646, 0,         SMB2W_GID },
    { "grp_0604.bin",   0,   0604, 0,         SMB2W_GID },
    { "times_0664.bin", 0,   0664, 0,         SMB2W_GID },
    { "stkw_0664.bin",  0,   0664, 0,         SMB2W_GID },
    { "stkr_0664.bin",  0,   0664, 0,         SMB2W_GID },
    { "sprs_0664.bin",  0,   0664, 0,         SMB2W_GID },
    { "rprs_0664.bin",  0,   0664, 0,         SMB2W_GID },
    { "ovw_0664.bin",   0,   0664, 0,         SMB2W_GID },
    { "strm_0644.bin",  0,   0644, 0,         0         },
    /* Stored ACLs.  dacl_nowa and dacl_nosync are in Windows terms and judged
     * as they stand: one grants everyone FILE_GENERIC_READ|FILE_GENERIC_WRITE
     * without FILE_WRITE_ATTRIBUTES, one grants the caller by name
     * FILE_GENERIC_READ without SYNCHRONIZE.  mode_0664 holds the entries a
     * 0664 mode gives, with no control flags.  chmod_0664 is created with the
     * default DACL and then chmodded 0664, which replaces its entries with
     * mode-derived ones.  dacl_noea grants the caller by name FILE_WRITE_DATA
     * and FILE_WRITE_ATTRIBUTES without FILE_WRITE_EA.  inh_dir is a directory
     * whose files inherit an entry granting the caller FILE_READ_DATA and
     * FILE_WRITE_DATA without FILE_WRITE_ATTRIBUTES or FILE_WRITE_EA, and
     * inh_nosync one whose files inherit FILE_READ_DATA and FILE_WRITE_DATA
     * alone.  dacl_shape holds one EVERYONE@ entry a client wrote with the
     * mask a rw- mode gives.
     * name             dir  perm  uid        gid        acl                acl_mask    chmod */
    { "dacl_nowa.bin",  0,   0666, 0,         0,         SEED_ACL_EVERYONE, 0x0012009f, 0     },
    { "dacl_nosync.bin",0,   0666, 0,         0,         SEED_ACL_USER,     0x00020089, 0     },
    { "mode_0664.bin",  0,   0664, 0,         SMB2W_GID, SEED_ACL_MODE,     0,          0     },
    { "chmod_0664.bin", 0,   0600, 0,         SMB2W_GID, SEED_ACL_DEFAULT,  0,          0664  },
    { "dacl_noea.bin",  0,   0600, 0,         0,         SEED_ACL_USER,     0x00120106, 0     },
    { "inh_dir",        1,   0777, 0,         0,         SEED_ACL_INHERIT,  0x00120003, 0     },
    { "inh_nosync",     1,   0777, 0,         0,         SEED_ACL_INHERIT,  0x00000003, 0     },
    { "dacl_shape.bin", 0,   0644, 0,         0,         SEED_ACL_EVERYONE, 0x0002009f, 0     },
};

#ifdef __linux__
/* The linux leg opens only the caller's own objects (see main). */
static const struct seed_obj own_objs[] = {
    /* name             dir  perm  uid        gid       */
    { "mine_0644.bin",  0,   0644, SMB2W_UID, SMB2W_GID },
    { "mine_0444.bin",  0,   0444, SMB2W_UID, SMB2W_GID },
    { "mine_0664.bin",  0,   0664, SMB2W_UID, SMB2W_GID },
};

#ifdef HAVE_IO_URING
/* The io_uring leg opens other users' objects (see io_uring_leg). */
static const struct seed_obj other_objs[] = {
    /* name             dir  perm  uid        gid       */
    { "root_0644.bin",  0,   0644, 0,         0         },
    { "root_0600.bin",  0,   0600, 0,         0         },
    { "grp_0664.bin",   0,   0664, 0,         SMB2W_GID },
};
#endif /* ifdef HAVE_IO_URING */
#endif /* ifdef __linux__ */
/* *INDENT-ON* */

static uint32_t
open_status(
    struct smb2_conn *c,
    const char       *name,
    uint32_t          access,
    uint32_t          create_options)
{
    struct smb2_create_out co;
    uint32_t               st;

    st = smb2_create_opts(c, name, MBT_FILE_OPEN, access, MBT_FILE_SHARE_RWD,
                          create_options, NULL, &co);
    if (st == ST_SUCCESS) {
        smb2_close(c, co.file_id);
    }
    return st;
} /* open_status */

/* Open `name` and return the status; on success also the handle's granted
 * access, from FILE_ACCESS_INFORMATION, in *granted. */
static uint32_t
open_granted(
    struct smb2_conn *c,
    const char       *name,
    uint32_t          access,
    uint32_t         *granted)
{
    struct smb2_create_out co;
    uint8_t                buf[8];
    uint32_t               len = 0;
    uint32_t               st;

    *granted = 0;
    st       = smb2_create_opts(c, name, MBT_FILE_OPEN, access, MBT_FILE_SHARE_RWD,
                                MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
    if (st != ST_SUCCESS) {
        return st;
    }
    if (smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_ACCESS_INFO_T, co.file_id,
                        0, buf, sizeof(buf), &len) == ST_SUCCESS && len >= 4) {
        *granted = (uint32_t) buf[0] | ((uint32_t) buf[1] << 8) |
            ((uint32_t) buf[2] << 16) | ((uint32_t) buf[3] << 24);
    }
    smb2_close(c, co.file_id);
    return st;
} /* open_granted */

/* Open `name` for FILE_READ_ATTRIBUTES with an MxAc create context and return
 * the status; on success also the maximal access the server reports for the
 * caller, in *maximal. */
static uint32_t
open_maximal(
    struct smb2_conn *c,
    const char       *name,
    uint32_t         *maximal)
{
    static const uint8_t   mxac[4] = { 'M', 'x', 'A', 'c' };
    struct smb2_cctx       ctx     = { mxac, 4, NULL, 0 };
    struct smb2_create_out co;

    *maximal = 0;
    smb2c_send(c, smb2c_build_create_full(c, name, MBT_FILE_OPEN,
                                          MBT_FILE_READ_ATTRIBUTES,
                                          MBT_FILE_SHARE_RWD,
                                          MBT_FILE_NON_DIRECTORY_FILE, 0,
                                          NULL, &ctx, 1));
    smb2c_wait(c);
    smb2c_parse_create(c, &co);
    if (co.status != ST_SUCCESS) {
        return co.status;
    }
    for (int i = 0; i < co.nctx; i++) {
        if (co.ctx[i].name_len == 4 && memcmp(co.ctx[i].name, mxac, 4) == 0 &&
            co.ctx[i].data_len >= 8 && g32(co.ctx[i].data, 0) == ST_SUCCESS) {
            *maximal = g32(co.ctx[i].data, 4);
        }
    }
    smb2_close(c, co.file_id);
    return co.status;
} /* open_maximal */

/* DELETE, if the share root, which holds the files these checks open, grants
 * the caller FILE_DELETE_CHILD: a MAXIMUM_ALLOWED grant then includes DELETE
 * (MS-FSA 2.1.5.1.2.1), beyond what the file's own descriptor grants. */
static uint32_t
parent_delete(struct smb2_conn *c)
{
    static const uint8_t   mxac[4] = { 'M', 'x', 'A', 'c' };
    struct smb2_cctx       ctx     = { mxac, 4, NULL, 0 };
    struct smb2_create_out co;
    uint32_t               maximal = 0;

    smb2c_send(c, smb2c_build_create_full(c, "", MBT_FILE_OPEN,
                                          MBT_FILE_READ_ATTRIBUTES,
                                          MBT_FILE_SHARE_RWD,
                                          MBT_FILE_DIRECTORY_FILE, 0,
                                          NULL, &ctx, 1));
    smb2c_wait(c);
    smb2c_parse_create(c, &co);
    if (co.status != ST_SUCCESS) {
        return 0;
    }
    for (int i = 0; i < co.nctx; i++) {
        if (co.ctx[i].name_len == 4 && memcmp(co.ctx[i].name, mxac, 4) == 0 &&
            co.ctx[i].data_len >= 8 && g32(co.ctx[i].data, 0) == ST_SUCCESS) {
            maximal = g32(co.ctx[i].data, 4);
        }
    }
    smb2_close(c, co.file_id);
    return (maximal & MASK_DELETE_CHILD) ? MASK_DELETE : 0;
} /* parent_delete */

/* Set FileBasicInformation through `file_id`: an explicit LastWriteTime and
 * the HIDDEN attribute, the other times left as they are. */
static uint32_t
set_basic(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
    uint8_t b[40];

    memset(b, 0, sizeof(b));
    p64(b, 16, 0x01d0000000000000ull); /* LastWriteTime, in 2014 */
    p32(b, 32, 0x00000002u);           /* FILE_ATTRIBUTE_HIDDEN */
    return smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, file_id,
                         b, sizeof(b));
} /* set_basic */

/* Set FileBasicInformation's LastAccessTime and LastWriteTime, each when
 * nonzero, and nothing else: FileAttributes 0 leaves the attributes as they
 * are. */
static uint32_t
set_times(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          atime,
    uint64_t          mtime)
{
    uint8_t b[40];

    memset(b, 0, sizeof(b));
    p64(b, 8, atime);
    p64(b, 16, mtime);
    return smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, file_id,
                         b, sizeof(b));
} /* set_times */

/* Query FileBasicInformation: LastAccessTime, LastWriteTime and
 * FileAttributes. */
static uint32_t
query_basic(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t         *atime,
    uint64_t         *mtime,
    uint32_t         *attrs)
{
    uint8_t  b[64];
    uint32_t len = 0, st;

    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, file_id, 0,
                         b, sizeof(b), &len);
    if (st == ST_SUCCESS && len >= 36) {
        *atime = g64(b, 8);
        *mtime = g64(b, 16);
        *attrs = g32(b, 32);
    } else if (st == ST_SUCCESS) {
        st = ST_UNSUCCESSFUL;
    }
    return st;
} /* query_basic */

/* Open `name` with `access` and set FileBasicInformation through the handle
 * (set_basic); return the open's status and the set's in *set_st. */
static uint32_t
open_set_basic(
    struct smb2_conn *c,
    const char       *name,
    uint32_t          access,
    uint32_t         *set_st)
{
    struct smb2_create_out co;
    uint32_t               st;

    *set_st = 0;
    st      = smb2_create_opts(c, name, MBT_FILE_OPEN, access, MBT_FILE_SHARE_RWD,
                               MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
    if (st == ST_SUCCESS) {
        *set_st = set_basic(c, co.file_id);
        smb2_close(c, co.file_id);
    }
    return st;
} /* open_set_basic */

/* Write to `name` through a GENERIC_WRITE open, then overwrite it with
 * FILE_OVERWRITE and `access`, as a client truncating it on open does.
 * Return the overwrite's status; *write_st is the write's, and *eof the
 * file's EndOfFile, read through a GENERIC_READ open afterwards. */
static uint32_t
write_then_overwrite(
    struct smb2_conn *c,
    const char       *name,
    uint32_t          access,
    uint32_t         *write_st,
    uint64_t         *eof)
{
    static const char      data[] = "hello";
    struct smb2_create_out co;
    uint8_t                buf[24];
    uint32_t               st, len = 0;

    *write_st = 0;
    *eof      = UINT64_MAX;
    st        = smb2_create_opts(c, name, MBT_FILE_OPEN, MBT_GENERIC_WRITE,
                                 MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE,
                                 NULL, &co);
    if (st != ST_SUCCESS) {
        *write_st = st;
        return st;
    }
    *write_st = smb2_write(c, co.file_id, 0, data, sizeof(data) - 1, NULL);
    smb2_close(c, co.file_id);

    st = smb2_create_opts(c, name, MBT_FILE_OVERWRITE, access,
                          MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
    if (st != ST_SUCCESS) {
        return st;
    }
    smb2_close(c, co.file_id);

    if (smb2_create_opts(c, name, MBT_FILE_OPEN, MBT_GENERIC_READ, MBT_FILE_SHARE_RWD,
                         MBT_FILE_NON_DIRECTORY_FILE, NULL, &co) == ST_SUCCESS) {
        if (smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T,
                            co.file_id, 0, buf, sizeof(buf), &len) == ST_SUCCESS &&
            len >= 16) {
            *eof = g64(buf, 8);
        }
        smb2_close(c, co.file_id);
    }
    return st;
} /* write_then_overwrite */

/* Create the named stream `name` (file:stream) of an existing file with
 * FILE_CREATE and FILE_WRITE_ATTRIBUTES|SYNCHRONIZE, and set
 * FileBasicInformation through the stream handle (set_basic); return the
 * create's status and the set's in *set_st. */
static uint32_t
stream_create_set_basic(
    struct smb2_conn *c,
    const char       *name,
    uint32_t         *set_st)
{
    struct smb2_create_out co;
    uint32_t               st;

    *set_st = 0;
    st      = smb2_create_opts(c, name, MBT_FILE_CREATE,
                               MBT_FILE_WRITE_ATTRIBUTES | MBT_SYNCHRONIZE,
                               MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE,
                               NULL, &co);
    if (st == ST_SUCCESS) {
        *set_st = set_basic(c, co.file_id);
        smb2_close(c, co.file_id);
    }
    return st;
} /* stream_create_set_basic */

/* Create the named stream `name` (file:stream) with FILE_CREATE and
 * FILE_WRITE_DATA and write to it, then open it again with `disp` and
 * FILE_WRITE_DATA, as a client replacing a stream's contents does.  Return
 * the second open's status; *prep_st is the create's or the write's. */
static uint32_t
stream_write_then_overwrite(
    struct smb2_conn *c,
    const char       *name,
    uint32_t          disp,
    uint32_t         *prep_st)
{
    static const char      data[] = "hello";
    struct smb2_create_out co;
    uint32_t               st;

    *prep_st = smb2_create_opts(c, name, MBT_FILE_CREATE, MBT_FILE_WRITE_DATA,
                                MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE,
                                NULL, &co);
    if (*prep_st != ST_SUCCESS) {
        return *prep_st;
    }
    *prep_st = smb2_write(c, co.file_id, 0, data, sizeof(data) - 1, NULL);
    smb2_close(c, co.file_id);

    st = smb2_create_opts(c, name, disp, MBT_FILE_WRITE_DATA, MBT_FILE_SHARE_RWD,
                          MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
    if (st == ST_SUCCESS) {
        smb2_close(c, co.file_id);
    }
    return st;
} /* stream_write_then_overwrite */

/* The DACL of the mode-only 0664 file (uid 0, gid SMB2W_GID) a client reads:
 * owner, group and everyone allow entries, in that order, in Windows terms. */
static void
check_mode_dacl(
    struct smb2_conn *c,
    const char       *leg,
    const char       *name)
{
    struct smb2_create_out co;
    struct smb2_sd         sd;
    uint8_t                buf[1024];
    uint32_t               st, len = 0;

    st = smb2_create_opts(c, name, MBT_FILE_OPEN,
                          0x00020000u /* READ_CONTROL */, MBT_FILE_SHARE_RWD,
                          MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
    CHECK(st == ST_SUCCESS, "%s: READ_CONTROL open of the 0664 file %s -> 0x%08x",
          leg, name, st);
    if (st != ST_SUCCESS) {
        return;
    }

    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id, SMB2_SEC_DACL,
                         buf, sizeof(buf), &len);
    smb2_close(c, co.file_id);
    CHECK(st == ST_SUCCESS && smb2_sd_parse(buf, len, &sd) == 0 && sd.nace == 3,
          "%s: QUERY security of the 0664 file -> 0x%08x, %d entries", leg, st,
          st == ST_SUCCESS ? sd.nace : -1);
    if (st != ST_SUCCESS || sd.nace != 3) {
        return;
    }

    CHECK(sd.ace[0].access_mask == MASK_OWNER_RW_ACE,
          "%s: its owner entry is FILE_GENERIC_READ|FILE_GENERIC_WRITE with "
          "WRITE_DAC|WRITE_OWNER (0x%08x)", leg, sd.ace[0].access_mask);
    CHECK(sd.ace[1].access_mask == MASK_GENERIC_RW,
          "%s: its group entry is FILE_GENERIC_READ|FILE_GENERIC_WRITE (0x%08x)",
          leg, sd.ace[1].access_mask);
    CHECK(sd.ace[2].access_mask == MASK_GENERIC_R,
          "%s: its everyone entry is FILE_GENERIC_READ (0x%08x)",
          leg, sd.ace[2].access_mask);
} /* check_mode_dacl */

/* The checks on a backend that hands the CREATE check an ACL synthesized
 * from the mode, as SMB2W_USER (uid/gid 1000). */
static void
run_checks(
    struct smb2_conn *c,
    const char       *leg)
{
    uint32_t st, st2, granted, maximal;
    uint32_t pdel = parent_delete(c);
    uint64_t eof;

    /* Another user's file whose "other" bits allow reading. */
    st = open_status(c, "root_0644.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of another user's 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "root_0644.bin", MBT_GENERIC_EXECUTE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_EXECUTE of another user's 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "root_0644.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of another user's 0644 file is denied -> 0x%08x", leg, st);

    st = open_status(c, "root_0600.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_READ of another user's 0600 file is denied -> 0x%08x", leg, st);

    /* Every class holds FILE_READ_ATTRIBUTES, but SYNCHRONIZE comes only with
     * an r, w or x, so a caller no mode-derived entry grants one of them, as
     * "other" on a 0600 file, is refused an open that asks for it, as Samba
     * refuses it. */
    st = open_status(c, "root_0600.bin", MBT_FILE_READ_ATTRIBUTES | MBT_SYNCHRONIZE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: FILE_READ_ATTRIBUTES|SYNCHRONIZE of another user's 0600 file is "
          "denied -> 0x%08x", leg, st);

    /* A class the mode keeps from a bit that a broader class holds, as the
     * group of a 0604 file, is denied that bit but keeps the SYNCHRONIZE the
     * broader entry carries. */
    st = open_status(c, "grp_0604.bin", MBT_FILE_READ_ATTRIBUTES | MBT_SYNCHRONIZE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: FILE_READ_ATTRIBUTES|SYNCHRONIZE of a 0604 file by its group -> "
          "0x%08x", leg, st);

    st = open_status(c, "grp_0604.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_READ of a 0604 file by its group is denied -> 0x%08x", leg, st);

    /* The caller's own file: the derived ACL is present, so the owner shortcut
     * does not apply and the mode-derived owner entry decides. */
    st = open_status(c, "mine_0644.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS, "%s: GENERIC_READ of own 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "mine_0644.bin", MBT_GENERIC_READ | MBT_GENERIC_WRITE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ|GENERIC_WRITE of own 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "mine_0444.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of own 0444 file is denied -> 0x%08x", leg, st);

    /* A directory: GENERIC_READ is what a client sends to list it. */
    st = open_status(c, "root_dir", MBT_GENERIC_READ, MBT_FILE_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of another user's 0755 directory -> 0x%08x", leg, st);

    /* Writers that are not the owner: the group class and "other". */
    st = open_status(c, "grp_0664.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of a group-writable 0664 file -> 0x%08x", leg, st);

    st = open_status(c, "grp_0664.bin", MBT_GENERIC_READ | MBT_GENERIC_WRITE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ|GENERIC_WRITE of a group-writable 0664 file -> 0x%08x", leg, st);

    st = open_status(c, "root_0666.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of another user's 0666 file -> 0x%08x", leg, st);

    /* What the descriptor grants, as MAXIMUM_ALLOWED and the MxAc maximal
     * access report it: a writer holds FILE_WRITE_ATTRIBUTES with the w bit. */
    st = open_granted(c, "grp_0664.bin", MBT_MAXIMUM_ALLOWED, &granted);
    CHECK(st == ST_SUCCESS && granted == (MASK_GENERIC_RW | pdel),
          "%s: MAXIMUM_ALLOWED of a group-writable 0664 file grants exactly "
          "FILE_GENERIC_READ|FILE_GENERIC_WRITE (0x%08x) -> 0x%08x", leg, granted, st);

    st = open_granted(c, "mine_0644.bin", MBT_MAXIMUM_ALLOWED, &granted);
    CHECK(st == ST_SUCCESS && granted == MASK_OWNER_RW,
          "%s: MAXIMUM_ALLOWED of own 0644 file grants exactly "
          "FILE_GENERIC_READ|FILE_GENERIC_WRITE and the owner's WRITE_DAC, "
          "WRITE_OWNER and DELETE (0x%08x) -> 0x%08x", leg, granted, st);

    st = open_granted(c, "root_0644.bin", MBT_MAXIMUM_ALLOWED, &granted);
    CHECK(st == ST_SUCCESS && granted == (MASK_GENERIC_R | pdel),
          "%s: MAXIMUM_ALLOWED of another user's 0644 file grants exactly "
          "FILE_GENERIC_READ (0x%08x) -> 0x%08x", leg, granted, st);

    st = open_maximal(c, "grp_0664.bin", &maximal);
    CHECK(st == ST_SUCCESS && maximal == MASK_GENERIC_RW,
          "%s: maximal access of a group-writable 0664 file is exactly "
          "FILE_GENERIC_READ|FILE_GENERIC_WRITE (0x%08x) -> 0x%08x", leg, maximal, st);

    st = open_maximal(c, "root_0644.bin", &maximal);
    CHECK(st == ST_SUCCESS && maximal == MASK_GENERIC_R,
          "%s: maximal access of another user's 0644 file is exactly "
          "FILE_GENERIC_READ (0x%08x) -> 0x%08x", leg, maximal, st);

    /* A class the mode keeps from writing is not handed FILE_WRITE_ATTRIBUTES
     * by the everyone entry below it. */
    st = open_status(c, "grp_0646.bin", MBT_FILE_WRITE_ATTRIBUTES,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: FILE_WRITE_ATTRIBUTES of a 0646 file by its group is denied "
          "-> 0x%08x", leg, st);

    /* A handle's FILE_WRITE_ATTRIBUTES, as the descriptor grants it, lets it
     * set explicit times and DOS attributes, whoever owns the file; a handle
     * opened without it is judged against the file's ACL, which here does
     * not grant a group writer that right in POSIX terms. */
    st = open_set_basic(c, "times_0664.bin", MBT_GENERIC_WRITE, &st2);
    CHECK(st == ST_SUCCESS && st2 == ST_SUCCESS,
          "%s: a GENERIC_WRITE handle of a group-writable 0664 file sets an "
          "explicit LastWriteTime and HIDDEN -> open 0x%08x, set 0x%08x",
          leg, st, st2);

    st = open_set_basic(c, "times_0664.bin", MBT_FILE_WRITE_DATA, &st2);
    CHECK(st == ST_SUCCESS && st2 == ST_ACCESS_DENIED,
          "%s: a FILE_WRITE_DATA handle of that file is refused the same set "
          "-> open 0x%08x, set 0x%08x", leg, st, st2);

    /* The other setattrs that handle's FILE_WRITE_ATTRIBUTES authorizes, each
     * as the same group writer, which the file's ACL does not grant that right
     * in POSIX terms. */
    {
        static const uint64_t  t_set = 0x01d0000000000000ull; /* in 2014 */
        struct smb2_create_out co;
        uint64_t               at = 0, mt = 0;
        uint32_t               attrs = 0, cnt = 0, rlen = 0;
        uint8_t                data[4] = { 1, 2, 3, 4 }, rbuf[4], rp[64];
        int                    n;

        /* Explicit times alone, with no attribute change. */
        st = smb2_create_opts(c, "times_0664.bin", MBT_FILE_OPEN, MBT_GENERIC_WRITE,
                              MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        st2 = st == ST_SUCCESS ? set_times(c, co.file_id, 0, t_set) : 0;
        if (st == ST_SUCCESS) {
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS && st2 == ST_SUCCESS,
              "%s: a GENERIC_WRITE handle of a group-writable 0664 file sets an "
              "explicit LastWriteTime alone -> open 0x%08x, set 0x%08x", leg, st, st2);

        /* A write keeps a LastWriteTime the handle set (the write path puts
         * it back). */
        st = smb2_create_opts(c, "stkw_0664.bin", MBT_FILE_OPEN,
                              MBT_GENERIC_READ | MBT_GENERIC_WRITE, MBT_FILE_SHARE_RWD,
                              MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            st = set_times(c, co.file_id, 0, t_set);
            if (st == ST_SUCCESS) {
                st = smb2_write(c, co.file_id, 0, data, sizeof(data), &cnt);
            }
            if (st == ST_SUCCESS) {
                st = query_basic(c, co.file_id, &at, &mt, &attrs);
            }
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS && mt == t_set,
              "%s: a write through that handle keeps the LastWriteTime it set "
              "-> 0x%08x, LastWriteTime 0x%016llx", leg, st, (unsigned long long) mt);

        /* A read keeps a LastAccessTime the handle set (the read path puts
         * it back). */
        st = smb2_create_opts(c, "stkr_0664.bin", MBT_FILE_OPEN,
                              MBT_GENERIC_READ | MBT_GENERIC_WRITE, MBT_FILE_SHARE_RWD,
                              MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            st = smb2_write(c, co.file_id, 0, data, sizeof(data), &cnt);
            if (st == ST_SUCCESS) {
                st = set_times(c, co.file_id, t_set, 0);
            }
            if (st == ST_SUCCESS) {
                st = smb2_read(c, co.file_id, 0, sizeof(rbuf), rbuf, &rlen);
            }
            if (st == ST_SUCCESS) {
                st = query_basic(c, co.file_id, &at, &mt, &attrs);
            }
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS && at == t_set,
              "%s: a read through that handle keeps the LastAccessTime it set "
              "-> 0x%08x, LastAccessTime 0x%016llx", leg, st, (unsigned long long) at);

        /* FSCTL_SET_SPARSE sets FILE_ATTRIBUTE_SPARSE_FILE. */
        st = smb2_create_opts(c, "sprs_0664.bin", MBT_FILE_OPEN,
                              MBT_GENERIC_READ | MBT_GENERIC_WRITE, MBT_FILE_SHARE_RWD,
                              MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            st = smb2_ioctl(c, SMB2_FSCTL_SET_SPARSE, co.file_id, NULL, 0);
            if (st == ST_SUCCESS) {
                st = query_basic(c, co.file_id, &at, &mt, &attrs);
            }
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS && (attrs & 0x00000200u),
              "%s: FSCTL_SET_SPARSE through that handle -> 0x%08x, attributes "
              "0x%08x", leg, st, attrs);

        /* FSCTL_SET_REPARSE_POINT sets FILE_ATTRIBUTE_REPARSE_POINT.  A tag
         * the server keeps verbatim, with a GUID (MS-FSCC 2.1.2.3). */
        memset(rp, 0, sizeof(rp));
        n = 24 + 7;
        p32(rp, 0, 0x00000123u);
        p16(rp, 4, 7);
        for (int i = 0; i < 16; i++) {
            rp[8 + i] = (uint8_t) (0x10 + i);
        }
        memcpy(rp + 24, "chimera", 7);
        st = smb2_create_opts(c, "rprs_0664.bin", MBT_FILE_OPEN,
                              MBT_GENERIC_READ | MBT_GENERIC_WRITE, MBT_FILE_SHARE_RWD,
                              MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            st = smb2_ioctl(c, SMB2_FSCTL_SET_REPARSE_POINT, co.file_id, rp,
                            (uint32_t) n);
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS,
              "%s: FSCTL_SET_REPARSE_POINT through that handle -> 0x%08x", leg, st);
    }

    /* An overwriting open by a writer that is not the owner. */
    st = write_then_overwrite(c, "ovw_0664.bin", MBT_GENERIC_WRITE, &st2, &eof);
    CHECK(st == ST_SUCCESS && st2 == ST_SUCCESS && eof == 0,
          "%s: FILE_OVERWRITE with GENERIC_WRITE of a group-writable 0664 file "
          "empties it -> write 0x%08x, overwrite 0x%08x, EndOfFile %llu",
          leg, st2, st, (unsigned long long) eof);

    /* An overwrite is checked for, and granted, FILE_WRITE_ATTRIBUTES whatever
     * it asked for (MS-FSA 2.1.5.1.2), so FILE_WRITE_DATA alone suffices. */
    st = write_then_overwrite(c, "ovw_0664.bin", MBT_FILE_WRITE_DATA, &st2, &eof);
    CHECK(st == ST_SUCCESS && st2 == ST_SUCCESS && eof == 0,
          "%s: FILE_OVERWRITE with FILE_WRITE_DATA of a group-writable 0664 file "
          "empties it -> write 0x%08x, overwrite 0x%08x, EndOfFile %llu",
          leg, st2, st, (unsigned long long) eof);

    /* A handle's FILE_WRITE_ATTRIBUTES counts only where the descriptor grants
     * it.  A FILE_CREATE of a new stream of another user's file is opened
     * without a check of the access asked for, so its handle may not set the
     * file's times and DOS attributes when the descriptor does not grant that
     * right.  The create's own success is checked too: these two checks are
     * what show the set refused for that reason. */
    st = stream_create_set_basic(c, "strm_0644.bin:x", &st2);
    CHECK(st == ST_SUCCESS,
          "%s: FILE_CREATE of a new stream of another user's 0644 file with "
          "FILE_WRITE_ATTRIBUTES -> 0x%08x", leg, st);
    CHECK(st2 == ST_ACCESS_DENIED,
          "%s: SET_INFO FileBasicInformation through that stream's handle is "
          "denied -> 0x%08x", leg, st2);

    st = open_status(c, "grp_0640.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of a group-readable 0640 file -> 0x%08x", leg, st);

    st = open_status(c, "grp_0640.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of a group-readable 0640 file is denied -> 0x%08x", leg, st);

    /* The descriptor a client reads is the one the check used. */
    check_mode_dacl(c, leg, "grp_0664.bin");

    /* Stored DACLs are judged as they stand. */
    st = open_status(c, "dacl_nowa.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of a stored DACL without FILE_WRITE_ATTRIBUTES is "
          "denied -> 0x%08x", leg, st);

    st = open_status(c, "dacl_nowa.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of that DACL's file -> 0x%08x", leg, st);

    /* A stored EVERYONE@ entry a client wrote with exactly the mask a rw- mode
     * gives has the shape of a mode-derived entry, so it is read in Windows
     * terms like one, by design: a writer it names may open it with
     * GENERIC_WRITE. */
    st = open_status(c, "dacl_shape.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of a stored EVERYONE@ entry with the mask a rw- mode "
          "gives -> 0x%08x", leg, st);

    st = open_status(c, "dacl_nosync.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_READ of a stored named entry without SYNCHRONIZE is denied "
          "-> 0x%08x", leg, st);

    st = open_status(c, "dacl_nosync.bin", MBT_FILE_READ_DATA | MBT_SYNCHRONIZE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: an explicit SYNCHRONIZE that DACL does not grant is denied "
          "-> 0x%08x", leg, st);

    st = open_status(c, "dacl_nosync.bin", MBT_FILE_READ_DATA,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: FILE_READ_DATA of that DACL's file -> 0x%08x", leg, st);

    /* A stored ACL whose owner, group and everyone entries have the shape the
     * mode gives them is presented in Windows terms, whatever control flags
     * it was stored with. */
    st = open_status(c, "mode_0664.bin", MBT_GENERIC_READ | MBT_GENERIC_WRITE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ|GENERIC_WRITE of a 0664 file with the mode's entries "
          "stored -> 0x%08x", leg, st);

    /* A chmod rewrites a stored ACL's owner, group and everyone entries from
     * the mode; those are presented in Windows terms too. */
    st = open_status(c, "chmod_0664.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of a chmodded 0664 file with a stored ACL "
          "-> 0x%08x", leg, st);

    st = open_granted(c, "chmod_0664.bin", MBT_MAXIMUM_ALLOWED, &granted);
    CHECK(st == ST_SUCCESS && granted == (MASK_GENERIC_RW | pdel),
          "%s: MAXIMUM_ALLOWED of that file grants exactly "
          "FILE_GENERIC_READ|FILE_GENERIC_WRITE (0x%08x) -> 0x%08x", leg, granted, st);

    /* An overwrite needs FILE_WRITE_ATTRIBUTES and FILE_WRITE_EA whatever it
     * asks for (MS-FSA 2.1.5.1.2), so a DACL without FILE_WRITE_EA refuses
     * it. */
    {
        struct smb2_create_out co;

        st = smb2_create_opts(c, "dacl_noea.bin", MBT_FILE_OVERWRITE, MBT_FILE_WRITE_DATA,
                              MBT_FILE_SHARE_RWD, MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_ACCESS_DENIED,
              "%s: FILE_OVERWRITE with FILE_WRITE_DATA of a stored DACL granting "
              "FILE_WRITE_DATA|FILE_WRITE_ATTRIBUTES without FILE_WRITE_EA is "
              "denied -> 0x%08x",
              leg, st);

        /* MS-FSA adds those rights only when the unnamed stream of an existing
         * file is overwritten: a named stream's overwrite needs FILE_WRITE_DATA
         * alone, and leaves the file's attributes as they are. */
        st = stream_write_then_overwrite(c, "dacl_noea.bin:s", MBT_FILE_OVERWRITE, &st2);
        CHECK(st == ST_SUCCESS && st2 == ST_SUCCESS,
              "%s: FILE_OVERWRITE with FILE_WRITE_DATA of a named stream of that "
              "file -> write 0x%08x, overwrite 0x%08x", leg, st2, st);

        st = stream_write_then_overwrite(c, "ovw_0664.bin:s", MBT_FILE_OVERWRITE_IF, &st2);
        CHECK(st == ST_SUCCESS && st2 == ST_SUCCESS,
              "%s: FILE_OVERWRITE_IF with FILE_WRITE_DATA of a named stream of a "
              "group-writable 0664 file -> write 0x%08x, overwrite 0x%08x",
              leg, st2, st);

        /* Nor are they added when the open creates the file. */
        st = smb2_create_opts(c, "inh_dir\\new.bin", MBT_FILE_OVERWRITE_IF,
                              MBT_FILE_WRITE_DATA, MBT_FILE_SHARE_RWD,
                              MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS,
              "%s: FILE_OVERWRITE_IF with FILE_WRITE_DATA that creates a file "
              "whose inherited DACL grants FILE_WRITE_DATA without "
              "FILE_WRITE_ATTRIBUTES -> 0x%08x", leg, st);

        /* The open that creates a file is not required to hold SYNCHRONIZE,
         * so an inherited entry without it does not refuse that open. */
        st = smb2_create_opts(c, "inh_nosync\\ovw.bin", MBT_FILE_OVERWRITE_IF,
                              MBT_FILE_WRITE_DATA | MBT_SYNCHRONIZE, MBT_FILE_SHARE_RWD,
                              MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS,
              "%s: FILE_OVERWRITE_IF with FILE_WRITE_DATA|SYNCHRONIZE that "
              "creates a file whose inherited DACL grants no SYNCHRONIZE -> "
              "0x%08x", leg, st);

        st = smb2_create_opts(c, "inh_nosync\\oif.bin", MBT_FILE_OPEN_IF,
                              MBT_FILE_WRITE_DATA | MBT_SYNCHRONIZE, MBT_FILE_SHARE_RWD,
                              MBT_FILE_NON_DIRECTORY_FILE, NULL, &co);
        if (st == ST_SUCCESS) {
            smb2_close(c, co.file_id);
        }
        CHECK(st == ST_SUCCESS,
              "%s: FILE_OPEN_IF with FILE_WRITE_DATA|SYNCHRONIZE that creates "
              "a file whose inherited DACL grants no SYNCHRONIZE -> 0x%08x",
              leg, st);
    }
} /* run_checks */

#ifdef __linux__
/* The checks on a backend that hands the CREATE check no ACL, only the mode:
 * the owner of such an object is granted what it asks for, on Windows owner
 * rules, and the access it holds is the mode's, in Windows terms. */
static void
run_owner_checks(
    struct smb2_conn *c,
    const char       *leg)
{
    uint32_t st, granted;

    st = open_status(c, "mine_0644.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS, "%s: GENERIC_READ of own 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "mine_0644.bin", MBT_GENERIC_READ | MBT_GENERIC_WRITE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ|GENERIC_WRITE of own 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "mine_0444.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of own 0444 file (owner rules) -> 0x%08x", leg, st);

    st = open_granted(c, "mine_0644.bin", MBT_MAXIMUM_ALLOWED, &granted);
    CHECK(st == ST_SUCCESS && granted == MASK_OWNER_RW,
          "%s: MAXIMUM_ALLOWED of own 0644 file grants exactly "
          "FILE_GENERIC_READ|FILE_GENERIC_WRITE and the owner's WRITE_DAC, "
          "WRITE_OWNER and DELETE (0x%08x) -> 0x%08x", leg, granted, st);

    /* linux synthesizes an ACL from the mode when QUERY security asks for
     * one, and the server returns it in Windows terms. */
    check_mode_dacl(c, leg, "mine_0664.bin");
} /* run_owner_checks */

#ifdef HAVE_IO_URING
/* A caller other than the owner, on a backend that hands the CREATE check the
 * mode alone: it is judged against the mode in Windows terms, as on memfs. */
static void
run_nonowner_checks(
    struct smb2_conn *c,
    const char       *leg)
{
    uint32_t st, granted, maximal;
    uint32_t pdel = parent_delete(c);

    st = open_status(c, "root_0644.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of another user's 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "root_0644.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of another user's 0644 file is denied -> 0x%08x", leg, st);

    st = open_status(c, "root_0600.bin", MBT_GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_READ of another user's 0600 file is denied -> 0x%08x", leg, st);

    st = open_status(c, "root_0600.bin", MBT_FILE_READ_ATTRIBUTES | MBT_SYNCHRONIZE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: FILE_READ_ATTRIBUTES|SYNCHRONIZE of another user's 0600 file is "
          "denied -> 0x%08x", leg, st);

    st = open_status(c, "grp_0664.bin", MBT_GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of a group-writable 0664 file -> 0x%08x", leg, st);

    st = open_granted(c, "grp_0664.bin", MBT_MAXIMUM_ALLOWED, &granted);
    CHECK(st == ST_SUCCESS && granted == (MASK_GENERIC_RW | pdel),
          "%s: MAXIMUM_ALLOWED of a group-writable 0664 file grants exactly "
          "FILE_GENERIC_READ|FILE_GENERIC_WRITE (0x%08x) -> 0x%08x", leg, granted, st);

    st = open_maximal(c, "root_0644.bin", &maximal);
    CHECK(st == ST_SUCCESS && maximal == MASK_GENERIC_R,
          "%s: maximal access of another user's 0644 file is exactly "
          "FILE_GENERIC_READ (0x%08x) -> 0x%08x", leg, maximal, st);
} /* run_nonowner_checks */
#endif /* ifdef HAVE_IO_URING */
#endif /* ifdef __linux__ */

/* Seed `o` on the mounted share, connect, and run `checks`. */
static void
run_leg(
    struct smb2_env *env,
    const char *leg,
    const char *module,
    const char *module_path,
    const struct seed_obj *o,
    int no,
    void ( *checks )(struct smb2_conn *, const char *))
{
    struct smb2_conn *c;

    seed_objects(env, module, module_path, o, no);

    c = smb2_conn_open(env);
    smb2_handshake_as(c, SMB2W_USER, SMB2W_PASSWORD);
    checks(c, leg);
    smb2_conn_disconnect(c);
} /* run_leg */

#ifdef __linux__
/* A passthrough leg: `module` over a scratch directory under the working
 * directory (the build tree, where file handles work), seeded with `o` as
 * root. */
static void
passthrough_leg(
    struct smb2_env *env,
    const char *module,
    const struct seed_obj *o,
    int no,
    void ( *checks )(struct smb2_conn *, const char *))
{
    char tmpl[] = "smb2_modeacl_pt_XXXXXX";
    char dir[PATH_MAX];
    char path[PATH_MAX + 64];

    if (!mkdtemp(tmpl) || !realpath(tmpl, dir)) {
        fprintf(stderr, "cannot make the %s leg's directory\n", module);
        smb2c_exit(1);
    }

    if (chimera_server_mount(env->server, "share", module, dir, NULL) != 0) {
        fprintf(stderr, "cannot mount %s on the %s backend\n", dir, module);
        smb2c_exit(1);
    }
    chimera_server_create_share(env->server, "share", "share", 0);

    run_leg(env, module, module, dir, o, no, checks);

    chimera_server_remove_share(env->server, "share");
    chimera_server_unmount(env->server, "share");

    for (int i = 0; i < no; i++) {
        snprintf(path, sizeof(path), "%s/%s", dir, o[i].name);
        unlink(path);
    }
    rmdir(dir);
} /* passthrough_leg */
#endif /* ifdef __linux__ */

#ifdef HAVE_CAIRN
/* cairn stores an ACL a client sets, and its reply to a named-stream open
 * carries the file's mode but not that ACL.  The stream's open is judged, and
 * granted, against the file's stored ACL nonetheless.  The stored ACL denies
 * the caller FILE_WRITE_DATA and FILE_WRITE_ATTRIBUTES by name and gives
 * everyone full control, so the mode, which a named entry does not reach,
 * would grant the caller both. */
/* *INDENT-OFF* */
static const struct seed_obj cairn_objs[] = {
    /* name             dir  perm  uid        gid        acl                acl_mask    chmod */
    { "cdeny.bin",      0,   0666, 0,         0,         SEED_ACL_DENY_USER,0x00000102, 0     },
};
/* *INDENT-ON* */

static void
run_cairn_checks(
    struct smb2_conn *c,
    const char       *leg)
{
    uint32_t st, st2, granted = 0;

    st = stream_create_set_basic(c, "cdeny.bin:x", &st2);
    CHECK(st == ST_SUCCESS,
          "%s: FILE_CREATE of a new named stream of a file whose stored ACL "
          "denies the caller FILE_WRITE_DATA|FILE_WRITE_ATTRIBUTES -> 0x%08x",
          leg, st);
    CHECK(st2 == ST_ACCESS_DENIED,
          "%s: SET_INFO FileBasicInformation through that stream's "
          "FILE_WRITE_ATTRIBUTES handle is denied -> 0x%08x", leg, st2);

    st = open_granted(c, "cdeny.bin:x", MBT_MAXIMUM_ALLOWED, &granted);
    CHECK(st == ST_SUCCESS &&
          !(granted & (MBT_FILE_WRITE_DATA | MBT_FILE_WRITE_ATTRIBUTES)),
          "%s: MAXIMUM_ALLOWED of that stream is granted neither "
          "FILE_WRITE_DATA nor FILE_WRITE_ATTRIBUTES -> 0x%08x, granted 0x%08x",
          leg, st, granted);
} /* run_cairn_checks */

/* The cairn leg: a filesystem of its own, mounted as the share. */
static void
cairn_leg(struct smb2_env *env)
{
    if (chimera_server_mkfs(env->server, "cairn", "cairnfs", NULL) != 0 ||
        chimera_server_mount(env->server, "share", "cairn", "cairnfs", NULL) != 0) {
        fprintf(stderr, "cannot make and mount a cairn filesystem\n");
        smb2c_exit(1);
    }
    chimera_server_create_share(env->server, "share", "share", 0);

    run_leg(env, "cairn", "cairn", "cairnfs", cairn_objs,
            (int) (sizeof(cairn_objs) / sizeof(cairn_objs[0])), run_cairn_checks);

    chimera_server_remove_share(env->server, "share");
    chimera_server_unmount(env->server, "share");
} /* cairn_leg */
#endif /* ifdef HAVE_CAIRN */

/* Exit status that tells ctest the run was skipped (SKIP_RETURN_CODE). */
#define PROBE_SKIPPED 77

/* With no argument: memfs, which hands the CREATE check an ACL synthesized
 * from the mode, and cairn.  With "passthrough": the backends that hand it the
 * mode alone.  The linux leg opens only the caller's own objects; the io_uring
 * leg opens other users' objects, and seeding those needs root, so this mode
 * runs only on Linux, as root, and reports a skip otherwise. */
int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env                       env;
    static const struct smb2_wire_profile w = {
        .name = "modeacl", .max_dialect = 0x0300, .ntlmv2 = 1
    };
    /* Named streams, for the stream rows; memfs and cairn implement them. */
    static const struct smb2_env_opts     opts = {
        .named_streams = 1,
#ifdef HAVE_CAIRN
        .cairn         = 1,
#endif /* ifdef HAVE_CAIRN */
    };
    int                                   passthrough =
        argc > 1 && strcmp(argv[1], "passthrough") == 0;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (passthrough) {
#ifdef __linux__
        if (geteuid() != 0) {
            printf("# skipped: seeding the passthrough legs' objects needs root\n");
            return PROBE_SKIPPED;
        }
#else  /* ifdef __linux__ */
        printf("# skipped: the passthrough backends are built only on Linux\n");
        return PROBE_SKIPPED;
#endif /* ifdef __linux__ */
    }

    smb2_env_open_wire(&env, &opts, &w);

    if (passthrough) {
#ifdef __linux__
        passthrough_leg(&env, "linux", own_objs,
                        (int) (sizeof(own_objs) / sizeof(own_objs[0])),
                        run_owner_checks);
#ifdef HAVE_IO_URING
        passthrough_leg(&env, "io_uring", other_objs,
                        (int) (sizeof(other_objs) / sizeof(other_objs[0])),
                        run_nonowner_checks);
#endif /* ifdef HAVE_IO_URING */
#endif /* ifdef __linux__ */
    } else {
        /* memfs hands the check an ACL synthesized from the mode. */
        smb2_env_fs_setup(&env, SEED_FS);
        run_leg(&env, "memfs", "memfs", SEED_FS, objs,
                (int) (sizeof(objs) / sizeof(objs[0])), run_checks);
        smb2_env_fs_teardown(&env, SEED_FS);

#ifdef HAVE_CAIRN
        cairn_leg(&env);
#endif /* ifdef HAVE_CAIRN */
    }

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d mode-only generic-access check(s) FAILED\n", failures);
        return 1;
    }

    printf("# all mode-only generic-access checks passed\n");
    return 0;
} /* main */
