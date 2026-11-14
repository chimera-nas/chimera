/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Generic-access CREATE of objects that carry no stored ACL.
 *
 * An object made over NFS or S3, or seeded by the server, has only a POSIX
 * mode.  The SMB CREATE access check evaluates it against the ACL derived from
 * that mode (chimera_acl_from_mode), whose entries never grant SYNCHRONIZE.
 * Every NT generic right expands to a mask that includes SYNCHRONIZE, so the
 * check has to leave that bit out of what it requires, or a GENERIC_READ open
 * -- what the Linux cifs client sends for O_RDONLY -- is refused to every
 * authenticated user except root, the object's owner included.
 *
 * GENERIC_WRITE, which the cifs client sends for O_WRONLY and O_RDWR, has the
 * same shape: FILE_GENERIC_WRITE includes FILE_WRITE_ATTRIBUTES, which the
 * mode-derived ACL grants to the owner only.  The check does not require it
 * for a generic write, and the handle reports it only when the object grants
 * it.
 *
 * No SMB client can make such an object: a CREATE stamps the default DACL,
 * which grants full control.  So this probe makes them through the VFS as the
 * server identity, as the NFS and S3 servers would, and then opens them over
 * SMB as an ordinary account.  The negative cases keep the mode authoritative:
 * a generic open must still be refused when the mode bits deny the access.
 */

#include "smb2_mbt_common.h"

#include <sys/stat.h>

#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_cred.h"

static int failures = 0;

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

#define GENERIC_READ    0x80000000u
#define GENERIC_WRITE   0x40000000u
#define GENERIC_EXECUTE 0x20000000u

#define SEED_FS         "fs0"
#define SEED_MOUNT      "__modeacl_seed"

/* ---- seeding mode-only objects through the VFS -------------------------- */

struct seed_obj {
    const char *name;
    int         dir;    /* a directory, else a regular file */
    uint32_t    perm;   /* permission bits */
    uint32_t    uid;
    uint32_t    gid;
};

struct seed_ctx {
    struct chimera_vfs_thread      *thread;
    struct chimera_vfs_open_handle *root_oh;
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

    if (oh) {
        chimera_vfs_release(ctx->thread, oh);
    }
    if (error_code != CHIMERA_VFS_OK) {
        seed_finish(ctx, error_code);
        return;
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
    const struct seed_obj   *o;
    struct chimera_vfs_attrs set_attr;

    if (ctx->next == ctx->nobjs) {
        seed_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    o = &ctx->objs[ctx->next++];

    memset(&set_attr, 0, sizeof(set_attr));
    set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    set_attr.va_mode = (o->dir ? S_IFDIR : S_IFREG) | o->perm;
    set_attr.va_uid  = o->uid;
    set_attr.va_gid  = o->gid;

    if (o->dir) {
        chimera_vfs_mkdir_at(ctx->thread, chimera_vfs_get_server_cred(),
                             ctx->root_oh, o->name, strlen(o->name), &set_attr,
                             CHIMERA_VFS_ATTR_FH, 0, 0, seed_dir_cb, ctx);
    } else {
        chimera_vfs_open_at(ctx->thread, chimera_vfs_get_server_cred(),
                            ctx->root_oh, o->name, strlen(o->name),
                            CHIMERA_VFS_OPEN_CREATE, &set_attr,
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

    chimera_vfs_open_fh(thread, chimera_vfs_get_server_cred(),
                        m->root_fh, m->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_PATH,
                        seed_root_cb, ctx);
} /* seed_mounted_cb */

/* Create `objs` in the root of `module_path` on `module` (the share's root),
 * as the server identity, with the given mode and ownership and no ACL. */
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
/* uncrustify 0.78.1 does not converge on this aligned table, so it is guarded. */
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
};
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

/* The checks on a backend that hands the CREATE check an ACL synthesized
 * from the mode, as SMB2W_USER (uid/gid 1000). */
static void
run_checks(
    struct smb2_conn *c,
    const char       *leg)
{
    uint32_t st, granted;

    /* Another user's file whose "other" bits allow reading. */
    st = open_status(c, "root_0644.bin", GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of another user's 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "root_0644.bin", GENERIC_EXECUTE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_EXECUTE of another user's 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "root_0644.bin", GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of another user's 0644 file is denied -> 0x%08x", leg, st);

    st = open_status(c, "root_0600.bin", GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_READ of another user's 0600 file is denied -> 0x%08x", leg, st);

    /* The caller's own file: the derived ACL is present, so the owner shortcut
     * does not apply and the mode-derived owner entry decides. */
    st = open_status(c, "mine_0644.bin", GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS, "%s: GENERIC_READ of own 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "mine_0644.bin", GENERIC_READ | GENERIC_WRITE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ|GENERIC_WRITE of own 0644 file -> 0x%08x", leg, st);

    st = open_status(c, "mine_0444.bin", GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of own 0444 file is denied -> 0x%08x", leg, st);

    /* A directory: GENERIC_READ is what a client sends to list it. */
    st = open_status(c, "root_dir", GENERIC_READ, MBT_FILE_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of another user's 0755 directory -> 0x%08x", leg, st);

    /* Writers that are not the owner: the group class and "other". */
    st = open_status(c, "grp_0664.bin", GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of a group-writable 0664 file -> 0x%08x", leg, st);

    st = open_status(c, "grp_0664.bin", GENERIC_READ | GENERIC_WRITE,
                     MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ|GENERIC_WRITE of a group-writable 0664 file -> 0x%08x", leg, st);

    st = open_status(c, "root_0666.bin", GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_WRITE of another user's 0666 file -> 0x%08x", leg, st);

    /* A generic-write open does not require WRITE_ATTRIBUTES, so the handle
     * must not claim it unless the object grants it: the mode-derived ACL
     * grants it to the owner only. */
    st = open_granted(c, "grp_0664.bin", GENERIC_WRITE, &granted);
    CHECK(st == ST_SUCCESS && (granted & 0x00000002u) &&
          !(granted & 0x00000100u),
          "%s: group writer's GENERIC_WRITE handle has WRITE_DATA, not "
          "WRITE_ATTRIBUTES (0x%08x) -> 0x%08x", leg, granted, st);

    st = open_granted(c, "mine_0644.bin", GENERIC_WRITE, &granted);
    CHECK(st == ST_SUCCESS && (granted & 0x00000102u) == 0x00000102u,
          "%s: owner's GENERIC_WRITE handle has WRITE_DATA and WRITE_ATTRIBUTES "
          "(0x%08x) -> 0x%08x", leg, granted, st);

    st = open_status(c, "grp_0640.bin", GENERIC_READ, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_SUCCESS,
          "%s: GENERIC_READ of a group-readable 0640 file -> 0x%08x", leg, st);

    st = open_status(c, "grp_0640.bin", GENERIC_WRITE, MBT_FILE_NON_DIRECTORY_FILE);
    CHECK(st == ST_ACCESS_DENIED,
          "%s: GENERIC_WRITE of a group-readable 0640 file is denied -> 0x%08x", leg, st);
} /* run_checks */

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

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env                       env;
    static const struct smb2_wire_profile w = {
        .name = "modeacl", .max_dialect = 0x0300, .ntlmv2 = 1
    };

    (void) argc;
    (void) argv;

    setvbuf(stdout, NULL, _IONBF, 0);

    smb2_env_open_wire(&env, NULL, &w);

    smb2_env_fs_setup(&env, SEED_FS);
    run_leg(&env, "memfs", "memfs", SEED_FS, objs,
            (int) (sizeof(objs) / sizeof(objs[0])), run_checks);
    smb2_env_fs_teardown(&env, SEED_FS);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d mode-only generic-access check(s) FAILED\n", failures);
        return 1;
    }

    printf("# all mode-only generic-access checks passed\n");
    return 0;
} /* main */
