// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * ".." at the root of an export whose path is a subdirectory of a mount.
 *
 * An export confines a client to its subtree, so ".." from the export root
 * must stay on the root, as it does at the root of a filesystem: handing back
 * the parent directory, wrapped in the export's own id, lets a client walk out
 * of the subtree and then create, remove or read above it.
 *
 * The probe exports directories of /fs0 under their own names and measures,
 * from the export root and from below it:
 *   - NFSv3 LOOKUP ".." (a plain path, one with a trailing slash and a deep
 *     one), and that it answers NFS3ERR_STALE where the export root cannot be
 *     told: an export whose directory has since been renamed away, or one
 *     that has since been removed;
 *   - the ".." entry in NFSv3 READDIRPLUS, which carries neither attributes
 *     nor a handle at an export root, whether that is a subdirectory or the
 *     root of a mount;
 *   - that nothing can be written above the export through the ".." handle;
 *   - NFSv3 MOUNT of a path below the export name, which may not hold ".",
 *     ".." or a symlink;
 *   - NFSv3 names that are not one component ("../x", "a/b", "", "a\0b"),
 *     refused by every procedure that takes a name before a backend can
 *     resolve them as a path;
 *   - NFSv3 "." and ".." as the name of an entry to create, remove or
 *     rename, refused before a backend can store one or answer with the
 *     parent's handle;
 *   - RENAME from one export into another, refused on NFSv3 and NFSv4: it
 *     would let a client move a directory out of an export and walk up from
 *     the handle it still holds;
 *   - NFSv4 LOOKUPP, which at the root of a subdirectory export must answer
 *     with the namespace root, as it does at the root of a mount export: the
 *     pseudo-root, or the root of the "/" export when there is one;
 *   - NFSv4 READDIR of the pseudo-root, which lists an export whose path goes
 *     through a symlink.
 *
 * Every cell runs even after a failure so one run shows the whole picture.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define OTHER_UID 1000

static int failures;

static void
expect(
    const char *label,
    int         ok,
    const char *detail)
{
    if (!ok) {
        printf("  FAIL %s: %s\n", label, detail);
        failures++;
    } else {
        printf("  ok  %s: %s\n", label, detail);
    }
} /* expect */

/* ---- NFSv3 helpers -------------------------------------------------------- */

/* LOOKUP `name` in `dir`: the status, and the handle in `*out` on success. */
static uint32_t
lookup_fh(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    const char          *name,
    struct mbt_fh       *out)
{
    struct mbt_result *res;

    memset(out, 0, sizeof(*out));
    res = mbt_lookup(env, dir, name, strlen(name));
    if (res->status == NFS3_OK) {
        *out = res->obj_fh;
    }
    return res->status;
} /* lookup_fh */

/* LOOKUP ".." in `dir` answers NFS3_OK with the handle `want`. */
static void
expect_dotdot_is(
    struct mbt_env      *env,
    const char          *label,
    const struct mbt_fh *dir,
    const struct mbt_fh *want,
    const char          *wantname)
{
    struct mbt_fh got;
    uint32_t      status;
    char          buf[160];

    status = lookup_fh(env, dir, "..", &got);
    snprintf(buf, sizeof(buf), "LOOKUP \"..\": status %u, handle %s %s",
             status, got.has && mbt_fh_eq(&got, want) ? "is" : "is not",
             wantname);
    expect(label, status == NFS3_OK && got.has && mbt_fh_eq(&got, want), buf);
} /* expect_dotdot_is */

/* The entry for `name` in the listing in `res`, or NULL. */
static const struct mbt_entry *
find_entry(
    const struct mbt_result *res,
    const char              *name)
{
    int i;

    for (i = 0; i < res->nentries; i++) {
        if (res->entries[i].name_len == strlen(name) &&
            memcmp(res->entries[i].name, name, res->entries[i].name_len) == 0) {
            return &res->entries[i];
        }
    }
    return NULL;
} /* find_entry */

/* READDIRPLUS of `dir` lists ".." bare: no attributes and no handle. */
static void
expect_rdplus_dotdot_bare(
    struct mbt_env      *env,
    const char          *label,
    const struct mbt_fh *dir)
{
    struct mbt_result      *res;
    const struct mbt_entry *e;
    char                    buf[160];

    res = mbt_readdirplus(env, dir);
    e   = res->status == NFS3_OK ? find_entry(res, "..") : NULL;
    snprintf(buf, sizeof(buf), "READDIRPLUS status %u, \"..\" %s, "
             "attributes %d, handle %d", res->status,
             e ? "listed" : "missing", e ? e->attrs.has : -1,
             e ? e->fh.has : -1);
    expect(label, e && !e->attrs.has && !e->fh.has, buf);
} /* expect_rdplus_dotdot_bare */

static struct mbt_fh
make_dir(
    struct mbt_env      *env,
    const struct mbt_fh *parent,
    const char          *name)
{
    struct mbt_result *res;

    res = mbt_mkdir(env, parent, name, strlen(name), 0755);
    if (res->status != NFS3_OK || !res->obj_fh.has) {
        fprintf(stderr, "MKDIR %s failed: %u\n", name, res->status);
        exit(2);
    }
    return res->obj_fh;
} /* make_dir */

static struct mbt_fh
mount_export(
    struct mbt_env *env,
    const char     *path)
{
    struct mbt_result *res;

    res = mbt_mnt(env, path);
    if (res->rpc_err || res->status != MNT3_OK || !res->obj_fh.has) {
        fprintf(stderr, "MNT %s failed: %u\n", path, res->status);
        exit(2);
    }
    return res->obj_fh;
} /* mount_export */

/* MNT `path`: the status, and the handle in `*out` on success. */
static uint32_t
mnt_fh(
    struct mbt_env *env,
    const char     *path,
    struct mbt_fh  *out)
{
    struct mbt_result *res;

    memset(out, 0, sizeof(*out));
    res = mbt_mnt(env, path);
    if (res->status == MNT3_OK) {
        *out = res->obj_fh;
    }
    return res->status;
} /* mnt_fh */

/* `what` answered `status`; the cell holds when that is `want`. */
static void
expect_status(
    const char *label,
    const char *what,
    uint32_t    status,
    uint32_t    want)
{
    char buf[160];

    snprintf(buf, sizeof(buf), "%s: status %u, expected %u", what, status,
             want);
    expect(label, status == want, buf);
} /* expect_status */

/* One "v3-dot-<proc>" cell, "-self" appended when the name is ".". */
static void
dot_cell(
    const char *proc,
    const char *self,
    const char *verb,
    const char *dot,
    uint32_t    status,
    uint32_t    want)
{
    char label[40];
    char what[40];

    snprintf(label, sizeof(label), "v3-dot-%s%s", proc, self);
    snprintf(what, sizeof(what), "%s \"%s\"", verb, dot);
    expect_status(label, what, status, want);
} /* dot_cell */

/* One "v3-name-<proc>-<kind>" cell: the name is refused with NFS3ERR_ACCES. */
static void
name_cell(
    const char *proc,
    const char *kind,
    uint32_t    status)
{
    char label[40];
    char what[60];

    snprintf(label, sizeof(label), "v3-name-%s-%s", proc, kind);
    snprintf(what, sizeof(what), "%s of a name that is %s", proc, kind);
    expect_status(label, what, status, NFS3ERR_ACCES);
} /* name_cell */

/* Every procedure that takes a filename3 is handed `name` in `dir`.  `dir`
 * holds a file "keep", the source of the RENAME to `name`; `file` is the
 * object LINK would name. */
static void
name_cells(
    struct mbt_env      *env,
    const char          *kind,
    const char          *name,
    uint32_t             len,
    const struct mbt_fh *dir,
    const struct mbt_fh *file)
{
    struct mbt_result *res;

    res = mbt_lookup(env, dir, name, len);
    name_cell("lookup", kind, res->status);
    res = mbt_create(env, dir, name, len, UNCHECKED, 0644, NULL);
    name_cell("create", kind, res->status);
    res = mbt_mkdir(env, dir, name, len, 0755);
    name_cell("mkdir", kind, res->status);
    res = mbt_symlink(env, dir, name, len, "x", 0777);
    name_cell("symlink", kind, res->status);
    res = mbt_mknod(env, dir, name, len, NF3FIFO, 0644);
    name_cell("mknod", kind, res->status);
    res = mbt_link(env, file, dir, name, len);
    name_cell("link", kind, res->status);
    res = mbt_remove(env, dir, name, len);
    name_cell("remove", kind, res->status);
    res = mbt_rmdir(env, dir, name, len);
    name_cell("rmdir", kind, res->status);
    res = mbt_rename(env, dir, name, len, dir, "moved", 5);
    name_cell("rename-from", kind, res->status);
    res = mbt_rename(env, dir, "keep", 4, dir, name, len);
    name_cell("rename-to", kind, res->status);
} /* name_cells */

static void
make_symlink(
    struct mbt_env      *env,
    const struct mbt_fh *parent,
    const char          *name,
    const char          *target)
{
    struct mbt_result *res;

    res = mbt_symlink(env, parent, name, strlen(name), target, 0777);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "SYMLINK %s -> %s failed: %u\n", name, target,
                res->status);
        exit(2);
    }
} /* make_symlink */

/* ---- NFSv4.0 compounds ---------------------------------------------------- */

#define PREP_MAX_ENTRIES 16

struct prep {
    int           done;
    int           rpc_err;
    uint32_t      status;
    struct mbt_fh fh;                /* last GETFH */
    int           nentries;          /* entries of the last READDIR */
    char          name[PREP_MAX_ENTRIES][32];
    int           mofid[PREP_MAX_ENTRIES]; /* entry has mounted_on_fileid */
};

static void
prep_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct prep *p = private_data;
    uint32_t     i;

    (void) evpl;
    (void) verf;
    p->rpc_err = status;
    if (status == 0) {
        p->status = reply->status;
        for (i = 0; i < reply->num_resarray; i++) {
            const struct nfs_resop4 *r = &reply->resarray[i];

            if (r->resop == OP_GETFH && r->opgetfh.status == NFS4_OK) {
                mbt_copy_fh(&p->fh, &r->opgetfh.resok4.object);
            }
            if (r->resop == OP_READDIR && r->opreaddir.status == NFS4_OK) {
                const struct entry4 *e;

                p->nentries = 0;
                for (e = r->opreaddir.resok4.reply.entries;
                     e && p->nentries < PREP_MAX_ENTRIES; e = e->nextentry) {
                    const struct fattr4 *fa = &e->attrs;
                    uint32_t             n  = e->name.len < 31 ? e->name.len : 31;

                    memcpy(p->name[p->nentries], e->name.data, n);
                    p->name[p->nentries][n] = '\0';
                    p->mofid[p->nentries]   = fa->num_attrmask > 1 &&
                        (fa->attrmask[1] &
                         (1U << (FATTR4_MOUNTED_ON_FILEID - 32)));
                    p->nentries++;
                }
            }
        }
    }
    p->done = 1;
} /* prep_cb */

/* One NFSv4.0 compound on the harness connection.  PUTROOTFH, PUTFH, LOOKUP,
 * LOOKUPP, GETFH, SAVEFH, RENAME and READDIR need no client id, so no
 * SETCLIENTID is sent. */
static struct prep
compound(
    struct mbt_env    *env,
    struct nfs_argop4 *ops,
    int                nops)
{
    struct COMPOUND4args args;
    struct prep          p;

    memset(&p, 0, sizeof(p));
    memset(&args, 0, sizeof(args));
    args.minorversion = 0;
    args.argarray     = ops;
    args.num_argarray = (uint32_t) nops;

    env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2, env->evpl,
                                            env->nfs_conn, &env->cred, &args,
                                            0, 0, NULL, 0, 0, prep_cb, &p);
    while (!p.done) {
        evpl_continue(env->evpl);
    }
    if (p.rpc_err) {
        fprintf(stderr, "transport error %d\n", p.rpc_err);
        exit(2);
    }
    return p;
} /* compound */

static struct nfs_argop4
op_simple(uint32_t argop)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = argop;
    return a;
} /* op_simple */

static struct nfs_argop4
op_putfh(const struct mbt_fh *fh)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop               = OP_PUTFH;
    a.opputfh.object.data = (void *) fh->data;
    a.opputfh.object.len  = fh->len;
    return a;
} /* op_putfh */

static struct nfs_argop4
op_lookup(const char *name)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                 = OP_LOOKUP;
    a.oplookup.objname.data = (void *) name;
    a.oplookup.objname.len  = (uint32_t) strlen(name);
    return a;
} /* op_lookup */

static struct nfs_argop4
op_rename(
    const char *from,
    const char *to)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                 = OP_RENAME;
    a.oprename.oldname.data = (void *) from;
    a.oprename.oldname.len  = (uint32_t) strlen(from);
    a.oprename.newname.data = (void *) to;
    a.oprename.newname.len  = (uint32_t) strlen(to);
    return a;
} /* op_rename */

/* READDIR from the first entry, asking for what the Linux client asks for in
 * every listing: rdattr_error and mounted_on_fileid. */
static struct nfs_argop4
op_readdir(void)
{
    static uint32_t   mask[2];
    struct nfs_argop4 a;

    mask[0] = 1U << FATTR4_RDATTR_ERROR;
    mask[1] = 1U << (FATTR4_MOUNTED_ON_FILEID - 32);

    memset(&a, 0, sizeof(a));
    a.argop                      = OP_READDIR;
    a.opreaddir.cookie           = 0;
    a.opreaddir.dircount         = 65536;
    a.opreaddir.maxcount         = 1048576;
    a.opreaddir.attr_request     = mask;
    a.opreaddir.num_attr_request = 2;
    return a;
} /* op_readdir */

/* The index of the READDIR entry `name` in `p`, or -1. */
static int
prep_entry(
    const struct prep *p,
    const char        *name)
{
    int i;

    for (i = 0; i < p->nentries; i++) {
        if (strcmp(p->name[i], name) == 0) {
            return i;
        }
    }
    return -1;
} /* prep_entry */

/* PUTROOTFH; LOOKUP `name` (when not NULL); GETFH.  Exits on failure: the
 * handle is the setup of a cell, not a cell. */
static struct mbt_fh
v4_handle(
    struct mbt_env *env,
    const char     *name)
{
    struct nfs_argop4 ops[3];
    struct prep       p;
    int               n = 0;

    ops[n++] = op_simple(OP_PUTROOTFH);
    if (name) {
        ops[n++] = op_lookup(name);
    }
    ops[n++] = op_simple(OP_GETFH);
    p        = compound(env, ops, n);
    if (p.status != NFS4_OK || !p.fh.has) {
        fprintf(stderr, "NFSv4 handle of %s failed: %u\n",
                name ? name : "pseudo-root", p.status);
        exit(2);
    }
    return p.fh;
} /* v4_handle */

/* LOOKUPP from `dir` (after LOOKUP `inside`, when not NULL) answers NFS4_OK
 * with the handle `want`. */
static void
expect_lookupp_is(
    struct mbt_env      *env,
    const char          *label,
    const struct mbt_fh *dir,
    const char          *inside,
    const struct mbt_fh *want,
    const char          *wantname)
{
    struct nfs_argop4 ops[4];
    struct prep       p;
    char              buf[160];
    int               n = 0, got_status, ok;

    ops[n++] = op_putfh(dir);
    if (inside) {
        ops[n++] = op_lookup(inside);
    }
    ops[n++]   = op_simple(OP_LOOKUPP);
    ops[n++]   = op_simple(OP_GETFH);
    p          = compound(env, ops, n);
    got_status = p.status != NFS4_OK ? (int) p.status : 0;
    ok         = p.status == NFS4_OK && p.fh.has && mbt_fh_eq(&p.fh, want);
    snprintf(buf, sizeof(buf), "LOOKUPP: compound status %d, handle %s %s",
             got_status, p.fh.has && mbt_fh_eq(&p.fh, want) ? "is" : "is not",
             wantname);
    expect(label, ok, buf);
} /* expect_lookupp_is */

/* RENAME `from` in `fromdir` to `to` in `todir` answers `want`. */
static void
expect_rename4(
    struct mbt_env      *env,
    const char          *label,
    const struct mbt_fh *fromdir,
    const char          *from,
    const struct mbt_fh *todir,
    const char          *to,
    uint32_t             want)
{
    struct nfs_argop4 ops[4];
    struct prep       p;

    ops[0] = op_putfh(fromdir);
    ops[1] = op_simple(OP_SAVEFH);
    ops[2] = op_putfh(todir);
    ops[3] = op_rename(from, to);
    p      = compound(env, ops, 4);
    expect_status(label, "PUTFH SAVEFH PUTFH RENAME", p.status, want);
} /* expect_rename4 */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env         *env = calloc(1, sizeof(*env));
    struct mbt_env_opts     opts;
    const char             *backend = "memfs";
    struct mbt_result      *res;
    struct mbt_fh           fs_root, sub, in, sl, a, up, g, scratch;
    struct mbt_fh           pseudo, s4, d4, want, m, t1, dotf;
    struct mbt_fh           dp, kid, g4, fs4, nm, nma;
    struct nfs_argop4       ops[2];
    struct prep             p;
    const char             *dot, *self;
    uint32_t                dotlen;
    const struct mbt_entry *e;
    uint32_t                status;
    char                    buf[200];
    int                     i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-b") == 0 && i + 1 < argc) {
            backend = argv[++i];
        }
    }
    mbt_watchdog_arm(60);

    memset(&opts, 0, sizeof(opts));
    opts.module = backend;
    mbt_env_start_opts(env, &opts);

    printf("backend: %s\n", backend);

    fs_root = mount_export(env, "/fs0");

    /* Setup as root: dirA/inside/deep, dirB and dirC beside dirA, and lnk, a
     * symlink to dirA. */
    mbt_cred_set_uid(env, 0);
    a  = make_dir(env, &fs_root, "dirA");
    in = make_dir(env, &a, "inside");
    (void) make_dir(env, &in, "deep");
    (void) make_dir(env, &fs_root, "dirB");
    (void) make_dir(env, &fs_root, "dirC");
    make_symlink(env, &fs_root, "lnk", "dirA");

    if (chimera_server_create_export(env->server, "/sub", "/fs0/dirA", 0,
                                     NULL) ||
        chimera_server_create_export(env->server, "/slash", "/fs0/dirA/", 0,
                                     NULL) ||
        chimera_server_create_export(env->server, "/deep",
                                     "/fs0/dirA/inside/deep", 0, NULL) ||
        chimera_server_create_export(env->server, "/gone", "/fs0/dirC", 0,
                                     NULL) ||
        chimera_server_create_export(env->server, "/vialink",
                                     "/fs0/lnk/inside", 0, NULL)) {
        fprintf(stderr, "chimera_server_create_export failed\n");
        return 2;
    }

    sub = mount_export(env, "/sub");
    sl  = mount_export(env, "/slash");
    dp  = mount_export(env, "/deep");
    g   = mount_export(env, "/gone");
    kid = make_dir(env, &g, "kid");
    g4  = v4_handle(env, "gone");

    printf("NFSv4 READDIR of the pseudo-root\n");

    /* The listing resolves the path of every export, as LOOKUP of its entry
     * does, following a symlink on the way.  It runs before dirC is renamed
     * away below: one export whose path no longer resolves fails it. */
    ops[0] = op_simple(OP_PUTROOTFH);
    ops[1] = op_readdir();
    p      = compound(env, ops, 2);
    i      = prep_entry(&p, "vialink");
    snprintf(buf, sizeof(buf), "PUTROOTFH READDIR: compound status %u, "
             "vialink %s, mounted_on_fileid %s", p.status,
             i >= 0 ? "listed" : "missing",
             i >= 0 && p.mofid[i] ? "returned" : "not returned");
    expect("v4-pseudo-readdir-symlink-path",
           p.status == NFS4_OK && i >= 0 && p.mofid[i], buf);

    printf("NFSv3 LOOKUP \"..\"\n");

    expect_dotdot_is(env, "v3-sub-root", &sub, &sub, "sub");

    status = lookup_fh(env, &sub, "inside", &in);
    if (status != NFS3_OK) {
        fprintf(stderr, "LOOKUP inside in /sub failed: %u\n", status);
        return 2;
    }
    expect_dotdot_is(env, "v3-sub-inside", &in, &sub, "sub");

    expect_dotdot_is(env, "v3-slash-root", &sl, &sl, "slash");
    expect_dotdot_is(env, "v3-deep-root", &dp, &dp, "deep");

    printf("NFSv3 READDIRPLUS \"..\"\n");

    /* At the export root ".." names the directory above the export: it is
     * listed, with neither that directory's attributes nor its handle. */
    expect_rdplus_dotdot_bare(env, "v3-rdplus-root", &sub);

    res = mbt_readdirplus(env, &in);
    e   = res->status == NFS3_OK ? find_entry(res, "..") : NULL;
    snprintf(buf, sizeof(buf), "READDIRPLUS status %u, \"..\" %s, handle %d, "
             "%s sub", res->status, e ? "listed" : "missing",
             e ? e->fh.has : -1,
             e && e->fh.has && mbt_fh_eq(&e->fh, &sub) ? "is" : "is not");
    /* Below the export root ".." is an ordinary entry: every backend gives its
     * handle, and it is the export root. */
    expect("v3-rdplus-inside", e && e->fh.has && mbt_fh_eq(&e->fh, &sub), buf);

    /* So it is at the root of a mount: what a backend holds for ".." there
     * is not a directory of the export (a passthrough backend answers with
     * the host directory above the mount). */
    expect_rdplus_dotdot_bare(env, "v3-rdplus-mount-root", &fs_root);

    /* dirA is the root of /sub, but a handle minted under the whole-mount
     * export is not at that export's root: its ".." is the mount root. */
    status = lookup_fh(env, &fs_root, "dirA", &a);
    if (status != NFS3_OK) {
        fprintf(stderr, "LOOKUP dirA failed: %u\n", status);
        return 2;
    }
    res = mbt_readdirplus(env, &a);
    e   = res->status == NFS3_OK ? find_entry(res, "..") : NULL;
    snprintf(buf, sizeof(buf), "READDIRPLUS status %u, \"..\" %s, handle %d, "
             "%s fs_root", res->status, e ? "listed" : "missing",
             e ? e->fh.has : -1,
             e && e->fh.has && mbt_fh_eq(&e->fh, &fs_root) ? "is" : "is not");
    expect("v3-rdplus-outer-dirA",
           e && e->fh.has && mbt_fh_eq(&e->fh, &fs_root), buf);

    printf("NFSv3 through the whole-mount export, and above the export\n");

    status = lookup_fh(env, &fs_root, "dirA", &a);
    if (status != NFS3_OK) {
        fprintf(stderr, "LOOKUP dirA failed: %u\n", status);
        return 2;
    }
    expect_dotdot_is(env, "v3-outer-walks-up", &a, &fs_root, "fs_root");

    /* Whatever ".." from the export root returns, nothing may be created above
     * the export through it. */
    status = lookup_fh(env, &sub, "..", &up);
    if (status == NFS3_OK && up.has) {
        mbt_mkdir(env, &up, "escape", 6, 0755);
    }
    status = lookup_fh(env, &fs_root, "escape", &scratch);
    snprintf(buf, sizeof(buf), "LOOKUP escape in fs_root: status %u, "
             "expected %u (NOENT)", status, (uint32_t) NFS3ERR_NOENT);
    expect("v3-no-write-above", status == NFS3ERR_NOENT, buf);

    /* A non-root caller reaches the export root from inside it even when the
     * directories above the export deny it search. */
    res = mbt_setattr(env, &fs_root, 0700, -1, NULL);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "SETATTR fs_root 0700 failed: %u\n", res->status);
        return 2;
    }
    mbt_cred_set_uid(env, OTHER_UID);
    expect_dotdot_is(env, "v3-nonroot-inside", &in, &sub, "sub");
    mbt_cred_set_uid(env, 0);
    res = mbt_setattr(env, &fs_root, 0755, -1, NULL);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "SETATTR fs_root 0755 failed: %u\n", res->status);
        return 2;
    }

    /* Once the export's directory has been renamed away its root cannot be
     * told from any other directory, so ".." is refused from all of them
     * rather than answered with a parent that may lie above the export. */
    res = mbt_rename(env, &fs_root, "dirC", 4, &fs_root, "dirD", 4);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "RENAME dirC failed: %u\n", res->status);
        return 2;
    }
    expect_status("v3-gone-root", "LOOKUP \"..\" at the old export root",
                  lookup_fh(env, &g, "..", &scratch), NFS3ERR_STALE);
    expect_status("v3-gone-below", "LOOKUP \"..\" below the old export root",
                  lookup_fh(env, &kid, "..", &scratch), NFS3ERR_STALE);
    expect_rdplus_dotdot_bare(env, "v3-rdplus-gone-root", &g);
    expect_rdplus_dotdot_bare(env, "v3-rdplus-gone-below", &kid);

    printf("NFSv3 MOUNT below an export\n");

    /* A path below the export name is resolved from the export root and may
    * not climb out of it: not by "." or "..", and not through a symlink. */
    status = lookup_fh(env, &sub, "inside", &want);
    if (status != NFS3_OK) {
        fprintf(stderr, "LOOKUP inside in /sub failed: %u\n", status);
        return 2;
    }
    status = mnt_fh(env, "/sub/inside", &m);
    snprintf(buf, sizeof(buf), "MNT /sub/inside: status %u, handle %s "
             "LOOKUP(sub, \"inside\")", status,
             m.has && mbt_fh_eq(&m, &want) ? "is" : "is not");
    expect("mnt-suffix-ok", status == MNT3_OK && m.has && mbt_fh_eq(&m, &want),
           buf);

    expect_status("mnt-dotdot", "MNT /sub/..",
                  mnt_fh(env, "/sub/..", &scratch), MNT3ERR_ACCES);
    expect_status("mnt-dotdot-sibling", "MNT /sub/../dirB",
                  mnt_fh(env, "/sub/../dirB", &scratch), MNT3ERR_ACCES);
    expect_status("mnt-dotdot-inner", "MNT /sub/inside/..",
                  mnt_fh(env, "/sub/inside/..", &scratch), MNT3ERR_ACCES);
    expect_status("mnt-dot", "MNT /sub/./inside",
                  mnt_fh(env, "/sub/./inside", &scratch), MNT3ERR_ACCES);

    make_symlink(env, &sub, "esc", "..");
    expect_status("mnt-symlink-up", "MNT /sub/esc (esc -> ..)",
                  mnt_fh(env, "/sub/esc", &scratch), MNT3ERR_ACCES);
    make_symlink(env, &sub, "abs", "/fs0");
    expect_status("mnt-symlink-abs", "MNT /sub/abs (abs -> /fs0)",
                  mnt_fh(env, "/sub/abs", &scratch), MNT3ERR_ACCES);
    make_symlink(env, &sub, "mid", "inside");
    expect_status("mnt-symlink-mid", "MNT /sub/mid/deep (mid -> inside)",
                  mnt_fh(env, "/sub/mid/deep", &scratch), MNT3ERR_ACCES);

    status = mnt_fh(env, "/sub/inside/", &scratch);
    snprintf(buf, sizeof(buf), "MNT /sub/inside/: status %u, handle %s "
             "that of MNT /sub/inside", status,
             scratch.has && mbt_fh_eq(&scratch, &m) ? "is" : "is not");
    expect("mnt-trailing-slash", status == MNT3_OK && scratch.has &&
           mbt_fh_eq(&scratch, &m), buf);

    printf("NFSv3 names\n");

    /* A filename3 is one component: a backend handed "a/b" may resolve it as
     * a path, ".." and all, so the server refuses it before any VFS call. */
    res = mbt_lookup(env, &sub, "inside/../..", 12);
    expect_status("v3-name-multi-lookup", "LOOKUP \"inside/../..\"",
                  res->status, NFS3ERR_ACCES);

    /* Each name is given entries it would reach if a backend resolved it as
     * a path: "x" beside the directory, "b" in its subdirectory "a". */
    nm  = make_dir(env, &sub, "nm");
    nma = make_dir(env, &nm, "a");
    if (mbt_create(env, &sub, "x", 1, UNCHECKED, 0644, NULL)->status ||
        mbt_create(env, &nma, "b", 1, UNCHECKED, 0644, NULL)->status) {
        fprintf(stderr, "CREATE of the name targets failed\n");
        return 2;
    }
    res = mbt_create(env, &nm, "keep", 4, UNCHECKED, 0644, NULL);
    if (res->status != NFS3_OK || !res->obj_fh.has) {
        fprintf(stderr, "CREATE keep failed: %u\n", res->status);
        return 2;
    }
    scratch = res->obj_fh;
    name_cells(env, "dotdot-slash", "../x", 4, &nm, &scratch);
    name_cells(env, "two-components", "a/b", 3, &nm, &scratch);
    name_cells(env, "empty", "", 0, &nm, &scratch);
    name_cells(env, "nul", "a\0b", 3, &nm, &scratch);

    status = lookup_fh(env, &sub, "x", &up) ||
        lookup_fh(env, &nma, "b", &up) || lookup_fh(env, &nm, "keep", &up) ||
        lookup_fh(env, &nm, "moved", &up) != NFS3ERR_NOENT;
    expect("v3-name-no-effect", !status,
           "the refused names removed, renamed and created nothing");

    /* A name that merely starts with dots is an ordinary name. */
    res = mbt_mkdir(env, &nm, "..x", 3, 0755);
    expect_status("v3-name-dotdot-prefix", "MKDIR \"..x\"", res->status,
                  NFS3_OK);
    res = mbt_mkdir(env, &nm, "...", 3, 0755);
    expect_status("v3-name-three-dots", "MKDIR \"...\"", res->status,
                  NFS3_OK);
    res = mbt_rename(env, &nm, "keep", 4, &nm, ".keep", 5);
    expect_status("v3-name-dot-prefix", "RENAME keep to \".keep\"",
                  res->status, NFS3_OK);
    expect_status("mnt-dotdot-prefix", "MNT /sub/nm/..x",
                  mnt_fh(env, "/sub/nm/..x", &up), MNT3_OK);

    printf("NFSv3 \".\" and \"..\" as entry names\n");

    /* "." and ".." are not entries a client may make, remove or rename.  A
     * backend that takes ".." as a new name stores it, and one that fills its
     * reply by looking the name up answers with the parent's handle: above
     * the export, at its root.  MKDIR is tried at the export root, where that
     * handle escapes.  The other creating procedures each get a directory of
     * their own: where a backend has stored the name, a later creation of it
     * in the same directory answers EXIST by collision, not by refusal.  The
     * VFS refuses RENAME of both names with the same status, so the two
     * RENAME cells pin the status and not the layer that gives it. */
    res = mbt_create(env, &sub, "dotf", 4, UNCHECKED, 0644, NULL);
    if (res->status != NFS3_OK || !res->obj_fh.has) {
        fprintf(stderr, "CREATE dotf failed: %u\n", res->status);
        return 2;
    }
    dotf = res->obj_fh;

    for (i = 0; i < 2; i++) {
        dot    = i ? ".." : ".";
        dotlen = strlen(dot);
        self   = i ? "" : "-self";

        res = mbt_mkdir(env, &sub, dot, dotlen, 0755);
        dot_cell("mkdir", self, "MKDIR", dot, res->status, NFS3ERR_EXIST);

        snprintf(buf, sizeof(buf), "dot_create%d", i);
        scratch = make_dir(env, &sub, buf);
        res     = mbt_create(env, &scratch, dot, dotlen, UNCHECKED, 0644, NULL);
        dot_cell("create", self, "CREATE", dot, res->status, NFS3ERR_EXIST);

        snprintf(buf, sizeof(buf), "dot_symlink%d", i);
        scratch = make_dir(env, &sub, buf);
        res     = mbt_symlink(env, &scratch, dot, dotlen, "x", 0777);
        dot_cell("symlink", self, "SYMLINK", dot, res->status, NFS3ERR_EXIST);

        snprintf(buf, sizeof(buf), "dot_mknod%d", i);
        scratch = make_dir(env, &sub, buf);
        res     = mbt_mknod(env, &scratch, dot, dotlen, NF3FIFO, 0644);
        dot_cell("mknod", self, "MKNOD", dot, res->status, NFS3ERR_EXIST);

        snprintf(buf, sizeof(buf), "dot_link%d", i);
        scratch = make_dir(env, &sub, buf);
        res     = mbt_link(env, &dotf, &scratch, dot, dotlen);
        dot_cell("link", self, "LINK as", dot, res->status, NFS3ERR_EXIST);

        res = mbt_remove(env, &sub, dot, dotlen);
        dot_cell("remove", self, "REMOVE", dot, res->status, NFS3ERR_ACCES);
        res = mbt_rmdir(env, &sub, dot, dotlen);
        dot_cell("rmdir", self, "RMDIR", dot, res->status, NFS3ERR_ACCES);
        res = mbt_rename(env, &sub, "dotf", 4, &sub, dot, dotlen);
        dot_cell("rename-to", self, "RENAME to", dot, res->status,
                 NFS3ERR_INVAL);
        res = mbt_rename(env, &sub, dot, dotlen, &sub, "stolen", 6);
        dot_cell("rename-from", self, "RENAME from", dot, res->status,
                 NFS3ERR_INVAL);
    }
    expect_dotdot_is(env, "v3-dot-lookup-still", &sub, &sub, "sub");

    printf("RENAME across exports\n");

    /* A directory renamed out of its export keeps the export's id in the
    * handles a client already holds, and ".." from one of them walks up from
    * the directory's new place.  So a RENAME whose two directories are in
    * different exports is refused, as a rename across filesystems is. */
    (void) make_dir(env, &sub, "mv");
    res = mbt_rename(env, &sub, "mv", 2, &fs_root, "mv_out", 6);
    expect_status("v3-xexport-rename-out", "RENAME sub/mv to fs_root/mv_out",
                  res->status, NFS3ERR_XDEV);
    res = mbt_rename(env, &fs_root, "dirB", 4, &sub, "dirB_in", 7);
    expect_status("v3-xexport-rename-in", "RENAME fs_root/dirB to sub/dirB_in",
                  res->status, NFS3ERR_XDEV);
    /* The same directory through handles of one export is an ordinary
     * rename. */
    res = mbt_rename(env, &a, "mv", 2, &a, "mv2", 3);
    expect_status("v3-xexport-same-export", "RENAME dirA/mv to dirA/mv2",
                  res->status, NFS3_OK);

    printf("NFSv3 removed export\n");

    /* A handle outlives its export, and the decode accepts it; with no export
     * record left to say where the root is, ".." is refused. */
    if (chimera_server_create_export(env->server, "/tmp1", "/fs0/dirB", 0,
                                     NULL)) {
        fprintf(stderr, "chimera_server_create_export /tmp1 failed\n");
        return 2;
    }
    t1 = mount_export(env, "/tmp1");
    if (chimera_server_remove_export(env->server, "/tmp1")) {
        fprintf(stderr, "chimera_server_remove_export /tmp1 failed\n");
        return 2;
    }
    expect_status("v3-removed-export",
                  "LOOKUP \"..\" after the export was removed",
                  lookup_fh(env, &t1, "..", &scratch), NFS3ERR_STALE);

    printf("NFSv4 LOOKUPP\n");

    pseudo = v4_handle(env, NULL);
    s4     = v4_handle(env, "sub");
    d4     = v4_handle(env, "deep");
    expect_lookupp_is(env, "v4-sub-root", &s4, NULL, &pseudo, "pseudo-root");
    expect_lookupp_is(env, "v4-sub-inside", &s4, "inside", &s4, "s4");
    expect_lookupp_is(env, "v4-deep-root", &d4, NULL, &pseudo, "pseudo-root");

    ops[0] = op_putfh(&g4);
    ops[1] = op_simple(OP_LOOKUPP);
    p      = compound(env, ops, 2);
    expect_status("v4-gone-root", "LOOKUPP at the old export root", p.status,
                  NFS4ERR_STALE);

    printf("NFSv4 RENAME across exports\n");

    fs4 = v4_handle(env, "fs0");
    expect_rename4(env, "v4-xexport-rename-out", &s4, "mv2", &fs4, "mv_out",
                   NFS4ERR_XDEV);
    expect_rename4(env, "v4-xexport-rename-in", &fs4, "dirB", &s4, "dirB_in",
                   NFS4ERR_XDEV);
    expect_rename4(env, "v4-xexport-same-export", &s4, "mv2", &s4, "mv3",
                   NFS4_OK);

    printf("NFSv4 LOOKUPP under a \"/\" export\n");

    /* With a "/" export the namespace root is that export's root, and it is
     * what lies above the root of every other export. */
    if (chimera_server_create_export(env->server, "/", "/fs0/dirB", 0, NULL)) {
        fprintf(stderr, "chimera_server_create_export / failed\n");
        return 2;
    }
    want = v4_handle(env, NULL);
    if (mbt_fh_eq(&want, &pseudo)) {
        fprintf(stderr, "the \"/\" export did not become the namespace root\n");
        return 2;
    }
    expect_lookupp_is(env, "v4-sub-root-slash-export", &s4, NULL, &want,
                      "the \"/\" export root");
    expect_lookupp_is(env, "v4-sub-inside-slash-export", &s4, "inside", &s4,
                      "s4");

    mbt_cred_set_uid(env, 0);
    mbt_env_stop(env);
    free(env);

    if (failures) {
        printf("%d failure(s)\n", failures);
        return 1;
    }
    printf("NFS export \"..\": PASS\n");
    return 0;
} /* main */
