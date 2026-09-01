/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Ground-truth probe for the POSIX-over-SMB2 loopback (posix client -> the smb
 * proxy VFS module -> an in-process chimera SMB server -> memfs).
 *
 * The full POSIX model corpus cannot run over this backend yet -- see the
 * SD1-SD5 list in CMakeLists.txt -- so this probe is what actually executes
 * src/vfs/smb in the quick tier.  It is a fixed script rather than a model
 * replay, and it asserts two different kinds of thing:
 *
 *   * the operations the proxy DOES implement, asserted as correctness.  These
 *     are ordinary regressions if they break.
 *
 *   * the SD2-SD5 defects, asserted as the DEFECT.  Pinning them is what keeps
 *     them visible: an unexercised bug is indistinguishable from a fixed one,
 *     and the whole module read 0% before this file existed.  A pin that starts
 *     failing means somebody fixed the underlying defect -- delete the pin,
 *     replace it with the correctness assertion named in its comment, and drop
 *     the corresponding SD entry from CMakeLists.txt.
 *
 * The pins deliberately assert behavioural SIGNATURES (an operation fails; a
 * created object cannot be seen afterwards; fewer entries come back than were
 * created) rather than exact errno numbers, because the numbers the proxy
 * produces are neither stable across platforms nor, in several cases, the ones
 * POSIX defines for the situation.  That imprecision is itself part of SD5.
 *
 * The op-execution engine is reused verbatim from posix_driver.c, as in
 * posix_mbt_replay.c.
 */

#define POSIX_DRIVER_ENGINE_ONLY
#include "posix_driver.c"

static int probe_failures;

/* ---- request/response helpers ------------------------------------------ */

/* Run one driver op.  Returns the response object; the caller owns it. */
static json_t *
probe_call(json_t *req)
{
    json_t *res;

    errno = 0;
    res   = handle(req);
    json_decref(req);
    if (!res) {
        fprintf(stderr, "smb_loopback_probe: driver returned no response\n");
        exit(1);
    }
    return res;
} /* probe_call */

static json_t *
probe_req(const char *op)
{
    json_t *req = json_object();

    json_object_set_new(req, "op", json_string(op));
    json_object_set_new(req, "pid", json_integer(0));
    return req;
} /* probe_req */

static void
probe_set_str(
    json_t     *req,
    const char *key,
    const char *val)
{
    json_object_set_new(req, key, json_string(val));
} /* probe_set_str */

static void
probe_set_int(
    json_t     *req,
    const char *key,
    long long   val)
{
    json_object_set_new(req, key, json_integer(val));
} /* probe_set_int */

static long long
probe_ret(json_t *res)
{
    return json_integer_value(json_object_get(res, "ret"));
} /* probe_ret */

static long long
probe_field(
    json_t     *res,
    const char *key)
{
    return json_integer_value(json_object_get(res, key));
} /* probe_field */

static void
probe_fail(
    const char *what,
    const char *detail)
{
    fprintf(stderr, "smb_loopback_probe: FAIL %s: %s\n", what, detail);
    probe_failures++;
} /* probe_fail */

/* Assert an operation succeeded (the working surface). */
static void
probe_ok(
    const char *what,
    json_t     *res)
{
    if (probe_ret(res) < 0) {
        char detail[128];

        snprintf(detail, sizeof(detail), "expected success, got errno %lld",
                 probe_field(res, "err"));
        probe_fail(what, detail);
    }
    json_decref(res);
} /* probe_ok */

/* Assert an operation failed, without pinning which errno.  Used both for
 * genuine POSIX errors whose value is not the point, and for the SD5 pins. */
static void
probe_err(
    const char *what,
    json_t     *res)
{
    if (probe_ret(res) >= 0) {
        probe_fail(what, "expected failure, got success");
    }
    json_decref(res);
} /* probe_err */

static void
probe_eq(
    const char *what,
    long long   got,
    long long   want)
{
    if (got != want) {
        char detail[128];

        snprintf(detail, sizeof(detail), "expected %lld, got %lld", want, got);
        probe_fail(what, detail);
    }
} /* probe_eq */

/* ---- op shorthands ------------------------------------------------------ */

static json_t *
op_stat(
    const char *path,
    int         follow)
{
    json_t *r = probe_req("stat");

    probe_set_str(r, "path", path);
    json_object_set_new(r, "follow", json_boolean(follow));
    return probe_call(r);
} /* op_stat */

static json_t *
op_path(
    const char *op,
    const char *path)
{
    json_t *r = probe_req(op);

    probe_set_str(r, "path", path);
    return probe_call(r);
} /* op_path */

static json_t *
op_mkdir(
    const char *path,
    int         mode)
{
    json_t *r = probe_req("mkdir");

    probe_set_str(r, "path", path);
    probe_set_int(r, "mode", mode);
    return probe_call(r);
} /* op_mkdir */

static json_t *
op_open(
    const char *path,
    int         flags,
    int         mode)
{
    json_t *r = probe_req("open");

    probe_set_str(r, "path", path);
    probe_set_int(r, "flags", flags);
    probe_set_int(r, "mode", mode);
    return probe_call(r);
} /* op_open */

static json_t *
op_fd(
    const char *op,
    int         fd)
{
    json_t *r = probe_req(op);

    probe_set_int(r, "fd", fd);
    return probe_call(r);
} /* op_fd */

static json_t *
op_two_path(
    const char *op,
    const char *old_path,
    const char *new_path)
{
    json_t *r = probe_req(op);

    probe_set_str(r, "old", old_path);
    probe_set_str(r, "new", new_path);
    return probe_call(r);
} /* op_two_path */

/* Create an empty file, asserting both legs. */
static void
probe_touch(const char *path)
{
    json_t   *res = op_open(path, O_CREAT | O_WRONLY, 0644);
    long long fd  = probe_ret(res);

    if (fd < 0) {
        probe_fail("touch", path);
        json_decref(res);
        return;
    }
    json_decref(res);
    probe_ok("touch close", op_fd("close", (int) fd));
} /* probe_touch */

/* ---- the working surface ------------------------------------------------ */

/* Everything the proxy implements correctly today.  A regression here is an
 * ordinary bug, not a pinned defect. */
static void
probe_working_surface(void)
{
    json_t   *res;
    long long fd;

    /* The mount root, as the driver normalized it. */
    res = op_stat("/test", 1);
    probe_ok("stat mount root", json_incref(res));
    {
        const char *ftype = json_string_value(json_object_get(res, "ftype"));

        if (!ftype || strcmp(ftype, "dir") != 0) {
            probe_fail("stat mount root", "not reported as a directory");
        }
    }
    json_decref(res);

    probe_ok("mkdir", op_mkdir("/test/w", 0755));
    probe_ok("mkdir nested", op_mkdir("/test/w/sub", 0755));

    /* Create, write, read back, at depth: the path-op strategy resolves the
     * whole path against the mount root, so depth is not a limit here (unlike
     * SD5's parent-handle route). */
    res = op_open("/test/w/sub/f", O_CREAT | O_RDWR, 0644);
    probe_ok("open O_CREAT", json_incref(res));
    fd = probe_ret(res);
    json_decref(res);

    if (fd >= 0) {
        json_t *w = probe_req("write");

        probe_set_int(w, "fd", (int) fd);
        probe_set_str(w, "data", "aGVsbG8=");     /* "hello" */
        res = probe_call(w);
        probe_eq("write returns the byte count", probe_ret(res), 5);
        json_decref(res);

        res = probe_req("pread");
        probe_set_int(res, "fd", (int) fd);
        probe_set_int(res, "off", 1);
        probe_set_int(res, "len", 3);
        res = probe_call(res);
        probe_eq("pread returns the byte count", probe_ret(res), 3);
        if (json_object_get(res, "data")) {
            probe_eq("pread returns the written bytes",
                     strcmp(json_string_value(json_object_get(res, "data")),
                            "ZWxs") == 0, 1);              /* "ell" */
        } else {
            probe_fail("pread", "no data in the response");
        }
        json_decref(res);

        probe_ok("fsync", op_fd("fsync", (int) fd));
        probe_ok("close", op_fd("close", (int) fd));
    }

    res = op_stat("/test/w/sub/f", 1);
    probe_ok("stat the written file", json_incref(res));
    probe_eq("size reflects the write", probe_field(res, "size"), 5);
    json_decref(res);

    /* Exclusive create over an existing name is refused. */
    probe_err("O_CREAT|O_EXCL over an existing file",
              op_open("/test/w/sub/f", O_CREAT | O_EXCL | O_WRONLY, 0644));

    /* Truncate by path, then observe the new size. */
    res = probe_req("truncate");
    probe_set_str(res, "path", "/test/w/sub/f");
    probe_set_int(res, "len", 2);
    probe_ok("truncate", probe_call(res));
    res = op_stat("/test/w/sub/f", 1);
    probe_eq("size reflects the truncate", probe_field(res, "size"), 2);
    json_decref(res);

    /* Rename and remove, both at depth. */
    probe_ok("rename", op_two_path("rename", "/test/w/sub/f", "/test/w/sub/g"));
    probe_ok("stat the renamed file", op_stat("/test/w/sub/g", 1));
    probe_err("stat the old name", op_stat("/test/w/sub/f", 1));
    probe_ok("unlink", op_path("unlink", "/test/w/sub/g"));

    /* Type assertions on removal. */
    probe_err("unlink of a directory", op_path("unlink", "/test/w/sub"));
    probe_ok("rmdir", op_path("rmdir", "/test/w/sub"));
    probe_err("rmdir of a missing name", op_path("rmdir", "/test/w/sub"));
    probe_err("stat of a missing name", op_stat("/test/w/missing", 1));

    probe_ok("statvfs", op_path("statvfs", "/test"));
} /* probe_working_surface */

/* ---- the pinned defects ------------------------------------------------- */

/* SD2: readdir returns fewer entries than the directory holds, and omits the
 * "." and ".." the other backends emit.  The QUERY_DIRECTORY buffer is walked
 * until the emit callback reports full and the unconsumed remainder is freed;
 * the resume query asks the server for the next batch, which is empty.
 *
 * When SD2 is fixed this must become: all five names come back. */
static void
probe_pin_readdir(void)
{
    static const char *const names[] = { "e0", "e1", "e2" };
    json_t                  *res;
    long long                sid;
    size_t                   i, n;

    probe_ok("readdir setup mkdir", op_mkdir("/test/rd", 0755));
    for (i = 0; i < 3; i++) {
        char path[64];

        snprintf(path, sizeof(path), "/test/rd/%s", names[i]);
        probe_touch(path);
    }

    res = op_path("opendir", "/test/rd");
    probe_ok("opendir", json_incref(res));
    sid = probe_ret(res);
    json_decref(res);
    if (sid < 0) {
        return;
    }

    res = probe_req("readdir");
    probe_set_int(res, "sid", sid);
    res = probe_call(res);
    n   = json_array_size(json_object_get(res, "names"));
    json_decref(res);

    /* PINNED DEFECT (SD2).  A correct backend returns 5 here: ".", "..", and
     * the three files.  Assert only that entries are LOST, so the pin does not
     * depend on how many happen to fit one emit buffer. */
    if (n >= 5) {
        probe_fail("SD2 pin",
                   "readdir returned every entry -- SD2 looks FIXED; replace "
                   "this pin with an equality assertion and drop SD2");
    } else if (n == 0) {
        probe_fail("SD2 pin", "readdir returned nothing at all (worse than SD2)");
    }

    res = probe_req("closedir");
    probe_set_int(res, "sid", sid);
    probe_ok("closedir", probe_call(res));
} /* probe_pin_readdir */

/* SD3: symlink directly under the mount root reports SUCCESS and creates
 * nothing.  SD5: below the first level the same call fails instead.  Either
 * way no symlink exists afterwards, which is the signature worth pinning.
 *
 * When SD3/SD5 are fixed this must become: symlink succeeds at both depths and
 * the link is visible to lstat with the target readlink returns. */
static void
probe_pin_symlink(void)
{
    json_t *res;

    res = probe_req("symlink");
    probe_set_str(res, "path", "/test/sym");
    probe_set_str(res, "target", "w");
    json_decref(probe_call(res));

    /* PINNED DEFECT (SD3/SD5): whatever the call reported, nothing was made. */
    res = op_stat("/test/sym", 0);
    if (probe_ret(res) >= 0) {
        probe_fail("SD3 pin",
                   "the symlink exists -- SD3 looks FIXED; assert readlink "
                   "returns the target instead and drop SD3");
    }
    json_decref(res);
} /* probe_pin_symlink */

/* SD4: SMB2 carries no POSIX owner or mode.  chmod and chown are accepted and
 * dropped, getattr reports the CALLING credential as the owner and a
 * synthesized mode, and directory nlink is always 1.
 *
 * When SD4 is fixed these must become: mode 0700 after the chmod, uid/gid
 * 4242/4243 after the chown, and nlink >= 2 on a directory. */
static void
probe_pin_no_posix_metadata(void)
{
    json_t   *res;
    long long mode_before, mode_after;

    probe_touch("/test/meta");

    res         = op_stat("/test/meta", 1);
    mode_before = probe_field(res, "mode");
    json_decref(res);

    res = probe_req("chmod");
    probe_set_str(res, "path", "/test/meta");
    probe_set_int(res, "mode", 0700);
    json_decref(probe_call(res));

    res        = op_stat("/test/meta", 1);
    mode_after = probe_field(res, "mode");
    json_decref(res);

    /* PINNED DEFECT (SD4). */
    if (mode_after != mode_before) {
        probe_fail("SD4 mode pin",
                   "chmod changed the reported mode -- SD4 looks FIXED for "
                   "mode; assert it equals 0700 and narrow SD4");
    }

    res = probe_req("chown");
    probe_set_str(res, "path", "/test/meta");
    probe_set_int(res, "uid", 4242);
    probe_set_int(res, "gid", 4243);
    json_object_set_new(res, "follow", json_boolean(1));
    json_decref(probe_call(res));

    res = op_stat("/test/meta", 1);
    /* PINNED DEFECT (SD4): the owner reported is the CALLER (root, uid 0 --
     * the identity the loopback's single SMB session authenticated as), not
     * the one just set. */
    if (probe_field(res, "uid") == 4242) {
        probe_fail("SD4 owner pin",
                   "chown took effect -- SD4 looks FIXED for ownership; "
                   "assert uid/gid equal 4242/4243 and narrow SD4");
    } else {
        probe_eq("SD4: getattr reports the calling credential",
                 probe_field(res, "uid"), 0);
    }
    json_decref(res);

    res = op_stat("/test/w", 1);
    /* PINNED DEFECT (SD4): a directory's link count is synthesized as 1; POSIX
     * requires at least 2 (itself and "."). */
    if (probe_field(res, "nlink") >= 2) {
        probe_fail("SD4 nlink pin",
                   "directory nlink is >= 2 -- SD4 looks FIXED for nlink; "
                   "assert it and narrow SD4");
    }
    json_decref(res);
} /* probe_pin_no_posix_metadata */

/* SD5: only the mount-root handle is re-openable by fh, so every operation the
 * client routes through a resolved PARENT handle fails.  link and utimens fail
 * at any depth; mknod reaches the module at the first level (and correctly
 * reports "unsupported" there) but fails the same way below it.
 *
 * When SD5 is fixed, link and mknod should report "unsupported" rather than a
 * handle error, and utimens should succeed. */
static void
probe_pin_parent_handle_ops(void)
{
    json_t *res;

    probe_touch("/test/src");

    probe_err("SD5: link", op_two_path("link", "/test/src", "/test/dst"));

    res = probe_req("utimens");
    probe_set_str(res, "path", "/test/src");
    probe_set_str(res, "atype", "val");
    probe_set_int(res, "asec", 1000);
    probe_set_int(res, "ansec", 0);
    probe_set_str(res, "mtype", "val");
    probe_set_int(res, "msec", 2000);
    probe_set_int(res, "mnsec", 0);
    json_object_set_new(res, "follow", json_boolean(1));
    probe_err("SD5: utimens by path", probe_call(res));

    res = probe_req("mknod");
    probe_set_str(res, "path", "/test/w/fifo");
    probe_set_int(res, "mode", 0644);
    probe_set_str(res, "ftype", "fifo");
    probe_err("SD5: mknod below the mount root", probe_call(res));
} /* probe_pin_parent_handle_ops */

/* ---- driver ------------------------------------------------------------- */

int
main(
    int    argc,
    char **argv)
{
    const char *vers        = NULL;
    int         expect_fail = 0;
    json_t     *res;
    int         i;

    /* Flags describe the WIRE the probe runs over: --vers pins the client's
     * dialect, the rest configure what the in-process server demands.  Any
     * combination may be declared --expect-mount-failure, which inverts the
     * verdict: the mount must be refused, and the body never runs. */
    for (i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--vers=", 7) == 0) {
            vers = argv[i][7] ? argv[i] + 7 : NULL;
        } else if (strcmp(argv[i], "--sign-required") == 0) {
            g_smb_sign_required = 1;
        } else if (strcmp(argv[i], "--encrypt") == 0) {
            g_smb_encryption = 1;
        } else if (strcmp(argv[i], "--encrypt-required") == 0) {
            g_smb_encryption = 2;
        } else if (strcmp(argv[i], "--compress") == 0) {
            g_smb_compression = 1;
        } else if (strcmp(argv[i], "--seal") == 0) {
            g_smb_seal = 1;
        } else if (strcmp(argv[i], "--leases") == 0) {
            g_smb_leases = 1;
        } else if (strcmp(argv[i], "--expect-mount-failure") == 0) {
            expect_fail = 1;
        } else {
            fprintf(stderr, "smb_loopback_probe: unknown flag '%s'\n", argv[i]);
            return 1;
        }
    }

    /* Optional argv[1] pins the mount's dialect (a vers= value).  With no
     * argument the client offers its whole set and the server selects, which
     * against a current server is always the highest -- so the older dialects
     * are only ever reached by naming them.
     *
     * A pinned mount offers exactly one dialect, so the server can only select
     * that one or fail to find a dialect in common.  A mount that comes up is
     * therefore PROOF the connection is speaking the requested dialect -- there
     * is no readback to trust, and a vers= that was quietly ignored would have
     * negotiated 3.1.1 and shown up as the expect-mount-failure case passing
     * when it should not. */
    g_smb_vers = vers;

    /* Clean result stream: chimera logs go to stderr (fd 1 -> stderr), our
     * output to the saved stdout -- the same three steps posix_mbt_replay.c
     * uses, including reusing posix_driver.c's proto_out.  Reusing it is not
     * incidental: proto_out is a file-scope static in posix_driver.c, and a TU
     * that includes the driver without touching it fails the Linux build with
     * -Werror=unused-variable (gcc warns where clang does not). */
    {
        int out_fd = dup(STDOUT_FILENO);

        proto_out = out_fd >= 0 ? fdopen(out_fd, "w") : NULL;
        if (!proto_out || dup2(STDERR_FILENO, STDOUT_FILENO) < 0 ||
            dup2(fileno(proto_out), STDOUT_FILENO) < 0) {
            fprintf(stderr, "smb_loopback_probe: stream setup failed\n");
            return 1;
        }
    }

    if (posix_env_setup("smb_memfs", NULL) != 0) {
        if (expect_fail) {
            printf("smb_loopback_probe: the mount was refused, as expected\n");
            return 0;
        }
        fprintf(stderr, "smb_loopback_probe: SMB loopback setup failed (vers=%s)\n",
                vers ? vers : "<negotiated>");
        return 1;
    }

    if (expect_fail) {
        fprintf(stderr,
                "smb_loopback_probe: the mount SUCCEEDED but was expected to be "
                "refused -- either the client gained the capability the server "
                "demanded (update the test) or the demand was not applied\n");
        return 1;
    }

    fprintf(stderr, "smb_loopback_probe: mounted with vers=%s\n",
            vers ? vers : "<negotiated>");

    probe_working_surface();
    probe_pin_readdir();
    probe_pin_symlink();
    probe_pin_no_posix_metadata();
    probe_pin_parent_handle_ops();

    /* The per-trace recycle: client umount, share/filesystem cycle, remount.
     * The MBT batch leans on this between every trace, so prove it works even
     * while the batch itself is parked -- and prove the fresh filesystem really
     * is empty. */
    res = probe_req("newfs");
    probe_ok("newfs recycle", probe_call(res));
    probe_err("the recycled filesystem is empty", op_stat("/test/w", 1));
    probe_ok("the recycled filesystem is usable", op_mkdir("/test/after", 0755));

    posix_env_teardown();

    if (probe_failures) {
        fprintf(stderr, "smb_loopback_probe: %d failure(s)\n", probe_failures);
        return 1;
    }
    printf("smb_loopback_probe: OK\n");
    return 0;
} /* main */
