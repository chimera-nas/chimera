/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 open-access probe: which access mode an SMB CREATE hands the backend,
 * and what the open is granted.
 *
 * CREATE opens the backend read-write only when DesiredAccess can write data
 * (FILE_WRITE_DATA, FILE_APPEND_DATA, GENERIC_WRITE, GENERIC_ALL,
 * MAXIMUM_ALLOWED); every other open is read-only.  A MAXIMUM_ALLOWED open the
 * backend refuses read-write is re-issued read-only, which is what keeps a
 * directory opened MAXIMUM_ALLOWED without FILE_DIRECTORY_FILE working on the
 * passthrough backends (their read-write open of a directory is O_RDWR, which
 * the kernel refuses with EISDIR).  That is why this probe runs on linux and
 * io_uring as well as memfs.
 *
 * Usage: smb2_open_access_probe [-b memfs|linux|io_uring]
 *
 * The passthrough backends mount a host directory, which must live on a
 * filesystem that supports name_to_handle_at: the probe roots it at
 * $CHIMERA_MBT_SCRATCH (default: the working directory) and exits 77 (skip)
 * when that filesystem cannot produce file handles.
 */

#include <errno.h>

#include "smb2_mbt_common.h"
#include "common/getopt.h"
#include "vfs/sdk/vfs_error.h"

#ifdef __linux__
#include <sys/xattr.h>
#endif /* ifdef __linux__ */
#ifndef _WIN32
/* The running-file case (fork, exec, ETXTBSY) is POSIX-only. */
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#endif /* ifndef _WIN32 */

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

#define OA_GENERIC_WRITE          0x40000000u
#define OA_GENERIC_ALL            0x10000000u
#define OA_FILE_WRITE_DATA        0x00000002u
#define OA_FILE_APPEND_DATA       0x00000004u
#define OA_FILE_DELETE_CHILD      0x00000040u
#define OA_FILE_WRITE_ATTRIBUTES  0x00000100u
#define OA_FILE_ATTRIBUTE_DIR     0x00000010u
#define OA_ST_FILE_IS_A_DIRECTORY 0xC00000BAu

/* memfs keeps DOS attributes; the passthrough backends keep none, so the
 * READONLY cases run on memfs only. */
static int keeps_dos_attrs = 1;

/* CREATE with explicit options and file attributes.  With `mxac` non-NULL the
 * request carries an MxAc context and *mxac receives the reply's
 * MaximalAccess (MS-SMB2 2.2.14.2.5: QueryStatus, then MaximalAccess). */
static uint32_t
oa_open(
    struct smb2_conn       *c,
    const char             *name,
    uint32_t                disp,
    uint32_t                access,
    uint32_t                create_options,
    uint32_t                file_attributes,
    uint32_t               *mxac,
    struct smb2_create_out *out)
{
    static const uint8_t       mxac_tag[4] = { 'M', 'x', 'A', 'c' };
    struct smb2_cctx           ctx         = {
        .name     = mxac_tag,
        .name_len = 4,
        .data     = NULL,
        .data_len = 0,
    };
    const struct smb2_rsp_ctx *rsp;

    smb2c_send(c, smb2c_build_create_full(c, name, disp, access,
                                          MBT_FILE_SHARE_RWD, create_options,
                                          file_attributes, NULL,
                                          mxac ? &ctx : NULL, mxac ? 1 : 0));
    smb2c_wait(c);
    smb2c_parse_create(c, out);

    if (mxac && out->status == ST_SUCCESS) {
        rsp = smb2c_create_ctx_find(out, "MxAc");
        if (rsp && rsp->data_len >= 8 && g32(rsp->data, 0) == ST_SUCCESS) {
            *mxac = g32(rsp->data, 4);
        } else {
            *mxac = 0;
            CHECK(0, "%s: CREATE reply carries an MxAc context", name);
        }
    }
    return out->status;
} /* oa_open */

/* FileAccessInformation (MS-FSCC 2.4.1): the handle's granted access. */
static uint32_t
oa_access_info(
    struct smb2_conn *c,
    const uint8_t     fid[16])
{
    uint8_t  buf[16];
    uint32_t len = 0;
    uint32_t st  = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_ACCESS_INFO_T,
                                   fid, 0, buf, sizeof(buf), &len);

    CHECK(st == ST_SUCCESS && len == 4,
          "  QUERY FileAccessInformation -> 0x%08x (%u bytes)", st, len);
    return (st == ST_SUCCESS && len == 4) ? g32(buf, 0) : 0;
} /* oa_access_info */

/* SET_INFO FileBasicInformation with only FileAttributes set. */
static uint32_t
oa_set_attributes(
    struct smb2_conn *c,
    const uint8_t     fid[16],
    uint32_t          attributes)
{
    uint8_t basic[40];

    memset(basic, 0, sizeof(basic));
    p32(basic, 32, attributes);
    return smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, fid,
                         basic, sizeof(basic));
} /* oa_set_attributes */

/* ---- a file being executed on the server host ---------------------------
 *
 * The kernel refuses a write open of a running executable with ETXTBSY.  A
 * MAXIMUM_ALLOWED open of one is re-issued read-only and granted no data-write
 * right; an explicit data-write open is refused ACCESS_DENIED.  Passthrough
 * backends only: `dir` is the shared host directory. */

#ifndef _WIN32

/* Copy /bin/sleep to `path` (named "sleep") as an executable every class may
 * write, so the session's MaximalAccess holds FILE_WRITE_DATA whoever owns the
 * copy. */
static int
oa_copy_sleep(const char *path)
{
    char    buf[65536];
    ssize_t n;
    int     in, out, ok = 1;

    in = open("/bin/sleep", O_RDONLY);
    if (in < 0) {
        return 0;
    }
    out = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0755);
    if (out < 0) {
        close(in);
        return 0;
    }
    while ((n = read(in, buf, sizeof(buf))) > 0) {
        if (write(out, buf, n) != n) {
            ok = 0;
            break;
        }
    }
    if (n < 0 || fchmod(out, 0777) < 0) {
        ok = 0;
    }
    close(in);
    close(out);
    return ok;
} /* oa_copy_sleep */

/* Wait until a write open of `path` fails ETXTBSY (the child has exec'd it).
 * Returns 0 if it never does: the exec failed, or this kernel lets a running
 * executable be opened for write. */
static int
oa_wait_txtbsy(const char *path)
{
    struct timespec ts = { 0, 20 * 1000 * 1000 };
    int             fd, i;

    for (i = 0; i < 100; i++) {
        fd = open(path, O_WRONLY);
        if (fd < 0 && errno == ETXTBSY) {
            return 1;
        }
        if (fd >= 0) {
            close(fd);
        }
        nanosleep(&ts, NULL);
    }
    return 0;
} /* oa_wait_txtbsy */

static void
probe_running_file(
    struct smb2_conn *c,
    const char       *dir)
{
    struct smb2_create_out o;
    char                   path[600];
    uint8_t                out[4];
    uint32_t               st, ga, mx = 0, n = 0;
    pid_t                  pid;

    printf("# running_file\n");
    if (!dir || !dir[0]) {
        printf("# skip running_file: memfs has no host file to execute\n");
        return;
    }
    snprintf(path, sizeof(path), "%s/sleep", dir);
    if (!oa_copy_sleep(path)) {
        printf("# skip running_file: cannot copy /bin/sleep (%s)\n",
               strerror(errno));
        return;
    }

    pid = fork();
    if (pid < 0) {
        printf("# skip running_file: fork: %s\n", strerror(errno));
        return;
    }
    if (pid == 0) {
        /* Named "sleep": /bin/sleep may be a multi-call binary that picks the
         * program from its own file name. */
        char *const     argv[] = { (char *) "sleep", (char *) "30", NULL };
        struct timespec ts     = { 0, 5 * 1000 * 1000 };
        int             i;

        /* The parent's own write open of the file (oa_wait_txtbsy) makes an
         * exec that lands while it is held fail ETXTBSY: try again. */
        for (i = 0; i < 400; i++) {
            execv(path, argv);
            if (errno != ETXTBSY) {
                break;
            }
            nanosleep(&ts, NULL);
        }
        _exit(127);
    }

    if (!oa_wait_txtbsy(path)) {
        int status = 0;

        /* A child that is gone could not exec the copy (a noexec scratch
         * file system, say): that is not the kernel allowing the write. */
        if (waitpid(pid, &status, WNOHANG) == pid) {
            CHECK(0, "the child could not exec %s (wait status 0x%x)", path,
                  status);
            return;
        }
        printf("# skip running_file: a write open of the running file is not "
               "refused ETXTBSY here\n");
        goto out;
    }

    st = oa_open(c, "sleep", MBT_FILE_OPEN, MBT_MAXIMUM_ALLOWED,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, &mx, &o);
    CHECK(st == ST_SUCCESS, "OPEN running sleep MAXIMUM_ALLOWED -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        CHECK((mx & (OA_FILE_WRITE_DATA | OA_FILE_APPEND_DATA)) ==
              (OA_FILE_WRITE_DATA | OA_FILE_APPEND_DATA),
              "  MxAc includes WRITE_DATA and APPEND_DATA (0x%08x)", mx);
        ga = oa_access_info(c, o.file_id);
        CHECK(ga == (mx & ~(OA_FILE_WRITE_DATA | OA_FILE_APPEND_DATA)),
              "  granted access 0x%08x == MxAc without WRITE_DATA, APPEND_DATA "
              "(0x%08x)", ga, mx & ~(OA_FILE_WRITE_DATA | OA_FILE_APPEND_DATA));
        st = smb2_read(c, o.file_id, 0, 4, out, &n);
        CHECK(st == ST_SUCCESS && n == 4, "  READ -> 0x%08x (%u bytes)", st, n);
        smb2_close(c, o.file_id);
    }

    st = oa_open(c, "sleep", MBT_FILE_OPEN, OA_FILE_WRITE_DATA,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_ACCESS_DENIED, "OPEN running sleep FILE_WRITE_DATA -> "
          "ACCESS_DENIED (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }

 out:
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
} /* probe_running_file */

#else /* ifndef _WIN32 */

static void
probe_running_file(
    struct smb2_conn *c,
    const char       *dir)
{
    (void) c;
    (void) dir;
    printf("# running_file\n");
    printf("# skip running_file: needs fork and exec\n");
} /* probe_running_file */

#endif /* ifndef _WIN32 */

/* A directory opened MAXIMUM_ALLOWED without FILE_DIRECTORY_FILE opens, and is
 * granted what MxAc reports. */
static void
probe_dir_max_allowed(struct smb2_conn *c)
{
    struct smb2_create_out o;
    uint32_t               st, mx = 0;

    printf("# dir_max_allowed\n");
    st = oa_open(c, "d1", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                 MBT_FILE_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "setup: CREATE directory d1 -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }

    st = oa_open(c, "d1", MBT_FILE_OPEN, MBT_MAXIMUM_ALLOWED, 0, 0, &mx, &o);
    CHECK(st == ST_SUCCESS, "OPEN d1 MAXIMUM_ALLOWED, no FILE_DIRECTORY_FILE -> "
          "0x%08x (0x%08x is FILE_IS_A_DIRECTORY)", st,
          OA_ST_FILE_IS_A_DIRECTORY);
    if (st != ST_SUCCESS) {
        return;
    }
    CHECK(mx & OA_FILE_WRITE_DATA, "  MxAc includes ADD_FILE (0x%08x)", mx);
    {
        uint32_t ga = oa_access_info(c, o.file_id);

        CHECK(ga == mx, "  granted access 0x%08x == MxAc 0x%08x", ga, mx);
    }
    smb2_close(c, o.file_id);
} /* probe_dir_max_allowed */

/* A file opened MAXIMUM_ALLOWED can be written and read back. */
static void
probe_file_max_allowed_writes(struct smb2_conn *c)
{
    struct smb2_create_out o;
    uint8_t                out[8];
    uint32_t               st, count = 0, n = 0;

    printf("# file_max_allowed_writes\n");
    st = oa_open(c, "f1", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "setup: CREATE f1 -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }

    st = oa_open(c, "f1", MBT_FILE_OPEN, MBT_MAXIMUM_ALLOWED,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "OPEN f1 MAXIMUM_ALLOWED -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_write(c, o.file_id, 0, "abc", 3, &count);
    CHECK(st == ST_SUCCESS && count == 3,
          "  WRITE 3 bytes -> 0x%08x (count %u)", st, count);
    st = smb2_read(c, o.file_id, 0, 3, out, &n);
    CHECK(st == ST_SUCCESS && n == 3 && memcmp(out, "abc", 3) == 0,
          "  READ them back -> 0x%08x (%u bytes)", st, n);
    smb2_close(c, o.file_id);
} /* probe_file_max_allowed_writes */

/* ---- metadata writes through a read-only backend handle ----------------
 *
 * An open that writes attributes, EAs or the security descriptor but no data
 * is opened read-only on the backend.  These opens ask for FILE_READ_DATA too,
 * so the backend open is a real O_RDONLY descriptor on linux and io_uring
 * (not O_PATH), and each SET_INFO must still take effect through it. */

#define OA_FILE_READ_DATA 0x00000001u
#define OA_FILE_READ_EA   0x00000008u
#define OA_FILE_WRITE_EA  0x00000010u
#define OA_WRITE_DAC      0x00040000u
#define OA_SEC_INFO_ALL   0x00000007u  /* owner | group | DACL */
#define OA_SEC_INFO_DACL  0x00000004u
#define OA_WRITE_TIME     132223104000000000ull  /* 2020-01-01T00:00:00Z */

/* Whether m1 holds the EA PROBE.EA = "v1".  On a passthrough backend, read
 * the host file's xattr (`dir` is the shared host directory): the SMB EA query
 * opens the file O_PATH there, so it cannot read an EA back.  On memfs, query
 * it over SMB. */
static int
oa_ea_present(
    struct smb2_conn *c,
    const uint8_t     fid[16],
    const char       *dir)
{
    uint8_t  buf[512];
    uint32_t st, len = 0;

    if (dir && dir[0]) {
#ifdef __linux__
        char    path[600];
        char    value[8];
        ssize_t n;

        snprintf(path, sizeof(path), "%s/m1", dir);
        n = getxattr(path, "user.PROBE.EA", value, sizeof(value));
        return n == 2 && memcmp(value, "v1", 2) == 0;
#else  /* ifdef __linux__ */
        return 0;
#endif /* ifdef __linux__ */
    }

    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                         fid, 0, buf, sizeof(buf), &len);
    for (uint32_t i = 0; st == ST_SUCCESS && i + 8 <= len; i++) {
        if (memcmp(buf + i, "PROBE.EA", 8) == 0) {
            return 1;
        }
    }
    return 0;
} /* oa_ea_present */

static void
probe_metadata_write_ro_handle(
    struct smb2_conn *c,
    const char       *dir)
{
    struct smb2_create_out o;
    uint8_t                buf[512];
    uint8_t                ea[32];
    uint32_t               st, len = 0, count = 0;

    printf("# metadata_write_ro_handle\n");
    st = oa_open(c, "m1", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "setup: CREATE m1 -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_write(c, o.file_id, 0, "abcd", 4, &count);
    CHECK(st == ST_SUCCESS && count == 4, "setup: WRITE m1 -> 0x%08x", st);
    smb2_close(c, o.file_id);

    /* FileBasicInformation: a new LastWriteTime. */
    st = oa_open(c, "m1", MBT_FILE_OPEN,
                 OA_FILE_READ_DATA | MBT_FILE_READ_ATTRIBUTES |
                 OA_FILE_WRITE_ATTRIBUTES,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "OPEN m1 READ_DATA|READ_ATTRIBUTES|"
          "WRITE_ATTRIBUTES -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        uint8_t basic[40];

        memset(basic, 0, sizeof(basic));
        p64(basic, 16, OA_WRITE_TIME);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                           o.file_id, basic, sizeof(basic));
        CHECK(st == ST_SUCCESS, "  SET LastWriteTime -> 0x%08x", st);
        st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                             o.file_id, 0, buf, sizeof(buf), &len);
        CHECK(st == ST_SUCCESS && len >= 40 && g64(buf, 16) == OA_WRITE_TIME,
              "  LastWriteTime reads back (0x%08x, %" PRIu64 ")", st,
              len >= 40 ? g64(buf, 16) : 0);
        smb2_close(c, o.file_id);
    }

    /* FileFullEaInformation: one EA, { Next 0, Flags 0, NameLen, ValueLen,
     * name, NUL, value } (MS-FSCC 2.4.15). */
    st = oa_open(c, "m1", MBT_FILE_OPEN,
                 OA_FILE_READ_DATA | OA_FILE_READ_EA | OA_FILE_WRITE_EA,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "OPEN m1 READ_DATA|READ_EA|WRITE_EA -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        memset(ea, 0, sizeof(ea));
        ea[5] = 8;                                 /* EaNameLength */
        p16(ea, 6, 2);                             /* EaValueLength */
        memcpy(ea + 8, "PROBE.EA", 8);
        memcpy(ea + 17, "v1", 2);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                           o.file_id, ea, 19);
        CHECK(st == ST_SUCCESS, "  SET EA PROBE.EA -> 0x%08x", st);
        CHECK(oa_ea_present(c, o.file_id, dir), "  the EA is stored");
        smb2_close(c, o.file_id);
    }

    /* Security: write back the DACL just read. */
    st = oa_open(c, "m1", MBT_FILE_OPEN,
                 OA_FILE_READ_DATA | MBT_READ_CONTROL | OA_WRITE_DAC,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "OPEN m1 READ_DATA|READ_CONTROL|WRITE_DAC -> 0x%08x",
          st);
    if (st == ST_SUCCESS) {
        st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, o.file_id,
                             OA_SEC_INFO_ALL, buf, sizeof(buf), &len);
        CHECK(st == ST_SUCCESS && len > 0, "  QUERY SECURITY -> 0x%08x", st);
        if (st == ST_SUCCESS && len > 0) {
            st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, o.file_id,
                                    OA_SEC_INFO_DACL, buf, len);
            CHECK(st == ST_SUCCESS, "  SET SECURITY(dacl) -> 0x%08x", st);
        }
        smb2_close(c, o.file_id);
    }
} /* probe_metadata_write_ro_handle */

/* An overwriting disposition with only metadata-write rights still replaces
 * the file's content, so it opens the backend read-write (the replacement is
 * a truncate through the handle). */
static void
probe_overwrite_metadata_open(struct smb2_conn *c)
{
    struct smb2_create_out o;
    uint8_t                out[4];
    uint32_t               st, count = 0, n = 0;

    printf("# overwrite_metadata_open\n");
    st = oa_open(c, "ow1", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "setup: CREATE ow1 -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_write(c, o.file_id, 0, "abc", 3, &count);
    CHECK(st == ST_SUCCESS && count == 3, "setup: WRITE 3 bytes -> 0x%08x", st);
    smb2_close(c, o.file_id);

    st = oa_open(c, "ow1", MBT_FILE_OVERWRITE_IF,
                 MBT_DELETE | OA_FILE_WRITE_ATTRIBUTES,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "OVERWRITE_IF ow1 DELETE|WRITE_ATTRIBUTES -> 0x%08x",
          st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }

    st = oa_open(c, "ow1", MBT_FILE_OPEN, OA_FILE_READ_DATA,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "  reopen ow1 FILE_READ_DATA -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        st = smb2_read(c, o.file_id, 0, 3, out, &n);
        CHECK(st == ST_END_OF_FILE, "  READ -> END_OF_FILE, content replaced "
              "(0x%08x, %u bytes)", st, n);
        smb2_close(c, o.file_id);
    }
} /* probe_overwrite_metadata_open */

/* A directory opened for a metadata write, without FILE_DIRECTORY_FILE, is
 * opened read-only on the backend.  Opened read-write it is O_RDWR on linux
 * and io_uring, which the kernel refuses with EISDIR, and without
 * MAXIMUM_ALLOWED nothing re-issues the open: CREATE answered
 * FILE_IS_A_DIRECTORY. */
static void
probe_dir_metadata_write(struct smb2_conn *c)
{
    struct smb2_create_out o;
    uint32_t               st;

    printf("# dir_metadata_write\n");
    st = oa_open(c, "d2", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                 MBT_FILE_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "setup: CREATE directory d2 -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_close(c, o.file_id);

    st = oa_open(c, "d2", MBT_FILE_OPEN, OA_FILE_READ_DATA | MBT_DELETE, 0, 0,
                 NULL, &o);
    CHECK(st == ST_SUCCESS, "OPEN d2 LIST_DIRECTORY|DELETE, no "
          "FILE_DIRECTORY_FILE -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }

    st = oa_open(c, "d2", MBT_FILE_OPEN,
                 OA_FILE_READ_DATA | OA_FILE_WRITE_ATTRIBUTES, 0, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "OPEN d2 LIST_DIRECTORY|WRITE_ATTRIBUTES, no "
          "FILE_DIRECTORY_FILE -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }
} /* probe_dir_metadata_write */

/* An existing directory cannot be overwritten or superseded: with neither
 * FILE_DIRECTORY_FILE nor FILE_NON_DIRECTORY_FILE, an overwriting disposition
 * that reaches a directory fails STATUS_OBJECT_NAME_COLLISION (MS-FSA 2.1.5.1.2
 * "Open of an Existing File") once it passes CREATE's access and attribute
 * checks, and the directory is left as it was.  Here it passes them: the
 * caller may write the directory, which has no READONLY, HIDDEN or SYSTEM
 * attribute (otherwise those checks answer ACCESS_DENIED first).  A
 * data-write right makes linux and io_uring open the directory read-write,
 * which the kernel refuses EISDIR; CREATE answers that
 * OBJECT_NAME_COLLISION too.  With FILE_NON_DIRECTORY_FILE the answer stays
 * FILE_IS_A_DIRECTORY. */
static void
probe_overwrite_existing_dir(struct smb2_conn *c)
{
    static const struct {
        uint32_t    disp;
        uint32_t    access;
        const char *what;
    } cases[] = {
        { MBT_FILE_OVERWRITE_IF, MBT_DELETE | OA_FILE_WRITE_ATTRIBUTES,
          "OVERWRITE_IF DELETE|WRITE_ATTRIBUTES" },
        { MBT_FILE_OVERWRITE,    MBT_DELETE | OA_FILE_WRITE_ATTRIBUTES,
          "OVERWRITE DELETE|WRITE_ATTRIBUTES" },
        { MBT_FILE_SUPERSEDE,    MBT_DELETE | OA_FILE_WRITE_ATTRIBUTES,
          "SUPERSEDE DELETE|WRITE_ATTRIBUTES" },
        { MBT_FILE_OVERWRITE_IF, MBT_MAXIMUM_ALLOWED,
          "OVERWRITE_IF MAXIMUM_ALLOWED" },
        { MBT_FILE_OVERWRITE_IF, OA_GENERIC_WRITE,
          "OVERWRITE_IF GENERIC_WRITE" },
        { MBT_FILE_OVERWRITE_IF, OA_FILE_WRITE_DATA,
          "OVERWRITE_IF FILE_WRITE_DATA" },
        { MBT_FILE_SUPERSEDE,    OA_GENERIC_ALL,
          "SUPERSEDE GENERIC_ALL" },
    };
    struct smb2_create_out o;
    uint32_t               st;
    size_t                 i;

    printf("# overwrite_existing_dir\n");
    st = oa_open(c, "od1", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                 MBT_FILE_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "setup: CREATE directory od1 -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_close(c, o.file_id);

    for (i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        st = oa_open(c, "od1", cases[i].disp, cases[i].access, 0, 0, NULL, &o);
        CHECK(st == ST_OBJECT_NAME_COLLISION,
              "%s of directory od1 -> OBJECT_NAME_COLLISION (0x%08x)",
              cases[i].what, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, o.file_id);
        }
    }

    st = oa_open(c, "od1", MBT_FILE_OVERWRITE_IF, OA_GENERIC_WRITE,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == OA_ST_FILE_IS_A_DIRECTORY,
          "OVERWRITE_IF GENERIC_WRITE FILE_NON_DIRECTORY_FILE of directory od1 "
          "-> FILE_IS_A_DIRECTORY (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }

    st = oa_open(c, "od1", MBT_FILE_OPEN, OA_FILE_READ_DATA,
                 MBT_FILE_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "  od1 is still a directory -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, o.file_id);
    }
} /* probe_overwrite_existing_dir */

/* An attribute-only open (the shape that clears READONLY) works. */
static void
probe_attr_only_clears_readonly(struct smb2_conn *c)
{
    struct smb2_create_out o;
    uint8_t                buf[64];
    uint32_t               st, len = 0;

    printf("# attr_only_clears_readonly\n");
    if (!keeps_dos_attrs) {
        printf("# skip attr_only_clears_readonly: backend keeps no DOS "
               "attributes\n");
        return;
    }

    st = oa_open(c, "f2", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "setup: CREATE f2 -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = oa_set_attributes(c, o.file_id, MBT_FILE_ATTRIBUTE_READONLY);
    CHECK(st == ST_SUCCESS, "setup: SET READONLY on f2 -> 0x%08x", st);
    smb2_close(c, o.file_id);

    st = oa_open(c, "f2", MBT_FILE_OPEN,
                 MBT_FILE_READ_ATTRIBUTES | OA_FILE_WRITE_ATTRIBUTES,
                 MBT_FILE_NON_DIRECTORY_FILE, 0, NULL, &o);
    CHECK(st == ST_SUCCESS, "OPEN f2 READ_ATTRIBUTES|WRITE_ATTRIBUTES -> 0x%08x",
          st);
    if (st != ST_SUCCESS) {
        return;
    }
    st = oa_set_attributes(c, o.file_id, MBT_FILE_ATTRIBUTE_NORMAL);
    CHECK(st == ST_SUCCESS, "  SET NORMAL (clears READONLY) -> 0x%08x", st);
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                         o.file_id, 0, buf, sizeof(buf), &len);
    CHECK(st == ST_SUCCESS && len >= 40 &&
          !(g32(buf, 32) & MBT_FILE_ATTRIBUTE_READONLY),
          "  READONLY is clear (0x%08x, attributes 0x%08x)", st,
          len >= 40 ? g32(buf, 32) : 0);
    smb2_close(c, o.file_id);
} /* probe_attr_only_clears_readonly */

/* Mount the share on a passthrough backend: a fresh host directory under
 * $CHIMERA_MBT_SCRATCH (default cwd).  Exits 77 when that filesystem cannot
 * produce file handles. */
static void
oa_passthrough_setup(
    struct smb2_env *env,
    const char      *module,
    char            *dir,
    size_t           dir_len)
{
    const char *scratch = getenv("CHIMERA_MBT_SCRATCH");
    char       *abs_scratch;
    int         rc;

    if (!scratch || !scratch[0]) {
        scratch = ".";
    }
    abs_scratch = chimera_test_absolute_path(scratch);
    if (!abs_scratch) {
        fprintf(stderr, "realpath(%s) failed: %s\n", scratch, strerror(errno));
        exit(1);
    }
    snprintf(dir, dir_len, "%s/smb2_oa_XXXXXX", abs_scratch);
    free(abs_scratch);
    if (!mkdtemp(dir)) {
        fprintf(stderr, "mkdtemp(%s) failed: %s\n", dir, strerror(errno));
        exit(1);
    }

    rc = chimera_server_mount(env->server, "share", module, dir, NULL);
    if (rc == CHIMERA_VFS_ENOTSUP) {
        fprintf(stderr, "SKIP: %s backend needs a name_to_handle_at-capable "
                "scratch fs; %s is not one (set CHIMERA_MBT_SCRATCH)\n",
                module, dir);
        chimera_test_remove_tree(dir);
        _exit(77);
    }
    if (rc != 0) {
        fprintf(stderr, "mount %s at %s failed: status=%d\n", module, dir, rc);
        _exit(1);
    }
    chimera_server_create_share(env->server, "share", "share", 0);
} /* oa_passthrough_setup */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env      env;
    struct smb2_env_opts opts = { 0 };
    struct smb2_conn    *c;
    const char          *module      = "memfs";
    char                 pt_dir[512] = { 0 };
    int                  opt;

    setvbuf(stdout, NULL, _IONBF, 0);

    while ((opt = getopt(argc, argv, "b:")) != -1) {
        switch (opt) {
            case 'b':
                module = optarg;
                break;
            default:
                fprintf(stderr, "usage: %s [-b memfs|linux|io_uring]\n",
                        argv[0]);
                return 2;
        } /* switch */
    }

    if (strcmp(module, "memfs") == 0) {
        smb2_env_start_opts(&env, &opts);
    } else if (strcmp(module, "linux") == 0 || strcmp(module, "io_uring") == 0) {
        keeps_dos_attrs = 0;
        smb2_env_open_opts(&env, &opts);
        oa_passthrough_setup(&env, module, pt_dir, sizeof(pt_dir));
    } else {
        fprintf(stderr, "unknown backend %s\n", module);
        return 2;
    }

    printf("# backend=%s\n", module);

    c = smb2_conn_open(&env);
    smb2_handshake(c);

    probe_dir_max_allowed(c);
    probe_file_max_allowed_writes(c);
    probe_metadata_write_ro_handle(c, pt_dir);
    probe_overwrite_metadata_open(c);
    probe_dir_metadata_write(c);
    probe_overwrite_existing_dir(c);
    probe_running_file(c, pt_dir);
    probe_attr_only_clears_readonly(c);

    smb2_env_stop(&env);

    if (pt_dir[0] && chimera_test_remove_tree(pt_dir) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", pt_dir);
    }

    if (failures) {
        printf("# %d check(s) FAILED\n", failures);
        return 1;
    }
    printf("# all checks passed\n");
    return 0;
} /* main */
