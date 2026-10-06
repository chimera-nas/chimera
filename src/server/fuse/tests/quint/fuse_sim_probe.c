// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Proof of concept for the in-process FUSE harness (fuse_sim.h): drive the
 * chimera FUSE server over a socketpair with no /dev/fuse, no mount, and no
 * privileges, and assert on things that are only visible ON THE WIRE.
 *
 * The first half is plumbing verification -- INIT, LOOKUP, CREATE, WRITE,
 * READ, UNLINK round trips prove the server cannot tell a simulated kernel
 * from a real one.  The second half is the actual point: reply fields that a
 * real mount consumes and hides (cache TTLs, FOPEN flags) and the
 * invalidation notifications the coherence machinery emits.
 */

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "fuse_sim.h"

static int failures;

#define CHECK(cond, ...) \
        do { \
            if (!(cond)) { \
                printf("FAIL: " __VA_ARGS__); printf("\n"); \
                failures++; \
            } else { \
                printf("ok:   " __VA_ARGS__); printf("\n"); \
            } \
        } while (0)

static void
test_unlinked_directory(struct fuse_sim *sim)
{
    struct fuse_entry_out entry  = { 0 };
    struct fuse_open_out  opened = { 0 };
    uint64_t              storage[16];
    uint8_t              *buf         = (uint8_t *) storage;
    size_t                dot_size    = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + 1);
    size_t                dotdot_size = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + 2);
    int                   rc;

    /* Root avoids the DAC gate's incidental backend open. No other user or
     * descriptor may pin this inode: OPENDIR alone must keep it alive. */
    rc = fuse_sim_mkdir(sim, FUSE_ROOT_ID, "open-removed-dir", 0700, 0, &entry);
    CHECK(rc == 0 && entry.nodeid != 0, "root creates directory to remove while open (rc %d)", rc);
    if (rc != 0 || entry.nodeid == 0) {
        return;
    }
    rc = fuse_sim_opendir(sim, entry.nodeid, &opened);
    CHECK(rc == 0 && opened.fh != 0, "root OPENDIR retains the directory (rc %d)", rc);
    if (rc != 0 || opened.fh == 0) {
        fuse_sim_rmdir(sim, FUSE_ROOT_ID, "open-removed-dir");
        return;
    }
    rc = fuse_sim_rmdir(sim, FUSE_ROOT_ID, "open-removed-dir");
    CHECK(rc == 0, "RMDIR removes an open empty directory (rc %d)", rc);

    /* FUSE rewinds a directory by issuing READDIR at offset zero again. Both
     * sweeps must return only dot entries, followed by a successful EOF. */
    for (unsigned int pass = 0; rc == 0 && pass < 2; pass++) {
        size_t outlen = 0;

        rc = fuse_sim_readdir(sim, entry.nodeid, opened.fh, 0,
                              buf, sizeof(storage), &outlen);
        CHECK(rc == 0 && outlen == dot_size + dotdot_size,
              "unlinked directory READDIR/rewind pass %u succeeds (rc %d, %zu bytes)",
              pass, rc, outlen);
        if (rc != 0 || outlen != dot_size + dotdot_size) {
            break;
        }
        const struct fuse_dirent *dot    = (const struct fuse_dirent *) buf;
        const struct fuse_dirent *dotdot = (const struct fuse_dirent *) (buf + dot_size);

        CHECK(dot->namelen == 1 && dot->name[0] == '.' &&
              dotdot->namelen == 2 && memcmp(dotdot->name, "..", 2) == 0,
              "unlinked directory pass %u contains only dot entries", pass);
        uint64_t                  cookie = dotdot->off;
        CHECK(cookie != 0, "unlinked directory pass %u advances its cursor", pass);
        if (cookie == 0) {
            break;
        }
        outlen = 0;
        rc     = fuse_sim_readdir(sim, entry.nodeid, opened.fh, cookie,
                                  buf, sizeof(storage), &outlen);
        CHECK(rc == 0 && outlen == 0,
              "unlinked directory pass %u reaches EOF (rc %d, %zu bytes)", pass, rc, outlen);
    }
    rc = fuse_sim_releasedir(sim, entry.nodeid, opened.fh);
    CHECK(rc == 0, "RELEASEDIR drops the unlinked directory (rc %d)", rc);
} /* test_unlinked_directory */

int
main(
    int   argc,
    char *argv[])
{
    struct fuse_sim       sim;
    struct fuse_entry_out entry;
    struct fuse_entry_out mknod_entry;
    struct fuse_open_out  open_out;
    struct fuse_attr_out  attr;
    char                  buf[64];
    size_t                outlen;
    uint32_t              written = 0;
    int                   rc;

    setvbuf(stdout, NULL, _IONBF, 0);

    /* Every reply struct is inspected on failure paths too (assertions report
     * what they saw), so none may hold indeterminate bytes. */
    memset(&entry, 0, sizeof(entry));
    memset(&mknod_entry, 0, sizeof(mknod_entry));
    memset(&open_out, 0, sizeof(open_out));
    memset(&attr, 0, sizeof(attr));

    fuse_sim_open(&sim, "simfs");

    /* --- the session itself --- */

    CHECK(sim.proto_minor > 0, "INIT completed over the socketpair (ABI 7.%u)",
          sim.proto_minor);
    CHECK(sim.max_write >= 4096, "negotiated max_write %u", sim.max_write);

    test_unlinked_directory(&sim);

    /* --- a file, created and used entirely through the wire --- */

    rc = fuse_sim_lookup(&sim, FUSE_ROOT_ID, "nothing", &entry);
    CHECK(rc == ENOENT || (rc == 0 && entry.nodeid == 0),
          "LOOKUP of a missing name reports absence (rc %d, nodeid %llu)",
          rc, (unsigned long long) entry.nodeid);

    rc = fuse_sim_create(&sim, FUSE_ROOT_ID, "poc", 0644, &entry, &open_out);
    CHECK(rc == 0 && entry.nodeid != 0, "CREATE returns a nodeid (rc %d)", rc);
    CHECK(S_ISREG(entry.attr.mode) && (entry.attr.mode & 07777) == 0644,
          "CREATE reports mode 0644 (0%o)", entry.attr.mode & 07777);

    rc = fuse_sim_write(&sim, entry.nodeid, open_out.fh, 0, "hello", 5,
                        &written);
    CHECK(rc == 0 && written == 5, "WRITE 5 bytes (rc %d, wrote %u)", rc,
          written);

    memset(buf, 0, sizeof(buf));
    outlen = 0;
    rc     = fuse_sim_read(&sim, entry.nodeid, open_out.fh, 0, sizeof(buf),
                           buf, &outlen);
    CHECK(rc == 0 && outlen == 5 && memcmp(buf, "hello", 5) == 0,
          "READ returns what WRITE stored (rc %d, %zu bytes)", rc, outlen);

    rc = fuse_sim_getattr(&sim, entry.nodeid, &attr);
    CHECK(rc == 0 && attr.attr.size == 5, "GETATTR reports size 5 (rc %d, %llu)",
          rc, (unsigned long long) attr.attr.size);

    /*
     * POSIX lets mknod(2) create a REGULAR file, and for that the kernel
     * sends MKNOD rather than CREATE.  No other protocol server reaches that
     * path: the POSIX client reroutes S_IFREG to open(O_CREAT) because NFS
     * has no wire form for it, and NFS3 MKNOD / NFS4 CREATE / SMB CREATE
     * cover special files only.  FUSE forwards it, which is how it found a
     * memfs inode recycled off the free list with a stale block-array
     * pointer that unlink then freed -- heap corruption, not a wrong answer,
     * so nothing short of running it would have shown it.
     */
    rc = fuse_sim_mknod(&sim, FUSE_ROOT_ID, "viamknod", S_IFREG | 0644, 0, 0,
                        &mknod_entry);
    CHECK(rc == 0 && mknod_entry.nodeid != 0,
          "MKNOD creates a regular file (rc %d)", rc);
    CHECK(S_ISREG(mknod_entry.attr.mode),
          "MKNOD reports it as regular (mode 0%o)", mknod_entry.attr.mode);

    rc = fuse_sim_unlink(&sim, FUSE_ROOT_ID, "viamknod");
    CHECK(rc == 0, "a mknod-created regular file unlinks cleanly (rc %d)", rc);

    /*
     * --- what only the wire shows ---
     *
     * The open reply's flags ARE the cache policy: coverage in force means
     * the kernel may keep its pages across open/close (FOPEN_KEEP_CACHE),
     * and no coverage under sync coherence must fall back to uncached
     * (FOPEN_DIRECT_IO) rather than risk serving pages we cannot invalidate.
     * Through a real mount this decision is invisible -- here it is a field.
     */
    rc = fuse_sim_open_file(&sim, entry.nodeid, O_RDONLY, &open_out);
    CHECK(rc == 0, "OPEN succeeds (rc %d)", rc);
    CHECK((open_out.open_flags & (FOPEN_KEEP_CACHE | FOPEN_DIRECT_IO)) != 0,
          "OPEN states a cache policy (flags 0x%x)", open_out.open_flags);
    CHECK(!((open_out.open_flags & FOPEN_KEEP_CACHE) &&
            (open_out.open_flags & FOPEN_DIRECT_IO)),
          "KEEP_CACHE and DIRECT_IO are never both set");

    /*
     * Entry replies carry the lifetimes the kernel will trust the dentry and
     * attributes for.  Under coherence=sync those are supposed to be
     * conditioned on live coverage rather than handed out unconditionally,
     * which is a claim about a reply field -- checkable only here.
     */
    rc = fuse_sim_lookup(&sim, FUSE_ROOT_ID, "poc", &entry);
    CHECK(rc == 0, "LOOKUP of the created file succeeds (rc %d)", rc);
    printf("      entry_valid=%llus.%uns attr_valid=%llus.%uns\n",
           (unsigned long long) entry.entry_valid, entry.entry_valid_nsec,
           (unsigned long long) entry.attr_valid, entry.attr_valid_nsec);

    fuse_sim_release(&sim, entry.nodeid, open_out.fh);

    /*
     * A namespace mutation through this same session must not be GATED on an
     * invalidation to itself: the mutating kernel holds the directory lock
     * while awaiting its reply, so blocking that reply on its own ack is a
     * self-deadlock.  That is what the origin exemption in
     * chimera_vfs_notify_emit_sync prevents, and completing this unlink at
     * all is the observable proof.
     *
     * Note what is deliberately NOT asserted: that no invalidation arrives.
     * The exemption covers the SYNCHRONOUS emit only -- the asynchronous emit
     * that follows the completion still reaches every watcher including the
     * origin, by design ("converges its view"), and it is racy by nature.  An
     * earlier version of this test counted notifications and asserted zero;
     * it passed by luck until more mutations were added ahead of it, then
     * failed ~10% of the time on stragglers from those earlier operations.
     * A redundant async invalidation to the originating kernel is wasted
     * work, never incorrectness, so counting them tests nothing real.
     */
    rc = fuse_sim_unlink(&sim, FUSE_ROOT_ID, "poc");
    CHECK(rc == 0, "a session's own mutation completes, ungated (rc %d)", rc);

    rc = fuse_sim_lookup(&sim, FUSE_ROOT_ID, "poc", &entry);
    CHECK(rc == ENOENT || (rc == 0 && entry.nodeid == 0),
          "the unlinked name is gone (rc %d)", rc);

    fuse_sim_close(&sim);

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
