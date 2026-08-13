// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4 root ("/") export.
 *
 * With a "/" export configured, the NFSv4 namespace root is the export's real
 * backend directory rather than the synthetic pseudo-root (the NFS-Ganesha
 * Pseudo="/" model): PUTROOTFH resolves the export's backing path, so OPEN,
 * CREATE, READDIR and GETATTR at the mount root are ordinary VFS operations,
 * while sibling exports stay reachable as junctions grafted over the root at
 * LOOKUP.
 *
 * Covered here, over the harness's "/" export backed by the "/share" mount:
 *
 *   - I/O at the namespace root: creating, writing and reading back a file
 *     directly in a mount of "server:/" (impossible against the synthetic
 *     pseudo-root, which had no backing store), plus mkdir.
 *   - The root is real: the same directory mounted via the sibling "/share"
 *     export shows the same inode number and content.
 *   - Junction grafting: an export ("/jroot", added at runtime, backed by the
 *     page00 subdirectory) with NO same-named entry in the root directory
 *     resolves by LOOKUP and is mountable, but does not appear in READDIR of
 *     the root -- junctions are mountable, not browsable.
 *   - knfsd-style subdir mounts: any real subdirectory of the root export is
 *     mountable by its path, over both NFSv4 (PUTROOTFH + LOOKUP) and NFSv3
 *     (MOUNT path resolution through chimera_nfs_find_export_path's
 *     root-export suffix handling).
 *   - Security: restricting the "/" export to krb5 makes an AUTH_SYS mount of
 *     "/" fail, and gates traversal to siblings through the root (matching
 *     knfsd, where a sec-locked root locks traversal); relaxing the policy
 *     restores mountability.
 */

#include "posix_test_common.h"

#define ROOT_EXPORT_JUNCTION_ID (POSIX_TEST_ROOT_EXPORT_ID + 1)

static void
check_file_content(
    struct posix_test_env *env,
    const char            *path,
    const char            *expected)
{
    char    buf[64];
    ssize_t len;
    int     fd;

    fd = chimera_posix_open(path, O_RDONLY, 0);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", path, strerror(errno));
        posix_test_fail(env);
    }

    len = chimera_posix_read(fd, buf, sizeof(buf));
    if (len != (ssize_t) strlen(expected) ||
        memcmp(buf, expected, len) != 0) {
        fprintf(stderr, "Content of %s mismatched\n", path);
        posix_test_fail(env);
    }

    chimera_posix_close(fd);
} /* check_file_content */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    struct stat           st, st_share;
    CHIMERA_DIR          *dir;
    struct dirent        *entry;
    const char           *root_data = "root-export";
    const char           *sub_data  = "root-export-subdir";
    ssize_t               len;
    int                   rc;
    int                   fd;
    int                   saw_rootfile = 0, saw_realsub = 0;

    /* One sibling export ("/page00") alongside "/share" and the "/" export. */
    posix_test_extra_exports = 1;
    posix_test_root_export   = 1;

    posix_test_init(&env, argv, argc);

    if (env.nfs_version != 4) {
        fprintf(stderr, "test_root_export requires an NFS4 backend\n");
        posix_test_fail(&env);
    }

    /* A junction-only sibling: an export name with NO same-named entry in the
     * root directory, backed by the page00 subdirectory.  Added at runtime,
     * which is also how a production export table mutates. */
    if (chimera_server_create_export(env.server, "/jroot", "/page00",
                                     ROOT_EXPORT_JUNCTION_ID, NULL) != 0) {
        fprintf(stderr, "Failed to create /jroot export\n");
        posix_test_fail(&env);
    }

    /* Mount the namespace root. */
    rc = chimera_posix_mount_with_options("/root", "nfs", "127.0.0.1:/",
                                          "vers=4");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount \"/\" export: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* OPEN/CREATE at the namespace root -- the operation the synthetic
     * pseudo-root could never serve. */
    fd = chimera_posix_open("/root/rootfile", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create file at namespace root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    len = chimera_posix_write(fd, root_data, strlen(root_data));
    if (len != (ssize_t) strlen(root_data)) {
        fprintf(stderr, "Failed to write at namespace root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    check_file_content(&env, "/root/rootfile", root_data);

    /* Mutation at the root directory. */
    rc = chimera_posix_mkdir("/root/realsub", 0755);
    if (rc != 0) {
        fprintf(stderr, "Failed to mkdir at namespace root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/root/realsub/subfile", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create file in root subdir: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    len = chimera_posix_write(fd, sub_data, strlen(sub_data));
    if (len != (ssize_t) strlen(sub_data)) {
        fprintf(stderr, "Failed to write in root subdir: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    /* READDIR of the root lists the real directory: the files just created
     * appear; junction-only names ("jroot") and the "/" and "/share" export
     * names do not.  (page00 does appear -- it is a real subdirectory the
     * harness created as the sibling export's backing store.) */
    dir = chimera_posix_opendir("/root");
    if (!dir) {
        fprintf(stderr, "Failed to open namespace root dir: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    for (;;) {
        errno = 0;
        entry = chimera_posix_readdir(dir);
        if (!entry) {
            break;
        }

        if (strcmp(entry->d_name, "rootfile") == 0) {
            saw_rootfile++;
        } else if (strcmp(entry->d_name, "realsub") == 0) {
            saw_realsub++;
        } else if (strcmp(entry->d_name, "jroot") == 0 ||
                   strcmp(entry->d_name, "share") == 0 ||
                   strcmp(entry->d_name, "/") == 0) {
            fprintf(stderr, "Namespace root READDIR leaked export name '%s'\n",
                    entry->d_name);
            posix_test_fail(&env);
        }
    }

    if (errno != 0) {
        fprintf(stderr, "READDIR of namespace root failed: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_closedir(dir);

    if (saw_rootfile != 1 || saw_realsub != 1) {
        fprintf(stderr, "Namespace root READDIR missed real entries "
                "(rootfile %d, realsub %d)\n", saw_rootfile, saw_realsub);
        posix_test_fail(&env);
    }

    /* The root is the same real directory the "/share" export serves. */
    rc = chimera_posix_stat("/root", &st);
    if (rc != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Failed to stat namespace root\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_mount_with_options("/share2", "nfs",
                                          "127.0.0.1:/share", "vers=4");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount /share alongside \"/\": %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/share2", &st_share);
    if (rc != 0 || st_share.st_ino != st.st_ino) {
        fprintf(stderr, "\"/\" and /share roots differ (ino %llu vs %llu); "
                "namespace root is not the real backend directory\n",
                (unsigned long long) st.st_ino,
                (unsigned long long) st_share.st_ino);
        posix_test_fail(&env);
    }

    check_file_content(&env, "/share2/rootfile", root_data);

    /* Junction grafting: "jroot" has no entry in the root directory, so this
    * stat can only succeed by the LOOKUP junction into the /jroot export. */
    rc = chimera_posix_stat("/root/jroot", &st);
    if (rc != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Junction lookup of /root/jroot failed: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_mount_with_options("/jrootm", "nfs",
                                          "127.0.0.1:/jroot", "vers=4");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount junction export /jroot: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/jrootm/marker", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create marker in /jroot: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    /* The junction and the sibling export share the page00 backing dir. */
    rc = chimera_posix_stat("/root/jroot/marker", &st);
    if (rc != 0) {
        fprintf(stderr, "Marker not visible through the junction: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/root/page00/marker", &st);
    if (rc != 0) {
        fprintf(stderr, "Marker not visible in the junction's backing dir: "
                "%s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* knfsd-style subdirectory mount: any real subdir of the root export is
     * mountable by path. */
    rc = chimera_posix_mount_with_options("/sub", "nfs",
                                          "127.0.0.1:/realsub", "vers=4");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount subdirectory of \"/\": %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    check_file_content(&env, "/sub/subfile", sub_data);

    /* A path that is neither an export, a junction, nor a real entry. */
    rc = chimera_posix_mount_with_options("/nosuch", "nfs",
                                          "127.0.0.1:/nosuchpath", "vers=4");
    if (rc == 0 || errno != ENOENT) {
        fprintf(stderr, "Mount of a nonexistent path under \"/\" returned "
                "%s, expected ENOENT\n", rc == 0 ? "success" : strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/root/nosuchpath", &st);
    if (rc == 0 || errno != ENOENT) {
        fprintf(stderr, "Lookup of a nonexistent root entry returned %s, "
                "expected ENOENT\n", rc == 0 ? "success" : strerror(errno));
        posix_test_fail(&env);
    }

    /* NFSv3: the same namespace resolves through the MOUNT protocol and
     * chimera_nfs_find_export_path's root-export suffix handling. */
    rc = chimera_posix_mount_with_options("/root3", "nfs", "127.0.0.1:/",
                                          "vers=3");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount \"/\" over NFSv3: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    check_file_content(&env, "/root3/rootfile", root_data);

    rc = chimera_posix_mount_with_options("/sub3", "nfs",
                                          "127.0.0.1:/realsub", "vers=3");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount subdirectory of \"/\" over NFSv3: "
                "%s\n", strerror(errno));
        posix_test_fail(&env);
    }

    check_file_content(&env, "/sub3/subfile", sub_data);

    /* Security: a krb5-only "/" export refuses an AUTH_SYS mount of "/", and
     * gates traversal to siblings through the root (knfsd behavior for a
     * sec-locked root).  The junction export itself has no policy -- the
     * denial can only come from the root. */
    rc = chimera_server_export_set_sec(env.server, "/", CHIMERA_NFS_SEC_KRB5);
    if (rc != 0) {
        fprintf(stderr, "Failed to set \"/\" export sec policy\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_mount_with_options("/rootk", "nfs", "127.0.0.1:/",
                                          "vers=4");
    if (rc == 0) {
        fprintf(stderr, "AUTH_SYS mount of a krb5-only \"/\" export "
                "unexpectedly succeeded\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_mount_with_options("/jk", "nfs", "127.0.0.1:/jroot",
                                          "vers=4");
    if (rc == 0) {
        fprintf(stderr, "Sibling mount through a krb5-only \"/\" export "
                "unexpectedly succeeded\n");
        posix_test_fail(&env);
    }

    rc = chimera_server_export_set_sec(env.server, "/", 0);
    if (rc != 0) {
        fprintf(stderr, "Failed to clear \"/\" export sec policy\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_mount_with_options("/rootk", "nfs", "127.0.0.1:/",
                                          "vers=4");
    if (rc != 0) {
        fprintf(stderr, "Mount of \"/\" after clearing sec policy failed: "
                "%s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_umount("/rootk");
    chimera_posix_umount("/sub3");
    chimera_posix_umount("/root3");
    chimera_posix_umount("/sub");
    chimera_posix_umount("/jrootm");
    chimera_posix_umount("/share2");
    chimera_posix_umount("/root");

    fprintf(stderr, "Root export test passed\n");

    posix_test_success(&env);

    return 0;
} /* main */
