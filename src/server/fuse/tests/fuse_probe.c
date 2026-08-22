// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * FUSE capability probe: can this environment actually establish a FUSE
 * mount?  Opens /dev/fuse (creating the device node if the container left it
 * unpopulated) and performs a real fuse mount in a private temp directory --
 * which succeeds without an INIT handshake -- then detaches it.  Exit 0 when
 * FUSE serving is possible, 2 when the environment lacks the capability;
 * scripts/fuse_test_wrapper.sh turns 2 into a ctest skip.
 *
 * Deliberately standalone (no chimera libraries) so gating never depends on
 * the code under test.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>

int
main(
    int   argc,
    char *argv[])
{
    char opts[128];
    char dir[] = "/tmp/chimera_fuse_probe.XXXXXX";
    int  fd;

    fd = open("/dev/fuse", O_RDWR | O_CLOEXEC);

    if (fd < 0 && errno == ENOENT) {
        /* Privileged container on a fuse-capable host, node unpopulated. */
        if (mknod("/dev/fuse", S_IFCHR | 0666, makedev(10, 229)) == 0) {
            fd = open("/dev/fuse", O_RDWR | O_CLOEXEC);
        }
    }

    if (fd < 0) {
        fprintf(stderr, "fuse_probe: cannot open /dev/fuse: %s\n",
                strerror(errno));
        return 2;
    }

    if (!mkdtemp(dir)) {
        fprintf(stderr, "fuse_probe: mkdtemp: %s\n", strerror(errno));
        return 2;
    }

    snprintf(opts, sizeof(opts), "fd=%d,rootmode=%o,user_id=%u,group_id=%u",
             fd, S_IFDIR | 0755, getuid(), getgid());

    if (mount("chimera_fuse_probe", dir, "fuse.chimera_probe",
              MS_NOSUID | MS_NODEV, opts) != 0) {
        fprintf(stderr, "fuse_probe: mount: %s\n", strerror(errno));
        rmdir(dir);
        return 2;
    }

    umount2(dir, MNT_DETACH);
    close(fd);
    rmdir(dir);

    printf("fuse_probe: FUSE mount capability present\n");

    return 0;
} /* main */
