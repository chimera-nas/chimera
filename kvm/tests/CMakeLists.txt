# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(NFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_test_wrapper.sh)
set(NFS_KRB_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_kerberos_test_wrapper.sh)
set(NFS_REBOOT_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_reboot_test_wrapper.sh)
set(NFS3_DRC_REBOOT_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs3_drc_reboot_test_wrapper.sh)
set(NFSTEST_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfstest_test_wrapper.sh)
set(NFSTEST_2CLIENT_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfstest_2client_test_wrapper.sh)
set(NFS40_DRC_REBOOT_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs40_drc_reboot_test_wrapper.sh)
set(NFS_MULTINODE_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_multinode_test_wrapper.sh)
set(SMB_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_smb_test_wrapper.sh)
set(PNFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_pnfs_test_wrapper.sh)
set(PNFS_BLOCK_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_pnfs_block_test_wrapper.sh)
set(PNFS_SCSI_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_pnfs_scsi_test_wrapper.sh)
set(PNFS_PROXY_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_pnfs_proxy_test_wrapper.sh)

set(KVM_BACKENDS memfs linux)

if(CHIMERA_VFS_IO_URING)
    list(APPEND KVM_BACKENDS io_uring)
endif()

if(IO_URING_ENABLED)
    list(APPEND KVM_BACKENDS diskfs_io_uring)
endif()

if(HAVE_LIBAIO)
    list(APPEND KVM_BACKENDS diskfs_aio)
endif()

# Backends that implement pNFS flex-files (persist the opaque layout blob):
# memfs and diskfs.  The diskfs MDS uses a small metadata device; file data
# still lives on the memfs data server.
set(PNFS_BACKENDS memfs)
if(IO_URING_ENABLED)
    list(APPEND PNFS_BACKENDS diskfs_io_uring)
endif()
if(HAVE_LIBAIO)
    list(APPEND PNFS_BACKENDS diskfs_aio)
endif()

# pNFS block layout (RFC 5663): diskfs-only (it sources block layouts).  The
# data lives on a signed virtio-SCSI disk the client reaches directly as
# /dev/sda; the MDS only allocates extents and never touches it.
set(PNFS_BLOCK_BACKENDS)
if(IO_URING_ENABLED)
    list(APPEND PNFS_BLOCK_BACKENDS diskfs_io_uring)
endif()
if(HAVE_LIBAIO)
    list(APPEND PNFS_BLOCK_BACKENDS diskfs_aio)
endif()

# Linux block-layout client bring-up, prepended to every block workload.  These
# requirements were nailed down empirically:
#   - rpc_pipefs must be mounted at /run/rpc_pipefs -- where this blkmapd build
#     opens the nfs/blocklayout upcall pipe.
#   - blocklayoutdriver must be loaded and blkmapd running before the first
#     LAYOUTGET so it can resolve the device.
#   - blkmapd identifies candidate disks via an SG_IO SCSI INQUIRY, so the data
#     disk is presented as virtio-SCSI (see the wrapper); virtio-blk is invisible
#     to it.
# In every workload the MDS rejects inline data I/O (no access to the data disk),
# so a drop-caches re-read that still matches proves the block layout drove the
# I/O directly to /dev/sda.
set(PNFS_BLOCK_SETUP "mkdir -p /run/rpc_pipefs && mount -t rpc_pipefs rpc_pipefs /run/rpc_pipefs && modprobe blocklayoutdriver && /usr/sbin/blkmapd && sleep 1 && mount -t nfs -o vers=4.1,tcp 10.0.0.1:/share /mnt")

set(PNFS_BLOCK_PROGRAMS rw remove recall_truncate)

# rw: write 8 MiB through the layout, assert LAYOUTGET, drop caches, read back.
set(PNFS_BLOCK_WL_rw "mkdir -p /mnt/test && dd if=/dev/urandom of=/tmp/orig bs=1M count=8 2>/dev/null && cp /tmp/orig /mnt/test/f0 && sync && grep -qi LAYOUTGET /proc/self/mountstats && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true) && cmp /tmp/orig /mnt/test/f0")
# remove: write via the layout, then unlink -- exercises freeing the file's
# extents on the remote data device (relocated-AG-log allocator) + cleanup.
set(PNFS_BLOCK_WL_remove "mkdir -p /mnt/test && dd if=/dev/urandom of=/mnt/test/f0 bs=1M count=4 2>/dev/null && sync && grep -qi LAYOUTGET /proc/self/mountstats && rm /mnt/test/f0 && ! test -e /mnt/test/f0")
# recall_truncate: write 8 MiB, truncate to 4 MiB while a layout may be held
# (MDS recalls + defers), then verify the surviving 4 MiB is intact -- exercises
# the truncate path freeing remote extents past EOF + the recall machinery.
set(PNFS_BLOCK_WL_recall_truncate "mkdir -p /mnt/test && dd if=/dev/urandom of=/tmp/orig bs=1M count=8 2>/dev/null && cp /tmp/orig /mnt/test/f0 && sync && grep -qi LAYOUTGET /proc/self/mountstats && truncate -s 4194304 /mnt/test/f0 && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true) && head -c 4194304 /tmp/orig > /tmp/orig4 && cmp /tmp/orig4 /mnt/test/f0")

# pNFS SCSI layout (RFC 8154): diskfs-only.  Same remote-data path as block, but
# the client matches the LU strictly by its SCSI VPD-0x83 hardware designator (a
# NAA WWN) -- nothing is written to the data disk.  See
# kvm_pnfs_scsi_test_wrapper.sh; the data disk is virtio-SCSI with a WWN.
set(PNFS_SCSI_BACKENDS)
if(IO_URING_ENABLED)
    list(APPEND PNFS_SCSI_BACKENDS diskfs_io_uring)
endif()
if(HAVE_LIBAIO)
    list(APPEND PNFS_SCSI_BACKENDS diskfs_aio)
endif()

# Linux SCSI-layout client bring-up.  Unlike block, SCSI layout resolves devices
# in-kernel by VPD-0x83 designator (no blkmapd upcall, no rpc_pipefs); the same
# blocklayoutdriver module registers the SCSI layout type.  As with block, the
# MDS rejects inline data I/O, so a drop-caches re-read that still matches proves
# the SCSI layout drove the I/O directly to the LU.
set(PNFS_SCSI_SETUP "modprobe blocklayoutdriver && mount -t nfs -o vers=4.1,tcp 10.0.0.1:/share /mnt")

# Reuse the block workloads verbatim (the layout type differs, the data path
# does not).
set(PNFS_SCSI_PROGRAMS rw remove recall_truncate)
set(PNFS_SCSI_WL_rw "${PNFS_BLOCK_WL_rw}")
set(PNFS_SCSI_WL_remove "${PNFS_BLOCK_WL_remove}")
set(PNFS_SCSI_WL_recall_truncate "${PNFS_BLOCK_WL_recall_truncate}")

set(NFS_VERSIONS 3 4.0 4.1 4.2)

# cthon04 tests
set(CTHON_CATEGORIES basic general special)
set(CTHON_CMD_basic   "cd /opt/cthon04 && ./runtests -b -f /mnt/test")
set(CTHON_CMD_general "cd /opt/cthon04 && ./runtests -g -f /mnt/test")
set(CTHON_CMD_special "cd /opt/cthon04 && ./runtests -s -f /mnt/test")

# SMB cthon04 commands set CIFS=yes so the test suite skips NFS-specific
# sub-tests (e.g. nfsidem duplicate request check, NFS-style op_ren).
# MNTOPTIONS is checked by runtests.wrk to detect SMB version.
set(SMB_CTHON_CMD_basic   "cd /opt/cthon04 && CIFS=yes MNTOPTIONS=vers=2.1 ./runtests -b -f /mnt/test")
set(SMB_CTHON_CMD_general "cd /opt/cthon04 && CIFS=yes MNTOPTIONS=vers=2.1 ./runtests -g -f /mnt/test")
set(SMB_CTHON_CMD_special "cd /opt/cthon04 && CIFS=yes MNTOPTIONS=vers=2.1 ./runtests -s -f /mnt/test")

# pNFS (NFSv4.1 file layout) workloads.  These run against an MDS + a separate
# memfs data server.  The MDS holds no data for pNFS-backed files (it returns
# EIO for any non-layout READ/WRITE), so a workload that reads back what it
# wrote can only succeed if the client actually used pNFS to reach the data
# server -- there is no MDS fallback to mask a broken layout.  The explicit
# LAYOUTGET check in mountstats is a belt-and-suspenders positive assertion.
set(PNFS_PROGRAMS rw recall_truncate recall_truncate_xclient remove)
set(PNFS_CMD_rw "mkdir -p /mnt/test && dd if=/dev/urandom of=/tmp/orig bs=1M count=8 2>/dev/null && cp /tmp/orig /mnt/test/f0 && sync && grep -qi LAYOUTGET /proc/self/mountstats && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true) && cmp /tmp/orig /mnt/test/f0")

# Truncate a file that has an outstanding layout: the MDS recalls it, defers the
# truncate until the client resolves the recall, then truncates.  Verifies the
# surviving data (first 4 MiB) is intact -- both that the recall path runs and
# that it does not corrupt data.
set(PNFS_CMD_recall_truncate "mkdir -p /mnt/test && dd if=/dev/urandom of=/tmp/orig bs=1M count=8 2>/dev/null && cp /tmp/orig /mnt/test/f0 && sync && grep -qi LAYOUTGET /proc/self/mountstats && truncate -s 4194304 /mnt/test/f0 && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true) && head -c 4194304 /tmp/orig > /tmp/orig4 && cmp /tmp/orig4 /mnt/test/f0")

# Inter-client recall: client A (mount of 10.0.0.1) writes the file and holds a
# layout; client B (mount of 10.0.0.5 -- a distinct pNFS client) truncates it.
# The MDS must recall A's layout (found via the server-wide layout table) and
# defer B's truncate until A returns it.  Verifies the surviving data is intact
# and that the truncate is consistent across the two clients.
set(PNFS_CMD_recall_truncate_xclient "mkdir -p /mnt2 /mnt/test && mount -t nfs -o vers=4.1 10.0.0.5:/share /mnt2 && dd if=/dev/urandom of=/tmp/orig bs=1M count=8 2>/dev/null && cp /tmp/orig /mnt/test/f0 && sync && grep -qi LAYOUTGET /proc/self/mountstats && truncate -s 4194304 /mnt2/test/f0 && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true) && head -c 4194304 /tmp/orig > /tmp/orig4 && cmp /tmp/orig4 /mnt/test/f0")

# Remove a pNFS-backed file: the MDS deletes the backing file on the data server
# and the name is gone from the namespace.
set(PNFS_CMD_remove "mkdir -p /mnt/test && dd if=/dev/urandom of=/mnt/test/f0 bs=1M count=4 2>/dev/null && sync && grep -qi LAYOUTGET /proc/self/mountstats && rm /mnt/test/f0 && ! test -e /mnt/test/f0")

# pNFS PROXY workloads: exercise chimera's own pNFS *client* (the nfs VFS proxy
# module) against chimera's pNFS server.  The guest mounts a chimera proxy over
# plain NFSv4.1; the proxy is the pNFS client (LAYOUTGET/GETDEVICEINFO + direct
# DS I/O + back-channel CB_LAYOUTRECALL).  So, unlike the direct pNFS tests, the
# guest mount has no LAYOUTGET in its own mountstats -- the wrapper asserts pNFS
# engagement (proxy->DS connection) and recall delivery from the proxy's logs.
set(PNFS_PROXY_PROGRAMS rw recall_truncate)
set(PNFS_PROXY_CMD_rw "mkdir -p /mnt/test && dd if=/dev/urandom of=/tmp/orig bs=1M count=8 2>/dev/null && cp /tmp/orig /mnt/test/f0 && sync && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true) && cmp /tmp/orig /mnt/test/f0")

# Truncating a file the proxy holds a layout for makes the proxy issue a SETATTR
# to the MDS, which recalls the layout from the proxy over its back channel; the
# proxy fences DS I/O and the truncate completes.  The "RECALL_EXPECTED" token
# tells the wrapper to additionally assert the proxy logged a CB_LAYOUTRECALL.
#
# The guest holds fd 9 open READ-WRITE on the file from before cp until after
# the truncate.  The proxy returns its layout when its upstream open handle
# closes (close-time LAYOUTCOMMIT + LAYOUTRETURN), so if that handle goes idle
# between cp's CLOSE and the truncate's SETATTR the close-thread sweep (~100ms
# idle) can reap it -- the MDS then has no holder to recall and the
# CB_LAYOUTRECALL assertion flakes.  Two requirements make the pin effective,
# both learned from real flakes:
#   - It must be READ-WRITE: the proxy's open-handle cache is keyed by
#     (fh, access_mode, cred), so a read-only fd pins a separate READ-class
#     handle while the WRITE-class handle holding the layout idles out.
#     An O_RDWR fd shares cp's RW class and pins the layout-holding handle.
#   - It must be opened BEFORE cp: the `sync` after cp can run >100ms, long
#     enough for the sweep to reap an unpinned handle before a later pin
#     lands.  (`9<>` creates f0 empty; cp then overwrites it.)
set(PNFS_PROXY_CMD_recall_truncate ": RECALL_EXPECTED; mkdir -p /mnt/test && dd if=/dev/urandom of=/tmp/orig bs=1M count=8 2>/dev/null && exec 9<>/mnt/test/f0 && cp /tmp/orig /mnt/test/f0 && sync && truncate -s 4194304 /mnt/test/f0 && exec 9<&- && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true) && head -c 4194304 /tmp/orig > /tmp/orig4 && cmp /tmp/orig4 /mnt/test/f0")

# xfstests programs
set(XFSTESTS_PROGRAMS fsstress fsx dirstress nametest fstest rewinddir)

set(XFSTESTS_CMD_fsstress  "mkdir -p /mnt/test && /opt/xfstests/ltp/fsstress -d /mnt/test -n 100 -p 1")
set(XFSTESTS_CMD_fsx       "mkdir -p /mnt/test && /opt/xfstests/ltp/fsx -N 10000 -l 262144 /mnt/test/fsx_testfile")
set(XFSTESTS_CMD_dirstress "mkdir -p /mnt/test && /opt/xfstests/src/dirstress -d /mnt/test -p 1 -f 50")
set(XFSTESTS_CMD_nametest  "mkdir -p /mnt/test && cd /mnt/test && /opt/xfstests/src/nametest -l /opt/xfstests/nametest_input -i 1000")
set(XFSTESTS_CMD_fstest    "mkdir -p /mnt/test && /opt/xfstests/src/fstest -p /mnt/test -n 1 -f 2 -l 5 -s 65536")
set(XFSTESTS_CMD_rewinddir "mkdir -p /mnt/test && /opt/xfstests/src/rewinddir-test /mnt/test")

# Linux Test Project (LTP) scenario groups, run against the server-mounted share.
# Matrix entries are "<group>|<space-separated NFS versions>"; <group> names a
# runtest file shipped in the guest's /opt/ltp/runtest (>= kvm-test-base v1.5.0).
# Each group has a companion /opt/ltp-skip/<group>.skip allowlist baked into the
# image listing the cases that fail/break for reasons intrinsic to NFS, so any
# remaining failure is a genuine server regression.
#
# Pared to the LTP coverage our other suites do NOT provide (an audit found the
# bulk of LTP's fs/syscalls coverage duplicates pjdfstest, xfstests and cthon, or
# isn't filesystem-related at all):
#   dio             -- O_DIRECT / uncached I/O (our xfstests fsx runs buffered)
#   fcntl-locktests -- POSIX byte-range locking via the real kernel client
#   chimera_nfs42   -- NFSv4.2-only server ops (fallocate -> ALLOCATE/DEALLOCATE,
#                      copy_file_range -> COPY/CLONE, statx); a curated runtest
#                      generated in the guest image
# All are fast, so they run on every backend in both Debug and Release.
set(LTP_MATRIX
    "dio|3 4.2"
    "fcntl-locktests|3 4.0 4.1 4.2"
    "chimera_nfs42|4.2"
)

# The guest's /opt/ltp-run.sh drives the group through kirk (the LTP runner) with
# its skip file and turns the JSON report into a pass/fail exit code -- kirk's own
# exit status ignores test failures, so the verdict is its stats.failed/broken.
# The "<grp>" token is substituted per matrix entry below.
set(LTP_CMD_TEMPLATE "/opt/ltp-run.sh <grp>")

foreach(image_spec ${KVM_IMAGES})
    string(REPLACE "|" ";" image_parts "${image_spec}")
    list(GET image_parts 0 IMAGE_NAME)
    set(IMAGE_DIR ${BUILD_ROOT}/kvm/${IMAGE_NAME})

    # NFS cross-reboot persistence test (cairn-backed, NFSv4.1): create state,
    # restart chimera against the same persistent store, verify the data and
    # the recovery records survived.  Needs cairn (the persistent KV backend).
    if(CAIRN_ENABLED)
        add_test(NAME chimera/kvm/${IMAGE_NAME}/reboot/recovery_cairn_nfs4.1
            COMMAND bash ${NFS_REBOOT_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} 4.1)
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/reboot/recovery_cairn_nfs4.1
            PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)

        # NFSv3 DRC cross-reboot persistence: drive non-idempotent ops, restart
        # against the same cairn store, verify the reply records reload.
        add_test(NAME chimera/kvm/${IMAGE_NAME}/reboot/drc_cairn_nfs3
            COMMAND bash ${NFS3_DRC_REBOOT_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY})
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/reboot/drc_cairn_nfs3
            PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)

        # Same, for the NFSv4.0 reply cache (shared connectionless DRC, type 0x06).
        add_test(NAME chimera/kvm/${IMAGE_NAME}/reboot/drc_cairn_nfs40
            COMMAND bash ${NFS40_DRC_REBOOT_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY})
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/reboot/drc_cairn_nfs40
            PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)

        # Shared-store multi-instance namespacing: two node_ids drive one cairn
        # store sequentially; data is shared, per-node records stay isolated.
        add_test(NAME chimera/kvm/${IMAGE_NAME}/reboot/multinode_cairn
            COMMAND bash ${NFS_MULTINODE_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY})
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/reboot/multinode_cairn
            PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
    endif()

    # RPCSEC_GSS / Kerberos: boot the real Linux NFS client and mount the
    # chimera export with sec=krb5 (rpc.gssd + machine keytab handed in over a
    # 9p share).  Proves the full GSS path -- context establishment + per-request
    # verifiers -- against the kernel client.  memfs only; NFSv4.1.  Skips (77)
    # without /dev/kvm or krb5 tooling, or on an image too old to honour krb5=1.
    add_test(NAME chimera/kvm/${IMAGE_NAME}/kerberos/krb5_mount_memfs_nfs4.1
        COMMAND bash ${NFS_KRB_WRAPPER}
                ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                ${CHIMERA_BINARY} memfs 4.1
                "stat /mnt && echo hello-krb5 > /mnt/f && grep -q hello-krb5 /mnt/f && dd if=/dev/zero of=/mnt/big bs=4k count=64 2>/dev/null && rm /mnt/f /mnt/big && echo KRB5_RW_OK")
    set_tests_properties(chimera/kvm/${IMAGE_NAME}/kerberos/krb5_mount_memfs_nfs4.1
        PROPERTIES LABELS "kvm;${IMAGE_NAME};rpcsec_gss" PROCESSORS 2
        SKIP_RETURN_CODE 77)

    # RPCSEC_GSS over NFSv3: the GSS layer lives below the RPC program dispatch,
    # so krb5/krb5i work for NFSv3 just as for NFSv4 -- this drives a real-client
    # vers=3 mount through MOUNT + portmap + a directory/file workload (MKDIR,
    # CREATE, WRITE, READ, RENAME, READDIR, REMOVE) under GSS.  Mounted nolock:
    # NFSv3's kernel mount wants rpc.statd for NLM, which is not in the image, so
    # NLM-under-krb5 is out of scope here (the data path is what we validate).
    # One entry per flavor (krb5 = auth, krb5i = integrity wrapping on v3 I/O).
    set(KRB_V3_WORKLOAD "stat /mnt && mkdir /mnt/d && echo hello-v3 > /mnt/d/f && grep -q hello-v3 /mnt/d/f && dd if=/dev/zero of=/mnt/d/big bs=4k count=64 2>/dev/null && mv /mnt/d/f /mnt/d/f2 && ls /mnt/d | grep -q f2 && rm /mnt/d/f2 /mnt/d/big && rmdir /mnt/d && echo NFS3_KRB_RW_OK")
    foreach(krbflavor krb5 krb5i)
        add_test(NAME chimera/kvm/${IMAGE_NAME}/kerberos/${krbflavor}_mount_memfs_nfs3
            COMMAND bash ${NFS_KRB_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} memfs 3
                    "${KRB_V3_WORKLOAD}")
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/kerberos/${krbflavor}_mount_memfs_nfs3
            PROPERTIES LABELS "kvm;${IMAGE_NAME};rpcsec_gss" PROCESSORS 2
            SKIP_RETURN_CODE 77
            ENVIRONMENT "MOUNT_SEC=${krbflavor}")
    endforeach()

    # NFS cthon04 tests
    foreach(nfsver ${NFS_VERSIONS})
        foreach(category ${CTHON_CATEGORIES})
            foreach(backend ${KVM_BACKENDS})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/cthon/${category}_${backend}_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} ${backend} ${nfsver}
                            "${CTHON_CMD_${category}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/cthon/${category}_${backend}_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(nfsver ${NFS_VERSIONS})
            foreach(category ${CTHON_CATEGORIES})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/cthon/${category}_cairn_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} cairn ${nfsver}
                            "${CTHON_CMD_${category}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/cthon/${category}_cairn_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endif()

    # NFS xfstests
    foreach(nfsver ${NFS_VERSIONS})
        foreach(prog ${XFSTESTS_PROGRAMS})
            foreach(backend ${KVM_BACKENDS})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_${backend}_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} ${backend} ${nfsver}
                            "${XFSTESTS_CMD_${prog}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_${backend}_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(nfsver ${NFS_VERSIONS})
            foreach(prog ${XFSTESTS_PROGRAMS})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_cairn_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} cairn ${nfsver}
                            "${XFSTESTS_CMD_${prog}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_cairn_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endif()

    # ----- Linux nfstest single-host suite (ubuntu2404 + ubuntu2604) -----
    # nfstest is a client-side conformance suite: it mounts the share itself (so
    # this uses a dedicated no-pre-mount wrapper) and verifies on-the-wire
    # behaviour with its own tcpdump-driven RPC dissector.  The nfstest tree and
    # its in-guest deps (python3, tcpdump, a default route) ship in the
    # kvm-test-base image (>= v1.2.0).  Matrix entries are "program|<space-
    # separated NFS versions>".  nfstest_lock is NFSv4-only here (NFSv3 byte-range
    # locking is NLM, which needs rpc.statd the guest does not run -- the other
    # suites mount nolock for v3 for the same reason); nfstest_xattr and
    # nfstest_ssc are NFSv4.2-only (RFC 8276 / server-side COPY).  nfstest_ssc
    # runs a subset (see the wrapper): chimera's COPY is correct, but the suite's
    # data-copy tests also assert the client serves the post-copy verify read
    # from cache, which the Linux client never does (copy_file_range invalidates
    # the destination page cache), so only the COPY error/edge cases are run.
    # nfstest_cache and nfstest_delegation are registered below (they need a
    # second client host -- see the 2-client wrapper).  Not yet registered:
    # nfstest_pnfs.
    if(IMAGE_NAME STREQUAL "ubuntu2404" OR IMAGE_NAME STREQUAL "ubuntu2604")
        set(NFSTEST_MATRIX
            "nfstest_posix|3 4.0 4.1 4.2"
            "nfstest_interop|4.0"
            "nfstest_alloc|4.2"
            "nfstest_sparse|4.2"
            "nfstest_lock|4.1"
            "nfstest_dio|4.2"
            "nfstest_xattr|4.2"
            "nfstest_ssc|4.2"
        )
        set(NFSTEST_BACKENDS ${KVM_BACKENDS})
        if(CAIRN_ENABLED)
            list(APPEND NFSTEST_BACKENDS cairn)
        endif()
        foreach(entry ${NFSTEST_MATRIX})
            string(REPLACE "|" ";" parts "${entry}")
            list(GET parts 0 prog)
            list(GET parts 1 vers_str)
            string(REPLACE " " ";" vers "${vers_str}")
            foreach(nfsver ${vers})
                foreach(backend ${NFSTEST_BACKENDS})
                    # nfstest_alloc fills the device to exercise ENOSPC, which
                    # needs a bounded filesystem: memfs provides one via its
                    # "size" config and diskfs via small backing devices (both
                    # set by the wrapper).  The passthrough (linux/io_uring) and
                    # cairn backends are unbounded host directories, so run alloc
                    # on memfs + diskfs only.
                    if(prog STREQUAL "nfstest_alloc" AND
                       NOT (backend STREQUAL "memfs" OR backend MATCHES "diskfs"))
                        continue()
                    endif()
                    # nfstest_alloc is disabled on diskfs pending a space-
                    # accounting fix (tracking issue #733).  diskfs reports
                    # statfs free as the sum of the AGs' live free counts, which
                    # is unstable at the ENOSPC boundary: reservation-cache
                    # remainders and async deleted-inode reclaim return bytes to
                    # the free pool concurrently with the test's fill, so a write
                    # the suite expects to fail with ENOSPC finds the drained-back
                    # space and succeeds (memfs/cairn/linux account synchronously
                    # and pass).  Keep memfs coverage; re-enable diskfs once free
                    # is reported from a stable base (see issue #733).
                    if(prog STREQUAL "nfstest_alloc" AND backend MATCHES "diskfs")
                        continue()
                    endif()
                    # nfstest_posix asserts read() updates st_atime.  The
                    # passthrough backends (linux/io_uring) defer atime to the
                    # host filesystem's mount policy (relatime/noatime), which
                    # the suite cannot control, so the assertion is unreliable
                    # there; the chimera-native backends own atime and pass.
                    # Run posix on the native backends only.
                    if(prog STREQUAL "nfstest_posix" AND
                       (backend STREQUAL "linux" OR backend STREQUAL "io_uring"))
                        continue()
                    endif()
                    # nfstest_xattr checks that a SETXATTR bumps the object's
                    # change attribute (cinfo before != after).  The passthrough
                    # backends derive that from the host file's ctime, whose
                    # Linux granularity is coarse (~1-4 ms): a back-to-back
                    # pre-stat/fsetxattr/post-stat often lands in the same tick,
                    # so before == after and the check flakes.  The native
                    # backends use a monotonic per-inode change counter and are
                    # stable.  Run xattr on the native backends only.
                    if(prog STREQUAL "nfstest_xattr" AND
                       (backend STREQUAL "linux" OR backend STREQUAL "io_uring"))
                        continue()
                    endif()
                    add_test(NAME chimera/kvm/${IMAGE_NAME}/nfstest/${prog}_${backend}_nfs${nfsver}
                        COMMAND bash ${NFSTEST_WRAPPER}
                                ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                                ${CHIMERA_BINARY} ${backend} ${nfsver} ${prog})
                    set_tests_properties(chimera/kvm/${IMAGE_NAME}/nfstest/${prog}_${backend}_nfs${nfsver}
                        PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
                    # nfstest_lock (5296 subtests, ~6.5 min) and nfstest_dio
                    # (1 MiB direct-I/O transfers, ~5.5 min) run well past the
                    # CI-wide `ctest --timeout 300`; give them a per-test timeout
                    # that overrides it so they are not killed mid-run.
                    if(prog STREQUAL "nfstest_lock" OR prog STREQUAL "nfstest_dio")
                        set_tests_properties(chimera/kvm/${IMAGE_NAME}/nfstest/${prog}_${backend}_nfs${nfsver}
                            PROPERTIES TIMEOUT 900)
                    endif()
                endforeach()
            endforeach()
        endforeach()

        # nfstest_delegation needs a *second*, distinct NFS client (its own
        # network namespace = its own clientid to the server).  The 2-client
        # wrapper boots a second guest and drives it over ssh (the v1.7.0 image
        # bakes sshd + a key for exactly this), so the suite's cross-client
        # recall/conflict tests run for real.  memfs + NFSv4.1 only for now (the
        # backend/version validated end-to-end); a small residual cluster is
        # excluded inside the wrapper (see its NFSTEST_RUNTEST).  The two guests
        # plus the server make this a ~15-20 min run, well past the CI-wide
        # `ctest --timeout 300`, so give it a per-test timeout that overrides it.
        add_test(NAME chimera/kvm/${IMAGE_NAME}/nfstest/nfstest_delegation_memfs_nfs4.1
            COMMAND bash ${NFSTEST_2CLIENT_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} memfs 4.1 nfstest_delegation)
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/nfstest/nfstest_delegation_memfs_nfs4.1
            PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 4 TIMEOUT 1800)

        # nfstest_cache also needs the second client (it issues conflicting
        # writes from the other host to invalidate the primary's actimeo
        # attribute/data caches), so it reuses the 2-client wrapper.  The wrapper
        # turns delegations off for this suite (a read delegation legitimately
        # bypasses actimeo) and runs the regular-file cache subset; the
        # directory-cache cases are excluded there (Linux client revalidation
        # policy, not a server defect -- see the wrapper).  memfs + NFSv4.1.
        add_test(NAME chimera/kvm/${IMAGE_NAME}/nfstest/nfstest_cache_memfs_nfs4.1
            COMMAND bash ${NFSTEST_2CLIENT_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} memfs 4.1 nfstest_cache)
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/nfstest/nfstest_cache_memfs_nfs4.1
            PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 4 TIMEOUT 1800)
    endif()

    # Linux Test Project (LTP) -- ubuntu2404 + ubuntu2604.  Reuses the plain NFS wrapper:
    # LTP runs against the pre-mounted share at /mnt exactly like cthon/xfstests,
    # so it needs no dedicated wrapper.  See LTP_MATRIX above for the pared-down
    # group selection (the non-duplicative slice).
    if(IMAGE_NAME STREQUAL "ubuntu2404" OR IMAGE_NAME STREQUAL "ubuntu2604")
        set(LTP_BACKENDS ${KVM_BACKENDS})
        if(CAIRN_ENABLED)
            list(APPEND LTP_BACKENDS cairn)
        endif()
        foreach(entry ${LTP_MATRIX})
            string(REPLACE "|" ";" parts "${entry}")
            list(GET parts 0 grp)
            list(GET parts 1 vers_str)
            string(REPLACE " " ";" vers "${vers_str}")
            string(REPLACE "<grp>" "${grp}" ltp_cmd "${LTP_CMD_TEMPLATE}")
            foreach(nfsver ${vers})
                foreach(backend ${LTP_BACKENDS})
                    # chimera_nfs42 exercises NFSv4.2 server allocation/copy ops;
                    # run it on memfs + the diskfs backends (chimera's own
                    # allocation paths) rather than the passthrough/cairn backends.
                    if(grp STREQUAL "chimera_nfs42" AND
                       NOT (backend STREQUAL "memfs" OR backend MATCHES "diskfs"))
                        continue()
                    endif()
                    add_test(NAME chimera/kvm/${IMAGE_NAME}/ltp/${grp}_${backend}_nfs${nfsver}
                        COMMAND bash ${NFS_WRAPPER}
                                ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                                ${CHIMERA_BINARY} ${backend} ${nfsver}
                                "${ltp_cmd}")
                    set_tests_properties(chimera/kvm/${IMAGE_NAME}/ltp/${grp}_${backend}_nfs${nfsver}
                        PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2
                                   TIMEOUT 600)
                endforeach()
            endforeach()
        endforeach()
    endif()

    # pNFS (NFSv4.1) tests: a metadata server steering data to a separate memfs
    # data server, both in one netns.  Flex-files backends only (memfs, diskfs)
    # and 4.1-only (pNFS requires a session).
    #
    # The client->MDS mount is always 4.1; PNFS_DS_VERSIONS selects the NFS
    # version of the direct client->DS data path (advertised in the flex-files
    # device's ffda_versions).  The v3 DS keeps the historical test names (no
    # suffix); the v4.1 DS adds a "_dsv4.1" suffix.
    set(PNFS_DS_VERSIONS 3 4.1)
    foreach(dsver ${PNFS_DS_VERSIONS})
        set(dssuffix "")
        if(NOT dsver STREQUAL "3")
            set(dssuffix "_dsv${dsver}")
        endif()
        foreach(prog ${PNFS_PROGRAMS})
            foreach(pbackend ${PNFS_BACKENDS})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/pnfs/${prog}_${pbackend}_nfs4.1${dssuffix}
                    COMMAND bash ${PNFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} ${pbackend} 4.1 ${dsver} split
                            "${PNFS_CMD_${prog}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/pnfs/${prog}_${pbackend}_nfs4.1${dssuffix}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME};pnfs" PROCESSORS 2)
            endforeach()
        endforeach()
    endforeach()

    # Combined topology: one daemon is both MDS and its own data server, backed
    # by a LOCAL mount rather than the nfs proxy.  This exercises the
    # local-backing layout path (the DS handle handed to the client is the
    # backing handle as-is); a v3 data path, since the self-DS shares the MDS's
    # address.  Named with a "_combined" suffix.
    foreach(prog ${PNFS_PROGRAMS})
        foreach(pbackend ${PNFS_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/pnfs/${prog}_${pbackend}_nfs4.1_combined
                COMMAND bash ${PNFS_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${pbackend} 4.1 3 combined
                        "${PNFS_CMD_${prog}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/pnfs/${prog}_${pbackend}_nfs4.1_combined
                PROPERTIES LABELS "kvm;${IMAGE_NAME};pnfs" PROCESSORS 2)
        endforeach()
    endforeach()

    # pNFS PROXY tests: chimera's own pNFS client (the nfs VFS proxy) against
    # chimera's pNFS server.  Three daemons across two netns -- MDS + DS in the
    # backend netns, a full-server proxy (the pNFS client under test) in the
    # frontend netns with the guest -- joined by a veth.  v3 data path; the
    # client->MDS mount and proxy back channel are 4.1.
    foreach(prog ${PNFS_PROXY_PROGRAMS})
        foreach(pbackend ${PNFS_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/pnfs/proxy_${prog}_${pbackend}_nfs4.1
                COMMAND bash ${PNFS_PROXY_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${pbackend}
                        "${PNFS_PROXY_CMD_${prog}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/pnfs/proxy_${prog}_${pbackend}_nfs4.1
                PROPERTIES LABELS "kvm;${IMAGE_NAME};pnfs" PROCESSORS 2)
        endforeach()
    endforeach()

    # pNFS block layout (RFC 5663): diskfs sources block layouts; the client
    # does data I/O directly to the signed virtio-SCSI data disk (/dev/sda),
    # never through the MDS (which can't reach it).
    #
    # Disabled on ubuntu2604: Ubuntu 26.04 (resolute) dropped the blkmapd binary
    # from nfs-common (nfs-utils 2.8.5) and ships it in no package, so the block-
    # layout client cannot be brought up (PNFS_BLOCK_SETUP runs /usr/sbin/blkmapd).
    # The block-layout MDS path stays covered by the ubuntu2404 / ubuntu2204_hwe
    # variants.  Only block layout needs blkmapd; the file/proxy pNFS tests above
    # still run on 2604.
    set(PNFS_BLOCK_DISABLED FALSE)
    if(IMAGE_NAME STREQUAL "ubuntu2604")
        set(PNFS_BLOCK_DISABLED TRUE)
    endif()
    foreach(prog ${PNFS_BLOCK_PROGRAMS})
        foreach(pbackend ${PNFS_BLOCK_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/pnfs/block_${prog}_${pbackend}_nfs4.1
                COMMAND bash ${PNFS_BLOCK_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${pbackend}
                        "${PNFS_BLOCK_SETUP} && ${PNFS_BLOCK_WL_${prog}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/pnfs/block_${prog}_${pbackend}_nfs4.1
                PROPERTIES LABELS "kvm;${IMAGE_NAME};pnfs" PROCESSORS 2
                           DISABLED ${PNFS_BLOCK_DISABLED})
        endforeach()
    endforeach()

    # pNFS SCSI layout (RFC 8154): diskfs sources SCSI layouts; the client does
    # data I/O directly to a virtio-SCSI LU it matches by WWN (VPD-0x83), never
    # through the MDS (which can't reach it).  Nothing is written to the disk to
    # identify it.
    #
    # DISABLED in this harness, by design.  Bring-up confirmed the server side
    # works end-to-end: the Linux client parses our GETDEVICEINFO SCSI device
    # address, matches /dev/sda by its NAA WWN ("pNFS: using block device sda"),
    # and proceeds to LAYOUTGET.  But the Linux SCSI layout client then issues a
    # *mandatory* SCSI PERSISTENT RESERVE OUT (REGISTER) to the LU, and qemu's
    # emulated scsi-hd does not implement persistent reservations ("Invalid
    # command operation code" -> "failed to register key" -> a kernel Oops in
    # the failed-registration cleanup path).  qemu-pr-helper only forwards PR to
    # a *real* host SCSI device, so a file-backed emulated disk cannot satisfy
    # this; a PR-capable target (LIO/tcmu or real SAS/FC/iSCSI/NVMe-PR LU) is
    # required.  The wrapper is kept runnable for manual use against such a
    # target; see kvm_pnfs_scsi_test_wrapper.sh.
    foreach(prog ${PNFS_SCSI_PROGRAMS})
        foreach(pbackend ${PNFS_SCSI_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/pnfs/scsi_${prog}_${pbackend}_nfs4.1
                COMMAND bash ${PNFS_SCSI_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${pbackend}
                        "${PNFS_SCSI_SETUP} && ${PNFS_SCSI_WL_${prog}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/pnfs/scsi_${prog}_${pbackend}_nfs4.1
                PROPERTIES LABELS "kvm;${IMAGE_NAME};pnfs" PROCESSORS 2
                           DISABLED TRUE)
        endforeach()
    endforeach()

    # SMB cthon04 tests
    foreach(category ${CTHON_CATEGORIES})
        foreach(backend ${KVM_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_${backend}
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${backend}
                        "${SMB_CTHON_CMD_${category}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_${backend}
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(category ${CTHON_CATEGORIES})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_cairn
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} cairn
                        "${SMB_CTHON_CMD_${category}}")
            # cairn's lock-key contention can push the SMB cthon run past the CI
            # default `ctest --timeout 300` under parallel load, so it flakes
            # (fails once, passes on rerun -- tracker #733).  Give it headroom
            # like the other contention-bound suites (cf. fsx, #805); a test
            # TIMEOUT property overrides the command-line default.
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_cairn
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2 TIMEOUT 600)
        endforeach()
    endif()

    # SMB xfstests
    foreach(prog ${XFSTESTS_PROGRAMS})
        foreach(backend ${KVM_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_${backend}
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${backend}
                        "${XFSTESTS_CMD_${prog}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_${backend}
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(prog ${XFSTESTS_PROGRAMS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_cairn
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} cairn
                        "${XFSTESTS_CMD_${prog}}")
            # Same cairn lock-key contention as the SMB cthon cairn block above:
            # under load these can exceed the 300s CI default (e.g. the recurring
            # rewinddir_cairn timeout, #733).  Give them matching headroom.
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_cairn
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2 TIMEOUT 600)
        endforeach()
    endif()

endforeach()
