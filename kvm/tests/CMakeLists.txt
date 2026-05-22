# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(NFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_test_wrapper.sh)
set(SMB_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_smb_test_wrapper.sh)
set(PNFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_pnfs_test_wrapper.sh)

set(KVM_BACKENDS memfs linux)

if(CHIMERA_VFS_IO_URING)
    list(APPEND KVM_BACKENDS io_uring)
endif()

if(IO_URING_ENABLED)
    list(APPEND KVM_BACKENDS demofs_io_uring)
endif()

if(HAVE_LIBAIO)
    list(APPEND KVM_BACKENDS demofs_aio)
endif()

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

# xfstests programs
set(XFSTESTS_PROGRAMS fsstress fsx dirstress nametest fstest rewinddir)

set(XFSTESTS_CMD_fsstress  "mkdir -p /mnt/test && /opt/xfstests/ltp/fsstress -d /mnt/test -n 500 -p 1")
set(XFSTESTS_CMD_fsx       "mkdir -p /mnt/test && /opt/xfstests/ltp/fsx -N 10000 -l 262144 /mnt/test/fsx_testfile")
set(XFSTESTS_CMD_dirstress "mkdir -p /mnt/test && /opt/xfstests/src/dirstress -d /mnt/test -p 1 -f 50")
set(XFSTESTS_CMD_nametest  "mkdir -p /mnt/test && cd /mnt/test && /opt/xfstests/src/nametest -l /opt/xfstests/nametest_input -i 1000")
set(XFSTESTS_CMD_fstest    "mkdir -p /mnt/test && /opt/xfstests/src/fstest -p /mnt/test -n 1 -f 2 -l 5 -s 65536")
set(XFSTESTS_CMD_rewinddir "mkdir -p /mnt/test && /opt/xfstests/src/rewinddir-test /mnt/test")

foreach(image_spec ${KVM_IMAGES})
    string(REPLACE "|" ";" image_parts "${image_spec}")
    list(GET image_parts 0 IMAGE_NAME)
    set(IMAGE_DIR ${BUILD_ROOT}/kvm/${IMAGE_NAME})

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

    # pNFS (NFSv4.1) tests: a metadata server steering data to a separate memfs
    # data server, both in one netns.  memfs-only (only memfs implements
    # layouts) and 4.1-only (pNFS requires a session).
    foreach(prog ${PNFS_PROGRAMS})
        add_test(NAME chimera/kvm/${IMAGE_NAME}/pnfs/${prog}_memfs_nfs4.1
            COMMAND bash ${PNFS_WRAPPER}
                    ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} memfs 4.1
                    "${PNFS_CMD_${prog}}")
        set_tests_properties(chimera/kvm/${IMAGE_NAME}/pnfs/${prog}_memfs_nfs4.1
            PROPERTIES LABELS "kvm;${IMAGE_NAME};pnfs" PROCESSORS 2)
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
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_cairn
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
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
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_cairn
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
        endforeach()
    endif()

endforeach()
