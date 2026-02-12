# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(NFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_test_wrapper.sh)
set(SMB_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_smb_test_wrapper.sh)

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

foreach(nfsver ${NFS_VERSIONS})
    foreach(category ${CTHON_CATEGORIES})
        foreach(backend ${KVM_BACKENDS})
            add_test(NAME chimera/kvm/cthon/${category}_${backend}_nfs${nfsver}
                COMMAND bash ${NFS_WRAPPER}
                        ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${backend} ${nfsver}
                        "${CTHON_CMD_${category}}")
            set_tests_properties(chimera/kvm/cthon/${category}_${backend}_nfs${nfsver}
                PROPERTIES LABELS kvm)
        endforeach()
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(nfsver ${NFS_VERSIONS})
        foreach(category ${CTHON_CATEGORIES})
            add_test(NAME chimera/kvm/cthon/${category}_cairn_nfs${nfsver}
                COMMAND bash ${NFS_WRAPPER}
                        ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} cairn ${nfsver}
                        "${CTHON_CMD_${category}}")
            set_tests_properties(chimera/kvm/cthon/${category}_cairn_nfs${nfsver}
                PROPERTIES LABELS kvm)
        endforeach()
    endforeach()
endif()

# xfstests programs
set(XFSTESTS_PROGRAMS fsstress fsx dirstress nametest fstest rewinddir)

set(XFSTESTS_CMD_fsstress  "mkdir -p /mnt/test && /opt/xfstests/ltp/fsstress -d /mnt/test -n 500 -p 1")
set(XFSTESTS_CMD_fsx       "mkdir -p /mnt/test && /opt/xfstests/ltp/fsx -N 10000 -l 262144 /mnt/test/fsx_testfile")
set(XFSTESTS_CMD_dirstress "mkdir -p /mnt/test && /opt/xfstests/src/dirstress -d /mnt/test -p 1 -f 50")
set(XFSTESTS_CMD_nametest  "mkdir -p /mnt/test && cd /mnt/test && /opt/xfstests/src/nametest -l /opt/xfstests/nametest_input -i 1000")
set(XFSTESTS_CMD_fstest    "mkdir -p /mnt/test && /opt/xfstests/src/fstest -p /mnt/test -n 1 -f 2 -l 5 -s 65536")
set(XFSTESTS_CMD_rewinddir "mkdir -p /mnt/test && /opt/xfstests/src/rewinddir-test /mnt/test")

foreach(nfsver ${NFS_VERSIONS})
    foreach(prog ${XFSTESTS_PROGRAMS})
        foreach(backend ${KVM_BACKENDS})
            add_test(NAME chimera/kvm/xfstests/${prog}_${backend}_nfs${nfsver}
                COMMAND bash ${NFS_WRAPPER}
                        ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${backend} ${nfsver}
                        "${XFSTESTS_CMD_${prog}}")
            set_tests_properties(chimera/kvm/xfstests/${prog}_${backend}_nfs${nfsver}
                PROPERTIES LABELS kvm)
        endforeach()
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(nfsver ${NFS_VERSIONS})
        foreach(prog ${XFSTESTS_PROGRAMS})
            add_test(NAME chimera/kvm/xfstests/${prog}_cairn_nfs${nfsver}
                COMMAND bash ${NFS_WRAPPER}
                        ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} cairn ${nfsver}
                        "${XFSTESTS_CMD_${prog}}")
            set_tests_properties(chimera/kvm/xfstests/${prog}_cairn_nfs${nfsver}
                PROPERTIES LABELS kvm)
        endforeach()
    endforeach()
endif()

# SMB tests
foreach(category ${CTHON_CATEGORIES})
    foreach(backend ${KVM_BACKENDS})
        add_test(NAME chimera/kvm/smb/cthon/${category}_${backend}
            COMMAND bash ${SMB_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} ${backend}
                    "${CTHON_CMD_${category}}")
        set_tests_properties(chimera/kvm/smb/cthon/${category}_${backend}
            PROPERTIES LABELS kvm)
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(category ${CTHON_CATEGORIES})
        add_test(NAME chimera/kvm/smb/cthon/${category}_cairn
            COMMAND bash ${SMB_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} cairn
                    "${CTHON_CMD_${category}}")
        set_tests_properties(chimera/kvm/smb/cthon/${category}_cairn
            PROPERTIES LABELS kvm)
    endforeach()
endif()

foreach(prog ${XFSTESTS_PROGRAMS})
    foreach(backend ${KVM_BACKENDS})
        add_test(NAME chimera/kvm/smb/xfstests/${prog}_${backend}
            COMMAND bash ${SMB_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} ${backend}
                    "${XFSTESTS_CMD_${prog}}")
        set_tests_properties(chimera/kvm/smb/xfstests/${prog}_${backend}
            PROPERTIES LABELS kvm)
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(prog ${XFSTESTS_PROGRAMS})
        add_test(NAME chimera/kvm/smb/xfstests/${prog}_cairn
            COMMAND bash ${SMB_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} cairn
                    "${XFSTESTS_CMD_${prog}}")
        set_tests_properties(chimera/kvm/smb/xfstests/${prog}_cairn
            PROPERTIES LABELS kvm)
    endforeach()
endif()
