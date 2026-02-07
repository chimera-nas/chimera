# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(NFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_test_wrapper.sh)

set(KVM_BACKENDS memfs linux io_uring demofs_io_uring)

if(HAVE_LIBAIO)
    list(APPEND KVM_BACKENDS demofs_aio)
endif()

# cthon04 tests
set(CTHON_CATEGORIES basic general special)
set(CTHON_CMD_basic   "cd /opt/cthon04 && ./runtests -b -f /mnt/test")
set(CTHON_CMD_general "cd /opt/cthon04 && ./runtests -g -f /mnt/test")
set(CTHON_CMD_special "cd /opt/cthon04 && ./runtests -s -f /mnt/test")

foreach(category ${CTHON_CATEGORIES})
    foreach(backend ${KVM_BACKENDS})
        add_test(NAME chimera/kvm/cthon/${category}_${backend}
            COMMAND bash ${NFS_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} ${backend}
                    "${CTHON_CMD_${category}}")
        set_tests_properties(chimera/kvm/cthon/${category}_${backend}
            PROPERTIES TIMEOUT 300
                       LABELS kvm)
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(category ${CTHON_CATEGORIES})
        add_test(NAME chimera/kvm/cthon/${category}_cairn
            COMMAND bash ${NFS_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} cairn
                    "${CTHON_CMD_${category}}")
        set_tests_properties(chimera/kvm/cthon/${category}_cairn
            PROPERTIES TIMEOUT 300
                       LABELS kvm)
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

foreach(prog ${XFSTESTS_PROGRAMS})
    foreach(backend ${KVM_BACKENDS})
        add_test(NAME chimera/kvm/xfstests/${prog}_${backend}
            COMMAND bash ${NFS_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} ${backend}
                    "${XFSTESTS_CMD_${prog}}")
        set_tests_properties(chimera/kvm/xfstests/${prog}_${backend}
            PROPERTIES TIMEOUT 300
                       LABELS kvm)
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(prog ${XFSTESTS_PROGRAMS})
        add_test(NAME chimera/kvm/xfstests/${prog}_cairn
            COMMAND bash ${NFS_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} cairn
                    "${XFSTESTS_CMD_${prog}}")
        set_tests_properties(chimera/kvm/xfstests/${prog}_cairn
            PROPERTIES TIMEOUT 300
                       LABELS kvm)
    endforeach()
endif()
