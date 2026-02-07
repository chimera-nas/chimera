# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(NFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_test_wrapper.sh)

set(CTHON_CATEGORIES basic general special)
set(CTHON_FLAGS_basic "-b")
set(CTHON_FLAGS_general "-g")
set(CTHON_FLAGS_special "-s")

set(KVM_BACKENDS memfs linux io_uring demofs)

foreach(category ${CTHON_CATEGORIES})
    foreach(backend ${KVM_BACKENDS})
        add_test(NAME chimera/kvm/cthon/${category}_${backend}
            COMMAND bash ${NFS_WRAPPER}
                    ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
                    ${CHIMERA_BINARY} ${backend}
                    ${CTHON_FLAGS_${category}})
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
                    ${CTHON_FLAGS_${category}})
        set_tests_properties(chimera/kvm/cthon/${category}_cairn
            PROPERTIES TIMEOUT 300
                       LABELS kvm)
    endforeach()
endif()
