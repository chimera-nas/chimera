# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# VM images are build-type independent, so place them in a shared directory
# above the per-configuration build dirs (e.g. build/kvm/ alongside build/Release/).
get_filename_component(BUILD_ROOT ${CMAKE_BINARY_DIR} DIRECTORY)
set(KVM_IMAGE_DIR ${BUILD_ROOT}/kvm/ubuntu2404)

add_custom_command(
    OUTPUT ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/build_vm_image.sh
            ${KVM_IMAGE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/Dockerfile.ubuntu2404
            ${CMAKE_SOURCE_DIR}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/Dockerfile.ubuntu2404
            ${CMAKE_CURRENT_SOURCE_DIR}/init.sh
    COMMENT "Building KVM test image (ubuntu2404)"
)

add_custom_target(kvm_ubuntu2404_image ALL
    DEPENDS ${KVM_IMAGE_DIR}/vmlinuz ${KVM_IMAGE_DIR}/rootfs.qcow2
)

add_subdirectory(tests)
