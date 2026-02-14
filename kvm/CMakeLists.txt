# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# VM images are build-type independent, so place them in a shared directory
# above the per-configuration build dirs (e.g. build/kvm/ alongside build/Release/).
get_filename_component(BUILD_ROOT ${CMAKE_BINARY_DIR} DIRECTORY)

# Linux VM image (requires Docker)
if(KVM_TESTING_AVAILABLE)
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
endif()

# Windows Server 2025 VM image (requires WINDOWS_SERVER_ISO, not part of ALL)
if(KVM_WINDOWS_TESTING_AVAILABLE)
    set(KVM_WINDOWS_IMAGE_DIR ${BUILD_ROOT}/kvm/windows2025)

    add_custom_command(
        OUTPUT ${KVM_WINDOWS_IMAGE_DIR}/windows.qcow2
               ${KVM_WINDOWS_IMAGE_DIR}/OVMF_VARS.fd
               ${KVM_WINDOWS_IMAGE_DIR}/ssh_key
        COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/build_windows_image.sh
                ${KVM_WINDOWS_IMAGE_DIR}
                ${WINDOWS_SERVER_ISO}
                ${CMAKE_SOURCE_DIR}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/build_windows_image.sh
                ${CMAKE_CURRENT_SOURCE_DIR}/autounattend.xml
                ${CMAKE_CURRENT_SOURCE_DIR}/provision_windows.sh
        COMMENT "Building Windows Server 2025 KVM test image"
    )

    add_custom_target(kvm_windows2025_image
        DEPENDS ${KVM_WINDOWS_IMAGE_DIR}/windows.qcow2
                ${KVM_WINDOWS_IMAGE_DIR}/OVMF_VARS.fd
                ${KVM_WINDOWS_IMAGE_DIR}/ssh_key
    )
endif()

add_subdirectory(tests)
