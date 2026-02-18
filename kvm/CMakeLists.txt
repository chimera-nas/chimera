# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# VM images are build-type independent, so place them in a shared directory
# above the per-configuration build dirs (e.g. build/kvm/ alongside build/Release/).
get_filename_component(BUILD_ROOT ${CMAKE_BINARY_DIR} DIRECTORY)

set(KVM_IMAGES
    "ubuntu2404|24.04|linux-image-generic"
    "ubuntu2404_hwe|24.04|linux-image-generic-hwe-24.04"
    "ubuntu2204|22.04|linux-image-generic"
    "ubuntu2204_hwe|22.04|linux-image-generic-hwe-22.04"
)

foreach(image_spec ${KVM_IMAGES})
    string(REPLACE "|" ";" image_parts "${image_spec}")
    list(GET image_parts 0 IMAGE_NAME)
    list(GET image_parts 1 UBUNTU_VERSION)
    list(GET image_parts 2 KERNEL_PACKAGE)

    set(IMAGE_DIR ${BUILD_ROOT}/kvm/${IMAGE_NAME})

    add_custom_command(
        OUTPUT ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/initrd ${IMAGE_DIR}/rootfs.qcow2
        COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/build_vm_image.sh
                ${IMAGE_DIR}
                ${CMAKE_CURRENT_SOURCE_DIR}/Dockerfile.kvm
                ${CMAKE_SOURCE_DIR}
                UBUNTU_VERSION=${UBUNTU_VERSION}
                KERNEL_PACKAGE=${KERNEL_PACKAGE}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/Dockerfile.kvm
                ${CMAKE_CURRENT_SOURCE_DIR}/init.sh
        COMMENT "Building KVM test image (${IMAGE_NAME})"
    )

    add_custom_target(kvm_${IMAGE_NAME}_image ALL
        DEPENDS ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/initrd ${IMAGE_DIR}/rootfs.qcow2
    )
endforeach()

add_subdirectory(tests)
