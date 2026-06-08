# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# VM images are build-type independent, so place them in a shared directory
# above the per-configuration build dirs (e.g. build/kvm/ alongside build/Release/).
get_filename_component(BUILD_ROOT ${CMAKE_BINARY_DIR} DIRECTORY)

# Prebuilt KVM guest images are published as OCI artifacts (vmlinuz + initrd +
# rootfs.qcow2) by the chimera-nas/kvm-test-base repo, and pulled here with
# ORAS instead of being built locally. We depend only on the published
# artifacts, not on that repo's source.
#
# NOTE: this first pass pulls directly from the public GHCR registry. Internal
# registry caching will be layered on in a follow-up.
set(KVM_IMAGE_REGISTRY "ghcr.io/chimera-nas/kvm-test-base" CACHE STRING
    "OCI registry base that hosts the prebuilt KVM guest images.")
# Pinned version of the prebuilt images to consume; bump to adopt a newer
# kvm-test-base release.
set(KVM_IMAGE_VERSION "v1.0.0" CACHE STRING
    "Version tag of the KVM guest images to consume.")

set(KVM_IMAGES
    ubuntu2404
    ubuntu2404_hwe
    ubuntu2204
    ubuntu2204_hwe
)

foreach(IMAGE_NAME ${KVM_IMAGES})
    set(IMAGE_DIR ${BUILD_ROOT}/kvm/${IMAGE_NAME})

    add_custom_command(
        OUTPUT ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/initrd ${IMAGE_DIR}/rootfs.qcow2
        COMMAND ${CMAKE_COMMAND} -E env
                KVM_IMAGE_REGISTRY=${KVM_IMAGE_REGISTRY}
                KVM_IMAGE_VERSION=${KVM_IMAGE_VERSION}
                bash ${CMAKE_CURRENT_SOURCE_DIR}/fetch_vm_image.sh
                ${IMAGE_DIR}
                ${IMAGE_NAME}
        COMMENT "Fetching KVM test image (${IMAGE_NAME}) @ ${KVM_IMAGE_VERSION}"
        VERBATIM
    )

    add_custom_target(kvm_${IMAGE_NAME}_image ALL
        DEPENDS ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/initrd ${IMAGE_DIR}/rootfs.qcow2
    )
endforeach()

add_subdirectory(tests)
