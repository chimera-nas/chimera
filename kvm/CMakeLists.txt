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
# kvm-test-base release.  v1.8.0 pins nfstest to the chimera fork tag
# v3.2-chimera2, carrying the Python 3.14 compatibility fixes so the nfstest
# suite runs on Ubuntu 26.04 (Python 3.14); v1.7.0 bakes sshd + a symmetric key
# so nfstest's cross-client suites can drive a second NFS client over ssh (and
# parameterizes the guest IP from the kernel cmdline); v1.6.0 pinned cthon04 to
# a gcc-15 fix; v1.5.0 pares LTP to the non-duplicative slice and adds the
# curated chimera_nfs42 runtest + an ltp-run.sh JSON-fallback; v1.4.0 added the
# ubuntu2604 variant; v1.3.0 added the Linux Test Project (LTP) tree under
# /opt/ltp{,-skip}; v1.2.0 added the Linux nfstest suite; v1.1.0 dropped the 22.04
# generic variant (see the KVM_IMAGES note below); v1.0.0 still has all four.
set(KVM_IMAGE_VERSION "v1.8.0" CACHE STRING
    "Version tag of the KVM guest images to consume.")

# Note: the 22.04 generic (non-HWE) kernel does not build the virtio block
# driver in -- it can only mount the root disk from its initrd.  Every other
# kernel here has virtio built in, so the wrappers boot with no initrd (saving
# ~0.9s/test, see kvm_*_test_wrapper.sh).  Rather than special-case an initrd
# for one image, 22.04 is covered by its HWE kernel only.
set(KVM_IMAGES
    ubuntu2404
    ubuntu2404_hwe
    ubuntu2204_hwe
    ubuntu2604
)

# When ON (default) the guest images are fetched as part of the default build
# target (ALL), so a plain `ninja` yields a ready-to-test tree.  CI's split
# build job sets this OFF so the build does not pull ~5GB of images it will not
# ship in its artifact; the test jobs then fetch on demand with
# `ninja kvm_<image>_image`.
option(KVM_FETCH_IMAGES "Fetch KVM guest images as part of the default build" ON)

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

    if(KVM_FETCH_IMAGES)
        add_custom_target(kvm_${IMAGE_NAME}_image ALL
            DEPENDS ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/initrd ${IMAGE_DIR}/rootfs.qcow2
        )
    else()
        add_custom_target(kvm_${IMAGE_NAME}_image
            DEPENDS ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/initrd ${IMAGE_DIR}/rootfs.qcow2
        )
    endif()
endforeach()

add_subdirectory(tests)
