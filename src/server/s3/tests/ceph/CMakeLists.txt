# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# ceph/s3-tests S3 compatibility suite. The suite is cloned into a venv by the
# devcontainer (.devcontainer/Dockerfile, S3TESTS_DIR=/opt/s3-tests). When that
# venv is present and netns testing is enabled, register a ctest that runs the
# curated allowlist of currently-passing cases (passing_tests.txt) against a
# freshly started chimera daemon. The full suite can be run by hand with
# run_ceph_s3tests.sh for compliance reporting.

set(S3TESTS_DIR "$ENV{S3TESTS_DIR}")
if(NOT S3TESTS_DIR)
    set(S3TESTS_DIR "/opt/s3-tests")
endif()

if(CHIMERA_NETNS_TESTING AND EXISTS "${S3TESTS_DIR}/.venv")
    add_test(
        NAME chimera/server/s3/ceph_s3tests
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                ${CMAKE_CURRENT_SOURCE_DIR}/run_ceph_s3tests.sh
                --chimera ${CMAKE_BINARY_DIR}/src/daemon/chimera
                --suite-dir ${S3TESTS_DIR}
                --backend memfs
    )
    set_tests_properties(chimera/server/s3/ceph_s3tests PROPERTIES
        ENVIRONMENT "PYTHONUNBUFFERED=1;S3TESTS_DIR=${S3TESTS_DIR}"
        LABELS "ceph-s3"
        TIMEOUT 600
        SKIP_RETURN_CODE 77
    )
else()
    message(STATUS "ceph s3-tests not found at ${S3TESTS_DIR} - ceph S3 compliance test will be skipped")
endif()
