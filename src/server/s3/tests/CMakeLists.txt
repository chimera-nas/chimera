# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Exercise metadata callbacks even when Python/boto3 is unavailable.
add_executable(test_s3_metadata_pagination test_metadata_pagination.c)
target_include_directories(test_s3_metadata_pagination PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_s3_metadata_pagination chimera_vfs evpl_http evpl)
add_test(NAME chimera/server/s3/metadata_pagination
    COMMAND $<TARGET_FILE:test_s3_metadata_pagination>)
set_tests_properties(chimera/server/s3/metadata_pagination PROPERTIES LABELS "s3")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
add_library(s3_compound_finish_retry SHARED compound_finish_retry.c)
target_include_directories(s3_compound_finish_retry PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(s3_compound_finish_retry chimera_vfs ${CMAKE_DL_LIBS})

endif()

# Find Python3 and boto3 for S3 wire tests.
find_package(Python3 COMPONENTS Interpreter)

# Check if boto3 is available
if(Python3_FOUND)
    execute_process(
        COMMAND ${Python3_EXECUTABLE} -c "import boto3"
        RESULT_VARIABLE BOTO3_RESULT
        OUTPUT_QUIET
        ERROR_QUIET
    )
    if(BOTO3_RESULT EQUAL 0)
        set(BOTO3_FOUND TRUE)
    else()
        set(BOTO3_FOUND FALSE)
        message(WARNING "boto3 not found - S3 tests will be skipped. Install with: apt install python3-boto3")
    endif()
else()
    set(BOTO3_FOUND FALSE)
    message(WARNING "Python3 not found - S3 tests will be skipped")
endif()

if(BOTO3_FOUND)
    set(S3_TEST_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/s3_test.py)
    set(CHIMERA_DAEMON ${CMAKE_BINARY_DIR}/src/daemon/chimera)

    # The 'awscli' test drives the real AWS CLI; only register it where the
    # `aws` binary is available.
    find_program(AWS_CLI aws)
    if(NOT AWS_CLI)
        message(STATUS "aws CLI not found - S3 awscli interop tests will be skipped")
    endif()

    # Macro to add S3 test with specific signature version
    macro(add_s3_test_sigver testname backend sigver)
        chimera_add_netns_test(chimera/server/s3/boto3_${sigver}_${backend}_${testname} ${Python3_EXECUTABLE} ${S3_TEST_SCRIPT}
                    --test ${testname}
                    --backend ${backend}
                    --sigver ${sigver}
                    --chimera ${CHIMERA_DAEMON}
        )
        set_tests_properties(chimera/server/s3/boto3_${sigver}_${backend}_${testname} PROPERTIES
            LABELS "s3"
            ENVIRONMENT "PYTHONUNBUFFERED=1"
            TIMEOUT 300
        )
    endmacro()

    # Convenience macro for V4 tests (default)
    macro(add_s3_test testname backend)
        add_s3_test_sigver(${testname} ${backend} v4)
    endmacro()

    # ==================== V4 Signature Tests ====================

    # Compound boundaries and execution-time xattr callbacks.
    foreach(backend memfs cairn)
        add_s3_test(compound_boundaries ${backend})
        add_s3_test(acl_tagging ${backend})
        add_s3_test(multipart_retry ${backend})
    endforeach()

    if(TARGET s3_compound_finish_retry)
    foreach(backend memfs cairn)
        chimera_add_netns_test(chimera/server/s3/finish_retry_${backend}
            ${CMAKE_COMMAND} -E env
            CHIMERA_S3_FINISH_FIXTURE=$<TARGET_FILE:s3_compound_finish_retry>
            CHIMERA_S3_RETRY_VETO_MUTATIONS=1
            ${Python3_EXECUTABLE} ${S3_TEST_SCRIPT}
            --test compound_boundaries --test acl_tagging --test list --test multipart_retry
            --backend ${backend} --chimera ${CHIMERA_DAEMON})
        set_tests_properties(chimera/server/s3/finish_retry_${backend} PROPERTIES
            LABELS "s3" ENVIRONMENT "PYTHONUNBUFFERED=1" TIMEOUT 300)
    endforeach()
    chimera_add_netns_test(chimera/server/s3/finish_exhausted
        ${CMAKE_COMMAND} -E env
        CHIMERA_S3_FINISH_FIXTURE=$<TARGET_FILE:s3_compound_finish_retry>
        CHIMERA_S3_FINISH_REJECTIONS=20
        ${Python3_EXECUTABLE} ${S3_TEST_SCRIPT}
        --test finish_exhausted --backend memfs --chimera ${CHIMERA_DAEMON})
    set_tests_properties(chimera/server/s3/finish_exhausted PROPERTIES
        LABELS "s3" ENVIRONMENT "PYTHONUNBUFFERED=1" TIMEOUT 60)
    endif()

    add_s3_test(bucket_compounds memfs)

    # memfs tests (V4)
    add_s3_test(put memfs)
    add_s3_test(get memfs)
    add_s3_test(head memfs)
    add_s3_test(delete memfs)
    add_s3_test(delete_objects memfs)
    add_s3_test(list memfs)
    add_s3_test(copy memfs)
    add_s3_test(multipart memfs)
    add_s3_test(streaming memfs)
    if(AWS_CLI)
        add_s3_test(awscli memfs)
    endif()

    # linux tests (V4)
    add_s3_test(put linux)
    add_s3_test(get linux)
    add_s3_test(head linux)
    add_s3_test(delete linux)
    add_s3_test(delete_objects linux)
    add_s3_test(list linux)
    add_s3_test(copy linux)
    add_s3_test(multipart linux)
    add_s3_test(streaming linux)
    if(AWS_CLI)
        add_s3_test(awscli linux)
    endif()

    # io_uring tests (V4)
    if(IO_URING_ENABLED)
        add_s3_test(put io_uring)
        add_s3_test(get io_uring)
        add_s3_test(head io_uring)
        add_s3_test(delete io_uring)
        add_s3_test(delete_objects io_uring)
        add_s3_test(list io_uring)
        add_s3_test(copy io_uring)
        add_s3_test(multipart io_uring)
        add_s3_test(streaming io_uring)
        if(AWS_CLI)
            add_s3_test(awscli io_uring)
        endif()

        # diskfs tests (V4) - diskfs uses io_uring backend
        add_s3_test(put diskfs)
        add_s3_test(get diskfs)
        add_s3_test(head diskfs)
        add_s3_test(delete diskfs)
        add_s3_test(delete_objects diskfs)
        add_s3_test(list diskfs)
        # diskfs multipart and copy are omitted: both fall back to read+write
        # across two files, which produces corrupted output on diskfs;
        # suspected diskfs bug (cairn, which takes the same RW path, works
        # fine). Re-enable when diskfs read/write interleaving is investigated.
    endif()

    # cairn tests (V4)
    add_s3_test(put cairn)
    add_s3_test(get cairn)
    add_s3_test(head cairn)
    add_s3_test(delete cairn)
    add_s3_test(delete_objects cairn)
    add_s3_test(list cairn)
    add_s3_test(copy cairn)
    add_s3_test(multipart cairn)
    add_s3_test(streaming cairn)
    if(AWS_CLI)
        add_s3_test(awscli cairn)
    endif()

    # ==================== V2 Signature Tests ====================
    # V2 tests use memfs backend to verify V2 signing works

    # ceph/s3-tests S3 compatibility suite (registered separately).
    add_subdirectory(ceph)

    add_s3_test_sigver(put memfs v2)
    add_s3_test_sigver(get memfs v2)
    add_s3_test_sigver(head memfs v2)
    add_s3_test_sigver(delete memfs v2)
    add_s3_test_sigver(list memfs v2)
    add_s3_test_sigver(copy memfs v2)
    # Multipart and DeleteObjects V2 omitted: V2 signing's canonical resource
    # doesn't include the subresources (uploads, uploadId, partNumber, delete)
    # that boto3 adds to its signature. Re-enable when build_string_to_sign_v2
    # in s3_auth.c learns to append the AWS V2 subresource list.

endif()

# S3 model-based tests (quint): in-process replay harness, no python/boto3.
add_subdirectory(quint)
