# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Find Python3 and boto3 for S3 tests
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

    # Macro to add S3 test with specific signature version
    macro(add_s3_test_sigver testname backend sigver)
        add_test(
            NAME chimera/server/s3/boto3_${sigver}_${backend}_${testname}
            COMMAND ${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh
                    ${Python3_EXECUTABLE} ${S3_TEST_SCRIPT}
                    --test ${testname}
                    --backend ${backend}
                    --sigver ${sigver}
                    --chimera ${CHIMERA_DAEMON}
        )
        set_tests_properties(chimera/server/s3/boto3_${sigver}_${backend}_${testname} PROPERTIES
            ENVIRONMENT "PYTHONUNBUFFERED=1"
            TIMEOUT 300
        )
    endmacro()

    # Convenience macro for V4 tests (default)
    macro(add_s3_test testname backend)
        add_s3_test_sigver(${testname} ${backend} v4)
    endmacro()

    # ==================== V4 Signature Tests ====================

    # memfs tests (V4)
    add_s3_test(put memfs)
    add_s3_test(get memfs)
    add_s3_test(head memfs)
    add_s3_test(delete memfs)
    add_s3_test(list memfs)

    # linux tests (V4)
    add_s3_test(put linux)
    add_s3_test(get linux)
    add_s3_test(head linux)
    add_s3_test(delete linux)
    add_s3_test(list linux)

    # io_uring tests (V4)
    if(IO_URING_ENABLED)
        add_s3_test(put io_uring)
        add_s3_test(get io_uring)
        add_s3_test(head io_uring)
        add_s3_test(delete io_uring)
        add_s3_test(list io_uring)

        # demofs tests (V4) - demofs uses io_uring backend
        add_s3_test(put demofs)
        add_s3_test(get demofs)
        add_s3_test(head demofs)
        add_s3_test(delete demofs)
        add_s3_test(list demofs)
    endif()

    # cairn tests (V4)
    add_s3_test(put cairn)
    add_s3_test(get cairn)
    add_s3_test(head cairn)
    add_s3_test(delete cairn)
    add_s3_test(list cairn)

    # ==================== V2 Signature Tests ====================
    # V2 tests use memfs backend to verify V2 signing works

    add_s3_test_sigver(put memfs v2)
    add_s3_test_sigver(get memfs v2)
    add_s3_test_sigver(head memfs v2)
    add_s3_test_sigver(delete memfs v2)
    add_s3_test_sigver(list memfs v2)

endif()
