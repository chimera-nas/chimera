# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# tools/ holds standalone developer scripts that are not built or installed.
# Only their unit tests participate in the build, so that `ctest` covers them.
if (NOT DISABLE_TESTS)
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_FOUND)
        add_test(
            NAME chimera/tools/smbtorture/report
            COMMAND ${Python3_EXECUTABLE} -m unittest discover
                    -s ${CMAKE_CURRENT_SOURCE_DIR}/smbtorture/tests -v
        )
        set_tests_properties(chimera/tools/smbtorture/report PROPERTIES
            LABELS "tools"
            ENVIRONMENT "PYTHONUNBUFFERED=1"
            TIMEOUT 60
        )
    else()
        message(WARNING "Python3 not found - smbtorture report tests will be skipped")
    endif()
endif()
