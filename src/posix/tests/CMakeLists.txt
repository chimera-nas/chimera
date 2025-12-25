# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

macro(add_posix_testprog name)
    add_executable(posix_${name} ${name}.c)
    target_link_libraries(posix_${name} chimera_posix chimera_client jansson)
    set_target_properties(posix_${name} PROPERTIES
        TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

macro(add_posix_test testname prog backend)
    add_test(NAME chimera/posix/${testname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_${prog} -b ${backend})
    get_target_property(test_file posix_${prog} TEST_FILE)
    set_tests_properties(chimera/posix/${testname} PROPERTIES
        ENVIRONMENT "TEST_FILE=${test_file}")
endmacro()
# List of test programs
set(POSIX_TEST_PROGRAMS
    test_symlink
    test_link
    test_remove
    test_rename
    test_readlink
    test_stat
    test_fstat
    test_lseek
    test_pio
    test_io_serialize
    test_opendir
    test_rmdir
    test_openat
    test_mkdirat
    test_renameat
    test_symlinkat
    test_linkat
    test_faccessat
    test_fopen
    test_fread
    test_fseek
    test_frewind
    test_feof
    test_fileno
    test_fgetpos
    test_fgetc
    test_fgets
    test_ungetc
    test_chmod
    test_fchmod
    test_fchmodat
    test_chown
    test_fchown
    test_fchownat
    test_truncate
    test_ftruncate
    test_fsync
    test_fdatasync
    test_dup
    test_fdopen
)

# Create test programs
foreach(prog ${POSIX_TEST_PROGRAMS})
    add_posix_testprog(${prog})
endforeach()

# Helper macro to generate tests for a backend
macro(_generate_posix_tests_for_backend backend)
    foreach(prog ${POSIX_TEST_PROGRAMS})
        string(REGEX REPLACE "^test_" "" test_name ${prog})
        add_posix_test(${test_name}_${backend} ${prog} ${backend})
    endforeach()
endmacro()

# Macro to generate all tests for a backend
macro(generate_posix_backend_tests backend condition_var)
    if(condition_var)
        if(${condition_var})
            _generate_posix_tests_for_backend(${backend})
        endif()
    else()
        _generate_posix_tests_for_backend(${backend})
    endif()
endmacro()

# Generate tests for each backend
generate_posix_backend_tests(memfs "")
generate_posix_backend_tests(demofs "")
generate_posix_backend_tests(cairn CAIRN_ENABLED)
generate_posix_backend_tests(linux "")
generate_posix_backend_tests(io_uring IO_URING_ENABLED)
