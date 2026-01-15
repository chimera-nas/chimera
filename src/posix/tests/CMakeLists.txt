# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# FSX - File System Exerciser ported to Chimera POSIX API
add_executable(posix_fsx fsx.c)
target_link_libraries(posix_fsx chimera_posix chimera_client chimera_server jansson)

macro(add_posix_testprog name)
    add_executable(posix_${name} ${name}.c)
    # Link with chimera_server for NFS backend tests
    target_link_libraries(posix_${name} chimera_posix chimera_client chimera_server jansson)
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
    test_lstat
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
    test_lchown
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

# ============================================================================
# NFS3 Backend Tests
# These tests run the POSIX tests through the Chimera NFS client,
# connecting to a Chimera NFS server in the same process.
# They require a network namespace for loopback networking.
# ============================================================================

# Wrapper script for running tests in a network namespace
set(NETNS_WRAPPER "${CMAKE_SOURCE_DIR}/scripts/netns_test_wrapper.sh")

# Macro to add a test that runs via NFS3 client (requires network namespace)
macro(add_posix_nfs3_test testname prog nfs_backend)
    add_test(NAME chimera/posix/${testname}_nfs3_${nfs_backend}
        COMMAND ${NETNS_WRAPPER} ${CMAKE_CURRENT_BINARY_DIR}/posix_${prog} -b nfs3_${nfs_backend}
    )
    get_target_property(test_file posix_${prog} TEST_FILE)
    set_tests_properties(chimera/posix/${testname}_nfs3_${nfs_backend} PROPERTIES
        ENVIRONMENT "TEST_FILE=${test_file}"
        TIMEOUT 60
    )
endmacro()

# Helper macro to generate NFS3 tests for a specific server-side backend
macro(_generate_posix_nfs3_tests_for_backend nfs_backend)
    foreach(prog ${POSIX_TEST_PROGRAMS})
        string(REGEX REPLACE "^test_" "" test_name ${prog})
        add_posix_nfs3_test(${test_name} ${prog} ${nfs_backend})
    endforeach()
endmacro()

# Macro to generate all NFS3 tests for a backend with optional condition
macro(generate_posix_nfs3_backend_tests backend condition_var)
    if(condition_var)
        if(${condition_var})
            _generate_posix_nfs3_tests_for_backend(${backend})
        endif()
    else()
        _generate_posix_nfs3_tests_for_backend(${backend})
    endif()
endmacro()

# Generate NFS3 tests for each backend
generate_posix_nfs3_backend_tests(memfs "")
generate_posix_nfs3_backend_tests(demofs "")
generate_posix_nfs3_backend_tests(cairn CAIRN_ENABLED)
generate_posix_nfs3_backend_tests(linux "")
generate_posix_nfs3_backend_tests(io_uring IO_URING_ENABLED)

# FSX tests for each backend
# Uses run_fsx.sh wrapper to generate config and run fsx
set(FSX_WRAPPER "${CMAKE_CURRENT_SOURCE_DIR}/run_fsx.sh")
set(FSX_NUM_OPS 10000)
set(FSX_MAX_LEN 262144)

macro(add_fsx_test backend)
    add_test(NAME chimera/posix/fsx_${backend}
        COMMAND ${FSX_WRAPPER} -b ${backend} -N ${FSX_NUM_OPS} -l ${FSX_MAX_LEN} -q
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
    set_tests_properties(chimera/posix/fsx_${backend} PROPERTIES
        ENVIRONMENT "BUILD_DIR=${CMAKE_BINARY_DIR}"
        TIMEOUT 300
    )
endmacro()

# Add FSX tests for each backend
add_fsx_test(memfs)
add_fsx_test(demofs)
if(CAIRN_ENABLED)
    add_fsx_test(cairn)
endif()
add_fsx_test(linux)
if(IO_URING_ENABLED)
    add_fsx_test(io_uring)
endif()

# NFS3 FSX tests
# These tests run FSX through the NFS3 protocol to an in-process server
add_fsx_test(nfs3_memfs)
add_fsx_test(nfs3_demofs)
if(CAIRN_ENABLED)
    add_fsx_test(nfs3_cairn)
endif()
add_fsx_test(nfs3_linux)
if(IO_URING_ENABLED)
    add_fsx_test(nfs3_io_uring)
endif()

# Cthon common library (shared helpers for all cthon tests)
add_library(cthon_common STATIC cthon_common.c)
# Link with chimera_server for NFS backend tests
target_link_libraries(cthon_common chimera_posix chimera_client chimera_server jansson)

# Macro to add a cthon test executable
macro(add_cthon_testprog name)
    add_executable(posix_${name} ${name}.c)
    # Link with chimera_server for NFS backend tests
    target_link_libraries(posix_${name} cthon_common chimera_posix chimera_client chimera_server jansson)
    set_target_properties(posix_${name} PROPERTIES
        TEST_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${name}.c")
endmacro()

# Macro to add a cthon test to ctest
macro(add_cthon_test testname prog backend)
    add_test(NAME chimera/posix/cthon/${testname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_${prog} -b ${backend})
    get_target_property(test_file posix_${prog} TEST_FILE)
    set_tests_properties(chimera/posix/cthon/${testname} PROPERTIES
        ENVIRONMENT "TEST_FILE=${test_file}"
        TIMEOUT 300)
endmacro()

# List of cthon basic test programs
set(CTHON_BASIC_TEST_PROGRAMS
    cthon_basic_1
    cthon_basic_2
    cthon_basic_3
    cthon_basic_4
    cthon_basic_4a
    cthon_basic_5
    cthon_basic_5a
    cthon_basic_5b
    cthon_basic_6
    cthon_basic_7
    cthon_basic_7a
    cthon_basic_7b
    cthon_basic_8
    cthon_basic_9
)

# List of cthon special test programs
set(CTHON_SPECIAL_TEST_PROGRAMS
    cthon_special_bigfile
    cthon_special_bigfile2
    cthon_special_holey
    cthon_special_op_unlk
    cthon_special_op_chmod
    cthon_special_op_ren
    cthon_special_dupreq
    cthon_special_negseek
    cthon_special_truncate
    cthon_special_rename
    cthon_special_rewind
    cthon_special_telldir
    cthon_special_nfsidem
    cthon_special_fstat
    cthon_special_excltest
    cthon_special_touchn
    cthon_special_nstat
    cthon_special_stat
    cthon_special_stat2
)

# List of cthon lock test programs (disabled - lock APIs not yet implemented)
# When lock support is added, move these to CTHON_LOCK_TEST_PROGRAMS and enable
# set(CTHON_LOCK_TEST_PROGRAMS
#     cthon_lock_tlock
# )

# All cthon test programs
set(CTHON_TEST_PROGRAMS
    ${CTHON_BASIC_TEST_PROGRAMS}
    ${CTHON_SPECIAL_TEST_PROGRAMS}
)

# Create cthon test executables
foreach(prog ${CTHON_TEST_PROGRAMS})
    add_cthon_testprog(${prog})
endforeach()

# Helper macro to generate cthon tests for a backend
macro(_generate_cthon_tests_for_backend backend)
    foreach(prog ${CTHON_TEST_PROGRAMS})
        add_cthon_test(${prog}_${backend} ${prog} ${backend})
    endforeach()
endmacro()

# Macro to generate all cthon tests for a backend
macro(generate_cthon_backend_tests backend condition_var)
    if(condition_var)
        if(${condition_var})
            _generate_cthon_tests_for_backend(${backend})
        endif()
    else()
        _generate_cthon_tests_for_backend(${backend})
    endif()
endmacro()

# Generate cthon tests for each backend
generate_cthon_backend_tests(memfs "")
generate_cthon_backend_tests(demofs "")
generate_cthon_backend_tests(cairn CAIRN_ENABLED)
generate_cthon_backend_tests(linux "")
generate_cthon_backend_tests(io_uring IO_URING_ENABLED)

# ============================================================================
# NFS3 Cthon Tests
# These tests run the cthon tests through the Chimera NFS client,
# connecting to a Chimera NFS server in the same process.
# They require a network namespace for loopback networking.
# ============================================================================

# Macro to add a cthon test that runs via NFS3 client (requires network namespace)
macro(add_cthon_nfs3_test testname prog nfs_backend)
    add_test(NAME chimera/posix/cthon/${testname}_nfs3_${nfs_backend}
        COMMAND ${NETNS_WRAPPER} ${CMAKE_CURRENT_BINARY_DIR}/posix_${prog} -b nfs3_${nfs_backend}
    )
    get_target_property(test_file posix_${prog} TEST_FILE)
    set_tests_properties(chimera/posix/cthon/${testname}_nfs3_${nfs_backend} PROPERTIES
        ENVIRONMENT "TEST_FILE=${test_file}"
        TIMEOUT 300
    )
endmacro()

# Helper macro to generate NFS3 cthon tests for a specific server-side backend
macro(_generate_cthon_nfs3_tests_for_backend nfs_backend)
    foreach(prog ${CTHON_TEST_PROGRAMS})
        add_cthon_nfs3_test(${prog} ${prog} ${nfs_backend})
    endforeach()
endmacro()

# Macro to generate all NFS3 cthon tests for a backend with optional condition
macro(generate_cthon_nfs3_backend_tests backend condition_var)
    if(condition_var)
        if(${condition_var})
            _generate_cthon_nfs3_tests_for_backend(${backend})
        endif()
    else()
        _generate_cthon_nfs3_tests_for_backend(${backend})
    endif()
endmacro()

# Generate NFS3 cthon tests for each backend
generate_cthon_nfs3_backend_tests(memfs "")
generate_cthon_nfs3_backend_tests(demofs "")
generate_cthon_nfs3_backend_tests(cairn CAIRN_ENABLED)
generate_cthon_nfs3_backend_tests(linux "")
generate_cthon_nfs3_backend_tests(io_uring IO_URING_ENABLED)

# ============================================================================
# Stress Tests ported from xfstests
# These are longer-running tests that exercise filesystem operations heavily
# ============================================================================

# Build stress test executables
set(POSIX_STRESS_TEST_PROGRAMS
    test_dirstress
    test_rewinddir
    test_fstest
    test_nametest
    test_fsstress
)

foreach(prog ${POSIX_STRESS_TEST_PROGRAMS})
    add_posix_testprog(${prog})
endforeach()

# Macro to add a stress test for a specific backend with custom timeout
macro(add_stress_test testname prog backend timeout)
    add_test(NAME chimera/posix/${testname}_${backend}
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_${prog} -b ${backend}
    )
    set_tests_properties(chimera/posix/${testname}_${backend} PROPERTIES
        TIMEOUT ${timeout}
    )
endmacro()

# Helper macro to generate stress tests for a backend
macro(_generate_stress_tests_for_backend backend)
    # dirstress: directory operations stress test (reduced params for testing)
    add_test(NAME chimera/posix/dirstress_${backend}
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_test_dirstress -b ${backend} -p 1 -f 50
    )
    set_tests_properties(chimera/posix/dirstress_${backend} PROPERTIES TIMEOUT 120)

    # rewinddir: tests rewinddir() POSIX semantics
    add_test(NAME chimera/posix/rewinddir_${backend}
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_test_rewinddir -b ${backend}
    )
    set_tests_properties(chimera/posix/rewinddir_${backend} PROPERTIES TIMEOUT 120)

    # fstest: data integrity verification test
    add_test(NAME chimera/posix/fstest_${backend}
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_test_fstest -b ${backend} -n 1 -f 2 -l 5 -s 65536
    )
    set_tests_properties(chimera/posix/fstest_${backend} PROPERTIES TIMEOUT 120)

    # nametest: namespace stress test
    add_test(NAME chimera/posix/nametest_${backend}
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_test_nametest -b ${backend} -n 50 -i 1000
    )
    set_tests_properties(chimera/posix/nametest_${backend} PROPERTIES TIMEOUT 120)

    # fsstress: comprehensive filesystem stress test
    add_test(NAME chimera/posix/fsstress_${backend}
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/posix_test_fsstress -b ${backend} -n 500 -p 1
    )
    set_tests_properties(chimera/posix/fsstress_${backend} PROPERTIES TIMEOUT 180)
endmacro()

# Macro to generate stress tests with optional condition
macro(generate_stress_tests backend condition_var)
    if(condition_var)
        if(${condition_var})
            _generate_stress_tests_for_backend(${backend})
        endif()
    else()
        _generate_stress_tests_for_backend(${backend})
    endif()
endmacro()

# Generate stress tests for each backend
generate_stress_tests(memfs "")
generate_stress_tests(demofs "")
generate_stress_tests(cairn CAIRN_ENABLED)
generate_stress_tests(linux "")
generate_stress_tests(io_uring IO_URING_ENABLED)

# ============================================================================
# NFS3 Stress Tests
# These tests run the stress tests through the Chimera NFS client,
# connecting to a Chimera NFS server in the same process.
# They require a network namespace for loopback networking.
# ============================================================================

# Helper macro to generate NFS3 stress tests for a backend
macro(_generate_stress_nfs3_tests_for_backend nfs_backend)
    # dirstress: directory operations stress test (reduced params for testing)
    add_test(NAME chimera/posix/dirstress_nfs3_${nfs_backend}
        COMMAND ${NETNS_WRAPPER} ${CMAKE_CURRENT_BINARY_DIR}/posix_test_dirstress -b nfs3_${nfs_backend} -p 1 -f 50
    )
    set_tests_properties(chimera/posix/dirstress_nfs3_${nfs_backend} PROPERTIES TIMEOUT 120)

    # rewinddir: tests rewinddir() POSIX semantics
    add_test(NAME chimera/posix/rewinddir_nfs3_${nfs_backend}
        COMMAND ${NETNS_WRAPPER} ${CMAKE_CURRENT_BINARY_DIR}/posix_test_rewinddir -b nfs3_${nfs_backend}
    )
    set_tests_properties(chimera/posix/rewinddir_nfs3_${nfs_backend} PROPERTIES TIMEOUT 120)

    # fstest: data integrity verification test
    add_test(NAME chimera/posix/fstest_nfs3_${nfs_backend}
        COMMAND ${NETNS_WRAPPER} ${CMAKE_CURRENT_BINARY_DIR}/posix_test_fstest -b nfs3_${nfs_backend} -n 1 -f 2 -l 5 -s 65536
    )
    set_tests_properties(chimera/posix/fstest_nfs3_${nfs_backend} PROPERTIES TIMEOUT 120)

    # nametest: namespace stress test
    add_test(NAME chimera/posix/nametest_nfs3_${nfs_backend}
        COMMAND ${NETNS_WRAPPER} ${CMAKE_CURRENT_BINARY_DIR}/posix_test_nametest -b nfs3_${nfs_backend} -n 50 -i 1000
    )
    set_tests_properties(chimera/posix/nametest_nfs3_${nfs_backend} PROPERTIES TIMEOUT 120)

    # fsstress: comprehensive filesystem stress test
    add_test(NAME chimera/posix/fsstress_nfs3_${nfs_backend}
        COMMAND ${NETNS_WRAPPER} ${CMAKE_CURRENT_BINARY_DIR}/posix_test_fsstress -b nfs3_${nfs_backend} -n 500 -p 1
    )
    set_tests_properties(chimera/posix/fsstress_nfs3_${nfs_backend} PROPERTIES TIMEOUT 180)
endmacro()

# Macro to generate NFS3 stress tests with optional condition
macro(generate_stress_nfs3_tests backend condition_var)
    if(condition_var)
        if(${condition_var})
            _generate_stress_nfs3_tests_for_backend(${backend})
        endif()
    else()
        _generate_stress_nfs3_tests_for_backend(${backend})
    endif()
endmacro()

# Generate NFS3 stress tests for each backend
generate_stress_nfs3_tests(memfs "")
generate_stress_nfs3_tests(demofs "")
generate_stress_nfs3_tests(cairn CAIRN_ENABLED)
generate_stress_nfs3_tests(linux "")
generate_stress_nfs3_tests(io_uring IO_URING_ENABLED)
