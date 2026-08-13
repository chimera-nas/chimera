# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Out-of-tree VFS module proof: vfs_example is compiled with ONLY the SDK
# directory (plus the SDK's declared external dependencies) on its include
# path.  If a change makes the SDK reach into a chimera-internal header,
# this target stops building -- that is the point of it.

add_library(vfs_example MODULE vfs_example.c)
set_target_properties(vfs_example PROPERTIES PREFIX "")

target_include_directories(vfs_example PRIVATE
    ${CMAKE_SOURCE_DIR}/src/vfs/sdk
    ${CMAKE_SOURCE_DIR}/ext/libevpl/include
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c/ext/stopwatch
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/oteltracing-c)

# The module references SDK helpers exported by chimera_vfs; like any
# out-of-tree module it resolves them from the hosting process at dlopen
# time rather than linking the library directly.
if (APPLE)
    target_link_options(vfs_example PRIVATE -undefined dynamic_lookup)
endif ()

# Smoke test: dlopen the module the way chimera_vfs_init loads a
# module_path share and verify the sdk_version handshake.
add_executable(vfs_example_load_test vfs_example_load_test.c)
target_include_directories(vfs_example_load_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/ext/libevpl/include
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c/ext/stopwatch
    ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/oteltracing-c)
target_link_libraries(vfs_example_load_test chimera_vfs ${CHIMERA_DL_LIB})

add_test(NAME chimera/vfs/example_module_load
         COMMAND vfs_example_load_test $<TARGET_FILE:vfs_example>)
