# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_linux ${CHIMERA_LIBRARY_TYPE}
    linux.c
)

target_link_libraries(chimera_vfs_linux evpl jansson)

install(TARGETS chimera_vfs_linux DESTINATION lib)
if (NOT DISABLE_TESTS)
    add_executable(linux_lock_geometry_test linux_lock_geometry_test.c)
    target_compile_options(linux_lock_geometry_test PRIVATE -ffunction-sections -fdata-sections)
    target_link_options(linux_lock_geometry_test PRIVATE -Wl,--gc-sections)
    target_link_libraries(linux_lock_geometry_test chimera_vfs chimera_common evpl jansson pthread)
    add_test(NAME chimera/vfs/linux_lock_geometry COMMAND linux_lock_geometry_test)
    set_tests_properties(chimera/vfs/linux_lock_geometry PROPERTIES SKIP_RETURN_CODE 77)
endif()
