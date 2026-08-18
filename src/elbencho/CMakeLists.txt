# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(/elbencho/source/backend)

add_library(elbencho_chimera SHARED elbencho_chimera.c)

target_link_libraries(elbencho_chimera chimera_client chimera_metrics)

install(TARGETS elbencho_chimera DESTINATION lib)

set(ELBENCHO_PLUGIN_PATH ${CMAKE_CURRENT_BINARY_DIR}/libelbencho_chimera.so)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
