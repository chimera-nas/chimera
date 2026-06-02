# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(/fio)
add_library(chimera_fio SHARED fio.c)

target_link_libraries(chimera_fio chimera_client chimera_metrics jansson)

install(TARGETS chimera_fio DESTINATION lib)

set(PLUGIN_PATH ${CMAKE_CURRENT_BINARY_DIR}/libchimera_fio.so)


add_subdirectory(tests)