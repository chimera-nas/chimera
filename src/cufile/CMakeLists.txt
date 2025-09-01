# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda/lib64)

add_library(chimera_cufile SHARED chimera_cufile.c)

target_link_libraries(chimera_cufile chimera_client chimera_common)

install(TARGETS chimera_cufile DESTINATION lib)

add_subdirectory(tests)
