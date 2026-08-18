# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Header and library locations come from the toolkit probe in the top-level
# CMakeLists (CUDA_TOOLKIT_ROOT), so this tree follows whichever install the
# probe accepted rather than assuming /usr/local/cuda.
include_directories(${CUDA_CUFILE_INCLUDE_DIR})
get_filename_component(CUDA_LIBRARY_DIR ${CUDA_CUFILE_LIBRARY} DIRECTORY)
link_directories(${CUDA_LIBRARY_DIR})

add_library(chimera_cufile SHARED chimera_cufile.c)

target_link_libraries(chimera_cufile chimera_client chimera_common)

install(TARGETS chimera_cufile DESTINATION lib)

add_subdirectory(tests)
