# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(common)
add_subdirectory(nfs_common)
add_subdirectory(metrics)
add_subdirectory(vfs)
add_subdirectory(server)
add_subdirectory(client)
add_subdirectory(posix)
add_subdirectory(daemon)

if (FIO_ENABLED)
    add_subdirectory(fio)
endif()

if (CUDA_ENABLED)
    add_subdirectory(cufile)
endif()
