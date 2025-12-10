# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_posix SHARED posix.c)

target_link_libraries(chimera_posix chimera_client)


install(TARGETS chimera_posix DESTINATION lib)

add_subdirectory(tests)

