# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

set(CHIMERA_EXT_SRC ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)

add_subdirectory(libevpl)

set(XDRZCC ${XDRZCC} PARENT_SCOPE)

remove_definitions(-fvisibility=hidden)

remove_definitions(-Wall)

add_subdirectory(libsmb2)