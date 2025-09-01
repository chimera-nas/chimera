# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

add_subdirectory(libevpl)

set(XDRZCC ${XDRZCC} PARENT_SCOPE)

remove_definitions(-fvisibility=hidden)

remove_definitions(-Wall)

add_subdirectory(libsmb2)