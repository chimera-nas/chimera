# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(chimera daemon.c)

target_link_libraries(chimera chimera_server chimera_metrics jansson)

install(TARGETS chimera DESTINATION sbin)
install(FILES chimera.json DESTINATION etc)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/daemon.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/daemon.h
)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
