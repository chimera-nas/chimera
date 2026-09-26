# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: MIT

# Also buildable standalone against an installed SDK:
# cmake -S examples/rest_module -B example-build -DCHIMERA_REST_SDK_DIR=/prefix/include/chimera/rest/sdk
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    cmake_minimum_required(VERSION 3.16)
    project(chimera_rest_example C)
endif()
find_package(Threads REQUIRED)
if(NOT CHIMERA_REST_SDK_DIR)
    set(CHIMERA_REST_SDK_DIR ${CMAKE_SOURCE_DIR}/src/server/rest/sdk)
endif()
add_library(rest_example MODULE rest_example.c)
# Override inherited include paths: this target must use only installed SDK-compatible headers.
set_property(TARGET rest_example PROPERTY INCLUDE_DIRECTORIES ${CHIMERA_REST_SDK_DIR})
set_target_properties(rest_example PROPERTIES PREFIX "" C_VISIBILITY_PRESET hidden)
target_link_libraries(rest_example PRIVATE Threads::Threads)
