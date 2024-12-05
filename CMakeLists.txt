# SPDX-FileCopyrightText: 2024 Ben Jarvis
#
# SPDX-License-Identifier: LGPL

cmake_minimum_required(VERSION 3.22)
project(chimera LANGUAGES C)

set(CHIMERA_VERSION "0.1.0")

add_definitions(-DCHIMERA_VERSION="${CHIMERA_VERSION}")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (NOT DISABLE_TESTS)
    enable_testing()
endif()

include_directories(3rdparty)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Enabling AddressSanitizer")
    add_definitions(-fsanitize=address -fno-omit-frame-pointer
                    -fno-optimize-sibling-calls -fstack-protector-all)
    add_link_options(-fsanitize=address)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_definitions(-O3)
endif()

add_definitions(-g -Wall -Werror -Wassign-enum -Wno-unused-function)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm.*|ARM.*|aarch64.*)")
    add_definitions(-march=armv8-a)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)")
    add_definitions(-march=x86-64-v3)
endif()



add_subdirectory(ext)

include_directories(ext/libevpl/src)
add_subdirectory(src)
