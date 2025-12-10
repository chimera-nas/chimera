# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

remove_definitions(-Wno-unused-function -fvisibility=hidden ${MARCH_FLAG} -Werror)

add_executable(cufile_test basic.cu)

target_link_libraries(cufile_test chimera_client chimera_cufile chimera_common cuda cudart cufile)

install(TARGETS cufile_test DESTINATION bin)