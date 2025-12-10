# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(client_basic basic.c)
target_link_libraries(client_basic chimera_client)

add_test(NAME chimera/client/basic COMMAND client_basic)
