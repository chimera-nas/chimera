# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Inline the liburcu read-side primitives for the server TUs that touch the RCU
# caches (nfs/s3/smb/rest + server.c).  See src/vfs/CMakeLists.txt; deliberately
# not set for the GPL fio plugin.  chimera is LGPL-2.1.  (URCU_API_MAP, selecting
# the unprefixed QSBR API, is set project-wide in the top-level CMakeLists.txt.)
add_compile_definitions(_LGPL_SOURCE)

add_library(chimera_server SHARED server.c)
target_link_libraries(chimera_server chimera_nfs chimera_s3 chimera_smb chimera_rest chimera_vfs chimera_common evpl unwind ${CHIMERA_OTEL_LIBS})

install(TARGETS chimera_server DESTINATION lib)

add_subdirectory(nfs)

add_subdirectory(s3)

add_subdirectory(smb)

add_subdirectory(rest)

add_subdirectory(tests)
