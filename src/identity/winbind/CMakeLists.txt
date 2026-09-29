# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Only configured when libwbclient was found (see src/identity/CMakeLists.txt):
# without the library there is no module, and the identity engine refuses a
# configuration that names one.
add_library(chimera_identity_winbind ${CHIMERA_LIBRARY_TYPE}
    winbind.c
)

target_include_directories(chimera_identity_winbind PRIVATE ${WBCLIENT_INCLUDE_DIRS})
target_link_libraries(chimera_identity_winbind evpl ${WBCLIENT_LIBRARIES})

install(TARGETS chimera_identity_winbind DESTINATION lib)
