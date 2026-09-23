# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

include(crypto.cmake)

add_library(chimera_common ${CHIMERA_LIBRARY_TYPE}
    logging.c snprintf.c chimera_tracing.c
)

if(CHIMERA_NATIVE_RCU)
    target_sources(chimera_common PRIVATE rcu.c)
endif()

if(WIN32)
    target_link_libraries(chimera_common bcrypt advapi32)
endif()

if(NOT WIN32)
    target_sources(chimera_common PRIVATE pthread_create_interpose.c)
endif()

target_link_libraries(chimera_common chimera_crypto ${CHIMERA_UNWIND_LIB} ${CHIMERA_DL_LIB} evpl jansson ${CHIMERA_OTEL_LIBS})

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()

install(TARGETS chimera_common chimera_crypto DESTINATION lib)
