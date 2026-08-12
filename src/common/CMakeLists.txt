# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_common SHARED
    logging.c snprintf.c pthread_create_interpose.c chimera_tracing.c
)

target_link_libraries(chimera_common ${CHIMERA_UNWIND_LIB} ${CHIMERA_DL_LIB} evpl jansson ${CHIMERA_OTEL_LIBS})

add_subdirectory(tests)

install(TARGETS chimera_common DESTINATION lib)