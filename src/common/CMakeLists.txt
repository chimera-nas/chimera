# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_common SHARED
    logging.c snprintf.c chimera_tracing.c
)

target_link_libraries(chimera_common unwind pthread dl evpl jansson ${CHIMERA_OTEL_LIBS})

add_subdirectory(tests)

install(TARGETS chimera_common DESTINATION lib)