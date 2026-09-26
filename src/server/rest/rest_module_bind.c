// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#undef CHIMERA_REST_BUNDLED_MODULE
#include "rest_services.h"

const struct chimera_rest_services *chimera_rest_services;

CHIMERA_REST_EXPORT int
chimera_rest_module_bind_v1(const struct chimera_rest_services *services)
{
    if (services->version != CHIMERA_REST_SERVICES_VERSION ||
        services->struct_size != sizeof(*services)) {
        return -1;
    }
    chimera_rest_services = services;
    return 0;
} /* chimera_rest_module_bind_v1 */
