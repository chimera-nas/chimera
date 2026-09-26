// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "rest_services.h"
#include "vfs/vfs_release.h"

const struct chimera_rest_services chimera_rest_host_services = {
    .version                           = CHIMERA_REST_SERVICES_VERSION,
    .struct_size                       = sizeof(struct chimera_rest_services),
    .sdk                               = &chimera_rest_host,
#define REST_SERVICE_VALUE(name) .name = name,
    CHIMERA_REST_SERVICE_FUNCTIONS(REST_SERVICE_VALUE)
#undef REST_SERVICE_VALUE
    .vfs_release                       = chimera_vfs_release,
};
