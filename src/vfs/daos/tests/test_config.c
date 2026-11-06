// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include "vfs_daos.h"

#define CHECK(condition) do { \
            if (!(condition)) { \
                fprintf(stderr, "line %d: %s\n", __LINE__, #condition); \
                exit(EXIT_FAILURE); \
            } \
} while (0)

int
daos_oclass_id2name(
    daos_oclass_id_t id,
    char            *name)
{
    (void) id;
    (void) name;
    return EINVAL;
} /* daos_oclass_id2name */

static int
parse(
    const char             *extra,
    struct vfs_daos_config *config)
{
    char json[1024];

    snprintf(json, sizeof(json), "{\"pool\":\"p\",\"container\":\"c\","
             "\"mount_uuid\":\"6c0f6325-07f3-46fd-97d8-7d6dfdfeac5b\"%s}", extra);
    return vfs_daos_config_parse(json, config);
} /* parse */

int
main(void)
{
    struct vfs_daos_config c;
    const char            *bad[] = {
        ",\"registry_max_entries\":0",     ",\"open_max_handles\":-1",
        ",\"identity_max_entries\":0",     ",\"registry_max_entries\":1.5",
        ",\"open_max_handles\":\"4\"",     ",\"identity_max_entries\":null",
        ",\"fh_profile\":\"nfs3_signed\"", ",\"fh_profile\":\"vfs64\"",
        ",\"fh_profile\":null",            ",\"unknown\":4"
    };

    CHECK(parse("", &c) == 0);
    CHECK(c.registry_max_entries == 65536 && c.open_max_handles == 65536);
    vfs_daos_config_free(&c);
    CHECK(parse(",\"registry_max_entries\":1,\"open_max_handles\":2", &c) == 0);
    CHECK(c.registry_max_entries == 1 && c.open_max_handles == 2);
    vfs_daos_config_free(&c);
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        CHECK(parse(bad[i], &c) == EINVAL);
        CHECK(!c.pool && !c.container);
    }
    puts("Configuration limits and unsupported option rejection passed");
    return 0;
} /* main */
