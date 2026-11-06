// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <jansson.h>
#include <uuid/uuid.h>
#include "vfs_daos.h"

void
vfs_daos_config_free(struct vfs_daos_config *c)
{
    free(c->pool);
    free(c->container);
    free(c->system);
    memset(c, 0, sizeof(*c));
} /* vfs_daos_config_free */

static int
config_string(
    json_t *value,
    char  **out)
{
    const char *s = json_string_value(value);

    if (!s || !*s || json_string_length(value) != strlen(s)) {
        return EINVAL;
    }
    *out = strdup(s);
    return *out ? 0 : ENOMEM;
} /* config_string */

int
vfs_daos_config_parse(
    const char             *text,
    struct vfs_daos_config *c)
{
    json_error_t error;
    json_t      *root = json_loads(text ? text : "{}", JSON_REJECT_DUPLICATES, &error);
    json_t      *value;
    const char  *key;
    bool         have_uuid = false;
    int          rc        = EINVAL;

    memset(c, 0, sizeof(*c));
    c->registry_max_entries = 65536;
    c->open_max_handles     = 65536;
    c->chunk_size           = 1048576;
    c->batch_entries        = 64;
    if (!json_is_object(root)) {
        goto out;
    }
    json_object_foreach(root, key, value)
    {
        json_int_t  n = json_integer_value(value);
        const char *s = json_string_value(value);

        rc = EINVAL;
        if (!strcmp(key, "pool") || !strcmp(key, "container") ||
            !strcmp(key, "system")) {
            char **dst = !strcmp(key, "pool") ? &c->pool :
                !strcmp(key, "container") ? &c->container : &c->system;
            if (dst == &c->system && json_is_null(value)) {
                continue;
            }
            rc = config_string(value, dst);
            if (rc) {
                goto out;
            }
        } else if (!strcmp(key, "mount_uuid")) {
            char canonical[37];
            if (!s || strlen(s) != 36 || json_string_length(value) != 36 ||
                uuid_parse(s, c->uuid)) {
                goto out;
            }
            uuid_unparse_lower(c->uuid, canonical);
            if (strcmp(s, canonical) || uuid_is_null(c->uuid)) {
                goto out;
            }
            have_uuid = true;
        } else if (!strcmp(key, "read_only")) {
            if (!json_is_boolean(value)) {
                goto out;
            }
            c->read_only = json_is_true(value);
        } else {
            if (!json_is_integer(value) || n < 0) {
                goto out;
            }
            if ((!strcmp(key, "registry_max_entries") ||
                 !strcmp(key, "open_max_handles")) && n > 0 &&
                (uint64_t) n <= SIZE_MAX) {
                size_t *limit = !strcmp(key, "registry_max_entries") ? &c->registry_max_entries :
                    &c->open_max_handles;
                *limit = (size_t) n;
            } else if (!strcmp(key, "oclass") && n <= UINT16_MAX) {
                char class_name[256];
                c->oclass = (daos_oclass_id_t) n;
                if (n && daos_oclass_id2name(c->oclass, class_name)) {
                    goto out;
                }
            } else if (!strcmp(key, "chunk_size") && n > 0) {
                c->chunk_size = (daos_size_t) n;
            } else if (!strcmp(key, "readdir_batch_entries") && n > 0 && n <= 4096) {
                c->batch_entries = (uint32_t) n;
            } else {
                goto out;
            }
        }
    }
    rc = c->pool && c->container && have_uuid ? 0 : EINVAL;
 out:
    json_decref(root);
    if (rc) {
        vfs_daos_config_free(c);
    }
    return rc;
} /* vfs_daos_config_parse */
