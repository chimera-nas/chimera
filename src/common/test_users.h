// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <jansson.h>

/* Note: include "server/server.h" before including this header */

#define CHIMERA_TEST_USER_ROOT_UID    0
#define CHIMERA_TEST_USER_ROOT_GID    0
#define CHIMERA_TEST_USER_JOHNDOE_UID 1000
#define CHIMERA_TEST_USER_JOHNDOE_GID 1000

#define CHIMERA_TEST_USER_PASSWORD \
        "$6$testsalt$51yaaBMXXXt5vK522YOlIMZ267vqHtMIzc2klcsu3EEv/bkRDU9g3UmkypXf.NvlpPpIPK1nX5zdbCeJBiQbB/"

#define CHIMERA_TEST_USER_SMBPASSWD   "secret"

static inline void
chimera_test_add_server_users(struct chimera_server *server)
{
    chimera_server_add_user(server, "root",
                            CHIMERA_TEST_USER_PASSWORD,
                            CHIMERA_TEST_USER_SMBPASSWD,
                            CHIMERA_TEST_USER_ROOT_UID,
                            CHIMERA_TEST_USER_ROOT_GID,
                            0, NULL, 1);

    chimera_server_add_user(server, "johndoe",
                            CHIMERA_TEST_USER_PASSWORD,
                            CHIMERA_TEST_USER_SMBPASSWD,
                            CHIMERA_TEST_USER_JOHNDOE_UID,
                            CHIMERA_TEST_USER_JOHNDOE_GID,
                            0, NULL, 1);
} /* chimera_test_add_server_users */

static inline void
chimera_test_write_users_json(json_t *config)
{
    json_t *users, *root_user, *johndoe_user;

    users = json_array();

    root_user = json_object();
    json_object_set_new(root_user, "username", json_string("root"));
    json_object_set_new(root_user, "password", json_string(CHIMERA_TEST_USER_PASSWORD));
    json_object_set_new(root_user, "smbpasswd", json_string(CHIMERA_TEST_USER_SMBPASSWD));
    json_object_set_new(root_user, "uid", json_integer(CHIMERA_TEST_USER_ROOT_UID));
    json_object_set_new(root_user, "gid", json_integer(CHIMERA_TEST_USER_ROOT_GID));
    json_array_append_new(users, root_user);

    johndoe_user = json_object();
    json_object_set_new(johndoe_user, "username", json_string("johndoe"));
    json_object_set_new(johndoe_user, "password", json_string(CHIMERA_TEST_USER_PASSWORD));
    json_object_set_new(johndoe_user, "smbpasswd", json_string(CHIMERA_TEST_USER_SMBPASSWD));
    json_object_set_new(johndoe_user, "uid", json_integer(CHIMERA_TEST_USER_JOHNDOE_UID));
    json_object_set_new(johndoe_user, "gid", json_integer(CHIMERA_TEST_USER_JOHNDOE_GID));
    json_array_append_new(users, johndoe_user);

    json_object_set_new(config, "users", users);
} /* chimera_test_write_users_json */
