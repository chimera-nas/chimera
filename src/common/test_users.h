// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdio.h>
#include <string.h>
#include <jansson.h>
#include "vfs/vfs_cred.h"

/* Note: include "server/server.h" before including this header */

#define CHIMERA_TEST_USER_ROOT_UID         0
#define CHIMERA_TEST_USER_ROOT_GID         0
#define CHIMERA_TEST_USER_JOHNDOE_UID      1000
#define CHIMERA_TEST_USER_JOHNDOE_GID      1000
#define CHIMERA_TEST_USER_MYUSER_UID       1001
#define CHIMERA_TEST_USER_MYUSER_GID       1001

#define CHIMERA_TEST_USER_PASSWORD \
        "$6$testsalt$51yaaBMXXXt5vK522YOlIMZ267vqHtMIzc2klcsu3EEv/bkRDU9g3UmkypXf.NvlpPpIPK1nX5zdbCeJBiQbB/"

#define CHIMERA_TEST_USER_SMBPASSWD        "secret"
#define CHIMERA_TEST_USER_MYUSER_SMBPASSWD "mypassword"

// Resolve a user spec ("root", "johndoe", "myuser", or "uid:gid") to a VFS
// credential.  Returns 1 on success, 0 if the spec is unrecognized.
static inline int
chimera_test_parse_user(
    const char              *user_spec,
    struct chimera_vfs_cred *cred)
{
    unsigned int uid, gid;

    if (strcmp(user_spec, "root") == 0) {
        chimera_vfs_cred_init_unix(cred,
                                   CHIMERA_TEST_USER_ROOT_UID,
                                   CHIMERA_TEST_USER_ROOT_GID,
                                   0, NULL);
        return 1;
    }
    if (strcmp(user_spec, "johndoe") == 0) {
        chimera_vfs_cred_init_unix(cred,
                                   CHIMERA_TEST_USER_JOHNDOE_UID,
                                   CHIMERA_TEST_USER_JOHNDOE_GID,
                                   0, NULL);
        return 1;
    }
    if (strcmp(user_spec, "myuser") == 0) {
        chimera_vfs_cred_init_unix(cred,
                                   CHIMERA_TEST_USER_MYUSER_UID,
                                   CHIMERA_TEST_USER_MYUSER_GID,
                                   0, NULL);
        return 1;
    }
    if (sscanf(user_spec, "%u:%u", &uid, &gid) == 2) {
        chimera_vfs_cred_init_unix(cred, uid, gid, 0, NULL);
        return 1;
    }
    return 0;
} // chimera_test_parse_user

static inline void
chimera_test_add_server_users(struct chimera_server *server)
{
    chimera_server_add_user(server, "root",
                            CHIMERA_TEST_USER_PASSWORD,
                            CHIMERA_TEST_USER_SMBPASSWD,
                            NULL,  // SID
                            CHIMERA_TEST_USER_ROOT_UID,
                            CHIMERA_TEST_USER_ROOT_GID,
                            0, NULL, 1);

    chimera_server_add_user(server, "johndoe",
                            CHIMERA_TEST_USER_PASSWORD,
                            CHIMERA_TEST_USER_SMBPASSWD,
                            NULL,  // SID
                            CHIMERA_TEST_USER_JOHNDOE_UID,
                            CHIMERA_TEST_USER_JOHNDOE_GID,
                            0, NULL, 1);

    chimera_server_add_user(server, "myuser",
                            CHIMERA_TEST_USER_PASSWORD,
                            CHIMERA_TEST_USER_MYUSER_SMBPASSWD,
                            NULL,  // SID
                            CHIMERA_TEST_USER_MYUSER_UID,
                            CHIMERA_TEST_USER_MYUSER_GID,
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

    json_t *myuser_user = json_object();
    json_object_set_new(myuser_user, "username", json_string("myuser"));
    json_object_set_new(myuser_user, "password", json_string(CHIMERA_TEST_USER_PASSWORD));
    json_object_set_new(myuser_user, "smbpasswd", json_string(CHIMERA_TEST_USER_MYUSER_SMBPASSWD));
    json_object_set_new(myuser_user, "uid", json_integer(CHIMERA_TEST_USER_MYUSER_UID));
    json_object_set_new(myuser_user, "gid", json_integer(CHIMERA_TEST_USER_MYUSER_GID));
    json_array_append_new(users, myuser_user);

    json_object_set_new(config, "users", users);
} /* chimera_test_write_users_json */
