// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <time.h>

struct chimera_rest_server;
struct evpl;
struct evpl_http_request;
struct chimera_rest_thread;

#define CHIMERA_REST_JWT_SECRET_LEN 32
#define CHIMERA_REST_JWT_EXPIRY     86400

struct chimera_rest_jwt_claims {
    char   sub[256];
    time_t iat;
    time_t exp;
};

void
chimera_rest_auth_init_secret(
    struct chimera_rest_server *rest);

int
chimera_rest_auth_validate_credentials(
    struct chimera_rest_server     *rest,
    const char                     *username,
    const char                     *password,
    struct chimera_rest_jwt_claims *claims);

int
chimera_rest_jwt_create(
    struct chimera_rest_server           *rest,
    const struct chimera_rest_jwt_claims *claims,
    char                                 *token_out,
    int                                   token_out_size);

int
chimera_rest_jwt_verify(
    struct chimera_rest_server     *rest,
    const char                     *token,
    struct chimera_rest_jwt_claims *claims);

int
chimera_rest_auth_check_bearer(
    struct chimera_rest_server     *rest,
    struct evpl_http_request       *request,
    struct chimera_rest_jwt_claims *claims);

void
chimera_rest_handle_auth_login(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len);
