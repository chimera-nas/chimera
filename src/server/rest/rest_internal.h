// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/logging.h"

#define chimera_rest_debug(...) chimera_debug("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_info(...)  chimera_info("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_error(...) chimera_error("rest", __FILE__, __LINE__, __VA_ARGS__)

struct chimera_rest_server {
    int                    http_port;
    int                    https_port;
    struct evpl_endpoint  *http_endpoint;
    struct evpl_endpoint  *https_endpoint;
    struct evpl_listener  *http_listener;
    struct evpl_listener  *https_listener;
    struct chimera_server *server;
};

struct chimera_rest_thread {
    struct evpl                *evpl;
    struct chimera_rest_server *shared;
    struct evpl_http_agent     *agent;
    struct evpl_http_server    *http_server;
    struct evpl_http_server    *https_server;
};
