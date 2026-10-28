// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdarg.h>
#include <stdio.h>
#include "evpl/evpl.h"
#include "common/compiler.h"

/*
 * Thread names are what ps -L, top -H and debuggers show.  chimera's start
 * with chm_, so they stand apart from libevpl's (evpl_) and from those of an
 * application that embeds chimera.  The OS keeps 15 bytes, and evpl
 * threadpools add _0, _1, ... to the name, so keep pool names to 12 bytes or
 * less.  Listeners take their name from evpl_listener_create_named().
 */

/* A thread config that only adds a name to the evpl thread defaults, so it
 * behaves exactly like passing NULL to evpl_thread_create(). */
static inline struct evpl_thread_config *
chimera_thread_config(
    const char *fmt,
    ...) CHIMERA_PRINTF(1, 2);

static inline struct evpl_thread_config *
chimera_thread_config(
    const char *fmt,
    ...)
{
    struct evpl_thread_config *config = evpl_thread_config_init();
    char                       name[32];
    va_list                    args;

    va_start(args, fmt);
    vsnprintf(name, sizeof(name), fmt, args);
    va_end(args);

    evpl_thread_config_set_name(config, name);

    return config;
} // chimera_thread_config

/* Name the calling thread, for threads chimera starts without evpl. */
static inline void
chimera_thread_set_name(
    const char *fmt,
    ...) CHIMERA_PRINTF(1, 2);

static inline void
chimera_thread_set_name(
    const char *fmt,
    ...)
{
    char    name[32];
    va_list args;

    va_start(args, fmt);
    vsnprintf(name, sizeof(name), fmt, args);
    va_end(args);

    evpl_thread_set_name(name);
} // chimera_thread_set_name
