// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "prometheus-c.h"

struct chimera_metrics;

struct chimera_metrics * chimera_metrics_init(
    int port);

void chimera_metrics_destroy(
    struct chimera_metrics *metrics);

struct prometheus_metrics * chimera_metrics_get(
    struct chimera_metrics *metrics);

/*
 * Scrape chimera's prometheus registry plus libevpl's metrics and write the
 * combined Prometheus text exposition (identical to GET :PORT/metrics) to
 * `path`, truncating it.  Intended to be called once at shutdown so that very
 * short-lived processes can retain their metrics after exiting.  No-op when
 * `path` is NULL.  Returns 0 on success, -1 on error.
 */
int chimera_metrics_dump_file(
    struct prometheus_metrics *metrics,
    const char                *path);