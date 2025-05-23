
#pragma once

#include "prometheus-c.h"

struct chimera_metrics;

struct chimera_metrics * chimera_metrics_init(
    int port);

void chimera_metrics_destroy(
    struct chimera_metrics *metrics);

struct prometheus_metrics * chimera_metrics_get(
    struct chimera_metrics *metrics);