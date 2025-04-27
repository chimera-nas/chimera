#pragma once

void
chimera_s3_add_bucket(
    void       *s3_shared,
    const char *name,
    const char *path);

extern struct chimera_server_protocol s3_protocol;