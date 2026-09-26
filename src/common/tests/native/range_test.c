// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "common/range.h"
#undef NDEBUG
#include <assert.h>

int
main(void)
{
    struct chimera_range_endpoint zero = chimera_range_offset(0);
    struct chimera_range_endpoint eof  = chimera_range_eof();

    assert(chimera_range_compare(zero, eof) < 0);
    assert(chimera_range_compare(chimera_range_end(UINT64_MAX, 1), eof) == 0);
    assert(chimera_range_compare(chimera_range_end(UINT64_MAX, 2), eof) > 0);
    assert(chimera_range_compare(chimera_range_end(100, 101), chimera_range_end(100, 100)) > 0);
    assert(chimera_range_compare(chimera_range_end(0, UINT64_MAX), eof) < 0);
    uint64_t                      start, length;
    assert(chimera_range_seek_end(100, -20, -30, &start, &length));
    assert(start == 50 && length == 30);
    assert(chimera_range_seek_end(100, -100, 0, &start, &length));
    assert(!start && !length);
    assert(!chimera_range_seek_end(100, -101, 1, &start, &length));
    assert(!chimera_range_seek_end(INT64_MAX, 1, 1, &start, &length));
    assert(chimera_range_seek_end(INT64_MAX, 0, 1, &start, &length));
    assert(start == INT64_MAX && length == 1);
    assert(chimera_range_seek_end(UINT64_MAX, INT64_MIN, 1, &start, &length));
    assert(start == INT64_MAX && length == 1);
    assert(!chimera_range_seek_end(UINT64_MAX, INT64_MAX, INT64_MIN, &start, &length));
    assert(chimera_range_seek_end(0, INT64_MAX, -INT64_MAX, &start, &length));
    assert(start == 0 && length == INT64_MAX);
    return 0;
} /* main */
