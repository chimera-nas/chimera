// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#ifdef _WIN32
#include <evpl/evpl_platform.h>
#include <stdio.h>

static inline int chimera_backtrace(void **frames, int count)
{
    return CaptureStackBackTrace(0, (DWORD) count, frames, NULL);
}
/* Address strings remain useful with the matching PDB and crash dump. */
static inline char **chimera_backtrace_symbols(void *const *frames, int count)
{
    size_t size = sizeof(char *) + 32;
    char **symbols;
    char *text;
    if (count < 0 || (size_t) count > SIZE_MAX / size) {
        return NULL;
    }
    symbols = malloc((size_t) count * size);
    if (!symbols) {
        return NULL;
    }
    text = (char *) (symbols + count);
    for (int i = 0; i < count; i++) {
        symbols[i] = text + i * 32;
        snprintf(symbols[i], 32, "%p", frames[i]);
    }
    return symbols;
}
#else
#include <execinfo.h>
#define chimera_backtrace backtrace
#define chimera_backtrace_symbols backtrace_symbols
#endif
