// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include "common/compiler.h"

#ifdef _WIN32
#define SYMBOL_EXPORT
#else // ifdef _WIN32
#define SYMBOL_EXPORT __attribute__((visibility("default")))
#endif // ifdef _WIN32

#ifndef offsetof
#define offsetof(type, member) ((size_t) &((type *) 0)->member)
#endif /* ifndef offsetof */

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

#ifndef FORCE_INLINE
#ifdef _MSC_VER
#define FORCE_INLINE __forceinline
#else // ifdef _MSC_VER
#define FORCE_INLINE __attribute__((always_inline)) inline
#endif // ifdef _MSC_VER
#endif /* ifndef FORCE_INLINE */