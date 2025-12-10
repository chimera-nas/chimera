// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#define SYMBOL_EXPORT __attribute__((visibility("default")))

#ifndef offsetof
#define offsetof(type, member) ((size_t) &((type *) 0)->member)
#endif /* ifndef offsetof */

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */