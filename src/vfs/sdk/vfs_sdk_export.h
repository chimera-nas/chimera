// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* The SDK's own spelling of SYMBOL_EXPORT (common/export.h), since an
 * out-of-tree module sees only this directory.  It marks the declarations of
 * the functions chimera_vfs and chimera_common export to modules: MSVC
 * requires a dllexport definition's declarations to carry dllexport too,
 * and on a declaration whose definition lives in another DLL it is inert. */
#ifndef CHIMERA_VFS_SDK_EXPORT
#ifdef _WIN32
#define CHIMERA_VFS_SDK_EXPORT      __declspec(dllexport)
#else // ifdef _WIN32
#define CHIMERA_VFS_SDK_EXPORT      __attribute__((visibility("default")))
#endif // ifdef _WIN32
#endif // ifndef CHIMERA_VFS_SDK_EXPORT

/* Data chimera_common exports to modules (the log level).  Code outside a
 * DLL must declare its data dllimport; chimera_common, which defines it,
 * declares it dllexport -- the same rule as CHIMERA_COMMON_DATA. */
#ifndef CHIMERA_VFS_SDK_COMMON_DATA
#if defined(_WIN32) && defined(CHIMERA_COMMON_BUILD)
#define CHIMERA_VFS_SDK_COMMON_DATA __declspec(dllexport)
#elif defined(_WIN32)
#define CHIMERA_VFS_SDK_COMMON_DATA __declspec(dllimport)
#else // if defined(_WIN32) && defined(CHIMERA_COMMON_BUILD)
#define CHIMERA_VFS_SDK_COMMON_DATA __attribute__((visibility("default")))
#endif // if defined(_WIN32) && defined(CHIMERA_COMMON_BUILD)
#endif // ifndef CHIMERA_VFS_SDK_COMMON_DATA
