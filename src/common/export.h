// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* A library's public interface.  Each library is a shared object (a DLL on
 * Windows) and exports only what is marked: SYMBOL_EXPORT on a function's
 * declaration or definition exports it, and callers in other libraries need
 * nothing more.
 *
 * Data is different on Windows: code outside the library that defines a
 * variable must declare it dllimport.  So each library exporting data defines
 * CHIMERA_<LIB>_BUILD while it is being built and declares that data with a
 * CHIMERA_<LIB>_DATA macro, which is CHIMERA_DATA_EXPORT inside the library
 * and CHIMERA_DATA_IMPORT everywhere else.  Thread-local data cannot cross a
 * DLL boundary at all; reach it through a function. */
#ifdef _WIN32
#define SYMBOL_EXPORT       __declspec(dllexport)
#define CHIMERA_DATA_EXPORT __declspec(dllexport)
#define CHIMERA_DATA_IMPORT __declspec(dllimport)
#else // ifdef _WIN32
#define SYMBOL_EXPORT       __attribute__((visibility("default")))
#define CHIMERA_DATA_EXPORT SYMBOL_EXPORT
#define CHIMERA_DATA_IMPORT SYMBOL_EXPORT
#endif // ifdef _WIN32

#ifdef CHIMERA_COMMON_BUILD
#define CHIMERA_COMMON_DATA CHIMERA_DATA_EXPORT
#else // ifdef CHIMERA_COMMON_BUILD
#define CHIMERA_COMMON_DATA CHIMERA_DATA_IMPORT
#endif // ifdef CHIMERA_COMMON_BUILD
