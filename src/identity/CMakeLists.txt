# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Identity modules: backends for the identity engine in src/vfs/vfs_identity.c,
# built against the module SDK in src/vfs/sdk/vfs_identity_module.h.  Each
# exports a `struct chimera_vfs_identity_module identity_<name>` symbol that
# the engine finds by the module's configured name.

if (NOT WIN32)
    add_subdirectory(nss)
endif()

if (WBCLIENT_FOUND)
    add_subdirectory(winbind)
endif()
