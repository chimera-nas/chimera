// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "smb_internal.h"

int
chimera_smb_lsarpc_transceive(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov);

/* A session has been taken off a live connection
 * (chimera_smb_conn_remove_session_handle): fail its calls still waiting on
 * the identity resolver, while it can still sign the replies. */
void
chimera_smb_lsarpc_session_closing(
    struct chimera_smb_conn           *conn,
    struct chimera_smb_session_handle *session_handle);
