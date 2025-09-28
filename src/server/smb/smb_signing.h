// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

int
chimera_smb_verify_signature(
    struct chimera_smb_request *request,
    struct evpl_iovec_cursor   *cursor,
    int                         length);

int
chimera_smb_sign_compound(
    struct chimera_smb_compound *compound,
    struct evpl_iovec           *iov,
    int                          niov,
    int                          length);

