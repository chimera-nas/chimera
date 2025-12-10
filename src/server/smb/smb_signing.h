// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct chimera_smb_request;
struct evpl_iovec_cursor;
struct chimera_smb_compound;
struct evpl_iovec;
struct chimera_smb_signing_ctx;

struct chimera_smb_signing_ctx *
chimera_smb_signing_ctx_create(
    void);

void
chimera_smb_signing_ctx_destroy(
    struct chimera_smb_signing_ctx *ctx);

int
chimera_smb_derive_signing_key(
    int    dialect,
    void  *output,
    void  *session_key,
    size_t session_key_len);


int
chimera_smb_verify_signature(
    struct chimera_smb_signing_ctx *ctx,
    struct chimera_smb_request     *request,
    struct evpl_iovec_cursor       *cursor,
    int                             length);

int
chimera_smb_sign_compound(
    struct chimera_smb_signing_ctx *ctx,
    struct chimera_smb_compound    *compound,
    struct evpl_iovec              *iov,
    int                             niov,
    int                             length);

