// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
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
    const uint8_t                  *signing_key,
    struct evpl_iovec_cursor       *cursor,
    int                             length);

int
chimera_smb_sign_compound(
    struct chimera_smb_signing_ctx *ctx,
    struct chimera_smb_compound    *compound,
    struct evpl_iovec              *iov,
    int                             niov,
    int                             length);

/*
 * Sign a single contiguous SMB2 message in place (for standalone async
 * responses such as CHANGE_NOTIFY).  smb2_buf must point at the SMB2
 * header (not the NetBIOS framing).  Sets SMB2_FLAGS_SIGNED and writes
 * the signature into hdr->signature.
 */
int
chimera_smb_sign_message(
    struct chimera_smb_signing_ctx *ctx,
    int                             dialect,
    const uint8_t                  *signing_key,
    uint8_t                        *smb2_buf,
    int                             smb2_len);

