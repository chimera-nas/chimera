// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Shared by the libdcerpc service stubs (smb_srvsvc.c, smb_wkssvc.c,
 * smb_lsarpc.c, smb_samr.c) and the pipe state in smb_rpc.c: the DCE/RPC side
 * of the seam described in smb_rpc_host.h.  These files include libdcerpc and
 * smb_rpc_host.h only -- never chimera's server headers.
 */

#include <stdint.h>
#include <string.h>

#include <dcerpc/dcerpc.h>
#include <dcerpc/dcerpc-dtyp.h>
#include <dcerpc/server.h>

#include "smb_rpc_host.h"

/* WERROR / NTSTATUS values the stubs return in their status fields. */
#define CHIMERA_SMB_RPC_WERR_OK                       0x00000000U
#define CHIMERA_SMB_RPC_WERR_UNKNOWN_LEVEL            0x0000007CU
#define CHIMERA_SMB_RPC_WERR_NET_NAME_NOT_FOUND       0x00000906U
#define CHIMERA_SMB_RPC_NERR_DEVICE_NOT_SHARED        0x00000907U

#define CHIMERA_SMB_RPC_STATUS_OK                     0x00000000U
#define CHIMERA_SMB_RPC_STATUS_SOME_NOT_MAPPED        0x00000107U
#define CHIMERA_SMB_RPC_STATUS_NO_MORE_ENTRIES        0x8000001AU
#define CHIMERA_SMB_RPC_STATUS_INVALID_PARAMETER      0xC000000DU
#define CHIMERA_SMB_RPC_STATUS_NONE_MAPPED            0xC0000073U
#define CHIMERA_SMB_RPC_STATUS_INSUFFICIENT_RESOURCES 0xC000009AU
#define CHIMERA_SMB_RPC_STATUS_NO_SUCH_DOMAIN         0xC00000DFU

/* What a context handle was opened as; a handle is only accepted by the calls
 * of its own kind. */
enum chimera_smb_rpc_handle_type {
    CHIMERA_SMB_RPC_HANDLE_LSA_POLICY  = 1,
    CHIMERA_SMB_RPC_HANDLE_SAMR_SERVER = 2,
};

struct chimera_smb_rpc_handle;

struct chimera_smb_rpc_state {
    struct dcerpc_server          *srv;
    struct dcerpc_server_pipe     *rpc;
    struct chimera_smb_rpc_host   *host;
    /* Context handles opened on this pipe; freed with it, so a client that
     * closes the pipe without closing its handles leaks nothing. */
    struct chimera_smb_rpc_handle *handles;
    uint32_t                       num_handles;
    uint32_t                       next_handle;
};

/* The pipe state a stub's call arrived on. */
struct chimera_smb_rpc_state *
chimera_smb_rpc_state_of_pdu(
    struct dcerpc_pdu *pdu);

/* Copy s into storage freed with the reply. */
char *
chimera_smb_rpc_strdup(
    struct dcerpc_pdu *pdu,
    const char        *s);

/* ASCII case-insensitive comparisons (share and account names). */
int
chimera_smb_rpc_strcasecmp(
    const char *a,
    const char *b);

int
chimera_smb_rpc_strncasecmp(
    const char *a,
    const char *b,
    size_t      n);

int
chimera_smb_rpc_handle_open(
    struct chimera_smb_rpc_state    *state,
    enum chimera_smb_rpc_handle_type type,
    struct dcerpc_context_handle    *handle);

int
chimera_smb_rpc_handle_valid(
    struct chimera_smb_rpc_state       *state,
    enum chimera_smb_rpc_handle_type    type,
    const struct dcerpc_context_handle *handle);

int
chimera_smb_rpc_handle_close(
    struct chimera_smb_rpc_state       *state,
    enum chimera_smb_rpc_handle_type    type,
    const struct dcerpc_context_handle *handle);

/* ---- SIDs shared by LSA and SAMR ---- */

/* The server's account (machine) domain SID, S-1-5-21-X-Y-Z. */
void
chimera_smb_rpc_account_sid(
    RPC_SID                           *sid,
    const struct chimera_smb_rpc_host *host);

void
chimera_smb_rpc_builtin_sid(
    RPC_SID *sid);

int
chimera_smb_rpc_sid_to_string(
    const RPC_SID *sid,
    char          *buf,
    size_t         buflen);

int
chimera_smb_rpc_string_to_sid(
    const char *str,
    RPC_SID    *sid);

int
chimera_smb_rpc_sid_equal(
    const RPC_SID *a,
    const RPC_SID *b);

RPC_SID *
chimera_smb_rpc_sid_dup(
    struct dcerpc_pdu *pdu,
    const RPC_SID     *sid);

/* ---- endpoints: one dcerpc_server per named pipe ---- */

struct dcerpc_server *
chimera_smb_srvsvc_endpoint(
    void);

struct dcerpc_server *
chimera_smb_wkssvc_endpoint(
    void);

struct dcerpc_server *
chimera_smb_lsarpc_endpoint(
    void);

struct dcerpc_server *
chimera_smb_samr_endpoint(
    void);
