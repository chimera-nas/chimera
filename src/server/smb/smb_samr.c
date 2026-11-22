// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SAMR (MS-SAMR) on libdcerpc, read-only foundation: connect, enumerate the
 * server's two domains (the account domain named after the server, and
 * Builtin) and resolve a domain name to its SID.
 */

#include <string.h>

#include <dcerpc/samr.h>

#include "smb_rpc_service.h"

static int
chimera_smb_samr_open_server(
    struct dcerpc_pdu            *pdu,
    struct dcerpc_context_handle *handle,
    uint32_t                     *status)
{
    if (chimera_smb_rpc_handle_open(chimera_smb_rpc_state_of_pdu(pdu),
                                    CHIMERA_SMB_RPC_HANDLE_SAMR_SERVER, handle)) {
        memset(handle, 0, sizeof(*handle));
        *status = CHIMERA_SMB_RPC_STATUS_INSUFFICIENT_RESOURCES;
    } else {
        *status = CHIMERA_SMB_RPC_STATUS_OK;
    }

    return 0;
} /* chimera_smb_samr_open_server */

static int
chimera_smb_samr_server_valid(
    struct dcerpc_pdu                  *pdu,
    const struct dcerpc_context_handle *handle)
{
    return chimera_smb_rpc_handle_valid(chimera_smb_rpc_state_of_pdu(pdu),
                                        CHIMERA_SMB_RPC_HANDLE_SAMR_SERVER, handle);
} /* chimera_smb_samr_server_valid */

/* SamrConnect (opnum 0). */
static int
chimera_smb_samr_connect(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct samr_Connect_rep *rep = repv;

    (void) dce;
    (void) reqv;

    return chimera_smb_samr_open_server(pdu, &rep->ServerHandle, &rep->status);
} /* chimera_smb_samr_connect */

/* SamrConnect2 (opnum 57). */
static int
chimera_smb_samr_connect2(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct samr_Connect2_rep *rep = repv;

    (void) dce;
    (void) reqv;

    return chimera_smb_samr_open_server(pdu, &rep->ServerHandle, &rep->status);
} /* chimera_smb_samr_connect2 */

/* SamrConnect5 (opnum 64). */
static int
chimera_smb_samr_connect5(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct samr_Connect5_rep *rep = repv;

    (void) dce;
    (void) reqv;

    /* Revision 3, no optional features. */
    rep->OutVersion                           = 1;
    rep->OutRevisionInfo.V1.Revision          = 3;
    rep->OutRevisionInfo.V1.SupportedFeatures = 0;

    return chimera_smb_samr_open_server(pdu, &rep->ServerHandle, &rep->status);
} /* chimera_smb_samr_connect5 */

/* SamrCloseHandle (opnum 1). */
static int
chimera_smb_samr_close_handle(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct samr_CloseHandle_req *req = reqv;
    struct samr_CloseHandle_rep *rep = repv;

    (void) dce;

    if (!chimera_smb_rpc_handle_close(chimera_smb_rpc_state_of_pdu(pdu),
                                      CHIMERA_SMB_RPC_HANDLE_SAMR_SERVER,
                                      &req->SamHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    rep->status = CHIMERA_SMB_RPC_STATUS_OK;
    return 0;
} /* chimera_smb_samr_close_handle */

/* SamrEnumerateDomainsInSamServer (opnum 6). */
static int
chimera_smb_samr_enum_domains(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct samr_EnumerateDomainsInSamServer_req *req   = reqv;
    struct samr_EnumerateDomainsInSamServer_rep *rep   = repv;
    struct chimera_smb_rpc_state                *state = chimera_smb_rpc_state_of_pdu(pdu);
    struct samr_SAMPR_RID_ENUMERATION           *e;

    (void) dce;

    if (!chimera_smb_samr_server_valid(pdu, &req->ServerHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    e = dcerpc_alloc_data(pdu, 2 * sizeof(*e));

    if (!e) {
        return -1;
    }

    e[0].Name = chimera_smb_rpc_strdup(pdu, chimera_smb_rpc_host_identity(state->host));
    e[1].Name = "Builtin";

    if (!e[0].Name) {
        return -1;
    }

    rep->Buffer.EntriesRead = 2;
    rep->Buffer.Buffer      = e;
    rep->CountReturned      = 2;
    rep->EnumerationContext = 0;
    rep->status             = CHIMERA_SMB_RPC_STATUS_OK;
    return 0;
} /* chimera_smb_samr_enum_domains */

/* SamrLookupDomainInSamServer (opnum 5). */
static int
chimera_smb_samr_lookup_domain(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct samr_LookupDomainInSamServer_req *req   = reqv;
    struct samr_LookupDomainInSamServer_rep *rep   = repv;
    struct chimera_smb_rpc_state            *state = chimera_smb_rpc_state_of_pdu(pdu);
    RPC_SID                                  sid;

    (void) dce;

    if (!chimera_smb_samr_server_valid(pdu, &req->ServerHandle)) {
        return DCERPC_FAULT_CONTEXT_MISMATCH;
    }

    if (req->Name && chimera_smb_rpc_strcasecmp(req->Name, "Builtin") == 0) {
        chimera_smb_rpc_builtin_sid(&sid);
    } else if (req->Name &&
               chimera_smb_rpc_strcasecmp(req->Name, chimera_smb_rpc_host_identity(state->host)) == 0) {
        chimera_smb_rpc_account_sid(&sid, state->host);
    } else {
        rep->status = CHIMERA_SMB_RPC_STATUS_NO_SUCH_DOMAIN;
        return 0;
    }

    rep->DomainId = chimera_smb_rpc_sid_dup(pdu, &sid);

    if (!rep->DomainId) {
        return -1;
    }

    rep->status = CHIMERA_SMB_RPC_STATUS_OK;
    return 0;
} /* chimera_smb_samr_lookup_domain */

static const struct dcerpc_stub chimera_smb_samr_stubs[] = {
    { SAMR_CONNECT,                     "Connect",                          chimera_smb_samr_connect
    },
    { SAMR_CLOSEHANDLE,                 "CloseHandle",                      chimera_smb_samr_close_handle
    },
    { SAMR_LOOKUPDOMAININSAMSERVER,     "LookupDomainInSamServer",          chimera_smb_samr_lookup_domain
    },
    { SAMR_ENUMERATEDOMAINSINSAMSERVER, "EnumerateDomainsInSamServer",      chimera_smb_samr_enum_domains
    },
    { SAMR_CONNECT2,                    "Connect2",                         chimera_smb_samr_connect2
    },
    { SAMR_CONNECT5,                    "Connect5",                         chimera_smb_samr_connect5
    },
    { -1,                               NULL,                               NULL
    }
};

struct dcerpc_server *
chimera_smb_samr_endpoint(void)
{
    struct dcerpc_server *srv = dcerpc_server_create("samr");

    if (srv && dcerpc_server_add_interface(srv, &samr_interface, samr_procs,
                                           chimera_smb_samr_stubs,
                                           DCERPC_SERVER_ALL_NDR)) {
        dcerpc_server_destroy(srv);
        srv = NULL;
    }

    return srv;
} /* chimera_smb_samr_endpoint */
