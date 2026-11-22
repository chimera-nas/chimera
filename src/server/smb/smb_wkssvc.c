// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * WKSSVC (MS-WKST) on libdcerpc: NetrWkstaGetInfo, the computer and workgroup
 * name a client shows for the server.
 */

#include <dcerpc/wkssvc.h>

#include "smb_rpc_service.h"

#define WKS_PLATFORM_ID_NT 500
#define WKS_VERSION_MAJOR  6
#define WKS_VERSION_MINOR  1

/* NetrWkstaGetInfo (opnum 0). */
static int
chimera_smb_wkssvc_wksta_get_info(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct wkssvc_NetrWkstaGetInfo_req *req   = reqv;
    struct wkssvc_NetrWkstaGetInfo_rep *rep   = repv;
    struct chimera_smb_rpc_state       *state = chimera_smb_rpc_state_of_pdu(pdu);
    union wkssvc_WKSTA_INFO            *u     = &rep->WkstaInfo;
    char                               *name, *group;

    (void) dce;

    name  = chimera_smb_rpc_strdup(pdu, chimera_smb_rpc_host_identity(state->host));
    group = chimera_smb_rpc_strdup(pdu, chimera_smb_rpc_host_workgroup(state->host));

    if (!name || !group) {
        return -1;
    }

    switch (req->Level) {
        case 100:
            u->WkstaInfo100.PlatformId   = WKS_PLATFORM_ID_NT;
            u->WkstaInfo100.ComputerName = name;
            u->WkstaInfo100.LanGroup     = group;
            u->WkstaInfo100.VerMajor     = WKS_VERSION_MAJOR;
            u->WkstaInfo100.VerMinor     = WKS_VERSION_MINOR;
            break;
        case 101:
            u->WkstaInfo101.PlatformId   = WKS_PLATFORM_ID_NT;
            u->WkstaInfo101.ComputerName = name;
            u->WkstaInfo101.LanGroup     = group;
            u->WkstaInfo101.VerMajor     = WKS_VERSION_MAJOR;
            u->WkstaInfo101.VerMinor     = WKS_VERSION_MINOR;
            u->WkstaInfo101.LanRoot      = "";
            break;
        case 102:
            u->WkstaInfo102.PlatformId    = WKS_PLATFORM_ID_NT;
            u->WkstaInfo102.ComputerName  = name;
            u->WkstaInfo102.LanGroup      = group;
            u->WkstaInfo102.VerMajor      = WKS_VERSION_MAJOR;
            u->WkstaInfo102.VerMinor      = WKS_VERSION_MINOR;
            u->WkstaInfo102.LanRoot       = "";
            u->WkstaInfo102.LoggedOnUsers = 1;
            break;
        case 502:
            /* Redirector tuning parameters: zeros, which clients accept. */
            break;
        default:
            rep->status = CHIMERA_SMB_RPC_WERR_UNKNOWN_LEVEL;
            return 0;
    } /* switch */

    rep->status = CHIMERA_SMB_RPC_WERR_OK;
    return 0;
} /* chimera_smb_wkssvc_wksta_get_info */

static const struct dcerpc_stub chimera_smb_wkssvc_stubs[] = {
    {
        WKSSVC_NETRWKSTAGETINFO, "NetrWkstaGetInfo", chimera_smb_wkssvc_wksta_get_info
    },
    {
        -1, NULL, NULL
    },
};

struct dcerpc_server *
chimera_smb_wkssvc_endpoint(void)
{
    struct dcerpc_server *srv = dcerpc_server_create("wkssvc");

    if (srv && dcerpc_server_add_interface(srv, &wkssvc_interface, wkssvc_procs,
                                           chimera_smb_wkssvc_stubs,
                                           DCERPC_SERVER_ALL_NDR)) {
        dcerpc_server_destroy(srv);
        srv = NULL;
    }

    return srv;
} /* chimera_smb_wkssvc_endpoint */
