// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SRVSVC (MS-SRVS) on libdcerpc: share enumeration and share / server
 * information, which back Explorer's share browsing and `net view`.
 */

#include <dcerpc/srvsvc.h>

#include "smb_rpc_service.h"

#define SRV_STYPE_IPC_HIDDEN (SRVSVC_STYPE_IPC | SRVSVC_STYPE_SPECIAL)

/* MS-SRVS SV_PLATFORM_ID_NT, and an NT-class server type. */
#define SRV_PLATFORM_ID_NT   500
#define SRV_TYPE_DEFAULT     (SRVSVC_SV_TYPE_WORKSTATION | SRVSVC_SV_TYPE_SERVER | 0x00001000)
#define SRV_VERSION_MAJOR    6
#define SRV_VERSION_MINOR    1

/* One share as reported to clients. */
struct srv_share {
    char    *name;
    uint32_t type;
    char    *remark;
    uint32_t flags;    /* SHI1005 flags */
};

/* State for walking the configured shares into reply storage. */
struct srv_share_walk {
    struct dcerpc_pdu *pdu;
    const char        *match;
    struct srv_share  *arr;
    int                count;
    int                max;
};

static int
chimera_smb_srvsvc_count_share(
    void       *arg,
    const char *name,
    int         access_based_enum)
{
    struct srv_share_walk *w = arg;

    (void) access_based_enum;

    if (!w->match || chimera_smb_rpc_strcasecmp(w->match, name) == 0) {
        w->max++;
    }

    return 0;
} /* chimera_smb_srvsvc_count_share */

static int
chimera_smb_srvsvc_add_share(
    void       *arg,
    const char *name,
    int         access_based_enum)
{
    struct srv_share_walk *w = arg;
    struct srv_share      *s;

    if (w->match && chimera_smb_rpc_strcasecmp(w->match, name) != 0) {
        return 0;
    }

    /* A share added since the counting pass waits for the next call. */
    if (w->count == w->max) {
        return 0;
    }

    s         = &w->arr[w->count];
    s->name   = chimera_smb_rpc_strdup(w->pdu, name);
    s->type   = SRVSVC_STYPE_DISKTREE;
    s->remark = "";
    s->flags  = access_based_enum ? SRVSVC_SHI1005_FLAGS_ACCESS_BASED_DIRECTORY_ENUM : 0;

    if (!s->name) {
        return -1;
    }

    w->count++;
    return 0;
} /* chimera_smb_srvsvc_add_share */

/*
 * Snapshot the configured shares plus a synthetic IPC$ into reply storage.
 * Returns the count, or -1 on allocation failure.  If `match` is set only the
 * share of that name (case-insensitive) is returned.
 */
static int
chimera_smb_srvsvc_shares(
    struct dcerpc_pdu                 *pdu,
    const struct chimera_smb_rpc_host *host,
    const char                        *match,
    struct srv_share                 **out)
{
    struct srv_share_walk w = { .pdu = pdu, .match = match };

    chimera_smb_rpc_host_shares(host, chimera_smb_srvsvc_count_share, &w);

    w.arr = dcerpc_alloc_data(pdu, (w.max + 1) * sizeof(*w.arr));

    if (!w.arr || chimera_smb_rpc_host_shares(host, chimera_smb_srvsvc_add_share, &w)) {
        return -1;
    }

    if (!match || chimera_smb_rpc_strcasecmp(match, "IPC$") == 0) {
        w.arr[w.count].name   = "IPC$";
        w.arr[w.count].type   = SRV_STYPE_IPC_HIDDEN;
        w.arr[w.count].remark = "Remote IPC";
        w.arr[w.count].flags  = 0;
        w.count++;
    }

    *out = w.arr;
    return w.count;
} /* chimera_smb_srvsvc_shares */

static int
chimera_smb_srvsvc_level_supported(uint32_t level)
{
    return level == 0 || level == 1 || level == 2 || level == 501 || level == 502;
} /* chimera_smb_srvsvc_level_supported */

/* NetrShareEnum (opnum 15). */
static int
chimera_smb_srvsvc_share_enum(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct srvsvc_NetrShareEnum_req *req   = reqv;
    struct srvsvc_NetrShareEnum_rep *rep   = repv;
    struct chimera_smb_rpc_state    *state = chimera_smb_rpc_state_of_pdu(pdu);
    uint32_t                         level = req->InfoStruct.Level;
    union srvsvc_SHARE_ENUM_UNION   *u     = &rep->InfoStruct.ShareInfo;
    struct srv_share                *shares;
    int                              n, i;

    (void) dce;

    /* The reply's union is switched on the level the client asked for. */
    rep->InfoStruct.Level = level;
    rep->ResumeHandle     = 0;

    if (!chimera_smb_srvsvc_level_supported(level)) {
        rep->status = CHIMERA_SMB_RPC_WERR_UNKNOWN_LEVEL;
        return 0;
    }

    n = chimera_smb_srvsvc_shares(pdu, state->host, NULL, &shares);

    if (n < 0) {
        return -1;
    }

    switch (level) {
        case 0:
            u->Level0.Buffer = dcerpc_alloc_data(pdu, n * sizeof(*u->Level0.Buffer));
            if (!u->Level0.Buffer) {
                return -1;
            }
            for (i = 0; i < n; i++) {
                u->Level0.Buffer[i].NetName = shares[i].name;
            }
            u->Level0.EntriesRead = n;
            break;
        case 1:
            u->Level1.Buffer = dcerpc_alloc_data(pdu, n * sizeof(*u->Level1.Buffer));
            if (!u->Level1.Buffer) {
                return -1;
            }
            for (i = 0; i < n; i++) {
                u->Level1.Buffer[i].NetName = shares[i].name;
                u->Level1.Buffer[i].Type    = shares[i].type;
                u->Level1.Buffer[i].Remark  = shares[i].remark;
            }
            u->Level1.EntriesRead = n;
            break;
        case 2:
            u->Level2.Buffer = dcerpc_alloc_data(pdu, n * sizeof(*u->Level2.Buffer));
            if (!u->Level2.Buffer) {
                return -1;
            }
            for (i = 0; i < n; i++) {
                u->Level2.Buffer[i].NetName = shares[i].name;
                u->Level2.Buffer[i].Type    = shares[i].type;
                u->Level2.Buffer[i].Remark  = shares[i].remark;
                u->Level2.Buffer[i].MaxUses = 0xffffffff;
                u->Level2.Buffer[i].Path    = "";
                u->Level2.Buffer[i].Passwd  = NULL;
            }
            u->Level2.EntriesRead = n;
            break;
        case 501:
            u->Level501.Buffer = dcerpc_alloc_data(pdu, n * sizeof(*u->Level501.Buffer));
            if (!u->Level501.Buffer) {
                return -1;
            }
            for (i = 0; i < n; i++) {
                u->Level501.Buffer[i].NetName = shares[i].name;
                u->Level501.Buffer[i].Type    = shares[i].type;
                u->Level501.Buffer[i].Remark  = shares[i].remark;
                u->Level501.Buffer[i].Flags   = shares[i].flags;
            }
            u->Level501.EntriesRead = n;
            break;
        case 502:
            u->Level502.Buffer = dcerpc_alloc_data(pdu, n * sizeof(*u->Level502.Buffer));
            if (!u->Level502.Buffer) {
                return -1;
            }
            for (i = 0; i < n; i++) {
                u->Level502.Buffer[i].NetName = shares[i].name;
                u->Level502.Buffer[i].Type    = shares[i].type;
                u->Level502.Buffer[i].Remark  = shares[i].remark;
                u->Level502.Buffer[i].MaxUses = 0xffffffff;
                u->Level502.Buffer[i].Path    = "";
            }
            u->Level502.EntriesRead = n;
            break;
    } /* switch */

    rep->TotalEntries = n;
    rep->status       = CHIMERA_SMB_RPC_WERR_OK;
    return 0;
} /* chimera_smb_srvsvc_share_enum */

/* NetrShareGetInfo (opnum 16). */
static int
chimera_smb_srvsvc_share_get_info(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct srvsvc_NetrShareGetInfo_req *req   = reqv;
    struct srvsvc_NetrShareGetInfo_rep *rep   = repv;
    struct chimera_smb_rpc_state       *state = chimera_smb_rpc_state_of_pdu(pdu);
    union srvsvc_SHARE_INFO            *u     = &rep->InfoStruct;
    struct srv_share                   *s;
    int                                 n;

    (void) dce;

    if (!chimera_smb_srvsvc_level_supported(req->Level) && req->Level != 1005) {
        rep->status = CHIMERA_SMB_RPC_WERR_UNKNOWN_LEVEL;
        return 0;
    }

    if (!req->NetName) {
        rep->status = CHIMERA_SMB_RPC_WERR_NET_NAME_NOT_FOUND;
        return 0;
    }

    n = chimera_smb_srvsvc_shares(pdu, state->host, req->NetName, &s);

    if (n < 0) {
        return -1;
    }

    if (n == 0) {
        rep->status = CHIMERA_SMB_RPC_WERR_NET_NAME_NOT_FOUND;
        return 0;
    }

    switch (req->Level) {
        case 0:
            u->ShareInfo0.NetName = s->name;
            break;
        case 1:
            u->ShareInfo1.NetName = s->name;
            u->ShareInfo1.Type    = s->type;
            u->ShareInfo1.Remark  = s->remark;
            break;
        case 2:
            u->ShareInfo2.NetName = s->name;
            u->ShareInfo2.Type    = s->type;
            u->ShareInfo2.Remark  = s->remark;
            u->ShareInfo2.MaxUses = 0xffffffff;
            u->ShareInfo2.Path    = "";
            break;
        case 501:
            u->ShareInfo501.NetName = s->name;
            u->ShareInfo501.Type    = s->type;
            u->ShareInfo501.Remark  = s->remark;
            u->ShareInfo501.Flags   = s->flags;
            break;
        case 502:
            u->ShareInfo502.NetName = s->name;
            u->ShareInfo502.Type    = s->type;
            u->ShareInfo502.Remark  = s->remark;
            u->ShareInfo502.MaxUses = 0xffffffff;
            u->ShareInfo502.Path    = "";
            break;
        case 1005:
            u->ShareInfo1005.Flags = s->flags;
            break;
    } /* switch */

    rep->status = CHIMERA_SMB_RPC_WERR_OK;
    return 0;
} /* chimera_smb_srvsvc_share_get_info */

/* NetrServerGetInfo (opnum 21). */
static int
chimera_smb_srvsvc_server_get_info(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct srvsvc_NetrServerGetInfo_req *req   = reqv;
    struct srvsvc_NetrServerGetInfo_rep *rep   = repv;
    struct chimera_smb_rpc_state        *state = chimera_smb_rpc_state_of_pdu(pdu);
    union srvsvc_SERVER_INFO            *u     = &rep->InfoStruct;
    char                                *name;

    (void) dce;

    name = chimera_smb_rpc_strdup(pdu, chimera_smb_rpc_host_identity(state->host));

    if (!name) {
        return -1;
    }

    switch (req->Level) {
        case 100:
            u->ServerInfo100.PlatformId = SRV_PLATFORM_ID_NT;
            u->ServerInfo100.Name       = name;
            break;
        case 101:
            u->ServerInfo101.PlatformId   = SRV_PLATFORM_ID_NT;
            u->ServerInfo101.Name         = name;
            u->ServerInfo101.VersionMajor = SRV_VERSION_MAJOR;
            u->ServerInfo101.VersionMinor = SRV_VERSION_MINOR;
            u->ServerInfo101.Type         = SRV_TYPE_DEFAULT;
            u->ServerInfo101.Comment      = "Chimera NAS";
            break;
        case 102:
            u->ServerInfo102.PlatformId   = SRV_PLATFORM_ID_NT;
            u->ServerInfo102.Name         = name;
            u->ServerInfo102.VersionMajor = SRV_VERSION_MAJOR;
            u->ServerInfo102.VersionMinor = SRV_VERSION_MINOR;
            u->ServerInfo102.Type         = SRV_TYPE_DEFAULT;
            u->ServerInfo102.Comment      = "Chimera NAS";
            u->ServerInfo102.Users        = 0xffffffff;
            u->ServerInfo102.Disc         = -1;
            u->ServerInfo102.Announce     = 240;
            u->ServerInfo102.AnnDelta     = 3000;
            u->ServerInfo102.UserPath     = "C:\\";
            break;
        default:
            rep->status = CHIMERA_SMB_RPC_WERR_UNKNOWN_LEVEL;
            return 0;
    } /* switch */

    rep->status = CHIMERA_SMB_RPC_WERR_OK;
    return 0;
} /* chimera_smb_srvsvc_server_get_info */

/* NetrShareCheck (opnum 20): is a local device shared?  Levels 2 and 502
 * report every share's Path as "" (the local path is not exposed), so that is
 * the one device name that matches. */
static int
chimera_smb_srvsvc_share_check(
    struct dcerpc_context *dce,
    struct dcerpc_pdu     *pdu,
    void                  *reqv,
    void                  *repv)
{
    struct srvsvc_NetrShareCheck_req *req = reqv;
    struct srvsvc_NetrShareCheck_rep *rep = repv;

    (void) dce;
    (void) pdu;

    if (req->Device && req->Device[0] == '\0') {
        rep->Type   = SRVSVC_STYPE_DISKTREE;
        rep->status = CHIMERA_SMB_RPC_WERR_OK;
    } else {
        rep->status = CHIMERA_SMB_RPC_NERR_DEVICE_NOT_SHARED;
    }

    return 0;
} /* chimera_smb_srvsvc_share_check */

static const struct dcerpc_stub chimera_smb_srvsvc_stubs[] = {
    {
        SRVSVC_NETRSHAREENUM, "NetrShareEnum", chimera_smb_srvsvc_share_enum
    },
    {
        SRVSVC_NETRSHAREGETINFO, "NetrShareGetInfo", chimera_smb_srvsvc_share_get_info
    },
    {
        SRVSVC_NETRSHARECHECK, "NetrShareCheck", chimera_smb_srvsvc_share_check
    },
    {
        SRVSVC_NETRSERVERGETINFO, "NetrServerGetInfo", chimera_smb_srvsvc_server_get_info
    },
    {
        -1, NULL, NULL
    },
};

struct dcerpc_server *
chimera_smb_srvsvc_endpoint(void)
{
    struct dcerpc_server *srv = dcerpc_server_create("srvsvc");

    if (srv && dcerpc_server_add_interface(srv, &srvsvc_interface, srvsvc_procs,
                                           chimera_smb_srvsvc_stubs,
                                           DCERPC_SERVER_ALL_NDR)) {
        dcerpc_server_destroy(srv);
        srv = NULL;
    }

    return srv;
} /* chimera_smb_srvsvc_endpoint */
