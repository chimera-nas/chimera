// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_srvsvc.h"
#include "smb_dcerpc.h"
#include "srvsvc_ndr.h"

/* SRVSVC interface 4b324fc8-1670-01d3-1278-5a47bf6ee188 v3.0 (GUID wire form:
 * first three fields little-endian, last two big-endian). */
static const dce_if_uuid_t SRV_INTERFACE = {
    .if_uuid       = { 0xc8, 0x4f, 0x32, 0x4b, 0x70, 0x16, 0xd3, 0x01,
                       0x12, 0x78, 0x5a, 0x47, 0xbf, 0x6e, 0xe1, 0x88 },
    .if_vers_major = 3,
    .if_vers_minor = 0,
};

#define SRV_OP_NETSHAREENUMALL     15
#define SRV_OP_NETSHAREGETINFO     16
#define SRV_OP_NETSRVGETINFO       21

#define SRV_STYPE_DISKTREE         0x00000000
#define SRV_STYPE_IPC_HIDDEN       0x80000003

/* MS-DTYP / lmerr.h: returned in the WERROR status when an operation cannot be
 * satisfied. */
#define SRV_WERR_OK                0x00000000
#define SRV_WERR_UNKNOWN_LEVEL     0x0000007C
#define SRV_WERR_NETNAME_NOT_FOUND 0x00000906

/* MS-SRVS SV_PLATFORM_ID_NT and a plausible NT-class server type bitmask
 * (workstation + server + Unix). rpcclient just echoes these fields. */
#define SRV_PLATFORM_ID_NT         500
#define SRV_TYPE_DEFAULT           0x00001003

#define SRV_RPC_STUB_MAX           (65535 - (int) (sizeof(dce_common_t) + sizeof(dce_co_response_t)))

static char *
chimera_smb_srvsvc_dbuf_strdup(
    struct ndr_dbuf *dbuf,
    const char      *s)
{
    size_t n = strlen(s) + 1;
    char  *p = ndr_dbuf_alloc(dbuf, n);

    memcpy(p, s, n);
    return p;
} /* chimera_smb_srvsvc_dbuf_strdup */

static void
chimera_smb_srvsvc_impl(
    int                               opnum,
    const void                       *in,
    void                             *out,
    struct ndr_dbuf                  *dbuf,
    struct chimera_server_smb_shared *shared)
{
    (void) in;

    switch (opnum) {
        case SRV_OP_NETSHAREENUMALL: {
            struct srvsvc_NetShareEnumAll_out *o   = out;
            struct srvsvc_NetShareCtr1        *ctr = ndr_dbuf_alloc(dbuf, sizeof(*ctr));
            struct srvsvc_NetShareInfo1       *arr;
            struct chimera_smb_share          *cur;
            uint32_t                           count = 0, i = 0;

            /* Snapshot the share names under the lock into dbuf storage (which
            * outlives the lock and the marshalling), plus a synthetic IPC$. */
            pthread_mutex_lock(&shared->shares_lock);
            LL_FOREACH(shared->shares, cur)
            {
                count++;
            }
            arr = ndr_dbuf_alloc(dbuf, (count + 1) * sizeof(*arr));
            LL_FOREACH(shared->shares, cur)
            {
                arr[i].name    = chimera_smb_srvsvc_dbuf_strdup(dbuf, cur->name);
                arr[i].type    = SRV_STYPE_DISKTREE;
                arr[i].comment = "";
                i++;
            }
            pthread_mutex_unlock(&shared->shares_lock);

            arr[i].name    = chimera_smb_srvsvc_dbuf_strdup(dbuf, "IPC$");
            arr[i].type    = SRV_STYPE_IPC_HIDDEN;
            arr[i].comment = "Remote IPC";
            i++;

            ctr->count        = i;
            ctr->array        = arr;
            o->info_ctr.level = 1;
            o->info_ctr.ctr   = 1;   /* union discriminant = level 1 */
            o->info_ctr.ctr1  = ctr;
            o->total_entries  = i;
            o->resume_handle  = 0;
            o->status         = 0;   /* WERR_OK */
            break;
        }
        case SRV_OP_NETSHAREGETINFO: {
            const struct srvsvc_NetShareGetInfo_in *in_g = in;
            struct srvsvc_NetShareGetInfo_out      *o    = out;
            struct srvsvc_NetShareInfo1            *info;
            struct chimera_smb_share               *cur;
            const char                             *netname = in_g->netname;
            int                                     found   = 0;

            /* Echo the requested level as the union switch value regardless of
             * outcome so the client decodes the arm it expects. */
            o->info.level = in_g->level;
            o->info.info1 = NULL;

            if (in_g->level != 1) {
                o->status = SRV_WERR_UNKNOWN_LEVEL;
                break;
            }

            info = ndr_dbuf_alloc(dbuf, sizeof(*info));

            if (netname && strcasecmp(netname, "IPC$") == 0) {
                info->name    = chimera_smb_srvsvc_dbuf_strdup(dbuf, "IPC$");
                info->type    = SRV_STYPE_IPC_HIDDEN;
                info->comment = "Remote IPC";
                found         = 1;
            } else if (netname) {
                pthread_mutex_lock(&shared->shares_lock);
                LL_FOREACH(shared->shares, cur)
                {
                    if (strcasecmp(cur->name, netname) == 0) {
                        info->name    = chimera_smb_srvsvc_dbuf_strdup(dbuf, cur->name);
                        info->type    = SRV_STYPE_DISKTREE;
                        info->comment = "";
                        found         = 1;
                        break;
                    }
                }
                pthread_mutex_unlock(&shared->shares_lock);
            }

            if (found) {
                o->info.info1 = info;
                o->status     = SRV_WERR_OK;
            } else {
                o->status = SRV_WERR_NETNAME_NOT_FOUND;
            }
            break;
        }
        case SRV_OP_NETSRVGETINFO: {
            const struct srvsvc_NetSrvGetInfo_in *in_s = in;
            struct srvsvc_NetSrvGetInfo_out      *o    = out;
            struct srvsvc_NetSrvInfo101          *info;

            o->info.level   = in_s->level;
            o->info.info101 = NULL;

            if (in_s->level != 101) {
                o->status = SRV_WERR_UNKNOWN_LEVEL;
                break;
            }

            info = ndr_dbuf_alloc(dbuf, sizeof(*info));

            info->platform_id   = SRV_PLATFORM_ID_NT;
            info->name          = chimera_smb_srvsvc_dbuf_strdup(dbuf, shared->config.identity);
            info->version_major = 6;
            info->version_minor = 1;
            info->type          = SRV_TYPE_DEFAULT;
            info->comment       = "Chimera NAS";

            o->info.info101 = info;
            o->status       = SRV_WERR_OK;
            break;
        }
        default:
            break;
    } /* switch */
} /* chimera_smb_srvsvc_impl */

static int
chimera_smb_srvsvc_handler(
    int                       opnum,
    struct evpl_iovec_cursor *cursor,
    void                     *output,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    const struct ndr_op_desc   *op;
    struct ndr_cursor           c;
    struct ndr_writer           w;
    struct ndr_dbuf             dbuf;
    void                       *in, *out;
    int                         n;

    op = ndr_find_op(srvsvc_op_table, srvsvc_op_count, opnum);
    if (!op) {
        return -1;
    }

    ndr_cursor_init(&c, evpl_iovec_cursor_data(cursor),
                    cursor->iov->length - cursor->offset);
    /* The arena grows by linking new blocks, so the held in/out pointers stay
     * valid across later allocations; this is only a starting capacity hint. */
    ndr_dbuf_init(&dbuf, 4096);

    in  = ndr_dbuf_alloc(&dbuf, op->in_size);
    out = ndr_dbuf_alloc(&dbuf, op->out_size);

    op->pull_in(&c, in, &dbuf);
    chimera_smb_srvsvc_impl(op->opnum, in, out, &dbuf,
                            request->compound->thread->shared);

    ndr_writer_init(&w, output, SRV_RPC_STUB_MAX);
    n = op->push_out(&w, out);

    ndr_dbuf_destroy(&dbuf);
    return n;
} /* chimera_smb_srvsvc_handler */

int
chimera_smb_srvsvc_transceive(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov)
{
    int status;

    status = dce_rpc(&SRV_INTERFACE, input_iov, input_niov, output_iov,
                     chimera_smb_srvsvc_handler, request);

    if (status != 0) {
        chimera_smb_error("SRVSVC RPC transceive failed");
        return status;
    }

    return 0;
} /* chimera_smb_srvsvc_transceive */
