// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4.1 pNFS metadata-server operations, flex-files layout (RFC 8435).
 *
 * The MDS hands out whole-file flex-files layouts steering each file to a
 * single data server, which the client reaches over NFSv3 using the DS's
 * native file handle.  The opaque bodies of device_addr4 (GETDEVICEINFO ->
 * ff_device_addr4) and layout_content4 (LAYOUTGET -> ff_layout4) are
 * hand-encoded here; flex-files XDR is not in the generated nfs4.x.
 */

#include <string.h>
#include <sys/stat.h>

#include "nfs4_procs.h"
#include "nfs4_state.h"
#include "nfs4_status.h"
#include "nfs4_callback.h"
#include "nfs_internal.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

#define NFS4_PNFS_STRIPE_UNIT    1048576U /* 1 MiB                              */
#define LAYOUT4_FLEX_FILES       0x4     /* RFC 8435; not in the generated XDR */
#define LAYOUT4_BLOCK_VOLUME     0x3     /* RFC 5663; not in the generated XDR */
#define LAYOUT4_SCSI             0x5     /* RFC 8154; not in the generated XDR */

/* RFC 5663 §2.2.1 pnfs_block_volume_type4 */
#define PNFS_BLOCK_VOLUME_SIMPLE 0

/* RFC 8154 §2.4.1 pnfs_scsi_volume_type4 (SLICE=1, CONCAT=2, STRIPE=3, BASE=4) */
#define PNFS_SCSI_VOLUME_BASE    4

/* RFC 8154 §2.2 pnfs_scsi_code_set4 / pnfs_scsi_designator_type4 */
#define PS_CODE_SET_BINARY       1
#define PS_CODE_SET_ASCII        2
#define PS_DESIGNATOR_T10        1
#define PS_DESIGNATOR_EUI64      2
#define PS_DESIGNATOR_NAA        3

/* --- minimal XDR append helpers (network byte order) --------------------- */

static inline void
pnfs_put_u32(
    void   **p,
    uint32_t value)
{
    *(uint32_t *) *p = chimera_nfs_hton32(value);
    *p              += sizeof(uint32_t);
} /* pnfs_put_u32 */

static inline void
pnfs_put_u64(
    void   **p,
    uint64_t value)
{
    *(uint64_t *) *p = chimera_nfs_hton64(value);
    *p              += sizeof(uint64_t);
} /* pnfs_put_u64 */

/* XDR opaque<>/string<>: 4-byte length, bytes, then zero padding to 4. */
static inline void
pnfs_put_opaque(
    void      **p,
    const void *data,
    uint32_t    len)
{
    uint32_t pad = (4 - (len & 3)) & 3;

    pnfs_put_u32(p, len);
    memcpy(*p, data, len);
    if (pad) {
        memset(*p + len, 0, pad);
    }
    *p += len + pad;
} /* pnfs_put_opaque */

/*
 * Encode an ff_device_addr4 (RFC 8435 §5.1) into buf for the
 * device_addr4.da_addr_body opaque: the data server's network address plus
 * the NFS version(s) it speaks (NFSv3 here).  Returns bytes written.
 */
static uint32_t
chimera_nfs4_encode_ff_device_addr(
    uint8_t    *buf,
    const char *netid0,
    const char *uaddr0,
    const char *netid1,
    const char *uaddr1,
    uint32_t    version,
    uint32_t    minorversion)
{
    void    *p         = buf;
    uint32_t num_addrs = (netid1 && uaddr1 && uaddr1[0]) ? 2 : 1;

    /* ffda_netaddrs: multipath_list4 (a netaddr4<>).  A client picks any entry
     * it can reach; the preferred transport (e.g. rdma) is listed first. */
    pnfs_put_u32(&p, num_addrs);
    pnfs_put_opaque(&p, netid0, strlen(netid0));
    pnfs_put_opaque(&p, uaddr0, strlen(uaddr0));
    if (num_addrs == 2) {
        pnfs_put_opaque(&p, netid1, strlen(netid1));
        pnfs_put_opaque(&p, uaddr1, strlen(uaddr1));
    }

    /* ffda_versions<>: one entry advertising the configured NFS version
     * (NFSv3 or NFSv4.x), loosely coupled. */
    pnfs_put_u32(&p, 1);
    pnfs_put_u32(&p, version);                /* ffdv_version          */
    pnfs_put_u32(&p, minorversion);           /* ffdv_minorversion     */
    pnfs_put_u32(&p, NFS4_PNFS_STRIPE_UNIT);  /* ffdv_rsize            */
    pnfs_put_u32(&p, NFS4_PNFS_STRIPE_UNIT);  /* ffdv_wsize            */
    pnfs_put_u32(&p, 0);                      /* ffdv_tightly_coupled=false */

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_ff_device_addr */

/* Defined below; used by GETDEVICEINFO for backend-sourced devices. */
static uint32_t
chimera_nfs4_encode_block_device_addr(
    uint8_t                                *buf,
    const struct chimera_vfs_layout_device *dev);
static uint32_t
chimera_nfs4_encode_scsi_device_addr(
    uint8_t                                *buf,
    const struct chimera_vfs_layout_device *dev);
static int
nfs_pnfs_devcache_find(
    struct nfs_pnfs_devcache         *cache,
    const uint8_t                    *deviceid,
    struct chimera_vfs_layout_device *out);

void
chimera_nfs4_getdeviceinfo(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETDEVICEINFO4args       *args = &argop->opgetdeviceinfo;
    struct GETDEVICEINFO4res        *res  = &resop->opgetdeviceinfo;
    struct chimera_vfs              *vfs  = thread->shared->vfs;
    const struct chimera_vfs_ds     *ds;
    struct chimera_vfs_layout_device dev;
    uint8_t                          body[1024];
    uint32_t                         body_len = 0;
    uint32_t                         out_type = 0;
    int                              rc;

    if (!chimera_vfs_pnfs_feature_enabled(vfs)) {
        res->gdir_status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    if (args->gdia_layout_type == LAYOUT4_FLEX_FILES &&
        (ds = chimera_vfs_pnfs_find_device(vfs, args->gdia_device_id)) != NULL) {
        /* Orchestrated flex: device lives in the chimera DS table.  When an RDMA
         * uaddr is configured, advertise it first (preferred) with the primary
         * (tcp) netaddr as a fallback. */
        out_type = LAYOUT4_FLEX_FILES;
        if (ds->rdma_uaddr[0]) {
            body_len = chimera_nfs4_encode_ff_device_addr(body, "rdma", ds->rdma_uaddr,
                                                          ds->netid, ds->uaddr,
                                                          (uint32_t) ds->version,
                                                          (uint32_t) ds->minorversion);
        } else {
            body_len = chimera_nfs4_encode_ff_device_addr(body, ds->netid, ds->uaddr,
                                                          NULL, NULL,
                                                          (uint32_t) ds->version,
                                                          (uint32_t) ds->minorversion);
        }
    } else if (nfs_pnfs_devcache_find(&thread->shared->nfs4_pnfs_devcache,
                                      args->gdia_device_id, &dev)) {
        /* Backend-sourced: device came from a get_layout response. */
        switch (dev.layout_class) {
            case CHIMERA_VFS_LAYOUT_CLASS_BLOCK:
                out_type = LAYOUT4_BLOCK_VOLUME;
                break;
            case CHIMERA_VFS_LAYOUT_CLASS_SCSI:
                out_type = LAYOUT4_SCSI;
                break;
            default:
                out_type = LAYOUT4_FLEX_FILES;
                break;
        } /* switch */
        if (args->gdia_layout_type != out_type) {
            res->gdir_status = NFS4ERR_UNKNOWN_LAYOUTTYPE;
            chimera_nfs4_compound_complete(req, res->gdir_status);
            return;
        }
        switch (out_type) {
            case LAYOUT4_BLOCK_VOLUME:
                body_len = chimera_nfs4_encode_block_device_addr(body, &dev);
                break;
            case LAYOUT4_SCSI:
                body_len = chimera_nfs4_encode_scsi_device_addr(body, &dev);
                break;
            default:
                /* Backend-sourced flex device: not driven by the data_servers
                 * config, so advertise NFSv3 over a single netaddr as before. */
                body_len = chimera_nfs4_encode_ff_device_addr(body, dev.netid, dev.uaddr,
                                                              NULL, NULL, 3, 0);
                break;
        } /* switch */
    } else {
        res->gdir_status = (args->gdia_layout_type != LAYOUT4_FLEX_FILES &&
                            args->gdia_layout_type != LAYOUT4_BLOCK_VOLUME &&
                            args->gdia_layout_type != LAYOUT4_SCSI)
                           ? NFS4ERR_UNKNOWN_LAYOUTTYPE : NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    /* gdia_maxcount bounds the bytes the client will accept for the
     * da_addr_body (RFC 8881 §18.40.3); signal TOOSMALL with the required
     * size if it cannot fit. */
    if (args->gdia_maxcount < body_len) {
        res->gdir_status   = NFS4ERR_TOOSMALL;
        res->gdir_mincount = body_len;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    res->gdir_resok4.gdir_device_addr.da_layout_type = out_type;

    rc = xdr_dbuf_opaque_copy(&res->gdir_resok4.gdir_device_addr.da_addr_body,
                              body,
                              body_len,
                              req->encoding->dbuf);
    if (rc) {
        res->gdir_status = NFS4ERR_RESOURCE;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    /* No device-id change notifications supported. */
    res->gdir_resok4.num_gdir_notification = 0;
    res->gdir_resok4.gdir_notification     = NULL;

    res->gdir_status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getdeviceinfo */

/*
 * Encode an ff_layout4 (RFC 8435 §5.1) into buf for the
 * layout_content4.loc_body opaque: one mirror, one data server holding the
 * whole file, addressed by its native (NFSv3) file handle.  ffl_flags leaves
 * NO_LAYOUTCOMMIT clear so the client reports the new size back via
 * LAYOUTCOMMIT (how the MDS, which doesn't share the DS backend, learns it).
 */
static uint32_t
chimera_nfs4_encode_ff_layout(
    uint8_t       *buf,
    const uint8_t *deviceid,
    const uint8_t *ds_fh,
    uint32_t       ds_fh_len,
    uint32_t       iomode)
{
    void         *p                = buf;
    const uint8_t zero_stateid[16] = { 0 };

    /* ffds_user is the synthetic principal the client uses for DS I/O.  It
     * varies by iomode -- RW segments use the cluster-trusted "0" (matching how
     * the MDS owns the backing files), READ segments a distinct read principal
     * -- so a client can tell two segments of the same file apart (RFC 8435
     * §5.1).  ffds_group is constant across iomodes. */
    const char   *ffds_user = (iomode == LAYOUTIOMODE4_RW) ? "0" : "1";

    pnfs_put_u64(&p, NFS4_PNFS_STRIPE_UNIT);          /* ffl_stripe_unit       */

    pnfs_put_u32(&p, 1);                              /* ffl_mirrors<> count   */
    pnfs_put_u32(&p, 1);                              /* ffm_data_servers<>    */

    memcpy(p, deviceid, NFS4_DEVICEID4_SIZE);         /* ffds_deviceid         */
    p += NFS4_DEVICEID4_SIZE;
    pnfs_put_u32(&p, 0);                              /* ffds_efficiency       */
    memcpy(p, zero_stateid, sizeof(zero_stateid));    /* ffds_stateid (anon)   */
    p += sizeof(zero_stateid);
    pnfs_put_u32(&p, 1);                              /* ffds_fh_vers<> count  */
    pnfs_put_opaque(&p, ds_fh, ds_fh_len);            /* the DS's v3 handle    */
    pnfs_put_opaque(&p, ffds_user, 1);                /* ffds_user  (per-iomode)*/
    pnfs_put_opaque(&p, "0", 1);                      /* ffds_group (synthetic)*/

    pnfs_put_u32(&p, 0);                              /* ffl_flags             */
    pnfs_put_u32(&p, 0);                              /* ffl_stats_collect_hint*/

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_ff_layout */

/*
 * Encode a pnfs_block_layout4 (RFC 5663 §2.3.1) into buf for the
 * layout_content4.loc_body opaque: blo_extents<>, one pnfs_block_extent4 per
 * backend-supplied segment.  First-draft: one extent per segment, no
 * pnfs_block_extent4 merging.  Block XDR is not in the generated nfs4.x, so it
 * is hand-encoded with the same helpers as flex-files.
 */
static uint32_t
chimera_nfs4_encode_block_layout(
    uint8_t                                 *buf,
    const struct chimera_vfs_layout_segment *segs,
    uint32_t                                 nseg)
{
    void    *p = buf;
    uint32_t i;

    pnfs_put_u32(&p, nseg);                           /* blo_extents<> count   */

    for (i = 0; i < nseg; i++) {
        memcpy(p, segs[i].deviceid, NFS4_DEVICEID4_SIZE); /* bex_vol_id        */
        p += NFS4_DEVICEID4_SIZE;
        pnfs_put_u64(&p, segs[i].offset);             /* bex_file_offset       */
        pnfs_put_u64(&p, segs[i].length);             /* bex_length            */
        pnfs_put_u64(&p, segs[i].blk_vol_offset);     /* bex_storage_offset    */
        pnfs_put_u32(&p, segs[i].blk_state);          /* bex_state             */
    }

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_block_layout */

/*
 * Encode a pnfs_block_deviceaddr4 (RFC 5663 §2.2.1) into buf for the
 * device_addr4.da_addr_body opaque: bda_volumes<> describing the volume
 * topology.  First-draft: a single PNFS_BLOCK_VOLUME_SIMPLE volume with one
 * signature component {bs_offset, bs_sig} the client matches to a local disk.
 */
static uint32_t
chimera_nfs4_encode_block_device_addr(
    uint8_t                                *buf,
    const struct chimera_vfs_layout_device *dev)
{
    void *p = buf;

    pnfs_put_u32(&p, 1);                              /* bda_volumes<> count   */
    pnfs_put_u32(&p, PNFS_BLOCK_VOLUME_SIMPLE);       /* pbv_type              */
    /* pnfs_block_simple_volume_info4: bsv_ds<> (one signature component). */
    pnfs_put_u32(&p, 1);                              /* bsv_ds<> count        */
    pnfs_put_u64(&p, dev->blk_sig_offset);            /* bs_offset             */
    pnfs_put_opaque(&p, dev->blk_sig, dev->blk_sig_len); /* bs_sig             */
    pnfs_put_u64(&p, dev->blk_vol_size);              /* bsv_volume_size       */

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_block_device_addr */

/*
 * Encode a pnfs_scsi_layout4 (RFC 8154 §2.4.3) into buf for the
 * layout_content4.loc_body opaque: sl_extents<>, one pnfs_scsi_extent4 per
 * backend-supplied segment.  The SCSI extent is wire-identical to the block
 * extent (deviceid, file offset/length, storage offset, state) so this mirrors
 * chimera_nfs4_encode_block_layout exactly.
 */
static uint32_t
chimera_nfs4_encode_scsi_layout(
    uint8_t                                 *buf,
    const struct chimera_vfs_layout_segment *segs,
    uint32_t                                 nseg)
{
    void    *p = buf;
    uint32_t i;

    pnfs_put_u32(&p, nseg);                           /* sl_extents<> count    */

    for (i = 0; i < nseg; i++) {
        memcpy(p, segs[i].deviceid, NFS4_DEVICEID4_SIZE); /* se_vol_id         */
        p += NFS4_DEVICEID4_SIZE;
        pnfs_put_u64(&p, segs[i].offset);             /* se_file_offset        */
        pnfs_put_u64(&p, segs[i].length);             /* se_length             */
        pnfs_put_u64(&p, segs[i].blk_vol_offset);     /* se_storage_offset     */
        pnfs_put_u32(&p, segs[i].blk_state);          /* se_state              */
    }

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_scsi_layout */

/*
 * Encode a pnfs_scsi_deviceaddr4 (RFC 8154 §2.4.1) into buf for the
 * device_addr4.da_addr_body opaque: sda_volumes<> describing the volume
 * topology.  v1: a single PNFS_SCSI_VOLUME_BASE volume identified by its SCSI
 * VPD-0x83 designator (a hardware id; nothing is written to the disk) plus the
 * persistent-reservation key the client registers with.
 */
static uint32_t
chimera_nfs4_encode_scsi_device_addr(
    uint8_t                                *buf,
    const struct chimera_vfs_layout_device *dev)
{
    void *p = buf;

    pnfs_put_u32(&p, 1);                              /* sda_volumes<> count   */
    pnfs_put_u32(&p, PNFS_SCSI_VOLUME_BASE);          /* pnfs_scsi_volume4 type*/
    /* pnfs_scsi_base_volume_info4: designator (inlined) + reservation key. */
    pnfs_put_u32(&p, dev->scsi_code_set);             /* sbv_code_set          */
    pnfs_put_u32(&p, dev->scsi_desig_type);           /* sbv_designator_type   */
    pnfs_put_opaque(&p, dev->scsi_desig, dev->scsi_desig_len); /* sbv_designator<> */
    pnfs_put_u64(&p, dev->scsi_pr_key);               /* sbv_pr_key            */

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_scsi_device_addr */

/* --- sourced-layout device cache (deviceid -> descriptor) ---------------- */

static void
nfs_pnfs_devcache_put(
    struct nfs_pnfs_devcache               *cache,
    const struct chimera_vfs_layout_device *dev)
{
    uint32_t i;

    pthread_mutex_lock(&cache->lock);

    for (i = 0; i < cache->count; i++) {
        if (cache->entries[i].valid &&
            memcmp(cache->entries[i].deviceid, dev->deviceid,
                   CHIMERA_VFS_DEVICEID_SIZE) == 0) {
            cache->entries[i].device = *dev;       /* refresh */
            pthread_mutex_unlock(&cache->lock);
            return;
        }
    }

    if (cache->count < NFS_PNFS_DEVCACHE_MAX) {
        i = cache->count++;
        memcpy(cache->entries[i].deviceid, dev->deviceid, CHIMERA_VFS_DEVICEID_SIZE);
        cache->entries[i].device = *dev;
        cache->entries[i].valid  = 1;
    }

    pthread_mutex_unlock(&cache->lock);
} /* nfs_pnfs_devcache_put */

/* Returns 1 and fills *out on hit, 0 on miss. */
static int
nfs_pnfs_devcache_find(
    struct nfs_pnfs_devcache         *cache,
    const uint8_t                    *deviceid,
    struct chimera_vfs_layout_device *out)
{
    uint32_t i;
    int      found = 0;

    pthread_mutex_lock(&cache->lock);

    for (i = 0; i < cache->count; i++) {
        if (cache->entries[i].valid &&
            memcmp(cache->entries[i].deviceid, deviceid,
                   CHIMERA_VFS_DEVICEID_SIZE) == 0) {
            *out  = cache->entries[i].device;
            found = 1;
            break;
        }
    }

    pthread_mutex_unlock(&cache->lock);
    return found;
} /* nfs_pnfs_devcache_find */

/*
 * Opaque pNFS-layout blob format (stored on the MDS file via the
 * CHIMERA_VFS_ATTR_PNFS_LAYOUT attribute): [deviceid:16][fhlen:1][backing-fh].
 * The backing-fh is the nfs-module chimera handle of the file's data on the DS;
 * its native (NFSv3) handle for the layout is recovered by skipping the
 * 16-byte mount_id + 1-byte server-index prefix the nfs module prepends.
 */
#define FF_BLOB_FH_SKIP (CHIMERA_VFS_MOUNTID_SIZE + 1)

static uint32_t
ff_blob_pack(
    uint8_t       *blob,
    const uint8_t *deviceid,
    const uint8_t *backing_fh,
    uint32_t       backing_fh_len)
{
    memcpy(blob, deviceid, CHIMERA_VFS_DEVICEID_SIZE);
    blob[CHIMERA_VFS_DEVICEID_SIZE] = (uint8_t) backing_fh_len;
    memcpy(blob + CHIMERA_VFS_DEVICEID_SIZE + 1, backing_fh, backing_fh_len);
    return CHIMERA_VFS_DEVICEID_SIZE + 1 + backing_fh_len;
} /* ff_blob_pack */

/*
 * LAYOUTGET is an async state machine.  The per-file pNFS layout state lives in
 * an opaque attribute the backend just persists, so the NFS server owns all the
 * logic: open the file -> GETATTR(PNFS_LAYOUT); if the blob is present, emit the
 * layout; otherwise steer to a DS, open its backing root, create the backing
 * file, SETATTR the blob onto the file, then emit.
 */
struct ff_layoutget_ctx {
    struct nfs_request             *req;
    struct chimera_vfs_open_handle *mds_handle;
    struct chimera_vfs_open_handle *ds_root_handle;
    struct chimera_vfs_ds          *ds;
    uint64_t                        fileid;
    struct chimera_vfs_attrs        set_attr;
    uint8_t                         blob[CHIMERA_VFS_PNFS_LAYOUT_MAX];
    uint32_t                        blob_len;
    char                            backing_name[24];
};

static void
ff_lg_fail(
    struct ff_layoutget_ctx *ctx,
    nfsstat4                 status)
{
    struct nfs_request   *req = ctx->req;
    struct LAYOUTGET4res *res = &req->res_compound.resarray[req->index].oplayoutget;

    if (ctx->ds_root_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->ds_root_handle);
        ctx->ds_root_handle = NULL;
    }
    if (ctx->mds_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->mds_handle);
        ctx->mds_handle = NULL;
    }
    res->logr_status = status;
    chimera_nfs4_compound_complete(req, status);
} /* ff_lg_fail */

static void
ff_lg_emit(struct ff_layoutget_ctx *ctx)
{
    struct nfs_request      *req    = ctx->req;
    struct LAYOUTGET4args   *args   = &req->args_compound->argarray[req->index].oplayoutget;
    struct LAYOUTGET4res    *res    = &req->res_compound.resarray[req->index].oplayoutget;
    struct nfs_state_table  *table  = &req->thread->shared->nfs4_state_table;
    struct nfs_client       *client = req->session ? req->session->client_unified : NULL;
    struct nfs_layout_state *layout;
    const uint8_t           *deviceid, *backing_fh, *native_fh;
    uint32_t                 backing_fh_len, native_fh_len, client_short_id;
    uint8_t                 *body;
    uint32_t                 body_len;
    struct layout4          *lo;
    int                      rc;
    uint8_t                  ds_fhwire[CHIMERA_NFS_FH_MAX];
    int                      ds_fhwire_len;

    if (!client) {
        ff_lg_fail(ctx, NFS4ERR_LAYOUTUNAVAILABLE);
        return;
    }

    /* Unpack [deviceid][backing-fh-len][backing-fh]. */
    deviceid       = ctx->blob;
    backing_fh_len = ctx->blob[CHIMERA_VFS_DEVICEID_SIZE];
    backing_fh     = ctx->blob + CHIMERA_VFS_DEVICEID_SIZE + 1;

    /* Recover the handle the client will use against the data server.  A remote
     * DS is reached through the nfs proxy module, so the backing handle is the
     * proxy's [mount-id][...] wrapper around the DS's native handle -- skip the
     * wrapper.  A *local* DS (the MDS is also a data server, backed by a local
     * mount) is served by this same server over NFS, so the DS handle IS the
     * backing handle as stored -- skipping would corrupt it (see backing_local
     * in chimera_server_pnfs_resolve). */
    const struct chimera_vfs_ds *ds =
        chimera_vfs_pnfs_find_device(req->thread->shared->vfs, deviceid);

    if (ds && ds->backing_local) {
        /* Local DS (this server, local mount): backing_fh is a raw VFS handle,
        * so wrap it into the on-wire form the DS's NFS decode expects.  The DS
        * is this same server, so it shares the signing key and verifies it. */
        chimera_nfs_fh_wrap(ds_fhwire, &ds_fhwire_len, req->export_id,
                            backing_fh, backing_fh_len,
                            req->thread->shared->fh_key, req->thread->shared->fh_sign);
        native_fh     = ds_fhwire;
        native_fh_len = ds_fhwire_len;
    } else {
        /* Remote DS reached through the nfs proxy: the proxy already holds the
         * DS's on-wire handle (signed by the DS itself), so strip the proxy's
         * mount wrapper and pass it through unchanged.  Re-wrapping here would
         * double-wrap it and the DS would fail to decode.  (A split-DS cluster
         * must share one nfs_fh_key so the DS-minted handle verifies after the
         * round-trip through the client.) */
        native_fh     = backing_fh + FF_BLOB_FH_SKIP;
        native_fh_len = backing_fh_len - FF_BLOB_FH_SKIP;
    }

    client_short_id = (uint32_t) client->client_id;

    layout = nfs_layout_state_find(client, req->fh, req->fhlen);
    if (layout) {
        nfs_layout_state_bump(layout, client_short_id, &res->logr_resok4.logr_stateid);
    } else {
        nfs_layout_state_create(client, req->fh, req->fhlen, req->export_id, args->loga_iomode,
                                client_short_id, table,
                                &req->thread->shared->nfs4_layout_table,
                                &res->logr_resok4.logr_stateid);
    }

    /* Open the client's callback channel so a later conflicting op can recall
     * this layout; CB_LAYOUTRECALL rides the shared delegation channel. */
    nfs4_cb_ensure_probe(req->thread, client, req);

    body = xdr_dbuf_alloc_space(256, req->encoding->dbuf);
    chimera_nfs_abort_if(body == NULL, "Failed to allocate space");
    body_len = chimera_nfs4_encode_ff_layout(body, deviceid, native_fh, native_fh_len,
                                             args->loga_iomode);

    lo = xdr_dbuf_alloc_space(sizeof(*lo), req->encoding->dbuf);
    chimera_nfs_abort_if(lo == NULL, "Failed to allocate space");

    lo->lo_offset           = 0;
    lo->lo_length           = UINT64_MAX;
    lo->lo_iomode           = args->loga_iomode;
    lo->lo_content.loc_type = LAYOUT4_FLEX_FILES;

    rc = xdr_dbuf_opaque_copy(&lo->lo_content.loc_body, body, body_len, req->encoding->dbuf);
    chimera_nfs_abort_if(rc, "Failed to copy layout body");

    res->logr_resok4.logr_return_on_close = 0;
    res->logr_resok4.num_logr_layout      = 1;
    res->logr_resok4.logr_layout          = lo;

    if (ctx->mds_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->mds_handle);
        ctx->mds_handle = NULL;
    }

    res->logr_status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* ff_lg_emit */

static void
ff_lg_setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct ff_layoutget_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        ff_lg_fail(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }
    ff_lg_emit(ctx);
} /* ff_lg_setattr_cb */

static void
ff_lg_create_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct ff_layoutget_ctx *ctx = private_data;
    struct nfs_request      *req = ctx->req;

    if (ctx->ds_root_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->ds_root_handle);
        ctx->ds_root_handle = NULL;
    }

    if (error_code != CHIMERA_VFS_OK ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        ff_lg_fail(ctx, error_code != CHIMERA_VFS_OK
                   ? chimera_nfs4_errno_to_nfsstat4(error_code)
                   : NFS4ERR_LAYOUTTRYLATER);
        return;
    }

    ctx->blob_len = ff_blob_pack(ctx->blob, ctx->ds->deviceid,
                                 attr->va_fh, attr->va_fh_len);
    if (oh) {
        chimera_vfs_release(req->thread->vfs_thread, oh);
    }

    memset(&ctx->set_attr, 0, sizeof(ctx->set_attr));
    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_PNFS_LAYOUT;
    ctx->set_attr.va_pnfs_len = ctx->blob_len;
    memcpy(ctx->set_attr.va_pnfs, ctx->blob, ctx->blob_len);

    chimera_vfs_setattr(req->thread->vfs_thread, &req->cred, ctx->mds_handle,
                        &ctx->set_attr, 0, 0, ff_lg_setattr_cb, ctx);
} /* ff_lg_create_cb */

static void
ff_lg_dsroot_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct ff_layoutget_ctx *ctx = private_data;
    struct nfs_request      *req = ctx->req;

    if (error_code != CHIMERA_VFS_OK) {
        ff_lg_fail(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    ctx->ds_root_handle = handle;

    /* One backing file per MDS file, named by its fileid (flat on the DS). */
    snprintf(ctx->backing_name, sizeof(ctx->backing_name), "%016lx", ctx->fileid);

    /* Backing files are internal data containers; real access control is the
     * client's OPEN against the MDS metadata file.  flex-files steers DS I/O
     * with synthetic, per-iomode principals (ffds_user "0" for RW, "1" for
     * READ), so the backing object must be reachable by both.  A dedicated DS
     * in data_server mode serves them statelessly (bypassing the permission
     * check), but a co-located DS (the MDS is also the data server, local
     * backing) enforces it -- with mode 0600/owner-root the synthetic READ
     * principal is denied and the client spins re-fetching the layout.  Create
     * them world-rw so both topologies work. */
    memset(&ctx->set_attr, 0, sizeof(ctx->set_attr));
    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    ctx->set_attr.va_mode     = S_IFREG | 0666;

    chimera_vfs_open_at(req->thread->vfs_thread, &req->cred, ctx->ds_root_handle,
                        ctx->backing_name, strlen(ctx->backing_name),
                        CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED,
                        &ctx->set_attr, CHIMERA_VFS_ATTR_FH, 0, 0,
                        ff_lg_create_cb, ctx);
} /* ff_lg_dsroot_cb */

static void
ff_lg_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct ff_layoutget_ctx *ctx = private_data;
    struct nfs_request      *req = ctx->req;

    if (error_code != CHIMERA_VFS_OK) {
        ff_lg_fail(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) {
        ctx->blob_len = attr->va_pnfs_len;
        memcpy(ctx->blob, attr->va_pnfs, attr->va_pnfs_len);
        ff_lg_emit(ctx);
        return;
    }

    /* No layout yet -> steer to a data server and create the backing file. */
    ctx->fileid = (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM) ? attr->va_ino : 0;
    ctx->ds     = chimera_vfs_pnfs_steer(req->thread->shared->vfs);
    if (!ctx->ds) {
        ff_lg_fail(ctx, NFS4ERR_LAYOUTUNAVAILABLE);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        ctx->ds->root_fh, ctx->ds->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_INFERRED,
                        ff_lg_dsroot_cb, ctx);
} /* ff_lg_getattr_cb */

/*
 * Backend-SOURCED path: the backend produced the layout itself; chimera only
 * encodes the returned protocol-neutral segments/devices.  Flex segments emit
 * one layout4 each; block segments collapse into one block layout4 carrying all
 * extents.  No DS orchestration, no opaque-blob round trip.
 */
static void
lg_sourced_cb(
    enum chimera_vfs_error                   error_code,
    uint32_t                                 layout_class,
    uint32_t                                 num_segments,
    const struct chimera_vfs_layout_segment *segments,
    uint32_t                                 num_devices,
    const struct chimera_vfs_layout_device  *devices,
    void                                    *private_data)
{
    struct ff_layoutget_ctx *ctx    = private_data;
    struct nfs_request      *req    = ctx->req;
    struct LAYOUTGET4args   *args   = &req->args_compound->argarray[req->index].oplayoutget;
    struct LAYOUTGET4res    *res    = &req->res_compound.resarray[req->index].oplayoutget;
    struct nfs_state_table  *table  = &req->thread->shared->nfs4_state_table;
    struct nfs_client       *client = req->session ? req->session->client_unified : NULL;
    struct nfs_layout_state *layout;
    uint32_t                 client_short_id, i, loc_type;

    if (error_code != CHIMERA_VFS_OK) {
        ff_lg_fail(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }
    if (!client || num_segments == 0) {
        ff_lg_fail(ctx, NFS4ERR_LAYOUTUNAVAILABLE);
        return;
    }

    switch (layout_class) {
        case CHIMERA_VFS_LAYOUT_CLASS_BLOCK:
            loc_type = LAYOUT4_BLOCK_VOLUME;
            break;
        case CHIMERA_VFS_LAYOUT_CLASS_SCSI:
            loc_type = LAYOUT4_SCSI;
            break;
        default:
            loc_type = LAYOUT4_FLEX_FILES;
            break;
    } /* switch */
    if (loc_type != args->loga_layout_type) {
        ff_lg_fail(ctx, NFS4ERR_UNKNOWN_LAYOUTTYPE);
        return;
    }

    /* Cache the device descriptors so GETDEVICEINFO (which has only a deviceid)
     * can answer for this sourcing backend. */
    for (i = 0; i < num_devices; i++) {
        nfs_pnfs_devcache_put(&req->thread->shared->nfs4_pnfs_devcache, &devices[i]);
    }

    client_short_id = (uint32_t) client->client_id;
    layout          = nfs_layout_state_find(client, req->fh, req->fhlen);
    if (layout) {
        nfs_layout_state_bump(layout, client_short_id, &res->logr_resok4.logr_stateid);
    } else {
        nfs_layout_state_create(client, req->fh, req->fhlen, req->export_id, args->loga_iomode,
                                client_short_id, table,
                                &req->thread->shared->nfs4_layout_table,
                                &res->logr_resok4.logr_stateid);
    }

    /* Open the client's callback channel so a later conflicting op can recall
     * this layout; CB_LAYOUTRECALL rides the shared delegation channel. */
    nfs4_cb_ensure_probe(req->thread, client, req);

    if (loc_type == LAYOUT4_BLOCK_VOLUME || loc_type == LAYOUT4_SCSI) {
        uint8_t        *body = xdr_dbuf_alloc_space(1024, req->encoding->dbuf);
        struct layout4 *lo   = xdr_dbuf_alloc_space(sizeof(*lo), req->encoding->dbuf);
        uint32_t        body_len;
        int             rc;

        chimera_nfs_abort_if(body == NULL || lo == NULL, "Failed to allocate space");

        /* The SCSI extent (RFC 8154) is wire-identical to the block extent
        * (RFC 5663); only the layout type and device descriptor differ. */
        body_len = (loc_type == LAYOUT4_SCSI)
                   ? chimera_nfs4_encode_scsi_layout(body, segments, num_segments)
                   : chimera_nfs4_encode_block_layout(body, segments, num_segments);

        /* Cover exactly the contiguous span the extents describe (from the
         * requested offset to the end of the last segment); a block/SCSI client
         * rejects a layout whose lo_length doesn't match its extents. */
        uint64_t blk_end = segments[num_segments - 1].offset +
            segments[num_segments - 1].length;

        lo->lo_offset = args->loga_offset;
        lo->lo_length = blk_end > args->loga_offset ?
            blk_end - args->loga_offset : 0;
        lo->lo_iomode           = args->loga_iomode;
        lo->lo_content.loc_type = loc_type;
        rc                      = xdr_dbuf_opaque_copy(&lo->lo_content.loc_body, body,
                                                       body_len, req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to copy layout body");

        res->logr_resok4.num_logr_layout = 1;
        res->logr_resok4.logr_layout     = lo;
    } else {
        struct layout4 *los = xdr_dbuf_alloc_space(num_segments * sizeof(*los),
                                                   req->encoding->dbuf);

        chimera_nfs_abort_if(los == NULL, "Failed to allocate space");

        for (i = 0; i < num_segments; i++) {
            uint8_t *body = xdr_dbuf_alloc_space(256, req->encoding->dbuf);
            uint32_t body_len;
            int      rc;

            chimera_nfs_abort_if(body == NULL, "Failed to allocate space");
            body_len = chimera_nfs4_encode_ff_layout(body, segments[i].deviceid,
                                                     segments[i].ds_fh,
                                                     segments[i].ds_fh_len,
                                                     segments[i].iomode);
            los[i].lo_offset           = segments[i].offset;
            los[i].lo_length           = segments[i].length;
            los[i].lo_iomode           = segments[i].iomode;
            los[i].lo_content.loc_type = LAYOUT4_FLEX_FILES;
            rc                         = xdr_dbuf_opaque_copy(&los[i].lo_content.loc_body, body,
                                                              body_len, req->encoding->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy layout body");
        }

        res->logr_resok4.num_logr_layout = num_segments;
        res->logr_resok4.logr_layout     = los;
    }

    res->logr_resok4.logr_return_on_close = 0;

    if (ctx->mds_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->mds_handle);
        ctx->mds_handle = NULL;
    }

    res->logr_status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* lg_sourced_cb */

static void
ff_lg_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct ff_layoutget_ctx *ctx  = private_data;
    struct nfs_request      *req  = ctx->req;
    struct LAYOUTGET4args   *args = &req->args_compound->argarray[req->index].oplayoutget;
    uint64_t                 caps;

    if (error_code != CHIMERA_VFS_OK) {
        ff_lg_fail(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    ctx->mds_handle = handle;
    caps            = handle->vfs_module->capabilities;

    if (caps & CHIMERA_VFS_CAP_LAYOUT_SOURCE) {
        /* Backend produces the layout.  Its class is fixed by its caps; the
         * client's requested type must match it. */
        uint32_t want_class, ok_type;

        if (caps & CHIMERA_VFS_CAP_LAYOUT_CLASS_SCSI) {
            want_class = CHIMERA_VFS_LAYOUT_CLASS_SCSI;
            ok_type    = LAYOUT4_SCSI;
        } else if (caps & CHIMERA_VFS_CAP_LAYOUT_CLASS_BLOCK) {
            want_class = CHIMERA_VFS_LAYOUT_CLASS_BLOCK;
            ok_type    = LAYOUT4_BLOCK_VOLUME;
        } else {
            want_class = CHIMERA_VFS_LAYOUT_CLASS_FLEX;
            ok_type    = LAYOUT4_FLEX_FILES;
        }

        if (args->loga_layout_type != ok_type) {
            ff_lg_fail(ctx, NFS4ERR_UNKNOWN_LAYOUTTYPE);
            return;
        }

        chimera_vfs_get_layout(req->thread->vfs_thread, &req->cred, handle,
                               args->loga_offset, args->loga_length, args->loga_iomode,
                               want_class, CHIMERA_VFS_LAYOUT_MAX_SEGMENTS,
                               lg_sourced_cb, ctx);
        return;
    }

    if (caps & CHIMERA_VFS_CAP_LAYOUT) {
        /* Orchestrated path: chimera produces a flex-files layout only. */
        if (args->loga_layout_type != LAYOUT4_FLEX_FILES) {
            ff_lg_fail(ctx, NFS4ERR_UNKNOWN_LAYOUTTYPE);
            return;
        }
        chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, handle,
                            CHIMERA_VFS_ATTR_PNFS_LAYOUT | CHIMERA_VFS_ATTR_INUM,
                            ff_lg_getattr_cb, ctx);
        return;
    }

    ff_lg_fail(ctx, NFS4ERR_LAYOUTUNAVAILABLE);
} /* ff_lg_open_cb */

void
chimera_nfs4_layoutget(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LAYOUTGET4args   *args = &argop->oplayoutget;
    struct LAYOUTGET4res    *res  = &resop->oplayoutget;
    struct ff_layoutget_ctx *ctx;

    req->handle = NULL;

    if (!chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
        res->logr_status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->logr_status);
        return;
    }

    /* Accept the two layout types chimera can encode; the actual type allowed
     * for this file depends on its backend (validated in ff_lg_open_cb once the
     * backend's capabilities are known). */
    if (args->loga_layout_type != LAYOUT4_FLEX_FILES &&
        args->loga_layout_type != LAYOUT4_BLOCK_VOLUME &&
        args->loga_layout_type != LAYOUT4_SCSI) {
        res->logr_status = NFS4ERR_UNKNOWN_LAYOUTTYPE;
        chimera_nfs4_compound_complete(req, res->logr_status);
        return;
    }

    if (req->fhlen == 0) {
        res->logr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->logr_status);
        return;
    }

    ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
    memset(ctx, 0, sizeof(*ctx));
    ctx->req = req;

    /* Open the current FH, then drive the GETATTR/create/SETATTR chain. */
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, req->fh, req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED, ff_lg_open_cb, ctx);
} /* chimera_nfs4_layoutget */

void
chimera_nfs4_layoutreturn(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LAYOUTRETURN4args *args = &argop->oplayoutreturn;
    struct LAYOUTRETURN4res  *res  = &resop->oplayoutreturn;
    struct nfs_client        *client;

    if (!chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
        res->lorr_status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->lorr_status);
        return;
    }

    if (args->lora_layout_type != LAYOUT4_FLEX_FILES &&
        args->lora_layout_type != LAYOUT4_BLOCK_VOLUME &&
        args->lora_layout_type != LAYOUT4_SCSI) {
        res->lorr_status = NFS4ERR_UNKNOWN_LAYOUTTYPE;
        chimera_nfs4_compound_complete(req, res->lorr_status);
        return;
    }

    client = req->session ? req->session->client_unified : NULL;

    /* v1 tracks a single whole-file layout per file, so a FILE return drops
     * the record entirely.  FSID/ALL returns are accepted as no-ops. */
    if (client &&
        args->lora_layoutreturn.lr_returntype == LAYOUTRETURN4_FILE) {
        struct nfs_layout_state *layout =
            nfs_layout_state_find(client, req->fh, req->fhlen);

        if (layout) {
            uint32_t in_seqid =
                args->lora_layoutreturn.lr_layout.lrf_stateid.seqid;

            /* RFC 8881 §12.5.3: the layout stateid carried by LAYOUTRETURN must
             * match the server's current seqid for this layout.  A stale seqid
             * is OLD_STATEID, a future one BAD_STATEID; seqid 0 is the wildcard
             * "current" and always matches.  (LAYOUTGET, by contrast, tolerates
             * an old seqid -- the client may race two LAYOUTGETs.) */
            if (in_seqid != 0 && in_seqid < layout->seqid) {
                res->lorr_status = NFS4ERR_OLD_STATEID;
                chimera_nfs4_compound_complete(req, res->lorr_status);
                return;
            }
            if (in_seqid > layout->seqid) {
                res->lorr_status = NFS4ERR_BAD_STATEID;
                chimera_nfs4_compound_complete(req, res->lorr_status);
                return;
            }

            /* Destroying the layout deregisters it from the server-wide table;
             * if it was the last holder of this file, that resumes any
             * operation deferred while its recall was outstanding (stage two). */
            nfs_layout_state_destroy(layout,
                                     &thread->shared->nfs4_state_table,
                                     thread->vfs_thread);
        }
    }

    /* The whole layout is gone, so no layout stateid is returned
     * (RFC 8881 §18.44.3). */
    res->lorr_stateid.lrs_present = 0;
    res->lorr_status              = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_layoutreturn */

void
chimera_nfs4_getdevicelist(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETDEVICELIST4res *res = &resop->opgetdevicelist;

    (void) thread;
    (void) argop;

    /* GETDEVICELIST is deprecated (RFC 8434); clients discover devices via
     * the deviceid returned in LAYOUTGET + GETDEVICEINFO. */
    res->gdlr_status = NFS4ERR_NOTSUPP;
    chimera_nfs4_compound_complete(req, res->gdlr_status);
} /* chimera_nfs4_getdevicelist */

void
chimera_nfs4_layoutstats(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LAYOUTSTATS4res *res = &resop->oplayoutstats;

    (void) argop;

    /* LAYOUTSTATS (RFC 7862 §15.x, used by flex-files RFC 8435 §6) lets the
     * client report per-layout I/O statistics to the MDS.  Chimera does not yet
     * consume them, so accept and acknowledge.  ffl_stats_collect_hint in the
     * layout we hand out is 0, but clients may report regardless. */
    if (!chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
        res->lsr_status = NFS4ERR_NOTSUPP;
    } else {
        res->lsr_status = NFS4_OK;
    }
    chimera_nfs4_compound_complete(req, res->lsr_status);
} /* chimera_nfs4_layoutstats */

void
chimera_nfs4_layouterror(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LAYOUTERROR4res *res = &resop->oplayouterror;

    (void) argop;

    /* LAYOUTERROR (RFC 7862 §15.x, used by flex-files RFC 8435 §6) lets the
     * client report a data-server I/O error to the MDS so it can, e.g., pick a
     * different mirror.  Chimera hands out a single-mirror layout and does not
     * yet act on the report, so accept and acknowledge rather than returning
     * NFS4ERR_NOTSUPP (which makes the client re-report). */
    if (!chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
        res->ler_status = NFS4ERR_NOTSUPP;
    } else {
        res->ler_status = NFS4_OK;
    }
    chimera_nfs4_compound_complete(req, res->ler_status);
} /* chimera_nfs4_layouterror */

static void
chimera_nfs4_layoutcommit_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request      *req = private_data;
    struct LAYOUTCOMMIT4res *res = &req->res_compound.resarray[req->index].oplayoutcommit;

    (void) pre_attr;
    (void) set_attr;

    if (req->handle) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    if (error_code != CHIMERA_VFS_OK) {
        res->locr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->locr_status);
        return;
    }

    res->locr_resok4.locr_newsize.ns_sizechanged = 1;
    res->locr_resok4.locr_newsize.ns_size        =
        (post_attr && (post_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE))
        ? post_attr->va_size : 0;

    res->locr_status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_layoutcommit_setattr_complete */

static void
chimera_nfs4_layoutcommit_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request       *req  = private_data;
    struct LAYOUTCOMMIT4args *args = &req->args_compound->argarray[req->index].oplayoutcommit;
    struct LAYOUTCOMMIT4res  *res  = &req->res_compound.resarray[req->index].oplayoutcommit;
    struct chimera_vfs_attrs *set_attr;

    if (error_code != CHIMERA_VFS_OK) {
        res->locr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->locr_status);
        return;
    }

    req->handle = handle;

    /* No new high-water byte reported: nothing to sync to the MDS. */
    if (!args->loca_last_write_offset.no_newoffset) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle                                  = NULL;
        res->locr_resok4.locr_newsize.ns_sizechanged = 0;
        res->locr_status                             = NFS4_OK;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* Compound-lifetime storage so it outlives this async setattr. */
    set_attr = xdr_dbuf_alloc_space(sizeof(*set_attr), req->encoding->dbuf);
    chimera_nfs_abort_if(set_attr == NULL, "Failed to allocate space");

    set_attr->va_req_mask = 0;
    set_attr->va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    set_attr->va_size     = args->loca_last_write_offset.no_offset + 1;

    if (args->loca_time_modify.nt_timechanged) {
        set_attr->va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
        set_attr->va_mtime.tv_sec  = args->loca_time_modify.nt_time.seconds;
        set_attr->va_mtime.tv_nsec = args->loca_time_modify.nt_time.nseconds;
    }

    chimera_vfs_setattr(req->thread->vfs_thread, &req->cred, handle,
                        set_attr, 0, CHIMERA_VFS_ATTR_SIZE,
                        chimera_nfs4_layoutcommit_setattr_complete, req);
} /* chimera_nfs4_layoutcommit_open_callback */

void
chimera_nfs4_layoutcommit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LAYOUTCOMMIT4res *res = &resop->oplayoutcommit;

    req->handle = NULL;

    if (!chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
        res->locr_status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->locr_status);
        return;
    }

    if (req->fhlen == 0) {
        res->locr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->locr_status);
        return;
    }

    /* Open the MDS file to apply the client-reported size/mtime.  Data lives
     * on the DS; with COMMIT_THRU_MDS the client reports the new high-water
     * mark here so MDS metadata catches up. */
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh, req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_layoutcommit_open_callback, req);
} /* chimera_nfs4_layoutcommit */
