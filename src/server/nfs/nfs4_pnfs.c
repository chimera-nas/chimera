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

#include "common/thread.h"
#include <inttypes.h>
#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "nfs4_procs.h"
#include "nfs4_protocol.h"
#include "vfs/vfs_compound.h"
#include "nfs4_state.h"
#include "nfs4_pnfs_compound.h"
#include "nfs4_status.h"
#include "nfs4_callback.h"
#include "nfs_internal.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_release.h"

#define NFS4_PNFS_STRIPE_UNIT    1048576U /* 1 MiB                              */

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
    *p               = (char *) *p + sizeof(uint32_t);
} /* pnfs_put_u32 */

static inline void
pnfs_put_u64(
    void   **p,
    uint64_t value)
{
    *(uint64_t *) *p = chimera_nfs_hton64(value);
    *p               = (char *) *p + sizeof(uint64_t);
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
        memset((char *) *p + len, 0, pad);
    }
    *p = (char *) *p + len + pad;
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
    /* RFC 8435 §4.1: if ffdv_version == 3 the server MUST set ffdv_minorversion
     * to 0 (and ffdv_tightly_coupled to false, hardcoded below). */
    pnfs_put_u32(&p, version == 3 ? 0 : minorversion); /* ffdv_minorversion */
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

nfsstat4
chimera_nfs4_getdeviceinfo_fill(
    struct chimera_server_nfs_thread       *thread,
    struct nfs_request                     *req,
    const struct GETDEVICEINFO4args        *args,
    struct GETDEVICEINFO4res               *res,
    const struct chimera_vfs_layout_device *private_device)
{
    struct chimera_vfs              *vfs = thread->shared->vfs;
    const struct chimera_vfs_ds     *ds;
    struct chimera_vfs_layout_device dev;
    uint8_t                          body[1024];
    uint32_t                         body_len = 0;
    uint32_t                         out_type = 0;
    int                              rc;

    if (!chimera_vfs_pnfs_feature_enabled(vfs)) {
        res->gdir_status = NFS4ERR_NOTSUPP;
        return res->gdir_status;
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
    } else if (private_device || nfs_pnfs_devcache_find(&thread->shared->nfs4_pnfs_devcache,
                                                        args->gdia_device_id, &dev)) {
        if (private_device) {
            dev = *private_device;
        }
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
            return res->gdir_status;
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
        /* Known layout type but unrecognized device id => NFS4ERR_NOENT so the
         * client can distinguish a stale deviceid from a malformed request
         * (RFC 8881 §18.40.3); an unsupported layout type => UNKNOWN_LAYOUTTYPE. */
        res->gdir_status = (args->gdia_layout_type != LAYOUT4_FLEX_FILES &&
                            args->gdia_layout_type != LAYOUT4_BLOCK_VOLUME &&
                            args->gdia_layout_type != LAYOUT4_SCSI)
                           ? NFS4ERR_UNKNOWN_LAYOUTTYPE : NFS4ERR_NOENT;
        return res->gdir_status;
    }

    /* gdia_maxcount bounds the bytes the client will accept for the da_addr_body
     * (RFC 8881 §18.40.3).  A maxcount of 0 is the defined way for a client to
     * validate a device id or (de)register notifications: TOOSMALL MUST NOT be
     * returned and the reply's da_addr_body is zero length.  Otherwise signal
     * TOOSMALL with the required size when the body cannot fit. */
    /* gdia_maxcount bounds the entire GETDEVICEINFO4resok, including XDR
     * overhead, not just the da_addr_body (RFC 8881 §18.40.3).  Beyond the
     * body the resok carries da_layout_type (4), the da_addr_body opaque
     * length prefix (4, with the body padded to a 4-byte boundary) and the
     * gdir_notification bitmap count (4).  Compare maxcount against -- and
     * report gdir_mincount as -- that full size, so a client that retries at
     * exactly gdir_mincount does not TOOSMALL again. */
    uint32_t resok_len = 4 + 4 + ((body_len + 3) & ~3u) + 4;

    if (args->gdia_maxcount == 0) {
        body_len = 0;
    } else if (args->gdia_maxcount < resok_len) {
        res->gdir_status   = NFS4ERR_TOOSMALL;
        res->gdir_mincount = resok_len;
        return res->gdir_status;
    }

    res->gdir_resok4.gdir_device_addr.da_layout_type = out_type;

    rc = xdr_dbuf_opaque_copy(&res->gdir_resok4.gdir_device_addr.da_addr_body,
                              body,
                              body_len,
                              req->encoding->dbuf);
    if (rc) {
        res->gdir_status = NFS4ERR_RESOURCE;
        return res->gdir_status;
    }

    /* No device-id change notifications supported. */
    res->gdir_resok4.num_gdir_notification = 0;
    res->gdir_resok4.gdir_notification     = NULL;

    res->gdir_status = NFS4_OK;
    return NFS4_OK;
} /* chimera_nfs4_getdeviceinfo_fill */

void
chimera_nfs4_getdeviceinfo(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    nfsstat4 status = chimera_nfs4_getdeviceinfo_fill(thread, req, &argop->opgetdeviceinfo,
                                                      &resop->opgetdeviceinfo, NULL);

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_getdeviceinfo */

/*
 * Encode an ff_layout4 (RFC 8435 §5.1) into buf for the
 * layout_content4.loc_body opaque: one mirror, one data server holding the
 * whole file, addressed by its native (NFSv3) file handle.  ffl_flags leaves
 * NO_LAYOUTCOMMIT clear so the client reports the new size back via
 * LAYOUTCOMMIT. The orchestrated path addresses the same authoritative
 * object through the MDS proxy and directly through the DS.
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

    /* One mirror holding the whole segment on one data server is a stripe
     * count of one, and RFC 8435 §5.1 requires ffl_stripe_unit to default to
     * zero in that case; a non-zero unit would let a strict client compute a
     * data-server index the layout does not have. */
    pnfs_put_u64(&p, 0);                              /* ffl_stripe_unit       */

    pnfs_put_u32(&p, 1);                              /* ffl_mirrors<> count   */
    pnfs_put_u32(&p, 1);                              /* ffm_data_servers<>    */

    memcpy(p, deviceid, NFS4_DEVICEID4_SIZE);         /* ffds_deviceid         */
    p = (char *) p + NFS4_DEVICEID4_SIZE;
    pnfs_put_u32(&p, 0);                              /* ffds_efficiency       */
    memcpy(p, zero_stateid, sizeof(zero_stateid));    /* ffds_stateid (anon)   */
    p = (char *) p + sizeof(zero_stateid);
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
        p = (char *) p + NFS4_DEVICEID4_SIZE;
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
        p = (char *) p + NFS4_DEVICEID4_SIZE;
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

    evpl_mutex_lock(&cache->lock);

    for (i = 0; i < cache->count; i++) {
        if (cache->entries[i].valid &&
            memcmp(cache->entries[i].deviceid, dev->deviceid,
                   CHIMERA_VFS_DEVICEID_SIZE) == 0) {
            cache->entries[i].device = *dev;       /* refresh */
            evpl_mutex_unlock(&cache->lock);
            return;
        }
    }

    if (cache->count < NFS_PNFS_DEVCACHE_MAX) {
        struct nfs_pnfs_devcache_entry entry = {
            .valid  = 1,
            .device = *dev,
        };
        memcpy(entry.deviceid, dev->deviceid, sizeof(entry.deviceid));
        cache->entries[cache->count++] = entry;
    }

    evpl_mutex_unlock(&cache->lock);
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

    evpl_mutex_lock(&cache->lock);

    for (i = 0; i < cache->count; i++) {
        if (cache->entries[i].valid &&
            memcmp(cache->entries[i].deviceid, deviceid,
                   CHIMERA_VFS_DEVICEID_SIZE) == 0) {
            *out  = cache->entries[i].device;
            found = 1;
            break;
        }
    }

    evpl_mutex_unlock(&cache->lock);
    return found;
} /* nfs_pnfs_devcache_find */

/*
 * The layout blob itself ([deviceid][fhlen][backing-fh]) is packed and unpacked
 * by the VFS (chimera_vfs_pnfs_blob_{pack,unpack}), because the VFS data
 * redirect reads it too.  What stays here is the wire derivation: the stored
 * backing handle is the nfs-module chimera handle of the file's data on the DS,
 * and the native (NFSv3) handle a CLIENT must use is recovered by skipping the
 * 16-byte mount_id + 1-byte server-index prefix the nfs module prepends.
 */
#define FF_BLOB_FH_SKIP (CHIMERA_VFS_MOUNTID_SIZE + 1)

/* LAYOUTGET uses a fixed sequence of conditional operations. No callback
 * allocates a second compound or performs filesystem I/O. The metadata and DS
 * writes share finish/retry, but atomic rollback across backends is a backend
 * capability, not a promise made by this frontend. */
struct nfs4_layoutget {
    struct nfs_request              *req;
    const struct LAYOUTGET4args     *args;
    struct nfs_layout_journal       *journal;
    struct nfs_layout_journal_entry *reservation;
    struct nfs_client               *client;
    nfsstat4                        *status;
    nfsstat4                         reserve_status;
    struct stateid4                  input;
    struct LAYOUTGET4res             result;
    const struct chimera_vfs_ds     *ds;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         fh_len, want_class, own_barriers;
    uint16_t                         export_id;
    uint64_t                         capabilities;
    uint8_t                          blob[CHIMERA_VFS_PNFS_LAYOUT_MAX];
    uint32_t                         blob_len;
    char                             backing_name[CHIMERA_VFS_PNFS_BACKING_NAME_MAX];
    bool                             materialize, ready;
    int                              first;
};

enum {
    LG_ADMIT,
    LG_RESERVE,
    LG_OPEN,
    LG_HANDLE,
    LG_NATIVE,
    LG_ATTR,
    LG_SELECT,
    LG_DS_ROOT,
    LG_DS_OPEN,
    LG_CREATE,
    LG_RESET,
    LG_MDS,
    LG_STORE,
    LG_RESULT,
    LG_COUNT
};

static uint32_t
lg_layout4_xdr_len(uint32_t body_len)
{
    return 28 + ((body_len + 3) & ~3u);
} /* lg_layout4_xdr_len */

struct nfs4_layoutget *
nfs4_layoutget_alloc(
    struct nfs_request        *req,
    uint32_t                   wire_index,
    struct nfs_layout_journal *journal,
    nfsstat4                  *status)
{
    struct nfs4_layoutget *ctx = calloc(1, sizeof(*ctx));

    if (ctx) {
        ctx->req     = req;
        ctx->args    = &req->args_compound->argarray[wire_index].oplayoutget;
        ctx->journal = journal;
        ctx->status  = status;
    }
    return ctx;
} /* nfs4_layoutget_alloc */

void
nfs4_layoutget_free(struct nfs4_layoutget *ctx)
{
    free(ctx);
} /* nfs4_layoutget_free */

void
nfs4_layoutget_reset(struct nfs4_layoutget *ctx)
{
    if (ctx) {
        ctx->materialize = ctx->ready = false;
        ctx->blob_len    = 0;
        memset(&ctx->result, 0, sizeof(ctx->result));
    }
} /* nfs4_layoutget_reset */

nfsstat4
nfs4_layoutget_admit(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx,
    struct nfs_client           *client,
    uint16_t                     export_id,
    const struct stateid4       *input,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    uint32_t                     own_barriers)
{
    struct chimera_server_nfs_thread *thread = ctx->req->thread;
    const struct LAYOUTGET4args      *args   = ctx->args;

    (void) compound;

    if (!chimera_vfs_pnfs_feature_enabled(thread->vfs)) {
        return NFS4ERR_NOTSUPP;
    }
    if (args->loga_layout_type != LAYOUT4_FLEX_FILES &&
        args->loga_layout_type != LAYOUT4_BLOCK_VOLUME && args->loga_layout_type != LAYOUT4_SCSI) {
        return NFS4ERR_UNKNOWN_LAYOUTTYPE;
    }
    if (args->loga_iomode != LAYOUTIOMODE4_READ && args->loga_iomode != LAYOUTIOMODE4_RW) {
        return NFS4ERR_BADIOMODE;
    }
    if (!fh || !fh_len) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (!client) {
        return NFS4ERR_LAYOUTUNAVAILABLE;
    }
    ctx->fh_len = fh_len;
    memcpy(ctx->fh, fh, fh_len);
    ctx->client       = client;
    ctx->own_barriers = own_barriers;
    ctx->export_id    = export_id;
    ctx->input        = *input;
    ctx->capabilities = chimera_vfs_module_capabilities(thread->vfs_thread, fh, fh_len);
    ctx->want_class   = 0;
    uint32_t expected = LAYOUT4_FLEX_FILES;
    if (ctx->capabilities & CHIMERA_VFS_CAP_LAYOUT_SOURCE) {
        ctx->want_class = (ctx->capabilities & CHIMERA_VFS_CAP_LAYOUT_CLASS_SCSI) ? CHIMERA_VFS_LAYOUT_CLASS_SCSI :
            (ctx->capabilities & CHIMERA_VFS_CAP_LAYOUT_CLASS_BLOCK) ? CHIMERA_VFS_LAYOUT_CLASS_BLOCK :
            CHIMERA_VFS_LAYOUT_CLASS_FLEX;
        expected = ctx->want_class == CHIMERA_VFS_LAYOUT_CLASS_SCSI ? LAYOUT4_SCSI :
            ctx->want_class == CHIMERA_VFS_LAYOUT_CLASS_BLOCK ? LAYOUT4_BLOCK_VOLUME : LAYOUT4_FLEX_FILES;
    }
    return args->loga_layout_type == expected ? NFS4_OK : NFS4ERR_UNKNOWN_LAYOUTTYPE;
} /* nfs4_layoutget_admit */

static void
lg_reserve(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct nfs4_layoutget *ctx = private_data;

    (void) index;
    ctx->reserve_status = nfs_layout_journal_reserve_grant(ctx->journal, ctx->client,
                                                           &ctx->req->thread->shared->nfs4_state_table, &ctx->req->
                                                           thread->
                                                           shared->nfs4_layout_table, fh, fh_len, ctx->own_barriers, &
                                                           ctx->reservation);
    /* Steering advances a shared round-robin counter, so it belongs to this
     * explicit memoized coordination action, never a replayable prepare. */
    if (ctx->reserve_status == NFS4_OK && !ctx->ds) {
        ctx->ds = chimera_vfs_pnfs_steer(ctx->req->thread->vfs);
    }
    chimera_vfs_compound_coordinate_done(compound, token,
                                         ctx->reserve_status == NFS4_OK ? CHIMERA_VFS_OK : CHIMERA_VFS_EAGAIN);
} /* lg_reserve */

static nfsstat4
lg_select(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx)
{
    const struct chimera_vfs_compound_op *query = chimera_vfs_compound_op(compound, ctx->first + LG_ATTR);
    const struct chimera_vfs_ds          *resident;

    if (ctx->want_class) {
        return NFS4_OK;
    }
    resident = chimera_vfs_pnfs_find_backing(ctx->req->thread->vfs, ctx->fh, ctx->fh_len);
    if (resident) {
        ctx->blob_len = chimera_vfs_pnfs_blob_pack(ctx->blob, resident->deviceid, ctx->fh, ctx->fh_len);
    } else if ((query->attr.va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) && query->attr.va_pnfs_len) {
        ctx->blob_len = query->attr.va_pnfs_len;
        if (ctx->blob_len > sizeof(ctx->blob)) {
            return NFS4ERR_LAYOUTUNAVAILABLE;
        }
        memcpy(ctx->blob, query->attr.va_pnfs, ctx->blob_len);
    }
    if (ctx->blob_len) {
        return NFS4_OK;
    }
    /* Existing MDS bytes are never silently replaced with an empty backing. */
    if (!(ctx->capabilities & CHIMERA_VFS_CAP_LAYOUT) ||
        chimera_vfs_pnfs_fh_is_ds_backing(ctx->req->thread->vfs, ctx->fh, ctx->fh_len) ||
        (query->attr.va_set_mask & (CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_INUM)) !=
        (CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_INUM) || query->attr.va_size || !ctx->ds) {
        return NFS4ERR_LAYOUTUNAVAILABLE;
    }
    ctx->materialize = true;
    chimera_vfs_pnfs_backing_name(ctx->backing_name, ctx->fh, query->attr.va_ino);
    return NFS4_OK;
} /* lg_select */

static nfsstat4
lg_encode(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx)
{
    struct nfs_request                   *req    = ctx->req;
    const struct LAYOUTGET4args          *args   = ctx->args;
    const struct chimera_vfs_compound_op *source = chimera_vfs_compound_op(compound, ctx->first + LG_NATIVE);
    struct layout4                       *layouts;
    uint32_t                              count = ctx->want_class ? source->layout_num_segments : 1;
    uint32_t                              type = args->loga_layout_type, total = 0;

    if (!count || count > CHIMERA_VFS_LAYOUT_MAX_SEGMENTS) {
        return NFS4ERR_LAYOUTUNAVAILABLE;
    }
    if (ctx->want_class && source->layout_returned_class != ctx->want_class) {
        return NFS4ERR_UNKNOWN_LAYOUTTYPE;
    }
    bool                                  block        = type == LAYOUT4_BLOCK_VOLUME || type == LAYOUT4_SCSI;
    uint32_t                              layout_count = block ? 1 : count;
    layouts = xdr_dbuf_alloc_space(layout_count * sizeof(*layouts), req->encoding->dbuf);
    chimera_nfs_abort_if(!layouts, "Failed to allocate layout reply");
    for (uint32_t i = 0; i < layout_count; i++) {
        uint8_t         body[1024], wire[CHIMERA_NFS_FH_MAX];
        uint32_t        body_len;
        struct layout4 *lo = &layouts[i];
        if (block) {
            body_len = type == LAYOUT4_SCSI ?
                chimera_nfs4_encode_scsi_layout(body, source->layout_segments, count) :
                chimera_nfs4_encode_block_layout(body, source->layout_segments, count);
            const struct chimera_vfs_layout_segment *last = &source->layout_segments[count - 1];
            uint64_t                                 end  = last->offset + last->length;
            lo->lo_offset = args->loga_offset;
            lo->lo_length = end > args->loga_offset ? end - args->loga_offset : 0;
        } else if (ctx->want_class) {
            const struct chimera_vfs_layout_segment *segment = &source->layout_segments[i];
            body_len = chimera_nfs4_encode_ff_layout(body, segment->deviceid, segment->ds_fh,
                                                     segment->ds_fh_len, segment->iomode);
            lo->lo_offset = segment->offset;
            lo->lo_length = segment->length;
        } else {
            const uint8_t               *deviceid, *backing, *native;
            uint32_t                     length, native_len;
            if (chimera_vfs_pnfs_blob_unpack(ctx->blob, ctx->blob_len, &deviceid, &backing, &length) ||
                length > CHIMERA_VFS_FH_SIZE) {
                return NFS4ERR_LAYOUTUNAVAILABLE;
            }
            const struct chimera_vfs_ds *ds = chimera_vfs_pnfs_find_device(req->thread->vfs, deviceid);
            if (ds && ds->backing_local) {
                int wire_len;
                chimera_nfs_fh_wrap(wire, &wire_len, ctx->export_id, backing, length,
                                    req->thread->shared->fh_key, req->thread->shared->fh_sign);
                native     = wire;
                native_len = wire_len;
            } else {
                if (length <= FF_BLOB_FH_SKIP) {
                    return NFS4ERR_LAYOUTUNAVAILABLE;
                }
                native     = backing + FF_BLOB_FH_SKIP;
                native_len = length - FF_BLOB_FH_SKIP;
            }
            body_len      = chimera_nfs4_encode_ff_layout(body, deviceid, native, native_len, args->loga_iomode);
            lo->lo_offset = 0;
            lo->lo_length = UINT64_MAX;
        }
        total += lg_layout4_xdr_len(body_len);
        if (total > args->loga_maxcount) {
            return NFS4ERR_TOOSMALL;
        }
        lo->lo_iomode           = ctx->want_class && !block ? source->layout_segments[i].iomode : args->loga_iomode;
        lo->lo_content.loc_type = type;
        chimera_nfs_abort_if(xdr_dbuf_opaque_copy(&lo->lo_content.loc_body, body, body_len, req->encoding->dbuf),
                             "Failed to copy layout body");
    }
    nfsstat4 status = nfs_layout_journal_stage(ctx->journal, ctx->fh, ctx->fh_len, ctx->export_id,
                                               args->loga_iomode, type, &ctx->input,
                                               &ctx->result.logr_resok4.logr_stateid);
    if (status != NFS4_OK) {
        return status;
    }
    ctx->result.logr_status                      = NFS4_OK;
    ctx->result.logr_resok4.logr_return_on_close = 0;
    ctx->result.logr_resok4.num_logr_layout      = layout_count;
    ctx->result.logr_resok4.logr_layout          = layouts;
    ctx->ready                                   = true;
    return NFS4_OK;
} /* lg_encode */

static void
lg_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_layoutget          *ctx      = private_data;
    struct chimera_vfs_compound_op *op       = chimera_vfs_compound_op_args(compound, index);
    int                             relative = index - ctx->first;

    if ((relative == LG_NATIVE && !ctx->want_class) || (relative == LG_ATTR && ctx->want_class) ||
        (relative >= LG_DS_ROOT && relative <= LG_STORE && !ctx->materialize)) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    nfsstat4                        error = NFS4_OK;
    if (relative == LG_OPEN) {
        nfs_layout_journal_activate(ctx->reservation);
        error = nfs_layout_journal_check(ctx->journal, ctx->fh, ctx->fh_len, &ctx->input);
    } else if (relative == LG_NATIVE) {
        op->layout_class = ctx->want_class;
    } else if (relative == LG_SELECT) {
        error = lg_select(compound, ctx);
    } else if (relative == LG_DS_ROOT || relative == LG_MDS) {
        const uint8_t *fh  = relative == LG_MDS ? ctx->fh : ctx->ds->root_fh;
        uint32_t       len = relative == LG_MDS ? ctx->fh_len : ctx->ds->root_fh_len;
        op->arg_fh_len = len;
        memcpy(op->arg_fh, fh, len);
    } else if (relative == LG_CREATE) {
        op->name_len = strlen(ctx->backing_name);
        memcpy(op->name, ctx->backing_name, op->name_len + 1);
    } else if (relative == LG_RESET) {
        const struct chimera_vfs_compound_op *created = chimera_vfs_compound_op(compound, ctx->first + LG_CREATE);
        if (created->created) {
            chimera_vfs_compound_op_skip(compound, index);
        }
    } else if (relative == LG_STORE) {
        const struct chimera_vfs_compound_op *created = chimera_vfs_compound_op(compound, ctx->first + LG_CREATE);
        if (!(created->attr.va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            error = NFS4ERR_LAYOUTTRYLATER;
        } else {
            ctx->blob_len = chimera_vfs_pnfs_blob_pack(ctx->blob, ctx->ds->deviceid,
                                                       created->attr.va_fh, created->attr.va_fh_len);
            op->set_attr.va_set_mask = CHIMERA_VFS_ATTR_PNFS_LAYOUT;
            op->set_attr.va_pnfs_len = ctx->blob_len;
            memcpy(op->set_attr.va_pnfs, ctx->blob, ctx->blob_len);
        }
    } else if (relative == LG_RESULT) {
        error = lg_encode(compound, ctx);
    }
    if (error != NFS4_OK) {
        *ctx->status = error;
        *status      = CHIMERA_VFS_EINVAL;
    }
} /* lg_prepare */

static void
lg_reserved(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_layoutget *ctx = private_data;

    (void) compound;
    (void) index;
    if (*status != CHIMERA_VFS_OK) {
        *ctx->status = ctx->reserve_status;
    }
} /* lg_reserved */

int
nfs4_layoutget_append(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx)
{
    struct chimera_vfs_attrs attrs       = { 0 };
    const uint8_t            placeholder = 0;

    ctx->first = chimera_vfs_compound_add_checkpoint(compound);
    if (ctx->first < 0) {
        return -1;
    }
    chimera_vfs_compound_add_coordinate(compound, lg_reserve, ctx);
    chimera_vfs_compound_add_open_current(compound, CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_gethandle(compound);
    chimera_vfs_compound_add_get_layout(compound, ctx->args->loga_offset, ctx->args->loga_length,
                                        ctx->args->loga_iomode, 0, CHIMERA_VFS_LAYOUT_MAX_SEGMENTS);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_PNFS_LAYOUT | CHIMERA_VFS_ATTR_SIZE |
                                     CHIMERA_VFS_ATTR_INUM);
    chimera_vfs_compound_add_checkpoint(compound);
    chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
    chimera_vfs_compound_add_open_current(compound, CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_INFERRED, 0);
    attrs.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    attrs.va_mode     = S_IFREG | 0666;
    chimera_vfs_compound_add_open(compound, "", 0, CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED, 0,
                                  &attrs, CHIMERA_VFS_ATTR_FH, 0, 0);
    memset(&attrs, 0, sizeof(attrs));
    attrs.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    chimera_vfs_compound_add_setattr(compound, NULL, &attrs, 0, 0);
    chimera_vfs_compound_op_use_handle(compound, ctx->first + LG_RESET, ctx->first + LG_CREATE);
    chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
    memset(&attrs, 0, sizeof(attrs));
    chimera_vfs_compound_add_setattr(compound, NULL, &attrs, 0, 0);
    int result = chimera_vfs_compound_add_checkpoint(compound);
    if (result != ctx->first + LG_RESULT) {
        return -1;
    }
    for (int i = LG_RESERVE; i < LG_COUNT; i++) {
        chimera_vfs_compound_set_op_prepare(compound, ctx->first + i, lg_prepare, ctx);
    }
    chimera_vfs_compound_set_op_callbacks(compound, ctx->first + LG_RESERVE, lg_prepare, lg_reserved, ctx);
    return result;
} /* nfs4_layoutget_append */

const struct LAYOUTGET4res *
nfs4_layoutget_result(struct nfs4_layoutget *ctx)
{
    return ctx->ready ? &ctx->result : NULL;
} /* nfs4_layoutget_result */

bool
nfs4_layoutget_device(
    struct nfs4_layoutget            *ctx,
    struct chimera_vfs_compound      *compound,
    const uint8_t                     deviceid[16],
    struct chimera_vfs_layout_device *out)
{
    if (!ctx || !ctx->ready || !ctx->want_class) {
        return false;
    }
    const struct chimera_vfs_compound_op *source = chimera_vfs_compound_op(compound, ctx->first + LG_NATIVE);
    for (uint32_t i = 0; i < source->layout_num_devices; i++) {
        if (!memcmp(source->layout_devices[i].deviceid, deviceid, CHIMERA_VFS_DEVICEID_SIZE)) {
            *out = source->layout_devices[i];
            return true;
        }
    }
    return false;
} /* nfs4_layoutget_device */

void
nfs4_layoutget_publish(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx)
{
    if (!ctx || !ctx->ready) {
        return;
    }
    if (ctx->want_class) {
        const struct chimera_vfs_compound_op *source = chimera_vfs_compound_op(compound, ctx->first + LG_NATIVE);
        for (uint32_t i = 0; i < source->layout_num_devices; i++) {
            nfs_pnfs_devcache_put(&ctx->req->thread->shared->nfs4_pnfs_devcache, &source->layout_devices[i]);
        }
    }
    nfs4_cb_ensure_probe(ctx->req->thread, ctx->client, ctx->req);
} /* nfs4_layoutget_publish */

void
chimera_nfs4_layoutget(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) argop;
    (void) resop;
    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_layoutget */

void
chimera_nfs4_layoutreturn(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) argop;
    (void) resop;
    chimera_nfs4_compound_state(thread, req);
} /* chimera_nfs4_layoutreturn */

void
chimera_nfs4_getdevicelist(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) thread;
    resop->opgetdevicelist.gdlr_status = chimera_nfs4_protocol_status(req, argop);
    chimera_nfs4_compound_complete(req, resop->opgetdevicelist.gdlr_status);
} /* chimera_nfs4_getdevicelist */

void
chimera_nfs4_layoutstats(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) thread;
    resop->oplayoutstats.lsr_status = chimera_nfs4_protocol_status(req, argop);
    chimera_nfs4_compound_complete(req, resop->oplayoutstats.lsr_status);
} /* chimera_nfs4_layoutstats */

void
chimera_nfs4_layouterror(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) thread;
    resop->oplayouterror.ler_status = chimera_nfs4_protocol_status(req, argop);
    chimera_nfs4_compound_complete(req, resop->oplayouterror.ler_status);
} /* chimera_nfs4_layouterror */

void
chimera_nfs4_layoutcommit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_layoutcommit */
