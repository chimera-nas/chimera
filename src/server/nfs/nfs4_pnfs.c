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
#include "nfs_internal.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

#define NFS4_PNFS_STRIPE_UNIT 1048576U   /* 1 MiB                              */
#define LAYOUT4_FLEX_FILES    0x4        /* RFC 8435; not in the generated XDR */

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
    uint8_t                     *buf,
    const struct chimera_vfs_ds *ds)
{
    void *p = buf;

    /* ffda_netaddrs: multipath_list4 (a netaddr4<>) with one entry. */
    pnfs_put_u32(&p, 1);
    pnfs_put_opaque(&p, ds->netid, strlen(ds->netid));
    pnfs_put_opaque(&p, ds->uaddr, strlen(ds->uaddr));

    /* ffda_versions<>: one entry advertising NFSv3, loosely coupled. */
    pnfs_put_u32(&p, 1);
    pnfs_put_u32(&p, 3);                      /* ffdv_version          */
    pnfs_put_u32(&p, 0);                      /* ffdv_minorversion     */
    pnfs_put_u32(&p, NFS4_PNFS_STRIPE_UNIT);  /* ffdv_rsize            */
    pnfs_put_u32(&p, NFS4_PNFS_STRIPE_UNIT);  /* ffdv_wsize            */
    pnfs_put_u32(&p, 0);                      /* ffdv_tightly_coupled=false */

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_ff_device_addr */

void
chimera_nfs4_getdeviceinfo(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETDEVICEINFO4args   *args = &argop->opgetdeviceinfo;
    struct GETDEVICEINFO4res    *res  = &resop->opgetdeviceinfo;
    struct chimera_vfs          *vfs  = thread->shared->vfs;
    const struct chimera_vfs_ds *ds;
    uint8_t                      body[256];
    uint32_t                     body_len;
    int                          rc;

    if (!chimera_vfs_pnfs_enabled(vfs)) {
        res->gdir_status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    if (args->gdia_layout_type != LAYOUT4_FLEX_FILES) {
        res->gdir_status = NFS4ERR_UNKNOWN_LAYOUTTYPE;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    ds = chimera_vfs_pnfs_find_device(vfs, args->gdia_device_id);
    if (!ds) {
        res->gdir_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    body_len = chimera_nfs4_encode_ff_device_addr(body, ds);

    /* gdia_maxcount bounds the bytes the client will accept for the
     * da_addr_body (RFC 8881 §18.40.3); signal TOOSMALL with the required
     * size if it cannot fit. */
    if (args->gdia_maxcount < body_len) {
        res->gdir_status   = NFS4ERR_TOOSMALL;
        res->gdir_mincount = body_len;
        chimera_nfs4_compound_complete(req, res->gdir_status);
        return;
    }

    res->gdir_resok4.gdir_device_addr.da_layout_type = LAYOUT4_FLEX_FILES;

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
    uint32_t       ds_fh_len)
{
    void         *p                = buf;
    const uint8_t zero_stateid[16] = { 0 };

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
    pnfs_put_opaque(&p, "0", 1);                      /* ffds_user  (synthetic)*/
    pnfs_put_opaque(&p, "0", 1);                      /* ffds_group (synthetic)*/

    pnfs_put_u32(&p, 0);                              /* ffl_flags             */
    pnfs_put_u32(&p, 0);                              /* ffl_stats_collect_hint*/

    return (uint8_t *) p - buf;
} /* chimera_nfs4_encode_ff_layout */

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

    if (!client) {
        ff_lg_fail(ctx, NFS4ERR_LAYOUTUNAVAILABLE);
        return;
    }

    /* Unpack [deviceid][backing-fh-len][backing-fh]; recover the DS's native v3
     * handle by skipping the nfs-module prefix. */
    deviceid       = ctx->blob;
    backing_fh_len = ctx->blob[CHIMERA_VFS_DEVICEID_SIZE];
    backing_fh     = ctx->blob + CHIMERA_VFS_DEVICEID_SIZE + 1;
    native_fh      = backing_fh + FF_BLOB_FH_SKIP;
    native_fh_len  = backing_fh_len - FF_BLOB_FH_SKIP;

    client_short_id = (uint32_t) client->client_id;

    layout = nfs_layout_state_find(client, req->fh, req->fhlen);
    if (layout) {
        nfs_layout_state_bump(layout, client_short_id, &res->logr_resok4.logr_stateid);
    } else {
        nfs_layout_state_create(client, req->fh, req->fhlen, args->loga_iomode,
                                client_short_id, table,
                                &req->thread->shared->nfs4_layout_table,
                                &res->logr_resok4.logr_stateid);
    }

    body = xdr_dbuf_alloc_space(256, req->encoding->dbuf);
    chimera_nfs_abort_if(body == NULL, "Failed to allocate space");
    body_len = chimera_nfs4_encode_ff_layout(body, deviceid, native_fh, native_fh_len);

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

    memset(&ctx->set_attr, 0, sizeof(ctx->set_attr));
    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    ctx->set_attr.va_mode     = S_IFREG | 0600;

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

static void
ff_lg_open_cb(
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

    ctx->mds_handle = handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, handle,
                        CHIMERA_VFS_ATTR_PNFS_LAYOUT | CHIMERA_VFS_ATTR_INUM,
                        ff_lg_getattr_cb, ctx);
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

    if (!chimera_vfs_pnfs_enabled(thread->shared->vfs)) {
        res->logr_status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->logr_status);
        return;
    }

    if (args->loga_layout_type != LAYOUT4_FLEX_FILES) {
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

    if (!chimera_vfs_pnfs_enabled(thread->shared->vfs)) {
        res->lorr_status = NFS4ERR_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->lorr_status);
        return;
    }

    if (args->lora_layout_type != LAYOUT4_FLEX_FILES) {
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

    if (!chimera_vfs_pnfs_enabled(thread->shared->vfs)) {
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
