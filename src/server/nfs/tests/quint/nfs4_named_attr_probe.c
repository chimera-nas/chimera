/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * NFSv4.1 named-attribute (OPENATTR) ground-truth probe.
 *
 * The MBT corpus has no OPENATTR / named-attribute coverage, so the whole
 * named-attribute lifecycle is dark to the model.  Named attributes are the
 * NFS view of the exact VFS named-stream storage that SMB alternate data
 * streams project onto (open_stream/list_streams/remove_stream,
 * CAP_NAMED_STREAMS, the memfs per-inode stream list), so this probe is the NFS
 * half of the cross-protocol stream coverage.
 *
 * It drives the full lifecycle end-to-end against the in-process server:
 *   - create a base file, confirm FATTR4_NAMED_ATTR is FALSE;
 *   - OPENATTR the file to reach its synthetic named-attribute directory;
 *   - OPEN(CLAIM_NULL, create) a named attribute, WRITE and READ its content
 *     back byte-identical (exercising the stream filehandle data path
 *     memfs_open_fh decodes);
 *   - confirm FATTR4_NAMED_ATTR is now TRUE;
 *   - READDIR the attr directory and LOOKUP the attribute by name;
 *   - REMOVE the attribute and confirm FATTR4_NAMED_ATTR is FALSE again and the
 *     attr directory is empty.
 * OPENATTR is gated on the shared smb_named_streams knob, enabled here.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

static int failures = 0;

static void
expect(
    const char *label,
    int         ok,
    const char *detail)
{
    if (!ok) {
        printf("  FAIL %s: %s\n", label, detail);
        failures++;
    } else {
        printf("  ok  %s: %s\n", label, detail);
    }
} /* expect */

/* ---- one probed 4.1 client ------------------------------------------------ */

struct pc {
    struct mbt_env        *env;
    struct evpl_rpc2_conn *conn;
    uint64_t               clientid;
    uint8_t                sessionid[16];
    uint32_t               slot_seq;
};

/* Minimal back-channel responder: named attributes never recall, but the
 * session negotiates a back channel, so answer CB_SEQUENCE (and refuse the rest)
 * to keep the transport clean. */
static void
na_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct pc             *pc = private_data;
    struct CB_COMPOUND4res res;
    struct nfs_cb_resop4  *resarray;
    uint32_t               i;
    int                    rc;

    (void) conn;
    (void) cred;

    memset(&res, 0, sizeof(res));
    resarray = xdr_dbuf_alloc_space(
        sizeof(*resarray) * (args->num_argarray ? args->num_argarray : 1),
        encoding->dbuf);
    if (!resarray) {
        res.status = NFS4ERR_RESOURCE;
        rc         = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res,
                                                               encoding);
        (void) rc;
        return;
    }

    for (i = 0; i < args->num_argarray; i++) {
        struct nfs_cb_argop4 *argop = &args->argarray[i];
        struct nfs_cb_resop4 *resop = &resarray[i];

        memset(resop, 0, sizeof(*resop));
        resop->resop = argop->argop;
        switch (argop->argop) {
            case OP_CB_SEQUENCE:
                memcpy(resop->opcbsequence.csr_resok4.csr_sessionid,
                       argop->opcbsequence.csa_sessionid, 16);
                resop->opcbsequence.csr_resok4.csr_sequenceid =
                    argop->opcbsequence.csa_sequenceid;
                resop->opcbsequence.csr_resok4.csr_slotid =
                    argop->opcbsequence.csa_slotid;
                resop->opcbsequence.csr_resok4.csr_highest_slotid =
                    argop->opcbsequence.csa_highest_slotid;
                resop->opcbsequence.csr_resok4.csr_target_highest_slotid =
                    argop->opcbsequence.csa_highest_slotid;
                resop->opcbsequence.csr_status = NFS4_OK;
                break;
            default:
                resop->opcbrecall.status = NFS4ERR_NOTSUPP;
                break;
        } /* switch */
    }
    res.status       = NFS4_OK;
    res.num_resarray = args->num_argarray;
    res.resarray     = resarray;
    rc               = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res,
                                                                 encoding);
    (void) rc;
} /* na_cb_compound */

/* ---- one compound's parsed reply ------------------------------------------ */

#define P_MAX_NAMES 16

struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[10];           /* per-op status */
    struct mbt_fh   fh;               /* last GETFH */
    struct stateid4 open_sid;         /* last OPEN stateid */
    uint32_t        write_count;      /* last WRITE */
    uint8_t         rdata[256];       /* last READ payload */
    uint32_t        rlen;
    int             named_attr;       /* last GETATTR FATTR4_NAMED_ATTR */
    int             named_attr_valid;
    int             nnames;           /* last READDIR entry names */
    char            names[P_MAX_NAMES][64];
    uint64_t        clientid;         /* EXCHANGE_ID */
    uint32_t        eir_seq;
    uint8_t         sessionid[16];    /* CREATE_SESSION */
};

/* Extract FATTR4_NAMED_ATTR from a fattr4 that requested exactly that bit. */
static int
fattr_named_attr(
    const struct fattr4 *f,
    int                 *valid)
{
    uint32_t w0 = f->num_attrmask > 0 ? f->attrmask[0] : 0;

    *valid = 0;
    if ((w0 & (1U << FATTR4_NAMED_ATTR)) && f->attr_vals.len >= 4) {
        const uint8_t *p = f->attr_vals.data;

        *valid = 1;
        return (p[0] | p[1] | p[2] | p[3]) ? 1 : 0; /* XDR bool */
    }
    return 0;
} /* fattr_named_attr */

static void
prep_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct prep *p = private_data;
    uint32_t     i;

    (void) verf;
    p->rpc_err = status;
    if (status == 0) {
        p->status = reply->status;
        p->nres   = (int) reply->num_resarray;
        for (i = 0; i < reply->num_resarray && i < 10; i++) {
            const struct nfs_resop4 *r = &reply->resarray[i];

            switch (r->resop) {
                case OP_SEQUENCE:
                    p->st[i] = r->opsequence.sr_status;
                    break;
                case OP_PUTFH:
                    p->st[i] = r->opputfh.status;
                    break;
                case OP_PUTROOTFH:
                    p->st[i] = r->opputrootfh.status;
                    break;
                case OP_LOOKUP:
                    p->st[i] = r->oplookup.status;
                    break;
                case OP_GETFH:
                    p->st[i] = r->opgetfh.status;
                    if (p->st[i] == NFS4_OK) {
                        mbt_copy_fh(&p->fh, &r->opgetfh.resok4.object);
                    }
                    break;
                case OP_OPENATTR:
                    p->st[i] = r->opopenattr.status;
                    break;
                case OP_OPEN:
                    p->st[i] = r->opopen.status;
                    if (p->st[i] == NFS4_OK) {
                        p->open_sid = r->opopen.resok4.stateid;
                    }
                    break;
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_WRITE:
                    p->st[i] = r->opwrite.status;
                    if (p->st[i] == NFS4_OK) {
                        p->write_count = r->opwrite.resok4.count;
                    }
                    break;
                case OP_READ:
                    p->st[i] = r->opread.status;
                    p->rlen  = 0;
                    if (p->st[i] == NFS4_OK) {
                        int k;

                        for (k = 0; k < r->opread.resok4.data.niov; k++) {
                            uint32_t n = r->opread.resok4.data.iov[k].length;

                            if (p->rlen + n <= sizeof(p->rdata)) {
                                memcpy(p->rdata + p->rlen,
                                       r->opread.resok4.data.iov[k].data, n);
                                p->rlen += n;
                            }
                            evpl_iovec_release(evpl,
                                               &r->opread.resok4.data.iov[k]);
                        }
                    }
                    break;
                case OP_READDIR:
                    p->st[i]  = r->opreaddir.status;
                    p->nnames = 0;
                    if (p->st[i] == NFS4_OK) {
                        const struct entry4 *e;

                        for (e = r->opreaddir.resok4.reply.entries;
                             e && p->nnames < P_MAX_NAMES; e = e->nextentry) {
                            uint32_t n = e->name.len < 63 ? e->name.len : 63;

                            memcpy(p->names[p->nnames], e->name.data, n);
                            p->names[p->nnames][n] = '\0';
                            p->nnames++;
                        }
                    }
                    break;
                case OP_GETATTR:
                    p->st[i] = r->opgetattr.status;
                    if (p->st[i] == NFS4_OK) {
                        p->named_attr =
                            fattr_named_attr(&r->opgetattr.resok4.obj_attributes,
                                             &p->named_attr_valid);
                    }
                    break;
                case OP_REMOVE:
                    p->st[i] = r->opremove.status;
                    break;
                case OP_EXCHANGE_ID:
                    p->st[i] = r->opexchange_id.eir_status;
                    if (p->st[i] == NFS4_OK) {
                        p->clientid = r->opexchange_id.eir_resok4.eir_clientid;
                        p->eir_seq  = r->opexchange_id.eir_resok4.eir_sequenceid;
                    }
                    break;
                case OP_CREATE_SESSION:
                    p->st[i] = r->opcreate_session.csr_status;
                    if (p->st[i] == NFS4_OK) {
                        memcpy(p->sessionid,
                               r->opcreate_session.csr_resok4.csr_sessionid, 16);
                    }
                    break;
                default:
                    p->st[i] = 0;
                    break;
            } /* switch */
        }
    }
    p->done = 1;
} /* prep_cb */

static struct prep
pc_compound(
    struct pc         *pc,
    struct nfs_argop4 *ops,
    int                nops,
    int                with_seq)
{
    struct nfs_argop4    argarray[10];
    struct COMPOUND4args args;
    struct prep          p;
    int                  base = 0;
    int                  i;

    memset(&p, 0, sizeof(p));
    memset(&args, 0, sizeof(args));

    if (with_seq) {
        memset(&argarray[0], 0, sizeof(argarray[0]));
        argarray[0].argop = OP_SEQUENCE;
        memcpy(argarray[0].opsequence.sa_sessionid, pc->sessionid, 16);
        argarray[0].opsequence.sa_sequenceid     = ++pc->slot_seq;
        argarray[0].opsequence.sa_slotid         = 0;
        argarray[0].opsequence.sa_highest_slotid = 0;
        argarray[0].opsequence.sa_cachethis      = 0;
        base                                     = 1;
    }
    for (i = 0; i < nops; i++) {
        argarray[base + i] = ops[i];
    }
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = (uint32_t) (base + nops);

    pc->env->nfs_v4.send_call_NFSPROC4_COMPOUND(&pc->env->nfs_v4.rpc2,
                                                pc->env->evpl, pc->conn,
                                                &pc->env->cred, &args,
                                                0, 0, NULL, 0, 0,
                                                prep_cb, &p);
    while (!p.done) {
        evpl_continue(pc->env->evpl);
    }
    if (p.rpc_err) {
        fprintf(stderr, "transport error %d\n", p.rpc_err);
        exit(2);
    }
    return p;
} /* pc_compound */

static void
pc_setup(
    struct pc      *pc,
    struct mbt_env *env)
{
    struct evpl_rpc2_program         *cb_programs[1];
    struct evpl_endpoint             *ep;
    struct nfs_argop4                 op;
    struct prep                       p;
    static char                       owner[32];
    struct channel_attrs4             chan = {
        .ca_headerpadsize          = 0,
        .ca_maxrequestsize         = 1048576,
        .ca_maxresponsesize        = 1048576,
        .ca_maxresponsesize_cached = 65536,
        .ca_maxoperations          = 16,
        .ca_maxrequests            = 32,
        .num_ca_rdma_ird           = 0,
    };
    static struct callback_sec_parms4 sec = { .cb_secflavor = 0 };

    memset(pc, 0, sizeof(*pc));
    pc->env = env;

    cb_programs[0] = &env->nfs_v4_cb.rpc2;
    ep             = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                        "127.0.0.1", 2049);
    pc->conn = evpl_rpc2_client_connect(env->rpc2_thread,
                                        EVPL_STREAM_INPROC, ep,
                                        cb_programs, 1, pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    snprintf(owner, sizeof(owner), "na-probe");
    memset(op.opexchange_id.eia_clientowner.co_verifier, 1, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  = (uint32_t) strlen(owner);
    op.opexchange_id.eia_state_protect.spa_how       = SP4_NONE;
    p                                                = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "EXCHANGE_ID failed: %u\n", p.status);
        exit(2);
    }
    pc->clientid = p.clientid;

    memset(&op, 0, sizeof(op));
    op.argop                                = OP_CREATE_SESSION;
    op.opcreate_session.csa_clientid        = pc->clientid;
    op.opcreate_session.csa_sequence        = p.eir_seq;
    op.opcreate_session.csa_flags           = CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
    op.opcreate_session.csa_fore_chan_attrs = chan;
    op.opcreate_session.csa_back_chan_attrs = chan;
    op.opcreate_session.csa_cb_program      = 0x40000000;
    op.opcreate_session.num_csa_sec_parms   = 1;
    op.opcreate_session.csa_sec_parms       = &sec;
    p                                       = pc_compound(pc, &op, 1, 0);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "CREATE_SESSION failed: %u\n", p.status);
        exit(2);
    }
    memcpy(pc->sessionid, p.sessionid, 16);

    memset(&op, 0, sizeof(op));
    op.argop                         = OP_RECLAIM_COMPLETE;
    op.opreclaim_complete.rca_one_fs = 0;
    p                                = pc_compound(pc, &op, 1, 1);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "RECLAIM_COMPLETE failed: %u\n", p.status);
        exit(2);
    }
} /* pc_setup */

/* ---- op builders ---------------------------------------------------------- */

static struct nfs_argop4
op_putfh(const struct mbt_fh *fh)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop               = OP_PUTFH;
    a.opputfh.object.data = (void *) fh->data;
    a.opputfh.object.len  = fh->len;
    return a;
} /* op_putfh */

static struct nfs_argop4
op_getfh(void)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_GETFH;
    return a;
} /* op_getfh */

static struct nfs_argop4
op_openattr(int createdir)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_OPENATTR;
    a.opopenattr.createdir = createdir != 0;
    return a;
} /* op_openattr */

static struct nfs_argop4
op_open(
    uint64_t    clientid,
    const char *owner,
    const char *name,
    uint32_t    access,
    int         create)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.seqid            = 0;
    a.opopen.share_access     = access;
    a.opopen.share_deny       = 0;
    a.opopen.owner.clientid   = clientid;
    a.opopen.owner.owner.data = (void *) owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(owner);
    if (create) {
        a.opopen.openhow.opentype                      = OPEN4_CREATE;
        a.opopen.openhow.how.mode                      = UNCHECKED4;
        a.opopen.openhow.how.createattrs.num_attrmask  = 0;
        a.opopen.openhow.how.createattrs.attr_vals.len = 0;
    } else {
        a.opopen.openhow.opentype = OPEN4_NOCREATE;
    }
    a.opopen.claim.claim     = CLAIM_NULL;
    a.opopen.claim.file.data = (void *) name;
    a.opopen.claim.file.len  = (uint32_t) strlen(name);
    return a;
} /* op_open */

static struct nfs_argop4
op_close(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_CLOSE;
    a.opclose.seqid        = 0;
    a.opclose.open_stateid = *sid;
    return a;
} /* op_close */

static struct nfs_argop4
op_write(
    struct mbt_env        *env,
    const struct stateid4 *sid,
    uint64_t               off,
    const void            *data,
    uint32_t               len,
    struct evpl_iovec     *iov)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    if (evpl_iovec_alloc(env->evpl, len, 4096, 1, 0, iov) < 0) {
        fprintf(stderr, "evpl_iovec_alloc(%u) failed\n", len);
        exit(2);
    }
    memcpy(iov->data, data, len);
    iov->length = (int) len;

    a.argop               = OP_WRITE;
    a.opwrite.stateid     = *sid;
    a.opwrite.offset      = off;
    a.opwrite.stable      = FILE_SYNC4;
    a.opwrite.data.iov    = iov;
    a.opwrite.data.niov   = 1;
    a.opwrite.data.length = len;
    return a;
} /* op_write */

static struct nfs_argop4
op_read(
    const struct stateid4 *sid,
    uint64_t               off,
    uint32_t               len)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop          = OP_READ;
    a.opread.stateid = *sid;
    a.opread.offset  = off;
    a.opread.count   = len;
    return a;
} /* op_read */

/* GETATTR requesting exactly FATTR4_NAMED_ATTR. */
static uint32_t na_mask[1] = { 1U << FATTR4_NAMED_ATTR };

static struct nfs_argop4
op_getattr_na(void)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                      = OP_GETATTR;
    a.opgetattr.attr_request     = na_mask;
    a.opgetattr.num_attr_request = 1;
    return a;
} /* op_getattr_na */

static struct nfs_argop4
op_readdir(void)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                      = OP_READDIR;
    a.opreaddir.cookie           = 0;
    a.opreaddir.dircount         = 65536;
    a.opreaddir.maxcount         = 1048576;
    a.opreaddir.attr_request     = NULL;
    a.opreaddir.num_attr_request = 0;
    return a;
} /* op_readdir */

static struct nfs_argop4
op_lookup(const char *name)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                 = OP_LOOKUP;
    a.oplookup.objname.data = (void *) name;
    a.oplookup.objname.len  = (uint32_t) strlen(name);
    return a;
} /* op_lookup */

static struct nfs_argop4
op_remove(const char *name)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_REMOVE;
    a.opremove.target.data = (void *) name;
    a.opremove.target.len  = (uint32_t) strlen(name);
    return a;
} /* op_remove */

/* names_contain: does the last READDIR reply carry `name`? */
static int
names_contain(
    const struct prep *p,
    const char        *name)
{
    int i;

    for (i = 0; i < p->nnames; i++) {
        if (strcmp(p->names[i], name) == 0) {
            return 1;
        }
    }
    return 0;
} /* names_contain */

int
main(void)
{
    struct mbt_env_opts opts = {
        .smb_named_streams = 1,
        .disable_caches    = 1,
    };
    struct mbt_env     *env = malloc(sizeof(*env));
    struct pc           pc;
    struct mbt_fh       root, base_fh, attrdir_fh, stream_fh;
    struct stateid4     base_sid, stream_sid;
    struct prep         p;
    struct evpl_iovec   wiov;
    const char         *owner = "na-open";
    const char         *aname = "myattr";
    const char         *adata = "NAMED-ATTRIBUTE-DATA";
    uint32_t            alen  = (uint32_t) strlen(adata);

    setvbuf(stdout, NULL, _IONBF, 0);
    mbt_watchdog_arm(60);
    mbt_env_start_opts(env, &opts);
    env->nfs_v4_cb.recv_call_CB_COMPOUND = na_cb_compound;

    /* Resolve the export root (minor 0, no session). */
    {
        struct nfs_argop4    ops[3];
        struct COMPOUND4args args;
        struct prep          rp;

        memset(ops, 0, sizeof(ops));
        ops[0].argop                 = OP_PUTROOTFH;
        ops[1].argop                 = OP_LOOKUP;
        ops[1].oplookup.objname.data = "fs0";
        ops[1].oplookup.objname.len  = 3;
        ops[2].argop                 = OP_GETFH;

        memset(&args, 0, sizeof(args));
        args.minorversion = 0;
        args.argarray     = ops;
        args.num_argarray = 3;
        memset(&rp, 0, sizeof(rp));
        env->nfs_v4.send_call_NFSPROC4_COMPOUND(&env->nfs_v4.rpc2, env->evpl,
                                                env->nfs_conn, &env->cred, &args,
                                                0, 0, NULL, 0, 0, prep_cb, &rp);
        while (!rp.done) {
            evpl_continue(env->evpl);
        }
        if (rp.status != NFS4_OK || !rp.fh.has) {
            fprintf(stderr, "resolve export root failed: %u\n", rp.status);
            return 2;
        }
        root = rp.fh;
    }

    pc_setup(&pc, env);

    printf("# --- NFSv4 named-attribute lifecycle ---\n");

    /* Create the base file: PUTFH(root) + OPEN(create) + GETFH. */
    {
        struct nfs_argop4 ops[3];

        ops[0] = op_putfh(&root);
        ops[1] = op_open(pc.clientid, owner, "basefile", 3 /* BOTH */, 1);
        ops[2] = op_getfh();
        p      = pc_compound(&pc, ops, 3, 1);
        expect("create-base", p.status == NFS4_OK && p.fh.has,
               "OPEN(create) basefile + GETFH");
        base_fh  = p.fh;
        base_sid = p.open_sid;

        /* Close the base open; the file persists. */
        {
            struct nfs_argop4 cops[2];

            cops[0] = op_putfh(&base_fh);
            cops[1] = op_close(&base_sid);
            p       = pc_compound(&pc, cops, 2, 1);
            expect("close-base", p.status == NFS4_OK, "CLOSE basefile");
        }
    }

    /* FATTR4_NAMED_ATTR is FALSE on a fresh file. */
    {
        struct nfs_argop4 ops[2];

        ops[0] = op_putfh(&base_fh);
        ops[1] = op_getattr_na();
        p      = pc_compound(&pc, ops, 2, 1);
        expect("named-attr-false", p.status == NFS4_OK && p.named_attr_valid &&
               p.named_attr == 0, "GETATTR NAMED_ATTR == FALSE before any attr");
    }

    /* OPENATTR: reach the synthetic named-attribute directory. */
    {
        struct nfs_argop4 ops[3];

        ops[0] = op_putfh(&base_fh);
        ops[1] = op_openattr(1);
        ops[2] = op_getfh();
        p      = pc_compound(&pc, ops, 3, 1);
        expect("openattr", p.status == NFS4_OK && p.fh.has,
               "OPENATTR + GETFH -> attr directory handle");
        attrdir_fh = p.fh;
    }

    /* OPEN(create) a named attribute in the attr directory, then GETFH. */
    {
        struct nfs_argop4 ops[3];

        ops[0] = op_putfh(&attrdir_fh);
        ops[1] = op_open(pc.clientid, owner, aname, 3 /* BOTH */, 1);
        ops[2] = op_getfh();
        p      = pc_compound(&pc, ops, 3, 1);
        expect("create-attr", p.status == NFS4_OK && p.fh.has,
               "OPEN(create) named attribute + GETFH");
        stream_fh  = p.fh;
        stream_sid = p.open_sid;
    }

    /* WRITE then READ the attribute content back byte-identical. */
    {
        struct nfs_argop4 ops[2];

        ops[0] = op_putfh(&stream_fh);
        ops[1] = op_write(env, &stream_sid, 0, adata, alen, &wiov);
        p      = pc_compound(&pc, ops, 2, 1);
        expect("write-attr", p.status == NFS4_OK && p.write_count == alen,
               "WRITE the named attribute's content");

        ops[0] = op_putfh(&stream_fh);
        ops[1] = op_read(&stream_sid, 0, sizeof(p.rdata));
        p      = pc_compound(&pc, ops, 2, 1);
        expect("read-attr", p.status == NFS4_OK && p.rlen == alen &&
               memcmp(p.rdata, adata, alen) == 0,
               "READ returns the attribute content byte-identical");

        ops[0] = op_putfh(&stream_fh);
        ops[1] = op_close(&stream_sid);
        p      = pc_compound(&pc, ops, 2, 1);
        expect("close-attr", p.status == NFS4_OK, "CLOSE the named attribute");
    }

    /* FATTR4_NAMED_ATTR is now TRUE. */
    {
        struct nfs_argop4 ops[2];

        ops[0] = op_putfh(&base_fh);
        ops[1] = op_getattr_na();
        p      = pc_compound(&pc, ops, 2, 1);
        expect("named-attr-true", p.status == NFS4_OK && p.named_attr_valid &&
               p.named_attr == 1, "GETATTR NAMED_ATTR == TRUE after create");
    }

    /* READDIR the attr directory lists the attribute; LOOKUP resolves it. */
    {
        struct nfs_argop4 ops[2];

        ops[0] = op_putfh(&attrdir_fh);
        ops[1] = op_readdir();
        p      = pc_compound(&pc, ops, 2, 1);
        expect("readdir-attr", p.status == NFS4_OK && names_contain(&p, aname),
               "READDIR the attr directory lists the attribute");

        ops[0] = op_putfh(&attrdir_fh);
        ops[1] = op_lookup(aname);
        p      = pc_compound(&pc, ops, 2, 1);
        expect("lookup-attr", p.status == NFS4_OK,
               "LOOKUP the attribute by name in the attr directory");
    }

    /* REMOVE the attribute; NAMED_ATTR falls back to FALSE and the dir empties. */
    {
        struct nfs_argop4 ops[2];

        ops[0] = op_putfh(&attrdir_fh);
        ops[1] = op_remove(aname);
        p      = pc_compound(&pc, ops, 2, 1);
        expect("remove-attr", p.status == NFS4_OK,
               "REMOVE the named attribute");

        ops[0] = op_putfh(&base_fh);
        ops[1] = op_getattr_na();
        p      = pc_compound(&pc, ops, 2, 1);
        expect("named-attr-false-again", p.status == NFS4_OK &&
               p.named_attr_valid && p.named_attr == 0,
               "GETATTR NAMED_ATTR == FALSE after remove");

        ops[0] = op_putfh(&attrdir_fh);
        ops[1] = op_readdir();
        p      = pc_compound(&pc, ops, 2, 1);
        expect("readdir-empty", p.status == NFS4_OK && !names_contain(&p, aname),
               "the attr directory no longer lists the attribute");
    }

    mbt_env_stop(env);
    free(env);

    if (failures) {
        fprintf(stderr, "%d NFSv4 named-attribute check(s) FAILED\n", failures);
        return 1;
    }
    printf("all NFSv4 named-attribute checks passed\n");
    return 0;
} /* main */
