/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * NFSv4 OPEN authorization ground-truth probe.
 *
 * A kernel NFS client does not re-check what it asked the server to open: for
 * MAY_OPEN on a regular file with NFS_CAP_ATOMIC_OPEN, nfs_permission()
 * returns 0 early and defers to the server entirely, so the status of the OPEN
 * operation is the only authorization in the path.  Nothing else in this tree
 * asserts that status.  The pjd ports reach the server through chimera's own
 * POSIX client, whose VFS engine gates the open a second time on the client
 * side (the vfs/nfs module carries CAP_REMOTE_DAC), so they stay green on a
 * server that grants every open regardless of mode; and the trace corpora run
 * every NFSv4 operation as uid 0, which is exempt from the gate outright.
 *
 * This drives the OPEN status directly, as a non-root AUTH_SYS caller:
 *   - as root, create a file 0644, then SETATTR it to the cell's mode and to
 *     OWNER_UID:OWNER_UID -- pjdfstest's own sequence, so the mode the gate
 *     must judge is one the create did not set;
 *   - OPEN(OPEN4_NOCREATE) it as either OWNER_UID or OTHER_UID, with one
 *     share_access;
 *   - require NFS4ERR_ACCESS exactly where the mode withholds that access, and
 *     NFS4_OK exactly where it grants it.
 *
 * Every cell runs twice, once claimed by name (CLAIM_NULL) and once by
 * filehandle (CLAIM_FH).  The two resolve through different paths in the
 * server, and a client sends whichever it sends -- Linux takes CLAIM_FH via
 * NFSPROC4_CLNT_OPEN_NOATTR whenever it holds valid cached attributes, which
 * is the ordinary case -- so both must reach the same verdict.
 *
 * All three share_access values are asserted independently.  A server that
 * authorizes one and not another is the failure this exists to catch, and a
 * matrix that only denied would be satisfied by a server that denies
 * everything.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

/* The identities the matrix below uses: the file is owned by OWNER_UID, and
 * each cell is probed either as its owner or as OTHER_UID, which is neither
 * the owner nor in its group.
 *
 * Deliberately not 65534/65533, the uids pjdfstest uses for the same matrix:
 * 65534 is also CHIMERA_VFS_ANON_UID, every export's default anonuid, so a
 * regression that squashed AUTH_SYS callers to anonymous would make the owner
 * cells indistinguishable from a squashed caller and keep them green. */
#define OWNER_UID 1000
#define OTHER_UID 2000

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

/* Minimal back-channel responder: this probe never takes a delegation, but the
 * session negotiates a back channel, so answer CB_SEQUENCE (and refuse the
 * rest) to keep the transport clean. */
static void
oa_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct pc             *pc = private_data;
    struct CB_COMPOUND4res res;
    struct nfs_cb_resop4   resarray[4];
    uint32_t               i;
    int                    rc;

    (void) conn;
    (void) cred;

    memset(&res, 0, sizeof(res));
    memset(resarray, 0, sizeof(resarray));

    for (i = 0; i < args->num_argarray && i < 4; i++) {
        const struct nfs_cb_argop4 *argop = &args->argarray[i];
        struct nfs_cb_resop4       *resop = &resarray[i];

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
    res.status = NFS4_OK;
    /* Only the entries actually filled above: resarray is a fixed four-slot
     * stack array, so reporting the request's full count would have the
     * encoder read past its end on a longer back-channel compound. */
    res.num_resarray = i;
    res.resarray     = resarray;
    rc               = pc->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res,
                                                                 encoding);
    (void) rc;
} /* oa_cb_compound */

/* ---- one compound's parsed reply ------------------------------------------ */

struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[10];           /* per-op status */
    struct mbt_fh   fh;               /* last GETFH */
    struct stateid4 open_sid;         /* last OPEN stateid */
    int             open_idx;         /* index of the last OPEN in st[] */
    uint64_t        clientid;         /* EXCHANGE_ID */
    uint32_t        eir_seq;
    uint8_t         sessionid[16];    /* CREATE_SESSION */
};

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

    (void) evpl;
    (void) verf;
    p->rpc_err  = status;
    p->open_idx = -1;
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
                case OP_OPEN:
                    p->st[i]    = r->opopen.status;
                    p->open_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        p->open_sid = r->opopen.resok4.stateid;
                    }
                    break;
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_SETATTR:
                    p->st[i] = r->opsetattr.status;
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
    snprintf(owner, sizeof(owner), "oa-probe");
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

static void
pack_be32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = (uint8_t) ((v >> 24) & 0xff);
    p[1] = (uint8_t) ((v >> 16) & 0xff);
    p[2] = (uint8_t) ((v >> 8) & 0xff);
    p[3] = (uint8_t) (v & 0xff);
} /* pack_be32 */

/* OPEN(CLAIM_NULL).  `mode` >= 0 makes it a creating open carrying that mode in
 * createattrs; `mode` < 0 makes it OPEN4_NOCREATE, which is the shape a client
 * opening an existing file produces and the one this probe measures.
 *
 * The bitmap and value blob are static because the argument structure only
 * borrows them, and the compound is sent before the next call overwrites them. */
static struct nfs_argop4
op_open(
    uint64_t    clientid,
    const char *owner,
    const char *name,
    uint32_t    access,
    int         mode)
{
    static uint32_t   bitmap[2];
    static uint8_t    blob[4];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.seqid            = 0;
    a.opopen.share_access     = access;
    a.opopen.share_deny       = 0;
    a.opopen.owner.clientid   = clientid;
    a.opopen.owner.owner.data = (void *) owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(owner);
    if (mode >= 0) {
        bitmap[0] = 0;
        bitmap[1] = 1U << (FATTR4_MODE - 32);
        pack_be32(blob, (uint32_t) mode);

        a.opopen.openhow.opentype                       = OPEN4_CREATE;
        a.opopen.openhow.how.mode                       = UNCHECKED4;
        a.opopen.openhow.how.createattrs.num_attrmask   = 2;
        a.opopen.openhow.how.createattrs.attrmask       = bitmap;
        a.opopen.openhow.how.createattrs.attr_vals.data = blob;
        a.opopen.openhow.how.createattrs.attr_vals.len  = 4;
    } else {
        a.opopen.openhow.opentype = OPEN4_NOCREATE;
    }
    a.opopen.claim.claim     = CLAIM_NULL;
    a.opopen.claim.file.data = (void *) name;
    a.opopen.claim.file.len  = (uint32_t) strlen(name);
    return a;
} /* op_open */

/* SETATTR(mode, owner, owner_group), the chmod+chown pjdfstest applies to an
 * already-created file before it probes the open.  Values are packed in
 * ascending attribute-bit order (RFC 7530 section 5): MODE(33), OWNER(36),
 * OWNER_GROUP(37).  owner and owner_group are utf8str_mixed, length-prefixed
 * and padded to a 4-byte boundary; chimera converts the text with strtoul(),
 * so a bare numeric string names the id directly. */
static struct nfs_argop4
op_setattr_mode_owner(
    int      mode,
    uint32_t uid)
{
    static uint32_t   bitmap[2];
    static uint8_t    blob[64];
    struct nfs_argop4 a;
    char              idtext[16];
    uint32_t          len = 0;
    uint32_t          idlen, padded;
    int               i;

    memset(&a, 0, sizeof(a));
    a.argop = OP_SETATTR;

    bitmap[0] = 0;
    bitmap[1] = (1U << (FATTR4_MODE - 32)) |
        (1U << (FATTR4_OWNER - 32)) |
        (1U << (FATTR4_OWNER_GROUP - 32));

    pack_be32(blob + len, (uint32_t) mode);
    len += 4;

    snprintf(idtext, sizeof(idtext), "%u", uid);
    idlen  = (uint32_t) strlen(idtext);
    padded = (idlen + 3) & ~3U;
    for (i = 0; i < 2; i++) {
        pack_be32(blob + len, idlen);
        len += 4;
        memset(blob + len, 0, padded);
        memcpy(blob + len, idtext, idlen);
        len += padded;
    }

    a.opsetattr.obj_attributes.num_attrmask   = 2;
    a.opsetattr.obj_attributes.attrmask       = bitmap;
    a.opsetattr.obj_attributes.attr_vals.data = blob;
    a.opsetattr.obj_attributes.attr_vals.len  = len;
    return a;
} /* op_setattr_mode_owner */

/* OPEN(CLAIM_FH): open the current filehandle rather than a name in a
 * directory.  This is the shape a Linux client sends whenever it already holds
 * valid cached attributes for the file -- NFSPROC4_CLNT_OPEN_NOATTR, which
 * PUTFHs the file itself and claims it by handle.  An ordinary
 * open(O_WRONLY) of a file the client has just looked at takes this path, not
 * the by-name one, so it is the path that actually carries most opens. */
static struct nfs_argop4
op_open_claim_fh(
    uint64_t    clientid,
    const char *owner,
    uint32_t    access)
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
    a.opopen.openhow.opentype = OPEN4_NOCREATE;
    a.opopen.claim.claim      = CLAIM_FH;
    return a;
} /* op_open_claim_fh */

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

/* ---- the matrix ----------------------------------------------------------- */

/* The file is owned OWNER_UID:OWNER_UID, and each mode is probed from both
 * sides -- as its owner, and as OTHER_UID, who is
 * neither the owner nor in its group.  Both share_access values are exercised
 * against every row, as is SHARE_ACCESS_BOTH -- the mode most real opens use,
 * and the one whose intent is carried by both flags at once.  A matrix that
 * only asserted denials would be satisfied by a server that denied
 * everything.
 *
 * Expectations are plain POSIX: the owner is judged by the owner digit alone,
 * even where the group or other digits are wider.
 * *INDENT-OFF* */
static const struct {
    int      mode;
    int      as_owner;
    uint32_t access;       /* OPEN4_SHARE_ACCESS_READ / _WRITE / _BOTH */
    uint32_t expect;
} cells[] = {
    { 0477, 1, OPEN4_SHARE_ACCESS_READ, NFS4_OK        },
    { 0477, 1, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0477, 1, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0477, 0, OPEN4_SHARE_ACCESS_READ, NFS4_OK        },
    { 0477, 0, OPEN4_SHARE_ACCESS_WRITE, NFS4_OK        },
    { 0477, 0, OPEN4_SHARE_ACCESS_BOTH, NFS4_OK        },
    { 0700, 1, OPEN4_SHARE_ACCESS_READ, NFS4_OK        },
    { 0700, 1, OPEN4_SHARE_ACCESS_WRITE, NFS4_OK        },
    { 0700, 1, OPEN4_SHARE_ACCESS_BOTH, NFS4_OK        },
    { 0700, 0, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0700, 0, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0700, 0, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0070, 1, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0070, 1, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0070, 1, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0070, 0, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0070, 0, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0070, 0, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0007, 1, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0007, 1, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0007, 1, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0007, 0, OPEN4_SHARE_ACCESS_READ, NFS4_OK        },
    { 0007, 0, OPEN4_SHARE_ACCESS_WRITE, NFS4_OK        },
    { 0007, 0, OPEN4_SHARE_ACCESS_BOTH, NFS4_OK        },
    { 0000, 1, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0000, 1, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0000, 1, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0000, 0, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0000, 0, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0000, 0, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0200, 1, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0200, 1, OPEN4_SHARE_ACCESS_WRITE, NFS4_OK        },
    { 0200, 1, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0200, 0, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0200, 0, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0200, 0, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0400, 1, OPEN4_SHARE_ACCESS_READ, NFS4_OK        },
    { 0400, 1, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0400, 1, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
    { 0400, 0, OPEN4_SHARE_ACCESS_READ, NFS4ERR_ACCESS },
    { 0400, 0, OPEN4_SHARE_ACCESS_WRITE, NFS4ERR_ACCESS },
    { 0400, 0, OPEN4_SHARE_ACCESS_BOTH, NFS4ERR_ACCESS },
};
/* *INDENT-ON* */

static const char *
access_name(uint32_t access)
{
    switch (access) {
        case OPEN4_SHARE_ACCESS_READ:
            return "READ";
        case OPEN4_SHARE_ACCESS_WRITE:
            return "WRITE";
        case OPEN4_SHARE_ACCESS_BOTH:
            return "BOTH";
        default:
            return "?";
    } /* switch */
} /* access_name */

int
main(void)
{
    struct mbt_env_opts opts = {
        /* Exact attributes and no name-cache short-circuit: an authorization
         * decision must be measured against what the backend holds now, not a
         * cached answer from the root-owned create that preceded it. */
        .disable_caches = 1,
    };
    struct mbt_env     *env = malloc(sizeof(*env));
    struct pc           pc;
    struct mbt_fh       root;
    struct prep         p;
    unsigned int        i;
    char                buf[160];

    setvbuf(stdout, NULL, _IONBF, 0);
    mbt_watchdog_arm(60);
    mbt_env_start_opts(env, &opts);
    env->nfs_v4_cb.recv_call_CB_COMPOUND = oa_cb_compound;

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

    printf("# --- NFSv4 OPEN authorization (file owned %d:%d) ---\n",
           OWNER_UID, OWNER_UID);

    for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
        uint32_t          actor = cells[i].as_owner ? OWNER_UID : OTHER_UID;
        char              fname[48];
        char              label[64];
        char              create_owner[48];
        char              probe_owner[48];
        struct nfs_argop4 ops[3];
        struct mbt_fh     file_fh;
        struct stateid4   create_sid;
        uint32_t          got;

        /* A fresh name per cell: no open-handle, name-cache or attribute-cache
         * entry an earlier cell left may answer this one.  A distinct open
         * owner per cell keeps the probing OPEN from coalescing onto the state
         * the creating open installed. */
        snprintf(fname, sizeof(fname), "f%u", i);
        snprintf(create_owner, sizeof(create_owner), "oa-create-%u", i);
        snprintf(probe_owner, sizeof(probe_owner), "oa-probe-%u", i);
        snprintf(label, sizeof(label), "%04o/%s/%s", (unsigned) cells[i].mode,
                 cells[i].as_owner ? "owner" : "other",
                 access_name(cells[i].access));

        /* Create it as root, 0644, then chmod+chown it into the cell's shape --
         * pjdfstest's sequence, and the one a kernel client reproduces with.
         * The mode the gate must judge is therefore one the create did not
         * set. */
        mbt_cred_set_uid(env, 0);
        ops[0] = op_putfh(&root);
        ops[1] = op_open(pc.clientid, create_owner, fname,
                         OPEN4_SHARE_ACCESS_BOTH, 0644);
        ops[2] = op_getfh();
        p      = pc_compound(&pc, ops, 3, 1);
        if (p.status != NFS4_OK || !p.fh.has) {
            snprintf(buf, sizeof(buf), "create %s failed: %u", fname, p.status);
            expect(label, 0, buf);
            continue;
        }
        file_fh    = p.fh;
        create_sid = p.open_sid;

        /* Close the creating open before probing: an open still held would let
         * the probe coalesce onto an already-granted share reservation. */
        ops[0] = op_putfh(&file_fh);
        ops[1] = op_close(&create_sid);
        p      = pc_compound(&pc, ops, 2, 1);
        if (p.status != NFS4_OK) {
            snprintf(buf, sizeof(buf), "close of created %s failed: %u",
                     fname, p.status);
            expect(label, 0, buf);
            continue;
        }

        ops[0] = op_putfh(&file_fh);
        ops[1] = op_setattr_mode_owner(cells[i].mode, OWNER_UID);
        p      = pc_compound(&pc, ops, 2, 1);
        if (p.status != NFS4_OK) {
            snprintf(buf, sizeof(buf), "chmod/chown of %s failed: %u",
                     fname, p.status);
            expect(label, 0, buf);
            continue;
        }

        /* Probe it.  This OPEN's own status is the whole measurement. */
        mbt_cred_set_uid(env, actor);
        ops[0] = op_putfh(&root);
        ops[1] = op_open(pc.clientid, probe_owner, fname, cells[i].access, -1);
        p      = pc_compound(&pc, ops, 2, 1);

        /* The OPEN's own status, or nothing: a compound that short-circuited
         * before OPEN (a failed SEQUENCE or PUTFH) must fail the cell rather
         * than substitute that operation's error, which could coincide with
         * the expected one and report a pass for an OPEN that never ran. */
        if (p.open_idx < 0) {
            snprintf(buf, sizeof(buf),
                     "compound returned %u without reaching OPEN", p.status);
            expect(label, 0, buf);
            mbt_cred_set_uid(env, 0);
            continue;
        }
        got = p.st[p.open_idx];

        snprintf(buf, sizeof(buf),
                 "OPEN(NOCREATE, share_access=%s) mode %04o as uid %u: "
                 "expected %u, got %u",
                 access_name(cells[i].access), (unsigned) cells[i].mode,
                 actor, cells[i].expect, got);
        expect(label, got == cells[i].expect, buf);

        /* Release any state the probe was granted, so a wrongly granted open
         * cannot hold the file across the remaining cells. */
        if (got == NFS4_OK) {
            struct stateid4 probe_sid = p.open_sid;

            ops[0] = op_putfh(&file_fh);
            ops[1] = op_close(&probe_sid);
            pc_compound(&pc, ops, 2, 1);
        }

        /* The same cell again, claimed by filehandle.  Authorization cannot
         * depend on how the client named the file: a client that happens to
         * hold cached attributes sends this form instead, and it must reach
         * the same verdict as the by-name open above. */
        snprintf(probe_owner, sizeof(probe_owner), "oa-probefh-%u", i);
        snprintf(label, sizeof(label), "%04o/%s/%s/CLAIM_FH",
                 (unsigned) cells[i].mode,
                 cells[i].as_owner ? "owner" : "other",
                 access_name(cells[i].access));

        ops[0] = op_putfh(&file_fh);
        ops[1] = op_open_claim_fh(pc.clientid, probe_owner, cells[i].access);
        p      = pc_compound(&pc, ops, 2, 1);

        if (p.open_idx < 0) {
            snprintf(buf, sizeof(buf),
                     "compound returned %u without reaching OPEN", p.status);
            expect(label, 0, buf);
            mbt_cred_set_uid(env, 0);
            continue;
        }
        got = p.st[p.open_idx];

        snprintf(buf, sizeof(buf),
                 "OPEN(CLAIM_FH, share_access=%s) mode %04o as uid %u: "
                 "expected %u, got %u",
                 access_name(cells[i].access), (unsigned) cells[i].mode,
                 actor, cells[i].expect, got);
        expect(label, got == cells[i].expect, buf);

        if (got == NFS4_OK) {
            struct stateid4 probe_sid = p.open_sid;

            ops[0] = op_putfh(&file_fh);
            ops[1] = op_close(&probe_sid);
            pc_compound(&pc, ops, 2, 1);
        }

        mbt_cred_set_uid(env, 0);
    }

    mbt_env_stop(env);
    free(env);

    if (failures) {
        fprintf(stderr, "%d NFSv4 OPEN authorization check(s) FAILED\n",
                failures);
        return 1;
    }
    printf("all NFSv4 OPEN authorization checks passed\n");
    return 0;
} /* main */
