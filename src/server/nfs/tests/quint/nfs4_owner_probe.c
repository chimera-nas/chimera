/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * NFSv4 owner strings the server cannot map.
 *
 * RFC 7530 section 5.9: owner and owner_group travel as "identifier@domain"
 * strings, and a server that cannot translate one SHOULD answer
 * NFS4ERR_BADOWNER rather than store a substitute.  A Linux client with
 * idmapping on sends "user@its-domain"; when the server's nss does not know
 * the user, the server used to answer NFS4_OK and store uid 0, so any client
 * allowed to chown could hand a file to root by naming a user the server did
 * not know.
 *
 * Each cell presents one who string through one operation -- SETATTR,
 * OPEN(create) with createattrs or EXCLUSIVE4_1 cva_attrs, or CREATE of a
 * directory -- and measures that operation's own status.  A refused SETATTR
 * must leave the file's owner alone, and a refused create must leave no
 * object behind: a LOOKUP of the name answers NFS4ERR_NOENT.  The numeric
 * cells run first and set the owner the refusal cells expect to find intact.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define WHO_MAX  256
#define BASE_UID "25201"
#define BASE_GID "2000"
#define BOGUS    "idmaptest_unmapped@totally-bogus-nonexistent-domain.invalid"

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

/* ---- one compound's parsed reply ------------------------------------------ */

struct prep {
    int             done;
    int             rpc_err;
    uint32_t        status;
    int             nres;
    uint32_t        st[10];          /* per-op status */
    struct mbt_fh   fh;              /* last GETFH */
    struct stateid4 open_sid;        /* last OPEN stateid */
    int             meas_idx;        /* index of the measured op (SETATTR,
                                      * OPEN or CREATE) in st[] */
    int             lookup_idx;      /* index of the last LOOKUP in st[] */
    int             has_owner;       /* GETATTR returned FATTR4_OWNER */
    char            owner[WHO_MAX];
    int             has_group;       /* GETATTR returned FATTR4_OWNER_GROUP */
    char            group[WHO_MAX];
    uint64_t        clientid;        /* EXCHANGE_ID */
    uint32_t        eir_seq;
    uint8_t         sessionid[16];   /* CREATE_SESSION */
};

static uint32_t
unpack_be32(const uint8_t *p)
{
    return ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) |
           ((uint32_t) p[2] << 8) | (uint32_t) p[3];
} /* unpack_be32 */

/* One XDR opaque out of a GETATTR value blob into `out` (NUL-terminated).
 * Returns the bytes consumed, 0 if the blob is short or the string too long. */
static uint32_t
unpack_who(
    const uint8_t *p,
    uint32_t       avail,
    char          *out)
{
    uint32_t len, padded;

    if (avail < 4) {
        return 0;
    }
    len    = unpack_be32(p);
    padded = (len + 3) & ~3u;
    if (len >= WHO_MAX || avail < 4 + padded) {
        return 0;
    }
    memcpy(out, p + 4, len);
    out[len] = '\0';
    return 4 + padded;
} /* unpack_who */

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
    p->rpc_err    = status;
    p->meas_idx   = -1;
    p->lookup_idx = -1;
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
                    p->st[i]      = r->oplookup.status;
                    p->lookup_idx = (int) i;
                    break;
                case OP_GETFH:
                    p->st[i] = r->opgetfh.status;
                    if (p->st[i] == NFS4_OK) {
                        mbt_copy_fh(&p->fh, &r->opgetfh.resok4.object);
                    }
                    break;
                case OP_OPEN:
                    p->st[i]    = r->opopen.status;
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        p->open_sid = r->opopen.resok4.stateid;
                    }
                    break;
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_SETATTR:
                    p->st[i]    = r->opsetattr.status;
                    p->meas_idx = (int) i;
                    break;
                case OP_CREATE:
                    p->st[i]    = r->opcreate.status;
                    p->meas_idx = (int) i;
                    break;
                case OP_GETATTR:
                    p->st[i] = r->opgetattr.status;
                    /* owner (bit 36) precedes owner_group (bit 37) in the
                     * value blob, each an XDR opaque. */
                    if (p->st[i] == NFS4_OK &&
                        r->opgetattr.resok4.obj_attributes.num_attrmask >= 2) {
                        const struct fattr4 *f     = &r->opgetattr.resok4.obj_attributes;
                        const uint8_t       *v     = f->attr_vals.data;
                        uint32_t             avail = f->attr_vals.len;
                        uint32_t             n;

                        if (f->attrmask[1] & (1U << (FATTR4_OWNER - 32))) {
                            n = unpack_who(v, avail, p->owner);
                            if (n) {
                                p->has_owner = 1;
                                v           += n;
                                avail       -= n;
                            }
                        }
                        if (f->attrmask[1] & (1U << (FATTR4_OWNER_GROUP - 32))) {
                            n = unpack_who(v, avail, p->group);
                            if (n) {
                                p->has_group = 1;
                            }
                        }
                    }
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
    static char                       owner[] = "owner-probe";
    struct channel_attrs4             chan    = {
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
    op.opcreate_session.csa_flags           = 0;
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
op_close(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_CLOSE;
    a.opclose.seqid        = 0;
    a.opclose.open_stateid = *sid;
    return a;
} /* op_close */

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

/* XDR opaque of `who` at `p`: big-endian length, bytes, zero pad to 4.
 * Returns the bytes written. */
static uint32_t
pack_who(
    uint8_t    *p,
    const char *who)
{
    uint32_t len    = (uint32_t) strlen(who);
    uint32_t padded = (len + 3) & ~3u;

    pack_be32(p, len);
    memcpy(p + 4, who, len);
    memset(p + 4 + len, 0, padded - len);
    return 4 + padded;
} /* pack_who */

/* MODE + OWNER create attributes: the bitmap and value blob are static
 * because the argument structure only borrows them, and the compound is
 * sent before the next call overwrites them. */
static void
create_attrs_who(
    struct fattr4 *f,
    uint32_t       mode,
    const char    *who)
{
    static uint32_t bitmap[2];
    static uint8_t  blob[4 + 4 + WHO_MAX + 4];
    uint32_t        n;

    bitmap[0] = 0;
    bitmap[1] = (1U << (FATTR4_MODE - 32)) | (1U << (FATTR4_OWNER - 32));
    pack_be32(blob, mode);
    n = 4 + pack_who(blob + 4, who);

    f->num_attrmask   = 2;
    f->attrmask       = bitmap;
    f->attr_vals.data = blob;
    f->attr_vals.len  = n;
} /* create_attrs_who */

/* OPEN(CLAIM_NULL).  `create` makes it a creating open with mode 0644;
 * otherwise OPEN4_NOCREATE. */
static struct nfs_argop4
op_open(
    uint64_t    clientid,
    const char *owner,
    const char *name,
    uint32_t    access,
    int         create)
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
    if (create) {
        bitmap[0] = 0;
        bitmap[1] = 1U << (FATTR4_MODE - 32);
        pack_be32(blob, 0644);

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

/* Creating OPEN that carries `who` as FATTR4_OWNER, through UNCHECKED4
 * createattrs or, with `exclusive41`, EXCLUSIVE4_1 cva_attrs. */
static struct nfs_argop4
op_open_create_who(
    uint64_t    clientid,
    const char *owner,
    const char *name,
    const char *who,
    int         exclusive41)
{
    struct nfs_argop4 a = op_open(clientid, owner, name,
                                  OPEN4_SHARE_ACCESS_BOTH, 1);

    if (exclusive41) {
        a.opopen.openhow.how.mode = EXCLUSIVE4_1;
        memset(a.opopen.openhow.how.ch_createboth.cva_verf, 0x5a,
               NFS4_VERIFIER_SIZE);
        create_attrs_who(&a.opopen.openhow.how.ch_createboth.cva_attrs,
                         0644, who);
    } else {
        a.opopen.openhow.how.mode = UNCHECKED4;
        create_attrs_who(&a.opopen.openhow.how.createattrs, 0644, who);
    }
    return a;
} /* op_open_create_who */

/* CREATE of a directory carrying `who` as FATTR4_OWNER. */
static struct nfs_argop4
op_create_dir_who(
    const char *name,
    const char *who)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                 = OP_CREATE;
    a.opcreate.objtype.type = NF4DIR;
    a.opcreate.objname.data = (void *) name;
    a.opcreate.objname.len  = (uint32_t) strlen(name);
    create_attrs_who(&a.opcreate.createattrs, 0755, who);
    return a;
} /* op_create_dir_who */

/* SETATTR of FATTR4_OWNER (is_group == 0) or FATTR4_OWNER_GROUP (1) through
 * the anonymous stateid, as a client's chown sends it. */
static struct nfs_argop4
op_setattr_who(
    const char *who,
    int         is_group)
{
    static uint32_t   bitmap[2];
    static uint8_t    blob[4 + WHO_MAX + 4];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    bitmap[0] = 0;
    bitmap[1] = is_group ? (1U << (FATTR4_OWNER_GROUP - 32))
                         : (1U << (FATTR4_OWNER - 32));

    a.argop                                   = OP_SETATTR;
    a.opsetattr.obj_attributes.num_attrmask   = 2;
    a.opsetattr.obj_attributes.attrmask       = bitmap;
    a.opsetattr.obj_attributes.attr_vals.data = blob;
    a.opsetattr.obj_attributes.attr_vals.len  = pack_who(blob, who);
    return a;
} /* op_setattr_who */

static struct nfs_argop4
op_getattr_owner(void)
{
    static uint32_t   bitmap[2];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop   = OP_GETATTR;
    bitmap[0] = 0;
    bitmap[1] = (1U << (FATTR4_OWNER - 32)) |
        (1U << (FATTR4_OWNER_GROUP - 32));
    a.opgetattr.num_attr_request = 2;
    a.opgetattr.attr_request     = bitmap;
    return a;
} /* op_getattr_owner */

/* ---- the cells ------------------------------------------------------------ */

enum cell_kind {
    K_SETATTR,   /* SETATTR on the probe file */
    K_OPEN,      /* OPEN(create, UNCHECKED4 createattrs) of `name` */
    K_EXCL41,    /* OPEN(create, EXCLUSIVE4_1 cva_attrs) of `name` */
    K_CREATE,    /* CREATE(NF4DIR) of `name` */
};

/* *INDENT-OFF* */
static const struct {
    const char    *label;
    enum cell_kind kind;
    const char    *who;
    int            is_group;
    const char    *name;         /* object the create cells name */
    uint32_t       expect;       /* the measured op's status */
    const char    *expect_who;   /* SETATTR: owner (or group) afterwards */
} cells[] = {
    { "setattr-numeric-owner",               K_SETATTR, BASE_UID, 0, NULL,          NFS4_OK,          BASE_UID },
    { "setattr-numeric-group",               K_SETATTR, BASE_GID, 1, NULL,          NFS4_OK,          BASE_GID },
    { "setattr-unresolvable-owner",          K_SETATTR, BOGUS,    0, NULL,          NFS4ERR_BADOWNER, BASE_UID },
    { "setattr-unresolvable-group",          K_SETATTR, BOGUS,    1, NULL,          NFS4ERR_BADOWNER, BASE_GID },
    { "setattr-special-who-owner",           K_SETATTR, "OWNER@", 0, NULL,          NFS4ERR_BADOWNER, BASE_UID },
    { "open-create-unresolvable-owner",      K_OPEN,    BOGUS,    0, "o_unchecked", NFS4ERR_BADOWNER, NULL     },
    { "exclusive41-create-unresolvable-owner", K_EXCL41, BOGUS,   0, "o_excl41",    NFS4ERR_BADOWNER, NULL     },
    { "create-dir-unresolvable-owner",       K_CREATE,  BOGUS,    0, "d_bad",       NFS4ERR_BADOWNER, NULL     },
};
/* *INDENT-ON* */

static void
run_setattr_cell(
    struct pc           *pc,
    const struct mbt_fh *file,
    unsigned int         i)
{
    const char       *label = cells[i].label;
    const char       *which = cells[i].is_group ? "owner_group" : "owner";
    char              buf[400];
    struct nfs_argop4 ops[3];
    struct prep       p;
    const char       *got_who;
    int               has;

    ops[0] = op_putfh(file);
    ops[1] = op_setattr_who(cells[i].who, cells[i].is_group);
    ops[2] = op_getattr_owner();
    p      = pc_compound(pc, ops, 3, 1);

    /* The SETATTR's own status, or nothing: a compound that short-circuited
     * before SETATTR must fail the cell rather than substitute an earlier
     * operation's error. */
    if (p.meas_idx < 0) {
        snprintf(buf, sizeof(buf),
                 "compound returned %u without reaching SETATTR", p.status);
        expect(label, 0, buf);
        return;
    }
    snprintf(buf, sizeof(buf), "SETATTR(%s=\"%s\"): expected %u, got %u",
             which, cells[i].who, cells[i].expect, p.st[p.meas_idx]);
    expect(label, p.st[p.meas_idx] == cells[i].expect, buf);

    /* What the file's owner is now, re-read on its own: the compound above
     * stops at a refused SETATTR before its GETATTR. */
    ops[0]  = op_putfh(file);
    ops[1]  = op_getattr_owner();
    p       = pc_compound(pc, ops, 2, 1);
    has     = cells[i].is_group ? p.has_group : p.has_owner;
    got_who = cells[i].is_group ? p.group : p.owner;
    snprintf(buf, sizeof(buf), "%s afterwards: expected \"%s\", got %s%s%s",
             which, cells[i].expect_who, has ? "\"" : "(none",
             has ? got_who : "", has ? "\"" : ")");
    expect(label, p.status == NFS4_OK && has &&
           strcmp(got_who, cells[i].expect_who) == 0, buf);
} /* run_setattr_cell */

static void
run_create_cell(
    struct pc           *pc,
    const struct mbt_fh *root,
    unsigned int         i)
{
    const char       *label = cells[i].label;
    const char       *opname;
    char              buf[400];
    char              owner[40];
    struct nfs_argop4 ops[3];
    struct prep       p;
    int               n = 0;

    snprintf(owner, sizeof(owner), "owner-create-%u", i);

    ops[n++] = op_putfh(root);
    switch (cells[i].kind) {
        case K_OPEN:
            opname   = "OPEN(create, UNCHECKED4)";
            ops[n++] = op_open_create_who(pc->clientid, owner, cells[i].name,
                                          cells[i].who, 0);
            ops[n++] = op_getfh();
            break;
        case K_EXCL41:
            opname   = "OPEN(create, EXCLUSIVE4_1)";
            ops[n++] = op_open_create_who(pc->clientid, owner, cells[i].name,
                                          cells[i].who, 1);
            ops[n++] = op_getfh();
            break;
        default:
            opname   = "CREATE(NF4DIR)";
            ops[n++] = op_create_dir_who(cells[i].name, cells[i].who);
            break;
    } /* switch */
    p = pc_compound(pc, ops, n, 1);

    if (p.meas_idx < 0) {
        snprintf(buf, sizeof(buf),
                 "compound returned %u without reaching %s", p.status, opname);
        expect(label, 0, buf);
        return;
    }
    snprintf(buf, sizeof(buf), "%s %s owner=\"%s\": expected %u, got %u",
             opname, cells[i].name, cells[i].who, cells[i].expect,
             p.st[p.meas_idx]);
    expect(label, p.st[p.meas_idx] == cells[i].expect, buf);

    /* An open the server should have refused still holds state: release it
     * so nothing outlives the cell. */
    if (cells[i].kind != K_CREATE && p.st[p.meas_idx] == NFS4_OK && p.fh.has) {
        ops[0] = op_putfh(&p.fh);
        ops[1] = op_close(&p.open_sid);
        pc_compound(pc, ops, 2, 1);
    }

    /* A refused create leaves no object behind. */
    ops[0] = op_putfh(root);
    ops[1] = op_lookup(cells[i].name);
    p      = pc_compound(pc, ops, 2, 1);
    snprintf(buf, sizeof(buf), "LOOKUP %s afterwards: expected %u, got %u",
             cells[i].name, NFS4ERR_NOENT,
             p.lookup_idx >= 0 ? p.st[p.lookup_idx] : p.status);
    expect(label, p.lookup_idx >= 0 && p.st[p.lookup_idx] == NFS4ERR_NOENT,
           buf);
} /* run_create_cell */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env_opts opts = {
        /* Exact attributes: the owner read back must be what the backend
         * holds now, not a cached answer from before the SETATTR. */
        .disable_caches = 1,
    };
    struct mbt_env     *env = malloc(sizeof(*env));
    struct pc           pc;
    struct mbt_fh       root, file;
    unsigned int        i;

    setvbuf(stdout, NULL, _IONBF, 0);
    /* Passthrough backends create the export root on the host; keep its mode
     * the one the model expects, as the replayers do. */
    umask(0);
    mbt_watchdog_arm(60);

    /* Usage: nfs4_owner_probe [<module>] */
    opts.module = argc > 1 ? argv[1] : "memfs";
    mbt_env_start_opts(env, &opts);

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

    printf("# --- NFSv4 owner strings the server cannot map (%s) ---\n",
           opts.module);

    /* The file the SETATTR cells act on. */
    {
        struct nfs_argop4 ops[3];
        struct prep       p;

        ops[0] = op_putfh(&root);
        ops[1] = op_open(pc.clientid, "owner-setup", "owner.bin",
                         OPEN4_SHARE_ACCESS_BOTH, 1);
        ops[2] = op_getfh();
        p      = pc_compound(&pc, ops, 3, 1);
        if (p.status != NFS4_OK || !p.fh.has) {
            fprintf(stderr, "create owner.bin failed: %u\n", p.status);
            return 2;
        }
        file   = p.fh;
        ops[0] = op_putfh(&file);
        ops[1] = op_close(&p.open_sid);
        p      = pc_compound(&pc, ops, 2, 1);
        if (p.status != NFS4_OK) {
            fprintf(stderr, "close owner.bin failed: %u\n", p.status);
            return 2;
        }
    }

    for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
        if (cells[i].kind == K_SETATTR) {
            run_setattr_cell(&pc, &file, i);
        } else {
            run_create_cell(&pc, &root, i);
        }
    }

    mbt_env_stop(env);
    free(env);

    if (failures) {
        fprintf(stderr, "%d NFSv4 owner check(s) FAILED\n", failures);
        return 1;
    }
    printf("all NFSv4 owner checks passed\n");
    return 0;
} /* main */
