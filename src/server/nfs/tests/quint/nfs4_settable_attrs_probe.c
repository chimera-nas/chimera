/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * NFSv4 attributes a client may set: what supported_attrs advertises, SETATTR,
 * OPEN and CREATE must accept, and who may set time_create.
 *
 * A client learns which attributes it can set from supported_attrs.  The
 * macOS client puts archive, mode and time_create in the createattrs of every
 * OPEN and CREATE when the server advertises them.  A server that advertises
 * a writable attribute and then refuses it on a set fails every create such
 * a client makes: no new file, directory or symbolic link, each one "Invalid
 * argument".  No other test sets these attributes, so nothing else would
 * notice the advertised and accepted sets drifting apart.
 *
 * The first part holds the two sets together.  For each attribute RFC 7530
 * section 5 defines as writable, one SETATTR sets it alone on a regular file:
 *   - advertised in supported_attrs: NFS4_OK;
 *   - not advertised: NFS4ERR_ATTRNOTSUPP, the answer for an attribute the
 *     server does not support (RFC 7530 section 16.32.5), from SETATTR and
 *     from a VERIFY that names it.
 * A VERIFY of time_create, which is advertised, compares it, or answers
 * NFS4ERR_ATTRNOTSUPP on an object that has no birth time.  This part runs
 * at NFSv4.0, NFSv4.1 and NFSv4.2, whose supported_attrs differ.
 *
 * The second part follows time_create, which a backend may or may not be able
 * to store.  Each set that carries it succeeds:
 *   - SETATTR of time_create alone, and with time_access_set and
 *     time_modify_set (and mode), as a copy that keeps a file's dates sends
 *     it.  An nseconds of 10^9 or more is NFS4ERR_INVAL and a short value
 *     NFS4ERR_BADXDR;
 *   - OPEN(create, UNCHECKED4) and CREATE of a directory with mode and
 *     time_create, the createattrs a macOS create carries;
 *   - CREATE of a symbolic link with the same createattrs.
 * The reply's attrset names time_create exactly when a following GETATTR
 * reads the value back.  A backend that stores a birth time (memfs, diskfs,
 * cairn) reports it set; one that cannot (the linux and io_uring
 * passthroughs: no system call sets a file's birth time) completes the
 * operation and leaves it out.  A symbolic link's CREATE reply never names it.
 * An UNCHECKED4 OPEN of a file that already exists ignores its createattrs
 * (RFC 7530 section 16.16.5): its attrset names none of them and the birth
 * time stays as it was.
 *
 * The third part is about who may set it.  RFC 7530 section 6.2.1.3.1 puts
 * SETATTR of time_create under ACE4_WRITE_ATTRIBUTES, as for the other times
 * set to a client's value.  Run as a user who neither owns a file nor holds
 * that right, a SETATTR that sets time_create -- alone, or with a size change
 * the user may make -- leaves the birth time alone.  With the size change it
 * answers NFS4ERR_PERM on every backend; alone, NFS4ERR_PERM where the backend
 * stores a birth time, while a passthrough may answer NFS4_OK having stored
 * nothing.  The file's owner may set it.
 *
 * Part two's SETATTRs and CREATEs run at NFSv4.0, the minor version the macOS
 * client mounts with; the creating OPENs run in an NFSv4.1 session, which
 * reaches the same attribute code without the open-owner sequencing of
 * NFSv4.0.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define W1(attr) (1U << ((attr) - 32))

/* The first birth time a cell sets: 2020-01-02T03:04:05.123456789Z. */
#define BTIME_SEC       1577934245ULL
#define BTIME_NSEC      123456789U

/* The access and modification time a SET_TO_CLIENT_TIME4 carries:
 * 2021-03-04T05:06:07Z. */
#define CLIENT_TIME_SEC 1614834367ULL

/* The values a fattr4 built by fattr_build carries.  Each time_create cell
 * moves the birth time on (btime_next) so that it cannot pass on a value an
 * earlier cell stored. */
static uint64_t    btime_sec    = BTIME_SEC;
static uint32_t    btime_nsec   = BTIME_NSEC;
static int         client_times = 0;   /* time_*_set: SET_TO_CLIENT_TIME4 */
static uint32_t    mode_value   = 0644;
static const char *owner_value  = "25201";

static void
btime_next(void)
{
    btime_sec += 86400;
} /* btime_next */

static int         failures = 0;

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

/* ---- one probed 4.1 or 4.2 client ----------------------------------------- */

struct pc {
    struct mbt_env        *env;
    struct evpl_rpc2_conn *conn;
    uint64_t               clientid;
    uint8_t                sessionid[16];
    uint32_t               slot_seq;
    int                    has_session;
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
                                      * VERIFY, OPEN or CREATE) in st[] */
    uint32_t        set[2];          /* that op's attrsset / attrset, words
                                      * 0-1 */
    int             getattr_idx;     /* index of the last GETATTR in st[] */
    uint32_t        gmask[3];        /* its reply bitmap, words 0-2 */
    uint8_t         gvals[64];       /* its value blob */
    uint32_t        gvals_len;
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

static uint64_t
unpack_be64(const uint8_t *p)
{
    return ((uint64_t) unpack_be32(p) << 32) | unpack_be32(p + 4);
} /* unpack_be64 */

static void
prep_set(
    struct prep    *p,
    uint32_t        num,
    const uint32_t *words)
{
    uint32_t w;

    for (w = 0; w < 2; w++) {
        p->set[w] = w < num ? words[w] : 0;
    }
} /* prep_set */

static void
prep_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct prep *p = private_data;
    uint32_t     i, w;

    (void) evpl;
    (void) verf;
    p->rpc_err     = status;
    p->meas_idx    = -1;
    p->getattr_idx = -1;
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
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        p->open_sid = r->opopen.resok4.stateid;
                        prep_set(p, r->opopen.resok4.num_attrset,
                                 r->opopen.resok4.attrset);
                    }
                    break;
                case OP_CLOSE:
                    p->st[i] = r->opclose.status;
                    break;
                case OP_SETATTR:
                    p->st[i]    = r->opsetattr.status;
                    p->meas_idx = (int) i;
                    prep_set(p, r->opsetattr.num_attrsset,
                             r->opsetattr.attrsset);
                    break;
                case OP_VERIFY:
                    p->st[i]    = r->opverify.status;
                    p->meas_idx = (int) i;
                    break;
                case OP_CREATE:
                    p->st[i]    = r->opcreate.status;
                    p->meas_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        prep_set(p, r->opcreate.resok4.num_attrset,
                                 r->opcreate.resok4.attrset);
                    }
                    break;
                case OP_GETATTR:
                    p->st[i]       = r->opgetattr.status;
                    p->getattr_idx = (int) i;
                    if (p->st[i] == NFS4_OK) {
                        const struct fattr4 *f = &r->opgetattr.resok4.obj_attributes;

                        for (w = 0; w < 3; w++) {
                            p->gmask[w] = w < f->num_attrmask ?
                                f->attrmask[w] : 0;
                        }
                        p->gvals_len = f->attr_vals.len;
                        if (p->gvals_len > sizeof(p->gvals)) {
                            p->gvals_len = sizeof(p->gvals);
                        }
                        memcpy(p->gvals, f->attr_vals.data, p->gvals_len);
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

/* One compound at `minor`: NFSv4.1 and NFSv4.2 go through the client's
 * session once it has one, NFSv4.0 is sent as it stands (none of the
 * stateless operations here needs a client). */
static struct prep
pc_compound(
    struct pc         *pc,
    uint32_t           minor,
    struct nfs_argop4 *ops,
    int                nops)
{
    struct nfs_argop4    argarray[10];
    struct COMPOUND4args args;
    struct prep          p;
    int                  base = 0;
    int                  i;

    memset(&p, 0, sizeof(p));
    memset(&args, 0, sizeof(args));

    if (minor > 0 && pc->has_session) {
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
    args.minorversion = minor;
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

/* A client of minor version `minor` (1 or 2) with a session, owner `owner`. */
static void
pc_setup(
    struct pc      *pc,
    struct mbt_env *env,
    uint32_t        minor,
    const char     *owner)
{
    struct evpl_rpc2_program         *cb_programs[1];
    struct evpl_endpoint             *ep;
    struct nfs_argop4                 op;
    struct prep                       p;
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
    pc->env        = env;
    cb_programs[0] = &env->nfs_v4_cb.rpc2;
    ep             = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                        "127.0.0.1", 2049);
    pc->conn = evpl_rpc2_client_connect(env->rpc2_thread, EVPL_STREAM_INPROC,
                                        ep, cb_programs, 1, pc);

    memset(&op, 0, sizeof(op));
    op.argop = OP_EXCHANGE_ID;
    memset(op.opexchange_id.eia_clientowner.co_verifier, 1, 8);
    op.opexchange_id.eia_clientowner.co_ownerid.data = (void *) owner;
    op.opexchange_id.eia_clientowner.co_ownerid.len  = (uint32_t) strlen(owner);
    op.opexchange_id.eia_state_protect.spa_how       = SP4_NONE;
    p                                                = pc_compound(pc, minor, &op, 1);
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
    p                                       = pc_compound(pc, minor, &op, 1);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "CREATE_SESSION failed: %u\n", p.status);
        exit(2);
    }
    memcpy(pc->sessionid, p.sessionid, 16);
    pc->has_session = 1;

    memset(&op, 0, sizeof(op));
    op.argop                         = OP_RECLAIM_COMPLETE;
    op.opreclaim_complete.rca_one_fs = 0;
    p                                = pc_compound(pc, minor, &op, 1);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "RECLAIM_COMPLETE failed: %u\n", p.status);
        exit(2);
    }
} /* pc_setup */

/* ---- attribute values ----------------------------------------------------- */

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

static void
pack_be64(
    uint8_t *p,
    uint64_t v)
{
    pack_be32(p, (uint32_t) (v >> 32));
    pack_be32(p + 4, (uint32_t) (v & 0xffffffffU));
} /* pack_be64 */

/* XDR opaque of `s` at `p`: big-endian length, bytes, zero pad to 4.  Returns
 * the bytes written. */
static uint32_t
pack_str(
    uint8_t    *p,
    const char *s)
{
    uint32_t len    = (uint32_t) strlen(s);
    uint32_t padded = (len + 3) & ~3u;

    pack_be32(p, len);
    memcpy(p + 4, s, len);
    memset(p + 4 + len, 0, padded - len);
    return 4 + padded;
} /* pack_str */

/* A well-formed value for one writable attribute, as it travels in a fattr4
 * value blob.  Returns the bytes written. */
static uint32_t
pack_value(
    uint8_t     *p,
    unsigned int attr)
{
    switch (attr) {
        case FATTR4_SIZE:
            pack_be64(p, 0);
            return 8;
        case FATTR4_ACL:
            /* One ACE: ALLOW EVERYONE@ read. */
            pack_be32(p, 1);
            pack_be32(p + 4, ACE4_ACCESS_ALLOWED_ACE_TYPE);
            pack_be32(p + 8, 0);
            pack_be32(p + 12, ACE4_READ_DATA);
            return 16 + pack_str(p + 16, "EVERYONE@");
        case FATTR4_ARCHIVE:
        case FATTR4_HIDDEN:
        case FATTR4_SYSTEM:
            pack_be32(p, 0);
            return 4;
        case FATTR4_MIMETYPE:
            return pack_str(p, "text/plain");
        case FATTR4_MODE:
            pack_be32(p, mode_value);
            return 4;
        case FATTR4_OWNER:
            return pack_str(p, owner_value);
        case FATTR4_OWNER_GROUP:
            return pack_str(p, "2000");
        case FATTR4_TIME_ACCESS_SET:
        case FATTR4_TIME_MODIFY_SET:
            if (client_times) {
                pack_be32(p, SET_TO_CLIENT_TIME4);
                pack_be64(p + 4, CLIENT_TIME_SEC);
                pack_be32(p + 12, 0);
                return 16;
            }
            pack_be32(p, SET_TO_SERVER_TIME4);
            return 4;
        case FATTR4_TIME_BACKUP:
        case FATTR4_TIME_CREATE:
            pack_be64(p, btime_sec);
            pack_be32(p + 8, btime_nsec);
            return 12;
        default:
            fprintf(stderr, "no value for attribute %u\n", attr);
            exit(2);
    } /* switch */
} /* pack_value */

/* The storage one fattr4 borrows: the caller keeps it alive until its compound
 * has been sent. */
struct fattr_buf {
    uint32_t bitmap[2];
    uint8_t  blob[128];
};

/* A fattr4 of `n` attributes, which `attrs` lists in ascending order -- the
 * order their values take in the blob. */
static void
fattr_build(
    struct fattr4      *f,
    struct fattr_buf   *b,
    const unsigned int *attrs,
    int                 n)
{
    uint32_t len = 0;
    int      i;

    b->bitmap[0] = 0;
    b->bitmap[1] = 0;
    for (i = 0; i < n; i++) {
        b->bitmap[attrs[i] / 32] |= 1U << (attrs[i] % 32);
        len                      += pack_value(b->blob + len, attrs[i]);
    }
    f->num_attrmask   = 2;
    f->attrmask       = b->bitmap;
    f->attr_vals.data = b->blob;
    f->attr_vals.len  = len;
} /* fattr_build */

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
op_close(const struct stateid4 *sid)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                = OP_CLOSE;
    a.opclose.seqid        = 0;
    a.opclose.open_stateid = *sid;
    return a;
} /* op_close */

/* GETATTR of one attribute.  The bitmap is static because the argument
 * structure only borrows it, and the compound is sent before the next call
 * overwrites it. */
static struct nfs_argop4
op_getattr(unsigned int attr)
{
    static uint32_t   bitmap[2];
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                      = OP_GETATTR;
    bitmap[0]                    = 0;
    bitmap[1]                    = 0;
    bitmap[attr / 32]            = 1U << (attr % 32);
    a.opgetattr.num_attr_request = 2;
    a.opgetattr.attr_request     = bitmap;
    return a;
} /* op_getattr */

/* SETATTR of `n` attributes through the anonymous stateid. */
static struct nfs_argop4
op_setattr(
    struct fattr_buf   *b,
    const unsigned int *attrs,
    int                 n)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_SETATTR;
    fattr_build(&a.opsetattr.obj_attributes, b, attrs, n);
    return a;
} /* op_setattr */

/* VERIFY that the object's attributes are the `n` given. */
static struct nfs_argop4
op_verify(
    struct fattr_buf   *b,
    const unsigned int *attrs,
    int                 n)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop = OP_VERIFY;
    fattr_build(&a.opverify.obj_attributes, b, attrs, n);
    return a;
} /* op_verify */

/* OPEN(CLAIM_NULL) creating `name` with UNCHECKED4 and `n` createattrs. */
static struct nfs_argop4
op_open_create(
    uint64_t            clientid,
    const char         *owner,
    const char         *name,
    struct fattr_buf   *b,
    const unsigned int *attrs,
    int                 n)
{
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                   = OP_OPEN;
    a.opopen.seqid            = 0;
    a.opopen.share_access     = OPEN4_SHARE_ACCESS_BOTH;
    a.opopen.share_deny       = 0;
    a.opopen.owner.clientid   = clientid;
    a.opopen.owner.owner.data = (void *) owner;
    a.opopen.owner.owner.len  = (uint32_t) strlen(owner);
    a.opopen.openhow.opentype = OPEN4_CREATE;
    a.opopen.openhow.how.mode = UNCHECKED4;
    fattr_build(&a.opopen.openhow.how.createattrs, b, attrs, n);
    a.opopen.claim.claim     = CLAIM_NULL;
    a.opopen.claim.file.data = (void *) name;
    a.opopen.claim.file.len  = (uint32_t) strlen(name);
    return a;
} /* op_open_create */

/* CREATE of a directory (NF4DIR) or of a symbolic link to "target" (NF4LNK)
 * with `n` createattrs. */
static struct nfs_argop4
op_create(
    nfs_ftype4          type,
    const char         *name,
    struct fattr_buf   *b,
    const unsigned int *attrs,
    int                 n)
{
    static char       target[] = "target";
    struct nfs_argop4 a;

    memset(&a, 0, sizeof(a));
    a.argop                 = OP_CREATE;
    a.opcreate.objtype.type = type;
    if (type == NF4LNK) {
        a.opcreate.objtype.linkdata.data = target;
        a.opcreate.objtype.linkdata.len  = (uint32_t) strlen(target);
    }
    a.opcreate.objname.data = (void *) name;
    a.opcreate.objname.len  = (uint32_t) strlen(name);
    fattr_build(&a.opcreate.createattrs, b, attrs, n);
    return a;
} /* op_create */

/* ---- part 1: supported_attrs and the set path agree ----------------------- */

/* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
/* Every attribute RFC 7530 section 5 defines as writable. */
static const struct {
    unsigned int attr;
    const char  *name;
} writable[] = {
    { FATTR4_SIZE,            "size"            },
    { FATTR4_ACL,             "acl"             },
    { FATTR4_ARCHIVE,         "archive"         },
    { FATTR4_HIDDEN,          "hidden"          },
    { FATTR4_MIMETYPE,        "mimetype"        },
    { FATTR4_MODE,            "mode"            },
    { FATTR4_OWNER,           "owner"           },
    { FATTR4_OWNER_GROUP,     "owner_group"     },
    { FATTR4_SYSTEM,          "system"          },
    { FATTR4_TIME_ACCESS_SET, "time_access_set" },
    { FATTR4_TIME_BACKUP,     "time_backup"     },
    { FATTR4_TIME_CREATE,     "time_create"     },
    { FATTR4_TIME_MODIFY_SET, "time_modify_set" },
};

/* The writable attributes of later minor versions and extensions, none of
 * which this server advertises.  The probe carries no value for them:
 * advertising one must come with a cell here that sets it. */
static const unsigned int writable_unprobed[] = {
    FATTR4_DACL, FATTR4_SACL, FATTR4_LAYOUT_HINT, FATTR4_RETENTION_SET,
    FATTR4_RETENTEVT_SET, FATTR4_RETENTION_HOLD, FATTR4_MODE_SET_MASKED,
    FATTR4_SEC_LABEL,
    81,   /* mode_umask */
    84,   /* time_deleg_access */
    85,   /* time_deleg_modify */
};
/* *INDENT-ON* */

static int
read_time(
    struct pc           *pc,
    const struct mbt_fh *obj,
    unsigned int         attr,
    uint64_t            *sec,
    uint32_t            *nsec);

static void
run_supported_cells(
    struct pc           *pc,
    uint32_t             minor,
    const struct mbt_fh *file,
    int                  stores)
{
    char              label[64];
    char              buf[400];
    struct nfs_argop4 ops[2];
    struct fattr_buf  fb;
    struct prep       p;
    uint32_t          supp[3] = { 0, 0, 0 };
    uint32_t          nwords, w;
    unsigned int      i, attr;
    int               advertised;
    uint32_t          want;

    /* supported_attrs is a bitmap4: a word count, then the words. */
    ops[0] = op_putfh(file);
    ops[1] = op_getattr(FATTR4_SUPPORTED_ATTRS);
    p      = pc_compound(pc, minor, ops, 2);
    snprintf(label, sizeof(label), "v4.%u-supported-attrs", minor);
    if (p.status != NFS4_OK || !(p.gmask[0] & (1U << FATTR4_SUPPORTED_ATTRS)) ||
        p.gvals_len < 4) {
        snprintf(buf, sizeof(buf), "GETATTR(supported_attrs): status %u, "
                 "reply word0 0x%08x, %u value bytes", p.status, p.gmask[0],
                 p.gvals_len);
        expect(label, 0, buf);
        return;
    }
    nwords = unpack_be32(p.gvals);
    for (w = 0; w < nwords && w < 3 && 4 + 4 * (w + 1) <= p.gvals_len; w++) {
        supp[w] = unpack_be32(p.gvals + 4 + 4 * w);
    }
    snprintf(buf, sizeof(buf), "%u words: 0x%08x 0x%08x 0x%08x", nwords,
             supp[0], supp[1], supp[2]);
    expect(label, 1, buf);

    for (i = 0; i < sizeof(writable) / sizeof(writable[0]); i++) {
        attr       = writable[i].attr;
        advertised = (supp[attr / 32] >> (attr % 32)) & 1;
        want       = advertised ? NFS4_OK : NFS4ERR_ATTRNOTSUPP;

        snprintf(label, sizeof(label), "v4.%u-setattr-%s", minor,
                 writable[i].name);

        ops[0] = op_putfh(file);
        ops[1] = op_setattr(&fb, &attr, 1);
        p      = pc_compound(pc, minor, ops, 2);
        if (p.meas_idx < 0) {
            snprintf(buf, sizeof(buf),
                     "compound returned %u without reaching SETATTR", p.status);
            expect(label, 0, buf);
            continue;
        }
        snprintf(buf, sizeof(buf), "%s (%u) is %s: SETATTR expected %u, got %u",
                 writable[i].name, attr,
                 advertised ? "advertised" : "not advertised", want,
                 p.st[p.meas_idx]);
        expect(label, p.st[p.meas_idx] == want, buf);

        if (advertised) {
            continue;
        }

        /* VERIFY names the same unsupported attribute. */
        snprintf(label, sizeof(label), "v4.%u-verify-%s", minor,
                 writable[i].name);
        ops[0] = op_putfh(file);
        ops[1] = op_verify(&fb, &attr, 1);
        p      = pc_compound(pc, minor, ops, 2);
        snprintf(buf, sizeof(buf), "%s (%u) is not advertised: VERIFY expected "
                 "%u, got %u", writable[i].name, attr, NFS4ERR_ATTRNOTSUPP,
                 p.meas_idx < 0 ? p.status : p.st[p.meas_idx]);
        expect(label, p.meas_idx >= 0 &&
               p.st[p.meas_idx] == NFS4ERR_ATTRNOTSUPP, buf);
    }

    /* VERIFY of time_create, which is advertised, compares the value: the
     * SETATTR above stored it where the backend keeps a birth time.  An
     * object with no birth time at all -- a passthrough on a filesystem whose
     * statx reports none -- does not support the attribute, and VERIFY
     * answers NFS4ERR_ATTRNOTSUPP rather than a comparison that could never
     * match (RFC 7530 section 16.35.5). */
    attr = FATTR4_TIME_CREATE;
    if ((supp[attr / 32] >> (attr % 32)) & 1) {
        uint64_t sec  = 0;
        uint32_t nsec = 0;
        int      has  = read_time(pc, file, attr, &sec, &nsec);

        snprintf(label, sizeof(label), "v4.%u-verify-time_create", minor);
        snprintf(buf, sizeof(buf), "GETATTR(time_create) before VERIFY: %s",
                 has < 0 ? "failed" : has ? "present" : "absent");
        expect(label, has >= 0, buf);
        want = has != 1 ? NFS4ERR_ATTRNOTSUPP :
            stores ? NFS4_OK : NFS4ERR_NOT_SAME;
        ops[0] = op_putfh(file);
        ops[1] = op_verify(&fb, &attr, 1);
        p      = pc_compound(pc, minor, ops, 2);
        snprintf(buf, sizeof(buf), "time_create is advertised: VERIFY expected "
                 "%u, got %u", want, p.meas_idx < 0 ? p.status :
                 p.st[p.meas_idx]);
        expect(label, p.meas_idx >= 0 && p.st[p.meas_idx] == want, buf);
    }

    snprintf(label, sizeof(label), "v4.%u-unprobed-writable", minor);
    for (i = 0; i < sizeof(writable_unprobed) / sizeof(writable_unprobed[0]);
         i++) {
        attr = writable_unprobed[i];
        if ((supp[attr / 32] >> (attr % 32)) & 1) {
            snprintf(buf, sizeof(buf), "supported_attrs advertises writable "
                     "attribute %u, which no cell here sets", attr);
            expect(label, 0, buf);
        }
    }
} /* run_supported_cells */

/* ---- part 2: time_create reaches the backend ------------------------------ */

/* The object's `attr` (an nfstime4 attribute), read with GETATTR at NFSv4.0.
 * Returns 1 and fills sec/nsec if the object has one, 0 if GETATTR leaves it
 * out, -1 if GETATTR fails. */
static int
read_time(
    struct pc           *pc,
    const struct mbt_fh *obj,
    unsigned int         attr,
    uint64_t            *sec,
    uint32_t            *nsec)
{
    struct nfs_argop4 ops[2];
    struct prep       p;

    ops[0] = op_putfh(obj);
    ops[1] = op_getattr(attr);
    p      = pc_compound(pc, 0, ops, 2);
    if (p.status != NFS4_OK) {
        return -1;
    }
    if (!(p.gmask[attr / 32] & (1U << (attr % 32))) || p.gvals_len < 12) {
        return 0;
    }
    *sec  = unpack_be64(p.gvals);
    *nsec = unpack_be32(p.gvals + 8);
    return 1;
} /* read_time */

/* SETATTR of `n` attributes on `obj` at `minor`.  Returns the SETATTR's
 * status (or the compound's, if it never reached SETATTR) and fills set[]
 * with its attrsset, words 0-1. */
static uint32_t
do_setattr(
    struct pc           *pc,
    uint32_t             minor,
    const struct mbt_fh *obj,
    const unsigned int  *attrs,
    int                  n,
    uint32_t            *set)
{
    struct nfs_argop4 ops[2];
    struct fattr_buf  fb;
    struct prep       p;

    ops[0] = op_putfh(obj);
    ops[1] = op_setattr(&fb, attrs, n);
    p      = pc_compound(pc, minor, ops, 2);
    set[0] = p.set[0];
    set[1] = p.set[1];
    return p.meas_idx < 0 ? p.status : p.st[p.meas_idx];
} /* do_setattr */

/* OPEN(create, UNCHECKED4) of `name` in `root` with `n` createattrs, in the
 * client's NFSv4.1 session, then CLOSE.  Returns the OPEN's status; on NFS4_OK
 * fills *fh and set[] (the attrset, words 0-1). */
static uint32_t
open_create_close(
    struct pc           *pc,
    const struct mbt_fh *root,
    const char          *owner,
    const char          *name,
    const unsigned int  *attrs,
    int                  n,
    struct mbt_fh       *fh,
    uint32_t            *set)
{
    struct nfs_argop4 ops[3];
    struct fattr_buf  fb;
    struct prep       p;
    uint32_t          st;

    ops[0] = op_putfh(root);
    ops[1] = op_open_create(pc->clientid, owner, name, &fb, attrs, n);
    ops[2] = op_getfh();
    p      = pc_compound(pc, 1, ops, 3);
    st     = p.meas_idx < 0 ? p.status : p.st[p.meas_idx];
    if (st != NFS4_OK) {
        return st;
    }
    if (!p.fh.has) {
        fprintf(stderr, "OPEN %s: no file handle in the reply\n", name);
        exit(2);
    }
    *fh    = p.fh;
    set[0] = p.set[0];
    set[1] = p.set[1];
    ops[0] = op_putfh(fh);
    ops[1] = op_close(&p.open_sid);
    p      = pc_compound(pc, 1, ops, 2);
    if (p.status != NFS4_OK) {
        fprintf(stderr, "CLOSE %s failed: %u\n", name, p.status);
        exit(2);
    }
    return NFS4_OK;
} /* open_create_close */

/* After an operation that carried the current birth time (btime_sec,
 * btime_nsec) succeeded on `obj`: its attrset names time_create exactly when
 * the object now reads that value back.  Where the cell knows which it must
 * be, `want` is 0 or 1 and `why` says so; -1 leaves it to the backend. */
static void
check_time_create(
    struct pc           *pc,
    const char          *label,
    const struct mbt_fh *obj,
    uint32_t             set_word1,
    int                  want,
    const char          *why)
{
    char     buf[400];
    int      named = (set_word1 & W1(FATTR4_TIME_CREATE)) != 0;
    int      has, reads_back;
    uint64_t sec  = 0;
    uint32_t nsec = 0;

    has = read_time(pc, obj, FATTR4_TIME_CREATE, &sec, &nsec);
    if (has < 0) {
        expect(label, 0, "GETATTR(time_create) failed");
        return;
    }
    reads_back = has && sec == btime_sec && nsec == btime_nsec;

    snprintf(buf, sizeof(buf), "attrset names time_create: %d; GETATTR reads "
             "%llu.%09u back: %d (object has %s%llu.%09u)", named,
             (unsigned long long) btime_sec, btime_nsec, reads_back,
             has ? "" : "no time_create, ", (unsigned long long) sec, nsec);
    expect(label, named == reads_back, buf);

    if (want >= 0) {
        snprintf(buf, sizeof(buf), "%s: attrset names time_create expected "
                 "%d, got %d", why, want, named);
        expect(label, named == want, buf);
    }
} /* check_time_create */

/* `obj` still has the birth time it had before (has0, sec0, nsec0). */
static void
check_btime_unchanged(
    struct pc           *pc,
    const char          *label,
    const struct mbt_fh *obj,
    int                  has0,
    uint64_t             sec0,
    uint32_t             nsec0)
{
    char     buf[400];
    int      has1;
    uint64_t sec1  = 0;
    uint32_t nsec1 = 0;

    has1 = read_time(pc, obj, FATTR4_TIME_CREATE, &sec1, &nsec1);
    snprintf(buf, sizeof(buf), "time_create before %d %llu.%09u, after %d "
             "%llu.%09u", has0, (unsigned long long) sec0, nsec0, has1,
             (unsigned long long) sec1, nsec1);
    /* Both reads must have reached the object: two failed GETATTRs would
     * otherwise compare equal and prove nothing. */
    expect(label, has0 >= 0 && has1 == has0 &&
           (has0 != 1 || (sec1 == sec0 && nsec1 == nsec0)), buf);
} /* check_btime_unchanged */

/* A SETATTR of time_create whose value is malformed: refused with `want`, and
 * the birth time of `file` left alone.  `nsec` is the nseconds sent; a
 * `short_len` other than 0 truncates the value blob to that many bytes. */
static void
bad_time_create_cell(
    struct pc           *pc,
    const char          *label,
    const struct mbt_fh *file,
    uint32_t             nsec,
    uint32_t             short_len,
    uint32_t             want)
{
    static const unsigned int btime_only[] = { FATTR4_TIME_CREATE };
    char                      buf[400];
    struct nfs_argop4         ops[2];
    struct fattr_buf          fb;
    struct prep               p;
    uint32_t                  st, saved = btime_nsec;
    int                       has0;
    uint64_t                  sec0  = 0;
    uint32_t                  nsec0 = 0;

    has0 = read_time(pc, file, FATTR4_TIME_CREATE, &sec0, &nsec0);

    btime_next();
    btime_nsec = nsec;
    ops[0]     = op_putfh(file);
    ops[1]     = op_setattr(&fb, btime_only, 1);
    if (short_len) {
        ops[1].opsetattr.obj_attributes.attr_vals.len = short_len;
    }
    p          = pc_compound(pc, 0, ops, 2);
    btime_nsec = saved;
    st         = p.meas_idx < 0 ? p.status : p.st[p.meas_idx];

    snprintf(buf, sizeof(buf), "SETATTR(time_create, nseconds %u, %u value "
             "bytes): expected %u, got %u", nsec, short_len ? short_len : 12,
             want, st);
    expect(label, st == want, buf);
    check_btime_unchanged(pc, label, file, has0, sec0, nsec0);
} /* bad_time_create_cell */

static void
run_time_create_cells(
    struct pc           *pc,
    const struct mbt_fh *root,
    int                  stores)
{
    static const unsigned int mode_only[]  = { FATTR4_MODE };
    static const unsigned int btime_only[] = { FATTR4_TIME_CREATE };
    static const unsigned int mode_btime[] = { FATTR4_MODE,
                                               FATTR4_TIME_CREATE };
    static const unsigned int times_btime[] = { FATTR4_TIME_ACCESS_SET,
                                                FATTR4_TIME_CREATE,
                                                FATTR4_TIME_MODIFY_SET };
    static const unsigned int mode_times_btime[] = { FATTR4_MODE,
                                                     FATTR4_TIME_ACCESS_SET,
                                                     FATTR4_TIME_CREATE,
                                                     FATTR4_TIME_MODIFY_SET };
    const char               *label;
    char                      buf[400];
    struct nfs_argop4         ops[3];
    struct fattr_buf          fb;
    struct prep               p;
    struct mbt_fh             file, obj;
    nfs_ftype4                type;
    int                       i, has0, has;
    uint32_t                  set[2], st;
    uint64_t                  sec0  = 0, sec = 0;
    uint32_t                  nsec0 = 0, nsec = 0;
    const char               *why = stores ? "backend stores a birth time" :
        "backend cannot store a birth time";

    /* The file the SETATTR cells act on. */
    st = open_create_close(pc, root, "settable-set", "set.bin", mode_only, 1,
                           &file, set);
    if (st != NFS4_OK) {
        fprintf(stderr, "create set.bin failed: %u\n", st);
        exit(2);
    }

    /* An UNCHECKED4 OPEN of a file that exists: created with one birth time,
     * opened again with another.  The second OPEN ignores its createattrs,
     * so its attrset names none of them and the birth time is unchanged. */
    label = "open-existing-keeps-time-create";
    btime_next();
    st = open_create_close(pc, root, "settable-existing", "existing.bin",
                           mode_btime, 2, &obj, set);
    snprintf(buf, sizeof(buf), "OPEN(create, UNCHECKED4) existing.bin, the "
             "create: expected %u, got %u", NFS4_OK, st);
    expect(label, st == NFS4_OK, buf);
    if (st == NFS4_OK) {
        has0 = read_time(pc, &obj, FATTR4_TIME_CREATE, &sec0, &nsec0);
        btime_next();
        st = open_create_close(pc, root, "settable-existing", "existing.bin",
                               mode_btime, 2, &obj, set);
        snprintf(buf, sizeof(buf), "OPEN(create, UNCHECKED4) existing.bin, "
                 "the file exists: expected %u, got %u", NFS4_OK, st);
        expect(label, st == NFS4_OK, buf);
        if (st == NFS4_OK) {
            snprintf(buf, sizeof(buf), "attrset: expected none, got word0 "
                     "0x%08x word1 0x%08x", set[0], set[1]);
            expect(label, set[0] == 0 && set[1] == 0, buf);
            check_btime_unchanged(pc, label, &obj, has0, sec0, nsec0);
        }
    }

    /* SETATTR of time_create alone. */
    label = "setattr-time-create";
    btime_next();
    st = do_setattr(pc, 0, &file, btime_only, 1, set);
    snprintf(buf, sizeof(buf), "SETATTR(time_create): expected %u, got %u",
             NFS4_OK, st);
    expect(label, st == NFS4_OK, buf);
    if (st == NFS4_OK) {
        check_time_create(pc, label, &file, set[1], stores, why);
    }

    /* time_create between time_access_set and time_modify_set, each set to a
     * client time, then the same with mode: what a copy that keeps a file's
     * dates sends.  The values decode in bitmap order (48, 50, 54), so a
     * decoder that took time_create out of order would misread the rest. */
    client_times = 1;
    for (i = 0; i < 2; i++) {
        label = i == 0 ? "setattr-times-and-time-create" :
            "setattr-mode-times-and-time-create";
        btime_next();
        st = i == 0 ? do_setattr(pc, 0, &file, times_btime, 3, set) :
            do_setattr(pc, 0, &file, mode_times_btime, 4, set);
        snprintf(buf, sizeof(buf), "SETATTR(%stime_access_set, time_create, "
                 "time_modify_set): expected %u, got %u",
                 i == 0 ? "" : "mode, ", NFS4_OK, st);
        expect(label, st == NFS4_OK, buf);
        if (st != NFS4_OK) {
            continue;
        }
        snprintf(buf, sizeof(buf), "attrsset word1 0x%08x names "
                 "time_access_set and time_modify_set", set[1]);
        expect(label, (set[1] & W1(FATTR4_TIME_ACCESS_SET)) &&
               (set[1] & W1(FATTR4_TIME_MODIFY_SET)), buf);
        has = read_time(pc, &file, FATTR4_TIME_MODIFY, &sec, &nsec);
        snprintf(buf, sizeof(buf), "time_modify: expected %llu.000000000, got "
                 "%d %llu.%09u", (unsigned long long) CLIENT_TIME_SEC, has,
                 (unsigned long long) sec, nsec);
        expect(label, has == 1 && sec == CLIENT_TIME_SEC && nsec == 0, buf);
        check_time_create(pc, label, &file, set[1], stores, why);
    }
    client_times = 0;

    /* nseconds must be below 10^9 (RFC 7530 section 2.2.1).  The largest
     * valid value is stored; 10^9 and the value the VFS uses internally to
     * mean "now" are refused, so a client cannot reach that marker.  A value
     * shorter than an nfstime4 is malformed XDR. */
    label = "setattr-time-create-max-nseconds";
    btime_next();
    btime_nsec = 999999999;
    st         = do_setattr(pc, 0, &file, btime_only, 1, set);
    snprintf(buf, sizeof(buf), "SETATTR(time_create, nseconds 999999999): "
             "expected %u, got %u", NFS4_OK, st);
    expect(label, st == NFS4_OK, buf);
    if (st == NFS4_OK) {
        check_time_create(pc, label, &file, set[1], stores, why);
    }
    btime_nsec = BTIME_NSEC;
    bad_time_create_cell(pc, "setattr-time-create-nseconds-1e9", &file,
                         1000000000, 0, NFS4ERR_INVAL);
    bad_time_create_cell(pc, "setattr-time-create-nseconds-time-now", &file,
                         (1U << 30) - 3, 0, NFS4ERR_INVAL);
    bad_time_create_cell(pc, "setattr-time-create-short", &file,
                         BTIME_NSEC, 8, NFS4ERR_BADXDR);

    /* OPEN(create, UNCHECKED4) with mode and time_create. */
    label = "open-create-mode-time-create";
    btime_next();
    st = open_create_close(pc, root, "settable-open", "created.bin",
                           mode_btime, 2, &obj, set);
    snprintf(buf, sizeof(buf), "OPEN(create, UNCHECKED4) created.bin: expected "
             "%u, got %u", NFS4_OK, st);
    expect(label, st == NFS4_OK, buf);
    if (st == NFS4_OK) {
        check_time_create(pc, label, &obj, set[1], stores, why);
    }

    /* CREATE of a directory, then of a symbolic link, with the same
     * createattrs.  The symbolic link's completion is not handed the set
     * attributes, so its attrset never names time_create, whatever the
     * backend did with it. */
    for (i = 0; i < 2; i++) {
        type  = i == 0 ? NF4DIR : NF4LNK;
        label = i == 0 ? "create-dir-mode-time-create" :
            "create-symlink-mode-time-create";

        btime_next();
        ops[0] = op_putfh(root);
        ops[1] = op_create(type, i == 0 ? "created.dir" : "created.lnk", &fb,
                           mode_btime, 2);
        ops[2] = op_getfh();
        p      = pc_compound(pc, 0, ops, 3);
        st     = p.meas_idx < 0 ? p.status : p.st[p.meas_idx];
        snprintf(buf, sizeof(buf), "CREATE(%s): expected %u, got %u",
                 i == 0 ? "NF4DIR" : "NF4LNK", NFS4_OK, st);
        expect(label, st == NFS4_OK && p.fh.has, buf);
        if (st != NFS4_OK || !p.fh.has) {
            continue;
        }
        if (i == 0) {
            obj = p.fh;
            check_time_create(pc, label, &obj, p.set[1], stores, why);
        } else {
            snprintf(buf, sizeof(buf), "attrset word1 0x%08x names "
                     "time_create: expected 0", p.set[1]);
            expect(label, !(p.set[1] & W1(FATTR4_TIME_CREATE)), buf);
        }
    }
} /* run_time_create_cells */

/* ---- part 3: who may set time_create -------------------------------------- */

#define OTHER_UID 1000

static void
run_permission_cells(
    struct mbt_env      *env,
    struct pc           *pc,
    const struct mbt_fh *root,
    int                  stores)
{
    static const unsigned int mode_only[]  = { FATTR4_MODE };
    static const unsigned int owner_only[] = { FATTR4_OWNER };
    static const unsigned int btime_only[] = { FATTR4_TIME_CREATE };
    static const unsigned int mtime_only[] = { FATTR4_TIME_MODIFY_SET };
    static const unsigned int size_btime[] = { FATTR4_SIZE,
                                               FATTR4_TIME_CREATE };
    static const unsigned int mode_times_btime[] = { FATTR4_MODE,
                                                     FATTR4_TIME_ACCESS_SET,
                                                     FATTR4_TIME_CREATE,
                                                     FATTR4_TIME_MODIFY_SET };
    const char               *label;
    char                      buf[400];
    struct mbt_fh             victim, owned;
    uint32_t                  set[2], st;
    int                       i, has0;
    uint64_t                  sec0  = 0;
    uint32_t                  nsec0 = 0;

    /* As root: a file anyone may write to (mode 0666, owned by root), and a
     * file owned by OTHER_UID. */
    mode_value = 0666;
    st         = open_create_close(pc, root, "settable-perm", "perm.bin",
                                   mode_only, 1, &victim, set);
    mode_value = 0644;
    if (st == NFS4_OK) {
        st = open_create_close(pc, root, "settable-perm", "owned.bin",
                               mode_only, 1, &owned, set);
    }
    if (st == NFS4_OK) {
        owner_value = "1000";
        st          = do_setattr(pc, 0, &owned, owner_only, 1, set);
        owner_value = "25201";
    }
    if (st != NFS4_OK) {
        fprintf(stderr, "permission cells setup failed: %u\n", st);
        exit(2);
    }
    has0 = read_time(pc, &victim, FATTR4_TIME_CREATE, &sec0, &nsec0);

    mbt_cred_set_uid(env, OTHER_UID);

    /* Control: a modification time set to a client value is the owner's to
     * set, so this user is refused on every backend.  It shows the
     * credential took effect. */
    label        = "nonowner-time-modify-control";
    client_times = 1;
    st           = do_setattr(pc, 0, &victim, mtime_only, 1, set);
    client_times = 0;
    snprintf(buf, sizeof(buf), "uid %u, SETATTR(time_modify_set client) on a "
             "root-owned file: expected %u, got %u", OTHER_UID, NFS4ERR_PERM,
             st);
    expect(label, st == NFS4ERR_PERM, buf);

    /* time_create alone, then with a size change the user may make.  Alone,
     * a backend that cannot store a birth time may complete the SETATTR
     * having changed nothing.  The size change takes the request through the
     * access check on every backend, and that check refuses it for want of
     * the WRITE_ATTRIBUTES right the birth time needs. */
    for (i = 0; i < 2; i++) {
        int may_be_ok = i == 0 && !stores;

        label = i == 0 ? "nonowner-setattr-time-create" :
            "nonowner-writer-size-and-time-create";
        btime_next();
        st = i == 0 ? do_setattr(pc, 0, &victim, btime_only, 1, set) :
            do_setattr(pc, 0, &victim, size_btime, 2, set);
        snprintf(buf, sizeof(buf), "uid %u, SETATTR(%stime_create) on a "
                 "root-owned 0666 file: expected %u%s, got %u", OTHER_UID,
                 i == 0 ? "" : "size, ", NFS4ERR_PERM,
                 may_be_ok ? " or NFS4_OK" : "", st);
        expect(label, st == NFS4ERR_PERM || (may_be_ok && st == NFS4_OK), buf);
        snprintf(buf, sizeof(buf), "attrsset word1 0x%08x names time_create: "
                 "expected 0", set[1]);
        expect(label, !(set[1] & W1(FATTR4_TIME_CREATE)), buf);
        check_btime_unchanged(pc, label, &victim, has0, sec0, nsec0);
    }

    /* The owner may set it. */
    label = "owner-setattr-time-create";
    btime_next();
    st = do_setattr(pc, 0, &owned, btime_only, 1, set);
    snprintf(buf, sizeof(buf), "uid %u, SETATTR(time_create) on its own file: "
             "expected %u, got %u", OTHER_UID, NFS4_OK, st);
    expect(label, st == NFS4_OK, buf);
    if (st == NFS4_OK) {
        check_time_create(pc, label, &owned, set[1], stores,
                          stores ? "backend stores a birth time" :
                          "backend cannot store a birth time");
    }

    /* The owner sets mode and all three times at once, as a copy that keeps
     * a file's dates does on its own new file. */
    label        = "owner-setattr-mode-times-and-time-create";
    client_times = 1;
    btime_next();
    st           = do_setattr(pc, 0, &owned, mode_times_btime, 4, set);
    client_times = 0;
    snprintf(buf, sizeof(buf), "uid %u, SETATTR(mode, time_access_set, "
             "time_create, time_modify_set) on its own file: expected %u, got "
             "%u", OTHER_UID, NFS4_OK, st);
    expect(label, st == NFS4_OK, buf);
    if (st == NFS4_OK) {
        check_time_create(pc, label, &owned, set[1], stores,
                          stores ? "backend stores a birth time" :
                          "backend cannot store a birth time");
    }

    mbt_cred_set_uid(env, 0);
} /* run_permission_cells */

int
main(
    int    argc,
    char **argv)
{
    static const unsigned int mode_only[] = { FATTR4_MODE };
    struct mbt_env_opts       opts        = {
        /* Exact attributes: time_create read back must be what the backend
         * holds now, not a cached answer from before the set. */
        .disable_caches = 1,
    };
    struct mbt_env           *env = malloc(sizeof(*env));
    struct pc                 pc, pc42;
    struct mbt_fh             root, file;
    uint32_t                  set[2], st;
    int                       stores;

    setvbuf(stdout, NULL, _IONBF, 0);
    /* Keep the modes the cells create files with exact on the passthrough
     * backends, as the other probes do. */
    umask(0);
    mbt_watchdog_arm(60);

    /* Usage: nfs4_settable_attrs_probe [<module>] */
    opts.module = argc > 1 ? argv[1] : "memfs";

    /* Whether the module stores a birth time a client sets: the passthrough
     * backends read a file's birth time but have no system call to set it.
     * A module not listed here needs its own answer before it can run. */
    if (strcmp(opts.module, "memfs") == 0 ||
        strcmp(opts.module, "diskfs") == 0 ||
        strcmp(opts.module, "cairn") == 0) {
        stores = 1;
    } else if (strcmp(opts.module, "linux") == 0 ||
               strcmp(opts.module, "io_uring") == 0) {
        stores = 0;
    } else {
        fprintf(stderr, "no birth-time expectation for module %s\n",
                opts.module);
        free(env);
        return 2;
    }

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

    pc_setup(&pc, env, 1, "settable-attrs-probe");
    pc_setup(&pc42, env, 2, "settable-attrs-probe-42");

    printf("# --- NFSv4 settable attributes (%s) ---\n", opts.module);

    /* The file part one acts on. */
    st = open_create_close(&pc, &root, "settable-setup", "attrs.bin",
                           mode_only, 1, &file, set);
    if (st != NFS4_OK) {
        fprintf(stderr, "create attrs.bin failed: %u\n", st);
        return 2;
    }

    run_supported_cells(&pc, 0, &file, stores);
    run_supported_cells(&pc, 1, &file, stores);
    run_supported_cells(&pc42, 2, &file, stores);
    run_time_create_cells(&pc, &root, stores);
    run_permission_cells(env, &pc, &root, stores);

    mbt_env_stop(env);
    free(env);

    if (failures) {
        fprintf(stderr, "%d NFSv4 settable attribute check(s) FAILED\n",
                failures);
        return 1;
    }
    printf("all NFSv4 settable attribute checks passed\n");
    return 0;
} /* main */
