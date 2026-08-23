/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * In-process SMB2 test harness: stands up the chimera SMB server with the
 * memfs backend over libevpl's inproc transport and drives it from a
 * hand-rolled SMB2 wire client on the same thread.  Nothing binds a real
 * port, needs a network namespace, or spawns a daemon -- every probe runs
 * fully parallel, exactly like the NFS MBT harness (nfs3_mbt_common.h).
 *
 * SMB2 is NOT ONC-RPC/XDR, so there are no code-generated stubs: this file
 * hand-builds each SMB2 message (NetBIOS framing + the 64-byte header + a
 * per-command body) and parses the reply, following the server's own
 * parser byte-for-byte (src/server/smb/smb.c, smb2.h, smb_proc_*.c,
 * smb_proc_oplock_break.c).
 *
 * A `smb2_env` owns the shared server + evpl loop; each `smb2_conn` is one
 * SMB2 connection (its own session/tree/FileIds).  Multiple connections
 * share the one loop, which the oplock/lease break lifecycle requires: an
 * ack-required break is delivered on the HOLDER's connection while the
 * conflicting opener's CREATE is deferred (STATUS_PENDING) on ITS
 * connection until the holder acknowledges (verified in the server source),
 * so a two-party test needs two connections.
 *
 * Conformance discipline (DEVIATIONS-SMB.md): this harness is spec-neutral
 * plumbing.  Probes built on it assert MS-SMB2/MS-FSA behavior and record
 * any chimera divergence explicitly -- the harness never encodes an
 * assumption that chimera is correct.
 *
 * Wire facts (all from the server source, verified):
 *   - Framing: 4-byte NetBIOS header, low 24 bits = big-endian length of the
 *     SMB2 message (smb.c:2613 / :1023).
 *   - Fields self-align to their natural boundary from the SMB2 header start;
 *     the header is 64 bytes, so standard MS-SMB2 body offsets match.  Send
 *     all Reserved/padding.
 *   - Auth: raw NTLMSSP (smb_auth.c:54).  Anonymous NTLM -> NULL session:
 *     SESSION_SETUP(Type-1) -> MORE_PROCESSING_REQUIRED + assigned SessionId,
 *     then SESSION_SETUP(Type-3 anonymous, NTLMSSP_NEGOTIATE_ANONYMOUS +
 *     empty NtChallengeResponse, >=88 bytes) -> SUCCESS, IS_NULL, no signing.
 *   - FileId is an opaque 16-byte (persistent, volatile) pair from the CREATE
 *     reply; echo it verbatim on READ/WRITE/CLOSE.
 *   - Lease state bits on the wire are R=0x01, H=0x02, W=0x04 (H and W are
 *     the reverse of the internal VFS mask; smb_internal.h:82).
 */

#ifndef SMB2_MBT_COMMON_H
#define SMB2_MBT_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>

#include "server/server.h"
#include "common/tcp_flavor.h"
#include "prometheus-c.h"

#include "evpl/evpl.h"

/* ---- SMB2 wire constants (values verified in smb2.h) ------------------- */

#define SMB2_HDR_SIZE                     64

/* smb2_command (smb2.h:683) */
#define SMB2_NEGOTIATE                    0x0000
#define SMB2_SESSION_SETUP                0x0001
#define SMB2_LOGOFF                       0x0002
#define SMB2_TREE_CONNECT                 0x0003
#define SMB2_TREE_DISCONNECT              0x0004
#define SMB2_CREATE                       0x0005
#define SMB2_CLOSE                        0x0006
#define SMB2_FLUSH                        0x0007
#define SMB2_READ                         0x0008
#define SMB2_WRITE                        0x0009
#define SMB2_IOCTL                        0x000B
#define SMB2_CANCEL                       0x000C
#define SMB2_LOCK                         0x000A
#define SMB2_QUERY_INFO                   0x0010
#define SMB2_SET_INFO                     0x0011
#define SMB2_ECHO                         0x000D
#define SMB2_OPLOCK_BREAK                 0x0012

/* smb2 header flags (smb2.h:789) */
#define SMB2_FLAGS_SERVER_TO_REDIR        0x00000001
#define SMB2_FLAGS_ASYNC_COMMAND          0x00000002
#define SMB2_FLAGS_RELATED_OPERATIONS     0x00000004
/* MS-SMB2 2.2.1.2: the client sets this on a request it is RE-SENDING after a
 * channel/transport failure, so the server can apply exactly-once semantics.
 * Note the value: 0x20000000, NOT 0x00000020 (smb2.h:795). */
#define SMB2_FLAGS_REPLAY_OPERATION       0x20000000

/* NTSTATUS (MS-ERREF) */
#define ST_SUCCESS                        0x00000000u
#define ST_PENDING                        0x00000103u
#define ST_MORE_PROCESSING_REQUIRED       0xC0000016u
#define ST_UNSUCCESSFUL                   0xC0000001u
#define ST_INVALID_PARAMETER              0xC000000Du
#define ST_SHARING_VIOLATION              0xC0000043u
#define ST_INVALID_DEVICE_STATE           0xC0000184u
#define ST_INVALID_OPLOCK_PROTOCOL        0xC00000E3u
#define ST_REQUEST_NOT_ACCEPTED           0xC00000D0u
#define ST_END_OF_FILE                    0xC0000011u
#define ST_ACCESS_DENIED                  0xC0000022u
#define ST_OBJECT_NAME_NOT_FOUND          0xC0000034u
#define ST_OBJECT_NAME_COLLISION          0xC0000035u
#define ST_OBJECT_PATH_NOT_FOUND          0xC000003Au
#define ST_NOT_SUPPORTED                  0xC00000BBu
#define ST_CANCELLED                      0xC0000120u
#define ST_FILE_CLOSED                    0xC0000128u
/* 0xC000022A, STATUS_DUPLICATE_OBJECTID -- the reply to a NON-replay CREATE
* whose DH2Q CreateGuid collides with a live durable open of the same client
* (MS-SMB2 3.3.5.9.10).  It is NOT 0xC000021B (STATUS_DATA_NOT_ACCEPTED). */
#define ST_DUPLICATE_OBJECTID             0xC000022Au
/* 0xC0000467, STATUS_FILE_NOT_AVAILABLE -- a stale ChannelSequence on a
 * mutating op (MS-SMB2 3.3.5.2.10), and a replayed create whose original is
 * still pending (3.3.5.9.10). */
#define ST_FILE_NOT_AVAILABLE             0xC0000467u

/* Dialects (smb2.h:20) */
#define SMB2_DIALECT_0210                 0x0210
#define SMB2_DIALECT_0300                 0x0300

/* Oplock levels (smb2.h:858) */
#define SMB2_OPLOCK_LEVEL_NONE            0x00
#define SMB2_OPLOCK_LEVEL_II              0x01
#define SMB2_OPLOCK_LEVEL_EXCLUSIVE       0x08
#define SMB2_OPLOCK_LEVEL_BATCH           0x09
#define SMB2_OPLOCK_LEVEL_LEASE           0xFF

/* Lease state bits ON THE WIRE (smb2.h:867): note H=0x02, W=0x04. */
#define SMB2_LEASE_NONE                   0x00
#define SMB2_LEASE_READ                   0x01
#define SMB2_LEASE_HANDLE                 0x02
#define SMB2_LEASE_WRITE                  0x04
#define SMB2_LEASE_RWH                    (SMB2_LEASE_READ | SMB2_LEASE_HANDLE | \
                                           SMB2_LEASE_WRITE)
#define SMB2_LEASE_RH                     (SMB2_LEASE_READ | SMB2_LEASE_HANDLE)

/* Lease flags (smb2.h:874) */
#define SMB2_LEASE_FLAG_BREAK_IN_PROGRESS 0x00000002

/* NTLMSSP negotiate flags (smb_ntlm.h) */
#define NTLMSSP_NEGOTIATE_ANONYMOUS       0x00000800u
#define NTLMSSP_NEGOTIATE_NTLM            0x00000200u
#define NTLMSSP_REQUEST_TARGET            0x00000004u
#define NTLMSSP_NEGOTIATE_UNICODE         0x00000001u

/* CREATE dispositions / access / options (MS-SMB2 2.2.13) */
#define FILE_SUPERSEDE                    0x00000000u
#define FILE_OPEN                         0x00000001u
#define FILE_CREATE                       0x00000002u
#define FILE_OPEN_IF                      0x00000003u
#define FILE_OVERWRITE                    0x00000004u
#define FILE_OVERWRITE_IF                 0x00000005u
#define FILE_ALL_ACCESS                   0x001F01FFu
#define FILE_READ_ATTRIBUTES              0x00000080u /* attribute-only access */
#define FILE_READ_ACCESS                  0x00120089u /* R data/attr/EA + SYNC */
#define FILE_WRITE_ACCESS                 0x00120116u /* W data/attr/EA + SYNC */
#define FILE_ATTRIBUTE_NORMAL             0x00000080u
#define FILE_DIRECTORY_FILE               0x00000001u
#define FILE_DELETE_ON_CLOSE              0x00001000u
#define FILE_NON_DIRECTORY_FILE           0x00000040u
#define FILE_SHARE_READ                   0x00000001u
#define FILE_SHARE_WRITE                  0x00000002u
#define FILE_SHARE_DELETE                 0x00000004u
#define FILE_SHARE_RWD                    0x00000007u

/* CreateAction (smb2.h) */
#define FILE_ACT_SUPERSEDED               0
#define FILE_ACT_OPENED                   1
#define FILE_ACT_CREATED                  2
#define FILE_ACT_OVERWRITTEN              3

/* Durable-handle context Flags (MS-SMB2 2.2.13.2.11). */
#define SMB2_DHANDLE_FLAG_PERSISTENT      0x00000002u

/* FSCTL codes + the IOCTL IsFsctl flag (MS-SMB2 2.2.31). */
/* SMB2_LOCK_ELEMENT Flags (MS-SMB2 2.2.26.1). */
#define SMB2_LOCKFLAG_SHARED              0x00000001u
#define SMB2_LOCKFLAG_EXCLUSIVE           0x00000002u
#define SMB2_LOCKFLAG_UNLOCK              0x00000004u
#define SMB2_LOCKFLAG_FAIL_IMMEDIATELY    0x00000010u

#define ST_LOCK_NOT_GRANTED               0xC0000055u
#define ST_FILE_LOCK_CONFLICT             0xC0000054u

#define SMB2_FSCTL_SET_SPARSE             0x000900C4u
#define SMB2_FSCTL_LMR_REQUEST_RESILIENCY 0x001401D4u
#define SMB2C_IOCTL_IS_FSCTL              1u

/* chimera's own resiliency/durable policy constants, mirrored here so a probe
 * asserts against a NAMED policy rather than a magic number (smb2.h:1073,
 * smb_proc_create.c).  A probe that disagrees with these is a finding. */
#define SMB2C_RESILIENCY_DEFAULT_MS       120000u
#define SMB2C_RESILIENCY_MAX_MS           300000u
#define SMB2C_RESILIENCY_MIN_MS           1000u
#define SMB2C_DURABLE_TIMEOUT_DEFAULT_MS  60000u
#define SMB2C_DURABLE_TIMEOUT_MAX_MS      300000u

#define SMB2C_BUFSZ                       (1 << 20) /* per-connection scratch */
#define SMB2C_MAX_BREAKS                  16

/* ---- little-endian field put/get --------------------------------------- */

static inline void
p16(
    uint8_t *b,
    int      off,
    uint16_t v)
{
    b[off]     = (uint8_t) v;
    b[off + 1] = (uint8_t) (v >> 8);
} /* p16 */

static inline void
p32(
    uint8_t *b,
    int      off,
    uint32_t v)
{
    b[off]     = (uint8_t) v;
    b[off + 1] = (uint8_t) (v >> 8);
    b[off + 2] = (uint8_t) (v >> 16);
    b[off + 3] = (uint8_t) (v >> 24);
} /* p32 */

static inline void
p64(
    uint8_t *b,
    int      off,
    uint64_t v)
{
    for (int i = 0; i < 8; i++) {
        b[off + i] = (uint8_t) (v >> (8 * i));
    }
} /* p64 */

static inline uint16_t
g16(
    const uint8_t *b,
    int            off)
{
    return (uint16_t) (b[off] | (b[off + 1] << 8));
} /* g16 */

static inline uint32_t
g32(
    const uint8_t *b,
    int            off)
{
    return (uint32_t) b[off] | ((uint32_t) b[off + 1] << 8) |
           ((uint32_t) b[off + 2] << 16) | ((uint32_t) b[off + 3] << 24);
} /* g32 */

static inline uint64_t
g64(
    const uint8_t *b,
    int            off)
{
    uint64_t v = 0;

    for (int i = 0; i < 8; i++) {
        v |= (uint64_t) b[off + i] << (8 * i);
    }
    return v;
} /* g64 */

/* ASCII -> UTF-16LE; returns byte length written. */
static inline int
utf16le(
    const char *s,
    uint8_t    *out)
{
    int n = 0;

    for (; *s; s++) {
        out[n++] = (uint8_t) *s;
        out[n++] = 0;
    }
    return n;
} /* utf16le */

/* ---- harness types ------------------------------------------------------ */

struct smb2_env_opts {
    int oplocks;            /* advertise legacy oplocks (set_smb_oplocks) */
    int leases;             /* advertise SMB2 leases (set_smb_leases) */
    int directory_leases;   /* advertise directory leases */
    int persistent_handles; /* advertise durable/persistent handles */
    /* SMB2_SHAREFLAG_FORCE_LEVELII_OPLOCK on the share: the server caps every
     * caching grant to a read (LEVEL_II / R) cache (MS-SMB2 2.2.10; the WPTS
     * OplockOnShareWithForceLevel2 / ShareForceLevel2 families). */
    int force_level2;
    /* SMB2_SHARE_CAP_CONTINUOUS_AVAILABILITY on the share.  This is what makes
     * a PERSISTENT durable handle (DH2Q with SMB2_DHANDLE_FLAG_PERSISTENT)
     * grantable at all (MS-SMB2 3.3.5.9.10); on a non-CA share the server
     * grants an ordinary durable handle instead. */
    int continuous_availability;
};

/* Connections are never recycled: a transport drop RETIRES a connection (it
 * keeps its slot so its struct outlives evpl_destroy) and the reconnect takes a
 * fresh one, so a long durable/replay walk consumes one slot per drop.
 * Measured on the registered stepDurable batch: 36 slots at depth 120, 44 at
 * 160, 67 at 240.  smb2_conn_open refuses to exceed this rather than running
 * off the end of the array, and smb2_mbt_replay.c static-asserts its own
 * MAX_SESS against it. */
#define SMB2C_MAX_CONNS 128

struct smb2_conn;

struct smb2_env {
    struct chimera_server     *server;
    struct prometheus_metrics *metrics;
    struct evpl               *evpl;
    char                       session_dir[256];
    /* Every connection stays owned by the env and is freed only AFTER
     * evpl_destroy: evpl dispatches a final DISCONNECTED to each bind during
     * teardown, and that notify dereferences the conn -- freeing earlier is a
     * use-after-free. */
    struct smb2_conn          *conns[SMB2C_MAX_CONNS];
    int                        nconns;
    /* Copied at open time so smb2_env_fs_setup(), which the batch replayer
     * calls once per trace with only (env, fsname), can still apply the
     * share-level feature options. */
    struct smb2_env_opts       opts;
};

/* A break notification the server pushed to this connection (the holder). */
struct smb2_break {
    int      is_lease;
    /* legacy oplock break */
    uint8_t  file_id[16];
    uint8_t  oplock_level;   /* level the holder is asked to break down to */
    /* lease break */
    uint8_t  lease_key[16];
    uint16_t new_epoch;
    uint32_t flags;
    uint32_t cur_state;      /* SMB lease bits */
    uint32_t new_state;      /* SMB lease bits */
    int      ack_required;   /* flags & FLAG_ACK_REQUIRED */
};

struct smb2_conn {
    struct smb2_env  *env;
    int               conn_index;   /* stable per-env index (ClientGuid seed) */
    /* ClientGuid seed actually put on the wire by smb2_negotiate.  Defaults to
     * conn_index (one client per connection).  Set it equal to another
     * connection's guid_tag BEFORE the handshake to make the two connections
     * ONE client -- chimera derives the caching-grant owner's client key from
     * the ClientGuid (chimera_smb_lease_client_key), so this is the only way
     * for the harness to present two connections of a single client, which is
     * what the same-OplockKey / same-client arbitration rules turn on. */
    int               guid_tag;
    struct evpl_bind *bind;
    int               connected;
    int               disconnected;
    /* Set by smb2_conn_disconnect: THIS side closed the transport.  Without it
     * `disconnected` conflates "the server hung up on us" (a finding) with "we
     * dropped the link on purpose" (the premise of every durable/replay test),
     * and the pump loops would report the former for the latter. */
    int               closed_by_client;

    uint64_t          session_id;
    uint32_t          tree_id;
    uint64_t          msg_id;
    uint16_t          dialect;

    uint8_t          *sbuf;       /* request scratch */
    uint8_t          *rbuf;       /* last reply (framed, NetBIOS included) */
    uint8_t          *xbuf;       /* receive scratch (breaks land here) */
    int               rlen;
    int               reply_ready;
    /* Identity and count of FINAL replies (not interims, not breaks) promoted
     * into rbuf.  smb2_echo_barrier() needs both: it waits for the reply that
     * carries ITS MessageId, and reports how many other final replies -- i.e.
     * previously parked requests completing -- landed while it was
     * outstanding. */
    uint64_t          reply_mid;
    int               nreply;
    /* Final replies EXCLUDING the harness's own ECHO barrier round trips, so
     * a caller can sample it before posting a request and ask afterwards
     * "did anything of mine complete?" without having to subtract the
     * barrier's own traffic. */
    int               nreply_app;

    /* Async interim (STATUS_PENDING) accounting.  An interim tells the caller
     * the request PARKED server-side (smb_async_interim.c: the interim is sent
     * the instant a handler decides it must block, never on a latency
     * threshold), so observing one is a first-class fact -- the model's
     * ST_PENDING maps onto exactly this.  The final response carries the same
     * AsyncId, which is the original MessageId. */
    int               ninterim;   /* interims seen on this connection */
    uint64_t          last_async_id;
    int               interim_pending; /* an interim arrived, final not yet */

    /* One-shot request-header modifiers, armed by the smb2c_set_next_* /
     * smb2c_pin_msg_id helpers and CONSUMED (cleared) by the next smb2c_send.
     * They are one-shot on purpose: a replay test arms exactly one request and
     * must not silently taint every request that follows it. */
    uint32_t          next_flags;         /* OR'd into the header Flags */
    int               next_flags_armed;
    uint16_t          next_cs;            /* header offset 8: ChannelSequence */
    int               next_cs_armed;
    uint64_t          pin_msg_id;         /* send with THIS MessageId ... */
    int               pin_armed;          /* ... and do not advance msg_id */
    uint64_t          last_msg_id;        /* MessageId actually stamped */

    /* unsolicited oplock/lease break notifications, oldest first */
    struct smb2_break brk[SMB2C_MAX_BREAKS];
    int               nbrk;
};

/* One CREATE context to serialize onto a request (MS-SMB2 2.2.13.2).  `name`
 * is the raw context-name bytes: a 4-byte tag ("RqLs", "DH2Q", ...) or a
 * 16-byte GUID for the GUID-named contexts. */
struct smb2_cctx {
    const uint8_t *name;
    int            name_len;
    const uint8_t *data;
    int            data_len;
};

/* A CREATE-response context, recorded VERBATIM.  The parser records every
 * context the server emitted, not just the ones it knows how to interpret:
 * "the server sent a context we did not expect" and "the server omitted one we
 * did" are both findings, and neither is observable if the parser only looks
 * for the tags it cares about. */
#define SMB2C_MAX_RSP_CTX      8
#define SMB2C_MAX_RSP_CTX_DATA 128

struct smb2_rsp_ctx {
    uint8_t  name[16];
    int      name_len;
    uint8_t  data[SMB2C_MAX_RSP_CTX_DATA];
    uint32_t data_len;     /* bytes captured (capped at the buffer size) */
    uint32_t wire_len;     /* DataLength as it was on the wire */
};

/* Durable-handle contexts to attach to a CREATE.  Request side: DHnQ (v1) /
 * DH2Q (v2).  Reconnect side: DHnC (v1) / DH2C (v2).  Combinations are
 * deliberately expressible -- several of them are ILLEGAL (MS-SMB2 3.3.5.9.12
 * rejects DH2C with any other durable context) and a probe must be able to
 * send an illegal one to pin the rejection. */
struct smb2_durable_req {
    int      dhnq;             /* request a v1 durable handle */
    int      dh2q;             /* request a v2 durable handle */
    uint32_t timeout_ms;       /* DH2Q Timeout (0 = server default) */
    uint32_t flags;            /* DH2Q Flags (SMB2_DHANDLE_FLAG_PERSISTENT) */
    uint8_t  create_guid[16];  /* DH2Q/DH2C CreateGuid: the replay identity */
    int      dhnc;             /* v1 reconnect */
    int      dh2c;             /* v2 reconnect */
    uint8_t  file_id[16];      /* DHnC/DH2C FileId being reclaimed */
    uint32_t reconnect_flags;  /* DH2C Flags */
};

/* Parsed CREATE reply. */
struct smb2_create_out {
    uint32_t            status;
    uint8_t             oplock; /* reply OplockLevel byte */
    uint32_t            action; /* CreateAction */
    uint64_t            end_of_file;
    uint64_t            change_time;
    uint8_t             file_id[16];
    int                 has_lease; /* an RqLs response context was present */
    uint32_t            lease_state; /* granted SMB lease bits */
    uint32_t            lease_flags;
    uint16_t            lease_epoch;

    /* Every response context, in wire order. */
    struct smb2_rsp_ctx ctx[SMB2C_MAX_RSP_CTX];
    int                 nctx;
    int                 ctx_overflow;   /* more than SMB2C_MAX_RSP_CTX arrived */

    /* Interpreted durable-handle response contexts. */
    int                 has_dh2q; /* a DH2Q response context was present */
    uint32_t            dh2q_timeout; /* granted Timeout, ms */
    uint32_t            dh2q_flags; /* granted Flags (PERSISTENT set or not) */
    int                 has_dhnq; /* a DHnQ response context was present */
};

/* An oplock/lease request attached to a CREATE. */
struct smb2_oplock_req {
    uint8_t  level;          /* legacy: SMB2_OPLOCK_LEVEL_*; 0 = none */
    int      is_lease;       /* request an RqLs lease instead */
    uint8_t  lease_key[16];
    uint32_t lease_state;    /* requested SMB lease bits (R/H/W) */
    uint16_t lease_epoch;    /* v2 request epoch */
    /* Force a v1 (32-byte) RqLs context even on a 3.x dialect.  A v1 lease is
     * not epoch-versioned (it breaks with epoch 0) and can never carry a
     * directory lease, so the v1/v2 split is a real behavioral axis rather
     * than a dialect artifact (MS-SMB2 2.2.13.2.8 vs 2.2.13.2.10). */
    int      force_v1;
};

/* ---- evpl stream plumbing ---------------------------------------------- */

/* NetBIOS framing: the next message is 4 + (big-endian low-24 of word). */
static int
smb2c_segment(
    struct evpl      *evpl,
    struct evpl_bind *bind,
    void             *private_data)
{
    uint8_t  hdr[4];
    int      n = evpl_peek(evpl, bind, hdr, 4);

    if (n < 4) {
        return 0;
    }
    uint32_t len = ((uint32_t) hdr[1] << 16) | ((uint32_t) hdr[2] << 8) |
        hdr[3];
    return 4 + (int) len;
} /* smb2c_segment */

static void
smb2c_record_break(
    struct smb2_conn *c,
    const uint8_t    *body)   /* points at the SMB2 body (rbuf+4+64) */
{
    if (c->nbrk >= SMB2C_MAX_BREAKS) {
        return;
    }
    struct smb2_break *b   = &c->brk[c->nbrk];
    uint16_t           ssz = g16(body, 0);

    memset(b, 0, sizeof(*b));
    if (ssz == 24) {
        /* OPLOCK_BREAK notification (MS-SMB2 2.2.23.1) */
        b->is_lease     = 0;
        b->oplock_level = body[2];
        memcpy(b->file_id, body + 8, 16);
        b->ack_required = 1;   /* legacy exclusive/batch breaks require ack */
        c->nbrk++;
    } else if (ssz == 44) {
        /* LEASE_BREAK notification (MS-SMB2 2.2.23.2) */
        b->is_lease  = 1;
        b->new_epoch = g16(body, 2);
        b->flags     = g32(body, 4);
        memcpy(b->lease_key, body + 8, 16);
        b->cur_state    = g32(body, 24);
        b->new_state    = g32(body, 28);
        b->ack_required = (b->flags & 0x01) != 0;   /* FLAG_ACK_REQUIRED */
        c->nbrk++;
    }
    /* any other struct size: not a break we model; ignore */
} /* smb2c_record_break */

static void
smb2c_notify(
    struct evpl        *evpl,
    struct evpl_bind   *bind,
    struct evpl_notify *notify,
    void               *private_data)
{
    struct smb2_conn *c = private_data;

    switch (notify->notify_type) {
        case EVPL_NOTIFY_CONNECTED:
            c->connected = 1;
            break;
        case EVPL_NOTIFY_DISCONNECTED:
            c->disconnected = 1;
            break;
        case EVPL_NOTIFY_RECV_MSG: {
            int off = 0;
            /* Receive into a scratch buffer, not rbuf: an unsolicited break
             * notification can arrive while a reply already sits in rbuf
             * (a holder connection that is itself mid-request), and copying
             * the break over it would corrupt the reply the caller is about
             * to parse.  Only a real reply is promoted into rbuf. */
            for (unsigned int i = 0; i < notify->recv_msg.niov; i++) {
                void *d   = evpl_iovec_data(&notify->recv_msg.iovec[i]);
                int   len = evpl_iovec_length(&notify->recv_msg.iovec[i]);
                if (off + len <= SMB2C_BUFSZ) {
                    memcpy(c->xbuf + off, d, len);
                }
                off += len;
            }
            for (unsigned int i = 0; i < notify->recv_msg.niov; i++) {
                evpl_iovec_release(evpl, &notify->recv_msg.iovec[i]);
            }
            if (off >= 4 + SMB2_HDR_SIZE) {
                uint16_t cmd    = g16(c->xbuf + 4, 12);
                uint64_t mid    = g64(c->xbuf + 4, 24);
                uint32_t status = g32(c->xbuf + 4, 8);
                uint32_t hflags = g32(c->xbuf + 4, 16);
                /* Unsolicited break: command 18, MessageId all-ones
                 * (smb_proc_oplock_break.c:58). */
                if (cmd == SMB2_OPLOCK_BREAK &&
                    mid == 0xFFFFFFFFFFFFFFFFull) {
                    smb2c_record_break(c, c->xbuf + 4 + SMB2_HDR_SIZE);
                    break;
                }
                /* Async interim: a parked op (e.g. a CREATE deferred behind
                 * an oplock/lease break) gets a STATUS_PENDING response with
                 * the ASYNC flag first, then the real reply on the SAME
                 * request, tagged with the same AsyncId (= the original
                 * MessageId).  Record it -- "did this request park?" is the
                 * wire fact the model's ST_PENDING names -- and keep waiting
                 * for the final response (smb_async_interim.c). */
                if (status == ST_PENDING &&
                    (hflags & SMB2_FLAGS_ASYNC_COMMAND)) {
                    c->ninterim++;
                    c->interim_pending = 1;
                    c->last_async_id   = g64(c->xbuf + 4, 32);
                    break;
                }
            }
            memcpy(c->rbuf, c->xbuf, (size_t) (off <= SMB2C_BUFSZ
                                               ? off : SMB2C_BUFSZ));
            c->rlen        = off;
            c->reply_ready = 1;
            if (off >= 4 + SMB2_HDR_SIZE) {
                c->reply_mid = g64(c->xbuf + 4, 24);
                c->nreply++;
                if (g16(c->xbuf + 4, 12) != SMB2_ECHO) {
                    c->nreply_app++;
                }
            }
            break;
        } /* EVPL_NOTIFY_RECV_MSG */
    } /* switch */
} /* smb2c_notify */

/* ---- server + connection lifecycle -------------------------------------- */

/* Bring up the server + client evpl with NO filesystem or share mounted.  The
 * MBT batch replayer calls this once and then cycles a fresh filesystem per
 * trace via smb2_env_fs_setup/teardown, amortizing server init/start over the
 * whole corpus.  smb2_env_start_opts (below) keeps the one-shot
 * open+fs("fs0") shape the smoke/oplock probes rely on. */
static inline void
smb2_env_open_opts(
    struct smb2_env            *env,
    const struct smb2_env_opts *opts)
{
    struct chimera_server_config *config;

    memset(env, 0, sizeof(*env));

    /* Sweep the VFS open cache aggressively.  When a trace's connections are
     * disconnected (smb2_conn_reset), the server closes their file handles
     * asynchronously via the close thread's periodic sweep, and the memfs
     * filesystem stays EBUSY until that drains -- the batch replayer waits on
     * it before rmfs.  The production default is 1s per sweep, which would
     * dominate per-trace teardown (~0.85s each); 10ms shrinks it to ~40ms and
     * is purely a close-timing knob (handles are already protocol-released, so
     * this changes no observable behavior).  Respect an external override
     * (overwrite=0) so CI can tune it. */
    setenv("CHIMERA_CLOSE_SWEEP_INTERVAL_MS", "10", 0);

    snprintf(env->session_dir, sizeof(env->session_dir),
             "/tmp/smb2_mbt_XXXXXX");
    if (!mkdtemp(env->session_dir)) {
        fprintf(stderr, "mkdtemp(%s) failed\n", env->session_dir);
        exit(1);
    }

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);

    config = chimera_server_config_init();
    chimera_server_config_set_state_dir(config, env->session_dir);
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);
    chimera_server_config_set_smb_enabled(config, 1);
    chimera_server_config_set_smb_signing_required(config, 0);

    if (opts) {
        chimera_server_config_set_smb_oplocks(config, opts->oplocks);
        chimera_server_config_set_smb_leases(config, opts->leases);
        chimera_server_config_set_smb_directory_leases(config,
                                                       opts->directory_leases);
        chimera_server_config_set_smb_persistent_handles(config,
                                                         opts->persistent_handles);
    }

    if (opts) {
        env->opts = *opts;
    }

    env->server = chimera_server_init(config, env->metrics);
    chimera_server_start(env->server);

    /* The CLIENT loop's idle poll interval.  Not a test deadline: every wait
     * here is a `while (!condition) { pump; }`, so the value only trades pump
     * latency against CPU.  A bound is required rather than merely nice -- the
     * default (-1) parks evpl_continue in the poller until something happens,
     * and a pump that runs while this side is idle would block forever.
     * MEASURED: 0 (pure spin) is 3x SLOWER end to end for the smb_mbt suite
     * (33.8 s against 11.3 s at 1 ms), because the spinning client thread
     * starves the server's evpl threads on the container's CPU budget.
     * evpl_create() takes ownership of the config. */
    struct evpl_thread_config *tcfg = evpl_thread_config_init();

    evpl_thread_config_set_wait_ms(tcfg, 1);
    env->evpl = evpl_create(tcfg);
} /* smb2_env_open_opts */

/* Create a fresh memfs filesystem `fsname` and mount the "share" onto it.  A
 * unique fsname per trace yields a distinct fsid -> distinct FH mount-id, so
 * no server-side attr/name/handle cache entry can be hit across traces (memfs
 * is a named-filesystem backend, CHIMERA_VFS_CAP_MKFS).  Safe on the running
 * server because no requests are in flight between traces. */
static inline void
smb2_env_fs_setup(
    struct smb2_env *env,
    const char      *fsname)
{
    if (chimera_server_mkfs(env->server, "memfs", fsname, NULL) != 0) {
        fprintf(stderr, "failed to create memfs filesystem %s\n", fsname);
        exit(1);
    }
    chimera_server_mount(env->server, "share", "memfs", fsname, NULL);
    /* The share carries the env's feature options: continuous availability
     * gates persistent handles, and force-level2 caps every caching grant on
     * the share (probe O10). */
    chimera_server_create_share(env->server, "share", "share",
                                env->opts.continuous_availability);

    if (env->opts.force_level2) {
        chimera_server_share_set_force_level2_oplock(env->server, "share");
    }
} /* smb2_env_fs_setup */

/* Tear the share/mount/filesystem back down.  rmfs is EBUSY while mounted, so
 * the order is remove_share -> unmount -> rmfs (mirrors the NFS harness).
 *
 * Unlike the stateless NFS3 harness, SMB holds file opens for the life of a
 * session; smb2_conn_reset disconnects those sessions, but the server closes
 * their VFS handles asynchronously on its close thread, so the filesystem
 * stays EBUSY (memfs open_count > 0) for a short window afterward.  Retry rmfs
 * until that drains rather than racing it -- the same bounded-retry shape the
 * NFS4 harness uses for NFS4ERR_DELAY. */
#define SMB2_RMFS_RETRY_MAX 5000            /* * 1ms = 5s ceiling */
static inline void
smb2_env_fs_teardown(
    struct smb2_env *env,
    const char      *fsname)
{
    int tries = 0;

    chimera_server_remove_share(env->server, "share");
    chimera_server_unmount(env->server, "share");
    while (chimera_server_rmfs(env->server, "memfs", fsname) != 0) {
        if (++tries >= SMB2_RMFS_RETRY_MAX) {
            fprintf(stderr, "failed to remove memfs filesystem %s "
                    "(still busy after %d retries)\n", fsname, tries);
            exit(1);
        }
        usleep(1000);
    }
} /* smb2_env_fs_teardown */

static inline void
smb2_env_start_opts(
    struct smb2_env            *env,
    const struct smb2_env_opts *opts)
{
    smb2_env_open_opts(env, opts);
    smb2_env_fs_setup(env, "fs0");
} /* smb2_env_start_opts */

static inline void
smb2_env_start(struct smb2_env *env)
{
    struct smb2_env_opts opts = { 0 };

    smb2_env_start_opts(env, &opts);
} /* smb2_env_start */

static inline void
smb2_pump(struct smb2_env *env)
{
    evpl_continue(env->evpl);
} /* smb2_pump */

/* Open a fresh SMB2 connection to the in-process server. */
static inline struct smb2_conn *
smb2_conn_open(struct smb2_env *env)
{
    struct smb2_conn     *c;
    struct evpl_endpoint *ep;

    if (env->nconns >= SMB2C_MAX_CONNS) {
        fprintf(stderr,
                "smb2 harness: out of connection slots (SMB2C_MAX_CONNS=%d).\n"
                "Connections are retired, never recycled, so a long walk with\n"
                "many transport drops needs a larger bound -- raise it (and\n"
                "MAX_SESS in smb2_mbt_replay.c) rather than reusing a slot.\n",
                SMB2C_MAX_CONNS);
        exit(1);
    }

    c  = calloc(1, sizeof(*c));
    ep =
        chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                           "127.0.0.1", 445);

    c->env        = env;
    c->conn_index = env->nconns;
    c->guid_tag   = env->nconns;
    c->sbuf       = malloc(SMB2C_BUFSZ);
    c->rbuf       = malloc(SMB2C_BUFSZ);
    c->xbuf       = malloc(SMB2C_BUFSZ);

    c->bind = evpl_connect(env->evpl, EVPL_STREAM_INPROC, NULL, ep,
                           smb2c_notify, smb2c_segment, c);
    if (!c->bind) {
        fprintf(stderr, "failed to connect to in-process SMB server\n");
        exit(1);
    }
    while (!c->connected) {
        smb2_pump(env);
    }
    env->conns[env->nconns++] = c;
    return c;
} /* smb2_conn_open */

/* Disconnect and free every connection a trace opened, dropping the
 * server-side sessions/trees/opens with them, so the next trace starts from a
 * clean protocol state and the filesystem it held can be unmounted/rmfs'd.
 * The batch replayer calls this once per trace, before smb2_env_fs_teardown.
 * env->conns[] is bounded (SMB2C_MAX_CONNS), so resetting nconns to 0 each
 * trace is also what keeps a whole corpus from overflowing it. */
static inline void
smb2_conn_reset(struct smb2_env *env)
{
    for (int i = 0; i < env->nconns; i++) {
        struct smb2_conn *c = env->conns[i];

        /* A trace may already have dropped this connection itself: a durable
         * or replay trace calls smb2_conn_disconnect to park its handles and
         * reconnect.  evpl treats closing a bind twice as fatal ("bind %p
         * already closed"), so close only what is still open.  The wait below
         * still runs either way -- a client-initiated close is only complete
         * once evpl has delivered DISCONNECTED. */
        if (!c->closed_by_client) {
            evpl_close(env->evpl, c->bind);
        }
        while (!c->disconnected) {
            smb2_pump(env);
        }
    }
    for (int i = 0; i < env->nconns; i++) {
        free(env->conns[i]->sbuf);
        free(env->conns[i]->rbuf);
        /* xbuf too -- smb2_conn_open allocates three buffers per connection.
         * Missing it leaks SMB2C_BUFSZ per connection, which a batched run
         * that resets its connections once per trace turns into hundreds of
         * megabytes. */
        free(env->conns[i]->xbuf);
        free(env->conns[i]);
    }
    env->nconns = 0;
} /* smb2_conn_reset */

static inline void
smb2_env_stop(struct smb2_env *env)
{
    char cmd[300];

    /* evpl_destroy sends a final DISCONNECTED to each still-registered bind,
     * so the conn structs must outlive it. */
    evpl_destroy(env->evpl);
    for (int i = 0; i < env->nconns; i++) {
        free(env->conns[i]->sbuf);
        free(env->conns[i]->rbuf);
        free(env->conns[i]->xbuf);
        free(env->conns[i]);
    }
    chimera_server_destroy(env->server);
    prometheus_metrics_destroy(env->metrics);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", env->session_dir);
    if (system(cmd) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", env->session_dir);
    }
} /* smb2_env_stop */

/* ---- break queue helpers ------------------------------------------------ */

static inline int
smb2_conn_nbreaks(struct smb2_conn *c)
{
    return c->nbrk;
} /* smb2_conn_nbreaks */

static inline int
smb2_conn_pop_break(
    struct smb2_conn  *c,
    struct smb2_break *out)
{
    if (c->nbrk == 0) {
        return 0;
    }
    *out = c->brk[0];
    for (int i = 1; i < c->nbrk; i++) {
        c->brk[i - 1] = c->brk[i];
    }
    c->nbrk--;
    return 1;
} /* smb2_conn_pop_break */

/* What the harness is currently doing, for the wedge diagnostic.  A caller
 * that has a name for the step (the replayer names the trace and the command)
 * points this at it; NULL just omits the line. */
static const char *smb2c_context_str = NULL;

static inline void
smb2c_set_context(const char *ctx)
{
    smb2c_context_str = ctx;
} /* smb2c_set_context */

/* ---- one SMB2 request / response --------------------------------------- */

/* Build the 4-byte NetBIOS header + 64-byte SMB2 header into c->sbuf for
 * `command`; zero a generous body region so all Reserved bytes are clean;
 * returns the offset of the body (== 4 + SMB2_HDR_SIZE). */
static inline int
smb2c_begin(
    struct smb2_conn *c,
    uint16_t          command,
    uint32_t          flags)
{
    uint8_t *h;

    /* A PDU is built into a CONNECTION's send buffer, so there is no such
    * thing as building one without a connection.  Callers that resolve a
    * connection from a table -- the MBT replayer maps a model session id to
    * one -- can produce a NULL, and before this check that NULL reached
    * `c->sbuf` and died as a SEGV inside the harness, which reads as a server
    * crash and is not one.  Fail here instead, naming the command and the
    * replay context, so the caller's bookkeeping bug is the diagnosis. */
    if (!c) {
        fprintf(stderr,
                "smb2 harness: BUG -- request 0x%04x built with no connection"
                "%s%s\n", command,
                smb2c_context_str ? "\n  while replaying: " : "",
                smb2c_context_str ? smb2c_context_str : "");
        exit(5);
    }

    h = c->sbuf + 4;            /* SMB2 header start */

    memset(c->sbuf, 0, 4 + SMB2_HDR_SIZE + 256);
    h[0] = 0xFE;
    h[1] = 'S';
    h[2] = 'M';
    h[3] = 'B';
    p16(h, 4, SMB2_HDR_SIZE);
    p16(h, 6, 1);                 /* CreditCharge */
    p16(h, 12, command);
    p16(h, 14, 256);              /* CreditRequest */
    p32(h, 16, flags);
    p64(h, 24, c->msg_id);
    p32(h, 36, c->tree_id);
    p64(h, 40, c->session_id);
    return 4 + SMB2_HDR_SIZE;
} /* smb2c_begin */

/* Post a request (non-blocking): frame body_len bytes, send, clear the
 * reply flag.  The MessageId advances so the next request is fresh. */
static inline void
smb2c_send(
    struct smb2_conn *c,
    int               body_len)
{
    uint32_t payload = SMB2_HDR_SIZE + body_len;
    uint8_t *h       = c->sbuf + 4;

    /* Apply the one-shot header modifiers.  They patch the header AFTER the
     * per-command builder wrote it, which is what makes them command-agnostic:
     * any request can be marked a replay, given a ChannelSequence, or pinned to
     * a specific MessageId without every builder growing three parameters. */
    if (c->next_flags_armed) {
        p32(h, 16, g32(h, 16) | c->next_flags);
    }
    if (c->next_cs_armed) {
        /* Header offset 8..11 is the (Status, Reserved) pair on a RESPONSE and
         * (ChannelSequence, Reserved) on a REQUEST -- the same four bytes
         * (MS-SMB2 2.2.1.2).  Status is only ever read out of c->rbuf, never
         * out of c->sbuf, so writing here cannot perturb status parsing. */
        p16(h, 8, c->next_cs);
        p16(h, 10, 0);
    }
    if (c->pin_armed) {
        p64(h, 24, c->pin_msg_id);
    }
    c->last_msg_id = g64(h, 24);

    c->sbuf[0] = 0;
    c->sbuf[1] = (uint8_t) (payload >> 16);
    c->sbuf[2] = (uint8_t) (payload >> 8);
    c->sbuf[3] = (uint8_t) payload;

    c->reply_ready     = 0;
    c->interim_pending = 0;
    evpl_send(c->env->evpl, c->bind, c->sbuf, 4 + payload);
    if (!c->pin_armed) {
        c->msg_id++;
    }
    c->next_flags       = 0;
    c->next_flags_armed = 0;
    c->next_cs          = 0;
    c->next_cs_armed    = 0;
    c->pin_armed        = 0;
} /* smb2c_send */

/* ---- one-shot request-header modifiers ---------------------------------
 *
 * MS-SMB2 3.2.4.1.2: after a channel/transport failure the client RE-SENDS the
 * request with SMB2_FLAGS_REPLAY_OPERATION set, and the server must apply it
 * exactly once.  What a conformant client does NOT do is reuse the MessageId:
 * 3.3.5.2.3 makes the server drop the connection when a MessageId falls outside
 * the command-sequence window, and chimera does exactly that (measured: it logs
 * "MessageId N ... outside the command sequence window" and closes, with no
 * reply).  So a WIRE replay is a FRESH MessageId carrying the replay flag,
 * correlated by DH2Q CreateGuid (creates) or ChannelSequence (mutating ops).
 *
 * smb2c_pin_msg_id is therefore NOT the replay primitive.  It exists for the
 * two things that legitimately reuse a MessageId: testing the sequence-window
 * rule itself, and SMB2_CANCEL (which addresses its target BY MessageId). */

/* Send the next request with `mid` as its MessageId and do NOT advance the
 * connection's counter. */
static inline void
smb2c_pin_msg_id(
    struct smb2_conn *c,
    uint64_t          mid)
{
    c->pin_msg_id = mid;
    c->pin_armed  = 1;
} /* smb2c_pin_msg_id */

/* OR `flags` into the next request's header Flags (SMB2_FLAGS_REPLAY_OPERATION,
 * SMB2_FLAGS_DFS_OPERATIONS, ...). */
static inline void
smb2c_set_next_flags(
    struct smb2_conn *c,
    uint32_t          flags)
{
    c->next_flags       = flags;
    c->next_flags_armed = 1;
} /* smb2c_set_next_flags */

/* Stamp the next request's ChannelSequence (MS-SMB2 3.3.5.2.10). */
static inline void
smb2c_set_next_channel_sequence(
    struct smb2_conn *c,
    uint16_t          cs)
{
    c->next_cs       = cs;
    c->next_cs_armed = 1;
} /* smb2c_set_next_channel_sequence */

/* Disarm every pending one-shot without sending. */
static inline void
smb2c_clear_next(struct smb2_conn *c)
{
    c->next_flags       = 0;
    c->next_flags_armed = 0;
    c->next_cs          = 0;
    c->next_cs_armed    = 0;
    c->pin_armed        = 0;
} /* smb2c_clear_next */

static inline uint64_t
smb2c_now_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000u + (uint64_t) (ts.tv_nsec / 1000000);
} /* smb2c_now_ms */

/* ---- hang guard ---------------------------------------------------------
 *
 * No VERDICT in this harness is decided by elapsed wall-clock time.  Every
 * assertion is settled by an EVENT: a reply arriving, a break notification
 * arriving, or an echo barrier (below) completing.  What the ceiling here buys
 * is the opposite of a timing dependence -- when the server genuinely wedges,
 * the harness must fail loudly and name the wait it wedged on rather than
 * spin until ctest kills it and reports nothing.
 *
 * It is set far above any healthy wait (a healthy wait is microseconds, and
 * every one of them exits on its event) precisely so that a loaded machine can
 * never turn it into a verdict, and just under chimera's own oplock-break
 * deadline (CHIMERA_VFS_STATE_DEFAULT_BREAK_DEADLINE_MS = 30 s, armed by
 * smb_proc_create.c on a parked CREATE) so the harness's diagnostic fires
 * before the server starts force-revoking parked opens and rewriting the state
 * the diagnostic is meant to describe. */
#define SMB2C_HANG_MS 20000

/* The connection went away while we were waiting on it.  Which of the two
 * reasons it was decides whether this is a server finding or a harness bug, so
 * say which. */
static inline void
smb2c_dead(struct smb2_conn *c)
{
    if (c->closed_by_client) {
        fprintf(stderr,
                "smb2 harness: waiting for a reply on conn %d, which THIS side\n"
                "closed with smb2_conn_disconnect().  Use the connection\n"
                "returned by smb2_conn_reopen() instead.%s%s\n",
                c->conn_index,
                smb2c_context_str ? "\n  while replaying: " : "",
                smb2c_context_str ? smb2c_context_str : "");
        exit(4);
    }
    fprintf(stderr, "SMB server dropped the connection%s%s\n",
            smb2c_context_str ? "\n  while replaying: " : "",
            smb2c_context_str ? smb2c_context_str : "");
    exit(3);
} /* smb2c_dead */

static inline void
smb2c_hang(
    struct smb2_conn *c,
    const char       *what)
{
    fprintf(stderr,
            "smb2 harness: WEDGED waiting for %s on conn %d after %d ms "
            "(%d interim(s) seen, %d break(s) queued, reply_ready=%d).\n"
            "The request is parked on an event nobody is driving -- a server\n"
            "deferral that never resumed, or a harness step that forgot to\n"
            "acknowledge a break.\n",
            what, c->conn_index, SMB2C_HANG_MS, c->ninterim, c->nbrk,
            c->reply_ready);
    if (smb2c_context_str) {
        fprintf(stderr, "  while replaying: %s\n", smb2c_context_str);
    }
    exit(4);
} /* smb2c_hang */

/* Pump the shared loop until `c`'s reply lands.  A reply that never arrives is
 * a wedge, not an outcome: abort with a diagnostic naming `what`. */
static inline void
smb2c_pump_for_reply(
    struct smb2_conn *c,
    const char       *what)
{
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;

    while (!c->reply_ready) {
        smb2_pump(c->env);
        if (c->disconnected) {
            smb2c_dead(c);
        }
        if (smb2c_now_ms() >= deadline) {
            smb2c_hang(c, what);
        }
    }
} /* smb2c_pump_for_reply */

/* Pump until a final non-barrier reply beyond `mark` (a previously sampled
 * c->nreply_app) lands.  Unlike smb2c_pump_for_reply this tolerates
 * c->reply_ready already being set by an unrelated reply -- notably an ECHO
 * barrier's own -- so it is the right wait for "the request I parked earlier
 * now completes".
 *
 * Returns 1 when it arrives.  On the hang ceiling it returns 0 after printing
 * a diagnostic, so the CALLER's assertion is what fails: a wedged deferral
 * reports as a named probe failure, never as an opaque ctest timeout. */
static inline int
smb2c_pump_for_nreply(
    struct smb2_conn *c,
    int               mark,
    const char       *what)
{
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;

    while (c->nreply_app <= mark) {
        smb2_pump(c->env);
        if (c->disconnected) {
            smb2c_dead(c);
        }
        if (smb2c_now_ms() >= deadline) {
            fprintf(stderr,
                    "smb2 harness: WEDGED waiting for %s on conn %d after "
                    "%d ms (%d interim(s) seen, %d break(s) queued)%s%s\n",
                    what, c->conn_index, SMB2C_HANG_MS, c->ninterim, c->nbrk,
                    smb2c_context_str ? "\n  while replaying: " : "",
                    smb2c_context_str ? smb2c_context_str : "");
            return 0;
        }
    }
    return 1;
} /* smb2c_pump_for_nreply */

/* Pump until THIS connection's reply lands.  Only safe when the reply does
 * not depend on another connection acting first (setup + quiescent ops);
 * break scenarios drive the loop explicitly instead. */
static inline uint32_t
smb2c_wait(struct smb2_conn *c)
{
    smb2c_pump_for_reply(c, "a request reply");
    return g32(c->rbuf + 4, 8);   /* reply header Status */
} /* smb2c_wait */

/* ---- causal settle barrier ----------------------------------------------
 *
 * The hard question in this harness is "has the server finished reacting to
 * what I just did?", and its hard sub-case is "prove that NOTHING more is
 * coming" -- proving a negative.  Sleeping for N milliseconds answers neither:
 * it makes the verdict a function of machine load, which is the thing a test
 * must never be.
 *
 * SMB2 ECHO answers both, causally.  chimera completes an ECHO synchronously
 * on the connection's own server thread (smb_proc_echo.c: chimera_smb_echo ->
 * chimera_smb_complete_request, no VFS call, no deferral) and queues the reply
 * on the SAME evpl bind that thread uses for deferred completions and for the
 * oplock/lease break notifications it flushes from its doorbell.  So an ECHO
 * reply is proof that that thread ran complete event-loop passes after the
 * barrier was posted -- and it is proof no matter how long those passes took.
 *
 * TWO round trips, not one.  A break destined for this connection is queued by
 * whichever thread ran the conflicting operation, which then rings this
 * thread's doorbell (smb.c: lease_resume_doorbell / the lease-break flush).
 * That doorbell is pending before the barrier's first ECHO is even parsed, but
 * evpl does not promise to service doorbells ahead of socket reads within one
 * pass, so a single round trip could in principle overtake it.  It cannot
 * overtake two: whatever was pending when the barrier started has been
 * serviced by the end of the first pass, and the second round trip cannot
 * complete until a pass after that one has finished.  The second round trip is
 * load-bearing even inside smb2_quiesce()'s fixpoint loop, and for a reason
 * that is easy to miss: with one round the CLIENT stops pumping the instant
 * the echo reply lands, so a notification the server sent later in that same
 * server-side pass is still in flight when the quiesce loop reads its event
 * count, sees no change, and declares the system quiet.
 *
 * One barrier on one connection settles ONE thread against work that was
 * already queued for it.  It does NOT settle a CHAIN -- B's CREATE has to run
 * on B's thread before A's thread has anything to flush -- so a barrier is the
 * primitive, and smb2_quiesce() below is what callers should reach for.
 *
 * Returns the number of OTHER final replies that landed while the barrier was
 * outstanding -- i.e. how many previously-parked requests on this connection
 * completed.  Break notifications are unsolicited and keep accumulating in
 * c->brk as usual, so "has the break been delivered yet?" is answered by
 * reading smb2_conn_nbreaks() after the barrier returns.
 *
 * Costs one SMB2 round trip per round; it replaces multi-second sleeps. */
#define SMB2C_BARRIER_ROUNDS 2

static inline int
smb2_echo_barrier(struct smb2_conn *c)
{
    int others = 0;

    for (int round = 0; round < SMB2C_BARRIER_ROUNDS; round++) {
        uint64_t mid      = c->msg_id;
        int      napp0    = c->nreply_app;
        uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
        int      b        = smb2c_begin(c, SMB2_ECHO, 0);
        uint8_t *body     = c->sbuf + b;

        p16(body, 0, 4);      /* StructureSize */
        p16(body, 2, 0);      /* Reserved */
        smb2c_send(c, 4);

        while (!(c->reply_ready && c->reply_mid == mid)) {
            smb2_pump(c->env);
            if (c->disconnected) {
                smb2c_dead(c);
            }
            if (smb2c_now_ms() >= deadline) {
                smb2c_hang(c, "an ECHO barrier round trip");
            }
        }
        /* Everything that arrived in this window other than the barrier's own
         * ECHO reply was a parked request completing. */
        others += c->nreply_app - napp0;
    }
    return others;
} /* smb2_echo_barrier */

/* Barrier EVERY connection of the env: one pass over all of them. */
static inline int
smb2_settle(struct smb2_env *env)
{
    int others = 0;

    for (int i = 0; i < env->nconns; i++) {
        struct smb2_conn *c = env->conns[i];

        if (c && c->connected && !c->disconnected && c->session_id) {
            others += smb2_echo_barrier(c);
        }
    }
    return others;
} /* smb2_settle */

/* Everything the harness can observe the server doing, summed over all
 * connections: break notifications delivered, async interims sent, and final
 * replies other than the barrier's own echoes. */
static inline int
smb2_event_count(struct smb2_env *env)
{
    int n = 0;

    for (int i = 0; i < env->nconns; i++) {
        struct smb2_conn *c = env->conns[i];

        if (c) {
            n += c->nbrk + c->ninterim + c->nreply_app;
        }
    }
    return n;
} /* smb2_event_count */

/* Drive the server to QUIESCENCE: settle every connection repeatedly until a
 * whole pass produces no new observable event, then stop.
 *
 * This, not a single barrier, is what "the server has finished reacting to
 * what I just did" means, because reacting is a CHAIN across threads.  A
 * conflicting CREATE on B's thread has to run before it queues a break for A
 * and rings A's doorbell; only then does a barrier on A have anything to
 * flush.  Settling A first proves nothing -- and settling A then B exactly
 * once is a race, which is how a fixed two-barrier version of this failed
 * about 1 run in 20 under `ctest --repeat until-fail:20`, reporting
 * `A breaks pending=0` for a break that had simply not been handed off yet.
 *
 * The stopping rule is a fixpoint, not a duration: each pass advances every
 * server thread by at least one complete event-loop iteration, so a reaction
 * chain of depth d is exhausted in d passes and the (d+1)th pass observes
 * nothing new.  A quiet system therefore costs exactly ONE pass.  Nothing here
 * consults the clock except the per-round-trip wedge guard.
 *
 * The pass cap is a wedge guard, not a tuning knob: reaction chains in this
 * server are a handful of hops (break -> ack -> resume -> re-arbitrate), so
 * hitting 64 means something is generating events forever, which is a finding
 * rather than a wait to be extended.
 *
 * Returns the number of parked requests that completed while quiescing. */
#define SMB2C_QUIESCE_MAX_PASSES 64

static inline int
smb2_quiesce(struct smb2_env *env)
{
    int completed = 0;

    for (int pass = 0; pass < SMB2C_QUIESCE_MAX_PASSES; pass++) {
        int before = smb2_event_count(env);

        completed += smb2_settle(env);
        if (smb2_event_count(env) == before) {
            return completed;
        }
    }
    fprintf(stderr,
            "smb2 harness: the server never went quiet -- %d settle passes "
            "each produced new events%s%s\n", SMB2C_QUIESCE_MAX_PASSES,
            smb2c_context_str ? "\n  while replaying: " : "",
            smb2c_context_str ? smb2c_context_str : "");
    exit(4);
} /* smb2_quiesce */

static inline uint32_t
smb2c_xfer(
    struct smb2_conn *c,
    int               body_len)
{
    smb2c_send(c, body_len);
    return smb2c_wait(c);
} /* smb2c_xfer */

/* ---- NEGOTIATE ---------------------------------------------------------- */

static inline uint32_t
smb2_negotiate(struct smb2_conn *c)
{
    int      b    = smb2c_begin(c, SMB2_NEGOTIATE, 0);
    uint8_t *body = c->sbuf + b;
    uint32_t st;

    p16(body, 0, 36);                 /* StructureSize */
    p16(body, 2, 2);                  /* DialectCount */
    p16(body, 4, 1);                  /* SecurityMode = SIGNING_ENABLED */
    /* ClientGuid: DISTINCT per connection.  The server derives the lease owner
     * key from it (chimera_smb_lease_client_key, FNV-1a over the guid), so a
     * shared guid would make every connection ONE client -- silently coalescing
     * same-lease-key opens across what the model treats as separate clients,
     * and masking cross-client arbitration.  One guid per conn keeps the
     * harness's connections distinct clients, which is what the model means by
     * distinct sessions.  A probe that needs two connections of ONE client sets
     * c->guid_tag to the peer's before the handshake. */
    memset(body + 12, 0x11, 16);      /* ClientGuid */
    body[12] = (uint8_t) (0x40 + c->guid_tag);
    /* Offer 2.1 + 3.0: 3.0 unlocks lease v2 (epochs) needed for the cascade
     * test, and avoids the mandatory 3.1.1 negotiate contexts. */
    p16(body, 36, SMB2_DIALECT_0210);
    p16(body, 38, SMB2_DIALECT_0300);

    st = smb2c_xfer(c, 40);
    if (st == ST_SUCCESS) {
        c->dialect = g16(c->rbuf + 4 + SMB2_HDR_SIZE, 4);   /* DialectRevision */
    }
    return st;
} /* smb2_negotiate */

/* ---- SESSION_SETUP (anonymous NTLM -> NULL session) --------------------- */

static inline int
ntlm_type1(uint8_t *o)
{
    memcpy(o, "NTLMSSP\0", 8);
    p32(o, 8, 1);                     /* MessageType = NEGOTIATE */
    p32(o, 12, NTLMSSP_NEGOTIATE_UNICODE | NTLMSSP_NEGOTIATE_NTLM |
        NTLMSSP_REQUEST_TARGET);
    return 32;
} /* ntlm_type1 */

static inline int
ntlm_type3_anon(uint8_t *o)
{
    memset(o, 0, 88);
    memcpy(o, "NTLMSSP\0", 8);
    p32(o, 8, 3);                     /* MessageType = AUTHENTICATE */
    p32(o, 16, 88);                   /* LmChallengeResponse BufferOffset */
    p32(o, 24, 88);                   /* NtChallengeResponse BufferOffset */
    p32(o, 32, 88);                   /* DomainName BufferOffset */
    p32(o, 40, 88);                   /* UserName BufferOffset */
    p32(o, 48, 88);                   /* Workstation BufferOffset */
    p32(o, 56, 88);                   /* EncryptedRandomSessionKey Offset */
    p32(o, 60, NTLMSSP_NEGOTIATE_ANONYMOUS | NTLMSSP_NEGOTIATE_UNICODE);
    return 88;
} /* ntlm_type3_anon */

static inline uint32_t
smb2c_session_setup_leg(
    struct smb2_conn *c,
    const uint8_t    *blob,
    int               blob_len)
{
    int      b       = smb2c_begin(c, SMB2_SESSION_SETUP, 0);
    uint8_t *body    = c->sbuf + b;
    int      sec_off = SMB2_HDR_SIZE + 24;

    p16(body, 0, 25);                 /* StructureSize */
    body[3] = 1;                      /* SecurityMode = SIGNING_ENABLED */
    p16(body, 12, sec_off);           /* SecurityBufferOffset */
    p16(body, 14, blob_len);          /* SecurityBufferLength */
    memcpy(body + 24, blob, blob_len);

    return smb2c_xfer(c, 24 + blob_len);
} /* smb2c_session_setup_leg */

static inline uint32_t
smb2_session_setup(struct smb2_conn *c)
{
    uint8_t  blob[128];
    int      n;
    uint32_t st;

    n  = ntlm_type1(blob);
    st = smb2c_session_setup_leg(c, blob, n);
    if (st != ST_MORE_PROCESSING_REQUIRED) {
        return st;
    }
    c->session_id = g64(c->rbuf + 4, 40);

    n = ntlm_type3_anon(blob);
    return smb2c_session_setup_leg(c, blob, n);
} /* smb2_session_setup */

/* ---- TREE_CONNECT ------------------------------------------------------- */

static inline uint32_t
smb2_tree_connect(
    struct smb2_conn *c,
    const char       *unc)
{
    int      b        = smb2c_begin(c, SMB2_TREE_CONNECT, 0);
    uint8_t *body     = c->sbuf + b;
    int      path_off = SMB2_HDR_SIZE + 8;
    int      plen     = utf16le(unc, body + 8);
    uint32_t st;

    p16(body, 0, 9);                  /* StructureSize */
    p16(body, 4, path_off);           /* PathOffset */
    p16(body, 6, plen);               /* PathLength */

    st = smb2c_xfer(c, 8 + plen);
    if (st == ST_SUCCESS) {
        c->tree_id = g32(c->rbuf + 4, 36);
    }
    return st;
} /* smb2_tree_connect */

/* ---- CREATE ------------------------------------------------------------- */

/* Parse the CREATE reply currently in c->rbuf into `out`. */
static inline void
smb2c_parse_create(
    struct smb2_conn       *c,
    struct smb2_create_out *out)
{
    uint32_t       st = g32(c->rbuf + 4, 8);

    memset(out, 0, sizeof(*out));
    out->status = st;
    if (st != ST_SUCCESS) {
        return;
    }

    const uint8_t *rb = c->rbuf + 4 + SMB2_HDR_SIZE;   /* reply body */
    out->oplock      = rb[2];
    out->action      = g32(rb, 4);
    out->change_time = g64(rb, 32);
    out->end_of_file = g64(rb, 48);
    memcpy(out->file_id, rb + 64, 16);

    /* RqLs response context (MS-SMB2 2.2.14.2.10): CreateContextsOffset is
     * from the SMB2 header start (reply body offsets 80/84). */
    uint32_t       cc_off = g32(rb, 80);
    uint32_t       cc_len = g32(rb, 84);
    if (cc_len == 0 || cc_off == 0) {
        return;
    }
    const uint8_t *p     = c->rbuf + 4 + cc_off;
    const uint8_t *limit = c->rbuf + 4 + cc_off + cc_len;
    for (;;) {
        if (p + 16 > limit) {
            break;
        }
        uint32_t next     = g32(p, 0);
        uint16_t name_off = g16(p, 4);
        uint16_t name_len = g16(p, 6);
        uint16_t data_off = g16(p, 10);
        uint32_t data_len = g32(p, 12);

        /* Record it verbatim FIRST, whatever it is.  An unexpected response
         * context is as much a finding as a missing one, and neither is
         * observable to a caller if the parser only recognizes known tags. */
        if (out->nctx < SMB2C_MAX_RSP_CTX) {
            struct smb2_rsp_ctx *rc = &out->ctx[out->nctx];
            uint32_t             n  = data_len;

            memset(rc, 0, sizeof(*rc));
            rc->name_len = name_len > 16 ? 16 : name_len;
            if (p + name_off + rc->name_len <= limit) {
                memcpy(rc->name, p + name_off, (size_t) rc->name_len);
            }
            rc->wire_len = data_len;
            if (n > SMB2C_MAX_RSP_CTX_DATA) {
                n = SMB2C_MAX_RSP_CTX_DATA;
            }
            if (p + data_off + n <= limit) {
                memcpy(rc->data, p + data_off, (size_t) n);
                rc->data_len = n;
            }
            out->nctx++;
        } else {
            out->ctx_overflow = 1;
        }

        if (name_len == 4 && memcmp(p + name_off, "RqLs", 4) == 0 &&
            data_len >= 32) {
            const uint8_t *d = p + data_off;
            out->has_lease   = 1;
            out->lease_state = g32(d, 16);
            out->lease_flags = g32(d, 20);
            if (data_len >= 52) {
                out->lease_epoch = g16(d, 48);
            }
        }
        /* DH2Q response (MS-SMB2 2.2.14.2.12): Timeout(4) | Flags(4). */
        if (name_len == 4 && memcmp(p + name_off, "DH2Q", 4) == 0 &&
            data_len >= 8) {
            const uint8_t *d = p + data_off;
            out->has_dh2q     = 1;
            out->dh2q_timeout = g32(d, 0);
            out->dh2q_flags   = g32(d, 4);
        }
        /* DHnQ response (MS-SMB2 2.2.14.2.3) carries no fields at all: 8
         * reserved bytes.  Its PRESENCE is the entire grant signal. */
        if (name_len == 4 && memcmp(p + name_off, "DHnQ", 4) == 0) {
            out->has_dhnq = 1;
        }

        if (next == 0) {
            break;
        }
        p += next;
    }
} /* smb2c_parse_create */

/* Look up a recorded response context by its 4-byte tag; NULL if absent. */
static inline const struct smb2_rsp_ctx *
smb2c_create_ctx_find(
    const struct smb2_create_out *out,
    const char                   *tag)
{
    int tlen = (int) strlen(tag);

    for (int i = 0; i < out->nctx; i++) {
        if (out->ctx[i].name_len == tlen &&
            memcmp(out->ctx[i].name, tag, (size_t) tlen) == 0) {
            return &out->ctx[i];
        }
    }
    return NULL;
} /* smb2c_create_ctx_find */

/* Serialize a CREATE-context CHAIN into c->sbuf.
 *
 * `name_end` is the offset, FROM THE SMB2 HEADER START, one past the last byte
 * of the CREATE's Name field.  The chain starts at the next 8-aligned offset;
 * every context but the last carries Next = its own 8-PADDED size, and the last
 * carries Next = 0 and is NOT padded (MS-SMB2 2.2.13.2).
 *
 * Returns CreateContextsLength and stores the chain's header-relative offset in
 * *r_off (both 0 when n == 0).  With a single 4-byte-tagged context the output
 * is byte-identical to the hand-rolled single-RqLs builder this replaced.
 */
static inline uint32_t
smb2c_build_contexts(
    struct smb2_conn       *c,
    int                     name_end,
    const struct smb2_cctx *ctxs,
    int                     n,
    int                    *r_off)
{
    int base = (name_end + 7) & ~7;
    int pos  = 0;

    *r_off = 0;
    if (n <= 0) {
        return 0;
    }
    *r_off = base;

    /* Zero the alignment pad between the name and the chain. */
    memset(c->sbuf + 4 + name_end, 0, (size_t) (base - name_end));

    for (int i = 0; i < n; i++) {
        int      nl = ctxs[i].name_len;
        /* DataOffset is measured from the START OF THIS CONTEXT: the 16-byte
         * fixed part, then the name padded to 8.  A 4-byte tag gives 24; a
         * 16-byte GUID name gives 32. */
        int      doff   = 16 + ((nl + 7) & ~7);
        int      size   = doff + ctxs[i].data_len;
        int      padded = (size + 7) & ~7;
        int      last   = (i + 1 == n);
        uint8_t *p      = c->sbuf + 4 + base + pos;

        memset(p, 0, (size_t) padded);
        p32(p, 0, last ? 0u : (uint32_t) padded);   /* Next */
        p16(p, 4, 16);                              /* NameOffset */
        p16(p, 6, (uint16_t) nl);                   /* NameLength */
        p16(p, 10, (uint16_t) doff);                /* DataOffset */
        p32(p, 12, (uint32_t) ctxs[i].data_len);    /* DataLength */
        memcpy(p + 16, ctxs[i].name, (size_t) nl);
        if (ctxs[i].data_len > 0) {
            memcpy(p + doff, ctxs[i].data, (size_t) ctxs[i].data_len);
        }
        pos += last ? size : padded;
    }
    return (uint32_t) pos;
} /* smb2c_build_contexts */

/* Serialize a smb2_durable_req into up to four contexts appended to `out`
* (which must have room), using `buf` (>= 100 bytes) as their data scratch.
* Returns the number of contexts written.  Order is DHnQ, DH2Q, DHnC, DH2C.
*
* Every body layout below is cross-checked against the server's own parsers
* (smb_proc_create.c parse_ctx_dhnc / parse_ctx_dh2q / parse_ctx_dh2c). */
static inline int
smb2c_durable_contexts(
    const struct smb2_durable_req *d,
    uint8_t                       *buf,
    struct smb2_cctx              *out)
{
    int n = 0;

    if (!d) {
        return 0;
    }

    if (d->dhnq) {
        /* DHnQ request body: 16 reserved bytes (MS-SMB2 2.2.13.2.3). */
        memset(buf + 0, 0, 16);
        out[n].name     = (const uint8_t *) "DHnQ";
        out[n].name_len = 4;
        out[n].data     = buf + 0;
        out[n].data_len = 16;
        n++;
    }
    if (d->dh2q) {
        /* DH2Q: Timeout(4) | Flags(4) | Reserved(8) | CreateGuid(16). */
        memset(buf + 16, 0, 32);
        p32(buf + 16, 0, d->timeout_ms);
        p32(buf + 16, 4, d->flags);
        memcpy(buf + 16 + 16, d->create_guid, 16);
        out[n].name     = (const uint8_t *) "DH2Q";
        out[n].name_len = 4;
        out[n].data     = buf + 16;
        out[n].data_len = 32;
        n++;
    }
    if (d->dhnc) {
        /* DHnC: FileId(16). */
        memcpy(buf + 48, d->file_id, 16);
        out[n].name     = (const uint8_t *) "DHnC";
        out[n].name_len = 4;
        out[n].data     = buf + 48;
        out[n].data_len = 16;
        n++;
    }
    if (d->dh2c) {
        /* DH2C: FileId(16) | CreateGuid(16) | Flags(4). */
        memset(buf + 64, 0, 36);
        memcpy(buf + 64, d->file_id, 16);
        memcpy(buf + 64 + 16, d->create_guid, 16);
        p32(buf + 64, 32, d->reconnect_flags);
        out[n].name     = (const uint8_t *) "DH2C";
        out[n].name_len = 4;
        out[n].data     = buf + 64;
        out[n].data_len = 36;
        n++;
    }
    return n;
} /* smb2c_durable_contexts */

#define SMB2C_MAX_REQ_CTX 8

/* Build a CREATE into c->sbuf; returns the body length.  When req->is_lease an
 * RqLs context LEADS the chain (v2 with epoch on dialect >= 3.0, else v1);
 * `extra` follows in caller order. */
static inline int
smb2c_build_create_full(
    struct smb2_conn             *c,
    const char                   *name,
    uint32_t                      disp,
    uint32_t                      access,
    uint32_t                      share,
    uint32_t                      create_options,
    const struct smb2_oplock_req *req,
    const struct smb2_cctx       *extra,
    int                           nextra)
{
    int              b        = smb2c_begin(c, SMB2_CREATE, 0);
    uint8_t         *body     = c->sbuf + b;
    int              name_off = SMB2_HDR_SIZE + 56;
    int              nlen     = utf16le(name, body + 56);
    uint8_t          oplevel  =
        req ? (req->is_lease ? SMB2_OPLOCK_LEVEL_LEASE : req->level)
            : SMB2_OPLOCK_LEVEL_NONE;
    struct smb2_cctx ctxs[SMB2C_MAX_REQ_CTX];
    uint8_t          rqls[52];
    int              nctx = 0;
    int              cc_hoff;
    uint32_t         cc_len;

    p16(body, 0, 57);                 /* StructureSize */
    body[3] = oplevel;                /* RequestedOplockLevel */
    p32(body, 4, 2);                  /* ImpersonationLevel = Impersonation */
    p32(body, 24, access);
    p32(body, 28, FILE_ATTRIBUTE_NORMAL);
    p32(body, 32, share);
    p32(body, 36, disp);
    p32(body, 40, create_options);
    p16(body, 44, name_off);          /* NameOffset */
    p16(body, 46, nlen);              /* NameLength */

    if (req && req->is_lease) {
        int v2   = c->dialect >= SMB2_DIALECT_0300 && !req->force_v1;
        int dlen = v2 ? 52 : 32;

        memset(rqls, 0, sizeof(rqls));
        memcpy(rqls, req->lease_key, 16);
        p32(rqls, 16, req->lease_state);
        p32(rqls, 20, 0);             /* LeaseFlags */
        p64(rqls, 24, 0);             /* LeaseDuration */
        if (v2) {
            /* ParentLeaseKey (16, zero) + Epoch (2) + Reserved (2) */
            p16(rqls, 48, req->lease_epoch);
        }
        ctxs[nctx].name     = (const uint8_t *) "RqLs";
        ctxs[nctx].name_len = 4;
        ctxs[nctx].data     = rqls;
        ctxs[nctx].data_len = dlen;
        nctx++;
    }

    for (int i = 0; i < nextra && nctx < SMB2C_MAX_REQ_CTX; i++) {
        ctxs[nctx++] = extra[i];
    }

    if (nctx == 0) {
        p32(body, 48, 0);             /* CreateContextsOffset */
        p32(body, 52, 0);             /* CreateContextsLength */
        return 56 + nlen;
    }

    cc_len = smb2c_build_contexts(c, SMB2_HDR_SIZE + 56 + nlen, ctxs, nctx,
                                  &cc_hoff);
    p32(body, 48, (uint32_t) cc_hoff);  /* CreateContextsOffset */
    p32(body, 52, cc_len);            /* CreateContextsLength */

    return (cc_hoff - SMB2_HDR_SIZE) + (int) cc_len;
} /* smb2c_build_create_full */

static inline int
smb2c_build_create(
    struct smb2_conn             *c,
    const char                   *name,
    uint32_t                      disp,
    uint32_t                      access,
    uint32_t                      share,
    uint32_t                      create_options,
    const struct smb2_oplock_req *req)
{
    return smb2c_build_create_full(c, name, disp, access, share,
                                   create_options, req, NULL, 0);
} /* smb2c_build_create */

/* Non-blocking CREATE with explicit CreateOptions (post only). */
static inline void
smb2_create_post_opts(
    struct smb2_conn             *c,
    const char                   *name,
    uint32_t                      disp,
    uint32_t                      access,
    uint32_t                      share,
    uint32_t                      create_options,
    const struct smb2_oplock_req *req)
{
    smb2c_send(c, smb2c_build_create(c, name, disp, access, share,
                                     create_options, req));
} /* smb2_create_post_opts */

/* Non-blocking CREATE of a non-directory file (post only). */
static inline void
smb2_create_post(
    struct smb2_conn             *c,
    const char                   *name,
    uint32_t                      disp,
    uint32_t                      access,
    uint32_t                      share,
    const struct smb2_oplock_req *req)
{
    smb2_create_post_opts(c, name, disp, access, share,
                          FILE_NON_DIRECTORY_FILE, req);
} /* smb2_create_post */

/* Blocking CREATE with explicit CreateOptions. */
static inline uint32_t
smb2_create_opts(
    struct smb2_conn             *c,
    const char                   *name,
    uint32_t                      disp,
    uint32_t                      access,
    uint32_t                      share,
    uint32_t                      create_options,
    const struct smb2_oplock_req *req,
    struct smb2_create_out       *out)
{
    smb2_create_post_opts(c, name, disp, access, share, create_options, req);
    smb2c_wait(c);
    smb2c_parse_create(c, out);
    return out->status;
} /* smb2_create_opts */

/* Blocking CREATE of a non-directory file. */
static inline uint32_t
smb2_create(
    struct smb2_conn             *c,
    const char                   *name,
    uint32_t                      disp,
    uint32_t                      access,
    uint32_t                      share,
    const struct smb2_oplock_req *req,
    struct smb2_create_out       *out)
{
    return smb2_create_opts(c, name, disp, access, share,
                            FILE_NON_DIRECTORY_FILE, req, out);
} /* smb2_create */

/* ---- CREATE with durable-handle contexts -------------------------------- */

/* Non-blocking CREATE carrying an oplock/lease request and/or the durable
 * contexts described by `dur` (post only). */
static inline void
smb2_create_dur_post(
    struct smb2_conn              *c,
    const char                    *name,
    uint32_t                       disp,
    uint32_t                       access,
    uint32_t                       share,
    uint32_t                       create_options,
    const struct smb2_oplock_req  *req,
    const struct smb2_durable_req *dur)
{
    struct smb2_cctx ctxs[4];
    uint8_t          buf[100];
    int              n = smb2c_durable_contexts(dur, buf, ctxs);

    smb2c_send(c, smb2c_build_create_full(c, name, disp, access, share,
                                          create_options, req, ctxs, n));
} /* smb2_create_dur_post */

/* Blocking CREATE with durable contexts and explicit CreateOptions. */
static inline uint32_t
smb2_create_dur_opts(
    struct smb2_conn              *c,
    const char                    *name,
    uint32_t                       disp,
    uint32_t                       access,
    uint32_t                       share,
    uint32_t                       create_options,
    const struct smb2_oplock_req  *req,
    const struct smb2_durable_req *dur,
    struct smb2_create_out        *out)
{
    smb2_create_dur_post(c, name, disp, access, share, create_options, req,
                         dur);
    smb2c_wait(c);
    smb2c_parse_create(c, out);
    return out->status;
} /* smb2_create_dur_opts */

/* Blocking CREATE of a non-directory file with durable contexts.
 *
 * A DHnC/DH2C RECONNECT carries an EMPTY name: the open being reclaimed is
 * named by its FileId, not by a path (MS-SMB2 3.3.5.9.7 / 3.3.5.9.12). */
static inline uint32_t
smb2_create_dur(
    struct smb2_conn              *c,
    const char                    *name,
    uint32_t                       disp,
    uint32_t                       access,
    uint32_t                       share,
    const struct smb2_oplock_req  *req,
    const struct smb2_durable_req *dur,
    struct smb2_create_out        *out)
{
    return smb2_create_dur_opts(c, name, disp, access, share,
                                FILE_NON_DIRECTORY_FILE, req, dur, out);
} /* smb2_create_dur */

/* ---- WRITE / READ / CLOSE ----------------------------------------------- */

/* Build a WRITE into c->sbuf; returns the body length. */
static inline int
smb2c_build_write(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          offset,
    const void       *data,
    uint32_t          len)
{
    int      b        = smb2c_begin(c, SMB2_WRITE, 0);
    uint8_t *body     = c->sbuf + b;
    int      data_off = SMB2_HDR_SIZE + 48;

    p16(body, 0, 49);                 /* StructureSize */
    p16(body, 2, data_off);           /* DataOffset */
    p32(body, 4, len);                /* Length */
    p64(body, 8, offset);             /* Offset */
    memcpy(body + 16, file_id, 16);   /* FileId */
    memcpy(body + 48, data, len);
    return 48 + (int) len;
} /* smb2c_build_write */

/* Non-blocking WRITE (post only). */
static inline void
smb2_write_post(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          offset,
    const void       *data,
    uint32_t          len)
{
    smb2c_send(c, smb2c_build_write(c, file_id, offset, data, len));
} /* smb2_write_post */

/* Reply Count field of the WRITE currently in c->rbuf. */
static inline uint32_t
smb2c_write_count(struct smb2_conn *c)
{
    return g32(c->rbuf + 4 + SMB2_HDR_SIZE, 4);
} /* smb2c_write_count */

static inline uint32_t
smb2_write(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          offset,
    const void       *data,
    uint32_t          len,
    uint32_t         *count_out)
{
    uint32_t st = smb2c_xfer(c, smb2c_build_write(c, file_id, offset, data,
                                                  len));

    if (st == ST_SUCCESS && count_out) {
        *count_out = smb2c_write_count(c);
    }
    return st;
} /* smb2_write */

static inline uint32_t
smb2_read(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          offset,
    uint32_t          len,
    uint8_t          *out,
    uint32_t         *out_len)
{
    int      b    = smb2c_begin(c, SMB2_READ, 0);
    uint8_t *body = c->sbuf + b;
    uint32_t st;

    p16(body, 0, 49);                 /* StructureSize */
    body[2] = SMB2_HDR_SIZE + 16;     /* Padding: desired data offset (80) */
    p32(body, 4, len);                /* Length */
    p64(body, 8, offset);             /* Offset */
    memcpy(body + 16, file_id, 16);   /* FileId */

    st = smb2c_xfer(c, 49);
    if (out_len) {
        *out_len = 0;
    }
    if (st == ST_SUCCESS) {
        const uint8_t *rb   = c->rbuf + 4 + SMB2_HDR_SIZE;
        uint8_t        doff = rb[2];
        uint32_t       dlen = g32(rb, 4);
        if (dlen > len) {
            dlen = len;
        }
        if (out) {
            memcpy(out, c->rbuf + 4 + doff, dlen);
        }
        if (out_len) {
            *out_len = dlen;
        }
    }
    return st;
} /* smb2_read */

static inline uint32_t
smb2_close(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
    int      b    = smb2c_begin(c, SMB2_CLOSE, 0);
    uint8_t *body = c->sbuf + b;

    p16(body, 0, 24);                 /* StructureSize */
    memcpy(body + 8, file_id, 16);    /* FileId */

    return smb2c_xfer(c, 24);
} /* smb2_close */

/* ---- LOCK ---------------------------------------------------------------
 *
 * MS-SMB2 2.2.26: StructureSize 48, a 24-byte fixed body, then LockCount
 * SMB2_LOCK_ELEMENTs of 24 bytes each (Offset, Length, Flags, Reserved).  One
 * element is all this harness needs.  LOCK is NOT in the
 * ChannelSequence-checked set, which is exactly why it is useful for probing
 * WHERE the gate sits relative to lock enforcement on the ops that are. */
static inline uint32_t
smb2_lock(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          offset,
    uint64_t          length,
    uint32_t          flags)
{
    int      b    = smb2c_begin(c, SMB2_LOCK, 0);
    uint8_t *body = c->sbuf + b;

    p16(body, 0, 48);                 /* StructureSize */
    p16(body, 2, 1);                  /* LockCount */
    p32(body, 4, 0);                  /* LockSequenceNumber/Index */
    memcpy(body + 8, file_id, 16);    /* FileId */
    p64(body, 24, offset);            /* Element[0].Offset */
    p64(body, 32, length);            /* Element[0].Length */
    p32(body, 40, flags);             /* Element[0].Flags */
    p32(body, 44, 0);                 /* Element[0].Reserved */

    return smb2c_xfer(c, 48);
} /* smb2_lock */

/* ---- SET_INFO -----------------------------------------------------------
 *
 * MS-SMB2 2.2.39: StructureSize 33, a 32-byte fixed body, the input buffer
 * immediately after it (header(64) + 32 = 96, 8-aligned).  SET_INFO is one of
 * the ChannelSequence-checked mutating operations (MS-SMB2 3.3.5.2.10). */
#define SMB2_INFO_FILE_T              0x01
#define SMB2_INFO_FILESYSTEM_T        0x02
#define SMB2_FILE_BASIC_INFO_T        0x04
#define SMB2_FILE_STANDARD_INFO_T     0x05
#define SMB2_FILE_INTERNAL_INFO_T     0x06
#define SMB2_FILE_RENAME_INFO_T       0x0A
#define SMB2_FILE_DISPOSITION_T       0x0D
#define SMB2_FILE_ENDOFFILE_INFO_T    0x14
#define SMB2_FILE_ALL_INFO_T          0x12
#define SMB2_FILE_NETWORK_OPEN_T      0x22
#define SMB2_FS_SIZE_INFO_T           0x03

/* FILE_ATTRIBUTE_DIRECTORY (MS-FSCC 2.6): the one attribute bit the model
 * constrains, because it is the wire spelling of `ftype == FDir`. */
#define SMB2_FILE_ATTRIBUTE_DIRECTORY 0x00000010u

static inline uint32_t
smb2_set_info(
    struct smb2_conn *c,
    uint8_t           info_type,
    uint8_t           info_class,
    const uint8_t     file_id[16],
    const void       *buf,
    uint32_t          buf_len)
{
    int      b       = smb2c_begin(c, SMB2_SET_INFO, 0);
    uint8_t *body    = c->sbuf + b;
    int      buf_off = SMB2_HDR_SIZE + 32;

    p16(body, 0, 33);                  /* StructureSize */
    body[2] = info_type;
    body[3] = info_class;
    p32(body, 4, buf_len);             /* BufferLength */
    p16(body, 8, (uint16_t) buf_off);  /* BufferOffset */
    p16(body, 10, 0);                  /* Reserved */
    p32(body, 12, 0);                  /* AdditionalInformation */
    memcpy(body + 16, file_id, 16);    /* FileId */
    if (buf_len > 0) {
        memcpy(body + 32, buf, buf_len);
    }
    return smb2c_xfer(c, 32 + (int) buf_len);
} /* smb2_set_info */

/* FILE_END_OF_FILE_INFORMATION (MS-FSCC 2.4.13): a single EndOfFile LONGLONG.
 * Truncating a file is the cleanest NON-IDEMPOTENT-looking mutating op to test
 * the ChannelSequence rule against, because its EFFECT (the file size) is
 * directly observable afterwards. */
static inline uint32_t
smb2_set_eof(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          eof)
{
    uint8_t buf[8];

    p64(buf, 0, eof);
    return smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_ENDOFFILE_INFO_T,
                         file_id, buf, sizeof(buf));
} /* smb2_set_eof */

/* FILE_RENAME_INFORMATION (MS-FSCC 2.4.37): ReplaceIfExists(1) | Reserved(7) |
 * RootDirectory(8) | FileNameLength(4) | FileName.  A rename is genuinely
 * non-idempotent -- applying it twice cannot succeed twice -- which is what
 * makes it the sharpest probe of whether a replayed mutation is re-applied. */
static inline uint32_t
smb2_rename(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    const char       *new_name,
    int               replace_if_exists)
{
    uint8_t buf[20 + 512];
    int     nlen = utf16le(new_name, buf + 20);

    memset(buf, 0, 20);
    buf[0] = (uint8_t) (replace_if_exists ? 1 : 0);
    p64(buf, 8, 0);                    /* RootDirectory */
    p32(buf, 16, (uint32_t) nlen);     /* FileNameLength */
    return smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_RENAME_INFO_T,
                         file_id, buf, (uint32_t) (20 + nlen));
} /* smb2_rename */

/* FILE_DISPOSITION_INFORMATION (MS-FSCC 2.4.11): a single DeletePending byte.
 * This is the SMB spelling of "unlink on last close" -- there is no path-based
 * REMOVE -- so it is the mutation the delete-on-close lifecycle runs through
 * once a handle already exists. */
static inline uint32_t
smb2_set_disposition(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    int               delete_pending)
{
    uint8_t buf[1];

    buf[0] = (uint8_t) (delete_pending ? 1 : 0);
    return smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_DISPOSITION_T,
                         file_id, buf, sizeof(buf));
} /* smb2_set_disposition */

/* ---- QUERY_INFO ---------------------------------------------------------
 *
 * MS-SMB2 2.2.37: StructureSize 41, a 40-byte fixed body, the (unused here)
 * input buffer after it.  The reply (2.2.38) is StructureSize 9 with
 * OutputBufferOffset / OutputBufferLength naming the payload inside the PDU.
 *
 * OutputBufferLength on the REQUEST is deliberately generous: a value below
 * the info level's fixed size is answered STATUS_INFO_LENGTH_MISMATCH
 * (MS-SMB2 3.3.5.20.1), which is a different behavior from the one the model
 * predicts and must not be reached by accident. */
static inline uint32_t
smb2_query_info(
    struct smb2_conn *c,
    uint8_t           info_type,
    uint8_t           info_class,
    const uint8_t     file_id[16],
    uint32_t          addl_info,
    uint8_t          *out,
    uint32_t          out_cap,
    uint32_t         *out_len)
{
    int      b    = smb2c_begin(c, SMB2_QUERY_INFO, 0);
    uint8_t *body = c->sbuf + b;
    uint32_t st;

    p16(body, 0, 41);                  /* StructureSize */
    body[2] = info_type;               /* InfoType */
    body[3] = info_class;              /* FileInfoClass */
    p32(body, 4, 65536);               /* OutputBufferLength */
    p16(body, 8, 0);                   /* InputBufferOffset */
    p16(body, 10, 0);                  /* Reserved */
    p32(body, 12, 0);                  /* InputBufferLength */
    p32(body, 16, addl_info);          /* AdditionalInformation */
    p32(body, 20, 0);                  /* Flags */
    memcpy(body + 24, file_id, 16);    /* FileId */

    st = smb2c_xfer(c, 40);
    if (out_len) {
        *out_len = 0;
    }
    if (st == ST_SUCCESS) {
        const uint8_t *rb   = c->rbuf + 4 + SMB2_HDR_SIZE;
        uint16_t       doff = g16(rb, 2);
        uint32_t       dlen = g32(rb, 4);

        if (dlen > out_cap) {
            dlen = out_cap;
        }
        if (out) {
            memcpy(out, c->rbuf + 4 + doff, dlen);
        }
        if (out_len) {
            *out_len = dlen;
        }
    }
    return st;
} /* smb2_query_info */

/* ---- FLUSH --------------------------------------------------------------
 *
 * MS-SMB2 2.2.17: StructureSize 24, Reserved1(2) | Reserved2(4) | FileId(16). */
static inline uint32_t
smb2_flush(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
    int      b    = smb2c_begin(c, SMB2_FLUSH, 0);
    uint8_t *body = c->sbuf + b;

    p16(body, 0, 24);                 /* StructureSize */
    memcpy(body + 8, file_id, 16);    /* FileId */

    return smb2c_xfer(c, 24);
} /* smb2_flush */

/* ---- LOGOFF / TREE_DISCONNECT -------------------------------------------
 *
 * MS-SMB2 2.2.7 and 2.2.11: both are StructureSize 4 with a 2-byte Reserved,
 * and both are session/tree teardown the model owns -- a LOGOFF closes every
 * open on the session, a TREE_DISCONNECT every open on the tree. */
static inline uint32_t
smb2_logoff(struct smb2_conn *c)
{
    int      b    = smb2c_begin(c, SMB2_LOGOFF, 0);
    uint8_t *body = c->sbuf + b;

    p16(body, 0, 4);                  /* StructureSize */
    p16(body, 2, 0);                  /* Reserved */

    return smb2c_xfer(c, 4);
} /* smb2_logoff */

static inline uint32_t
smb2_tree_disconnect(struct smb2_conn *c)
{
    int      b    = smb2c_begin(c, SMB2_TREE_DISCONNECT, 0);
    uint8_t *body = c->sbuf + b;

    p16(body, 0, 4);                  /* StructureSize */
    p16(body, 2, 0);                  /* Reserved */

    return smb2c_xfer(c, 4);
} /* smb2_tree_disconnect */

/* ---- IOCTL --------------------------------------------------------------
 *
 * MS-SMB2 2.2.31: StructureSize 57, a 56-byte fixed body, the input buffer
 * immediately after it.  header(64) + 56 = 120, already 8-aligned, so no pad. */
static inline uint32_t
smb2_ioctl(
    struct smb2_conn *c,
    uint32_t          ctl_code,
    const uint8_t     file_id[16],
    const void       *in,
    uint32_t          in_len)
{
    int      b      = smb2c_begin(c, SMB2_IOCTL, 0);
    uint8_t *body   = c->sbuf + b;
    int      in_off = SMB2_HDR_SIZE + 56;

    p16(body, 0, 57);                 /* StructureSize */
    p32(body, 4, ctl_code);           /* CtlCode */
    memcpy(body + 8, file_id, 16);    /* FileId */
    p32(body, 24, (uint32_t) in_off); /* InputOffset */
    p32(body, 28, in_len);            /* InputCount */
    p32(body, 32, 0);                 /* MaxInputResponse */
    p32(body, 36, (uint32_t) in_off); /* OutputOffset */
    p32(body, 40, 0);                 /* OutputCount */
    p32(body, 44, 4096);              /* MaxOutputResponse */
    p32(body, 48, SMB2C_IOCTL_IS_FSCTL);
    if (in_len > 0) {
        memcpy(body + 56, in, in_len);
    }
    return smb2c_xfer(c, 56 + (int) in_len);
} /* smb2_ioctl */

/* FSCTL_LMR_REQUEST_RESILIENCY (MS-SMB2 2.2.31.3): a NETWORK_RESILIENCY_REQUEST
 * of Timeout(4) | Reserved(4).  A resilient handle survives a transport drop
 * WITHOUT any durable-handle context and without a caching precondition, which
 * is exactly what distinguishes it from a durable handle. */
static inline uint32_t
smb2_resiliency(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint32_t          timeout_ms)
{
    uint8_t req[8];

    memset(req, 0, sizeof(req));
    p32(req, 0, timeout_ms);
    return smb2_ioctl(c, SMB2_FSCTL_LMR_REQUEST_RESILIENCY, file_id, req,
                      sizeof(req));
} /* smb2_resiliency */

/* ---- CANCEL -------------------------------------------------------------
 *
 * MS-SMB2 2.2.30 / 3.2.4.24.  SMB2_CANCEL is the ONE command that legitimately
 * reuses a MessageId: it addresses its target by that id (or, once the target
 * has sent an async interim, by AsyncId with the ASYNC flag set).  It gets no
 * response of its own, so this is post-only. */
static inline void
smb2_cancel_post(
    struct smb2_conn *c,
    uint64_t          target_mid,
    uint64_t          async_id)
{
    int      b;
    uint8_t *body;

    if (async_id) {
        b    = smb2c_begin(c, SMB2_CANCEL, SMB2_FLAGS_ASYNC_COMMAND);
        body = c->sbuf + b;
        p64(c->sbuf + 4, 32, async_id);   /* AsyncId */
    } else {
        b    = smb2c_begin(c, SMB2_CANCEL, 0);
        body = c->sbuf + b;
    }
    p16(body, 0, 4);                      /* StructureSize */
    p16(body, 2, 0);                      /* Reserved */

    smb2c_pin_msg_id(c, target_mid);
    smb2c_send(c, 4);
} /* smb2_cancel_post */

/* ---- oplock / lease break acknowledgments ------------------------------- */

/* OPLOCK_BREAK acknowledgment (MS-SMB2 2.2.24.1): the holder downgrades to
 * `level` (II or NONE).  Command 18, body StructureSize 24. */
static inline uint32_t
smb2_oplock_break_ack(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint8_t           level)
{
    int      b    = smb2c_begin(c, SMB2_OPLOCK_BREAK, 0);
    uint8_t *body = c->sbuf + b;

    p16(body, 0, 24);                 /* StructureSize */
    body[2] = level;                  /* OplockLevel */
    memcpy(body + 8, file_id, 16);    /* FileId */

    return smb2c_xfer(c, 24);
} /* smb2_oplock_break_ack */

/* LEASE_BREAK acknowledgment (MS-SMB2 2.2.24.2): the holder settles the
 * lease to `state`.  Command 18, body StructureSize 36 (the server
 * discriminates lease vs oplock ack by this size). */
static inline uint32_t
smb2_lease_break_ack(
    struct smb2_conn *c,
    const uint8_t     lease_key[16],
    uint32_t          state)
{
    int      b    = smb2c_begin(c, SMB2_OPLOCK_BREAK, 0);
    uint8_t *body = c->sbuf + b;

    p16(body, 0, 36);                 /* StructureSize */
    memcpy(body + 8, lease_key, 16);  /* LeaseKey */
    p32(body, 24, state);             /* LeaseState */

    return smb2c_xfer(c, 36);
} /* smb2_lease_break_ack */

/* ---- convenience: full bring-up handshake on a connection --------------- */

static inline void
smb2_handshake(struct smb2_conn *c)
{
    uint32_t st;

    st = smb2_negotiate(c);
    if (st != ST_SUCCESS) {
        fprintf(stderr, "NEGOTIATE failed: 0x%08x\n", st);
        exit(1);
    }
    st = smb2_session_setup(c);
    if (st != ST_SUCCESS) {
        fprintf(stderr, "SESSION_SETUP failed: 0x%08x\n", st);
        exit(1);
    }
    st = smb2_tree_connect(c, "\\\\server\\share");
    if (st != ST_SUCCESS) {
        fprintf(stderr, "TREE_CONNECT failed: 0x%08x\n", st);
        exit(1);
    }
} /* smb2_handshake */

/* ---- transport drop and reconnect ---------------------------------------
 *
 * A durable/persistent handle only means anything across a TRANSPORT FAILURE,
 * and a replay only happens because one occurred, so dropping the connection
 * from the client side is a first-class primitive rather than a teardown step.
 *
 * The connection struct is deliberately NOT freed: env->conns owns every
 * connection until after evpl_destroy (see smb2_env_stop), because evpl
 * dispatches a final DISCONNECTED to each registered bind during teardown and
 * that notify dereferences the conn.  A retired connection therefore keeps its
 * slot -- which is why SMB2C_MAX_CONNS is sized for one slot per drop. */
static inline void
smb2_conn_disconnect(struct smb2_conn *c)
{
    uint64_t deadline;

    if (!c || c->closed_by_client) {
        return;
    }
    c->closed_by_client = 1;
    evpl_close(c->env->evpl, c->bind);

    /* Return only once the drop has actually been DELIVERED.  "The connection
     * is gone" is then an event, not an elapsed interval -- a caller that goes
     * on to reclaim a durable handle is not racing a close that has merely been
     * requested.  (It still is not proof the server has PARKED the handle;
     * that needs a separate barrier -- see the park barrier in the durable
     * probe.) */
    deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!c->disconnected) {
        smb2_pump(c->env);
        if (smb2c_now_ms() >= deadline) {
            fprintf(stderr,
                    "smb2 harness: WEDGED waiting for conn %d to report the "
                    "close THIS side requested, after %d ms%s%s\n",
                    c->conn_index, SMB2C_HANG_MS,
                    smb2c_context_str ? "\n  while replaying: " : "",
                    smb2c_context_str ? smb2c_context_str : "");
            exit(4);
        }
    }
} /* smb2_conn_disconnect */

/* Is `c` usable?  0 once either side has closed it. */
static inline int
smb2c_check_live(struct smb2_conn *c)
{
    return c && c->connected && !c->disconnected && !c->closed_by_client;
} /* smb2c_check_live */

/* Open a fresh connection, NOT yet handshaked.
 *
 * When `old` is non-NULL the new connection inherits its ClientGuid
 * (guid_tag), i.e. it is the SAME CLIENT reconnecting.  That is load-bearing,
 * not cosmetic: chimera refuses a leased or persistent reclaim whose ClientGuid
 * does not match the parked open's (chimera_smb_durable_claim), so a reconnect
 * that quietly took a fresh guid would turn every reclaim into a cross-client
 * one and report OBJECT_NAME_NOT_FOUND.
 *
 * Pass old = NULL for the deliberate cross-client case (a "stranger" trying to
 * steal someone else's parked handle); the connection then keeps its own
 * distinct guid_tag.  The caller drives NEGOTIATE itself, which is the point of
 * the _raw form: a probe that wants to vary the negotiated dialect, or to send
 * something before SESSION_SETUP, cannot use the packaged handshake. */
static inline struct smb2_conn *
smb2_conn_reopen_raw(
    struct smb2_env  *env,
    struct smb2_conn *old)
{
    struct smb2_conn *c = smb2_conn_open(env);

    if (old) {
        c->guid_tag = old->guid_tag;
    }
    return c;
} /* smb2_conn_reopen_raw */

/* The same client, reconnecting: fresh connection, inherited ClientGuid, fully
 * handshaked and ready to reclaim. */
static inline struct smb2_conn *
smb2_conn_reopen(
    struct smb2_env  *env,
    struct smb2_conn *old)
{
    struct smb2_conn *c = smb2_conn_reopen_raw(env, old);

    smb2_handshake(c);
    return c;
} /* smb2_conn_reopen */

#endif /* SMB2_MBT_COMMON_H */
