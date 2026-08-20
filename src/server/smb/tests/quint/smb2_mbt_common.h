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
#define SMB2_OPLOCK_BREAK                 0x0012

/* smb2 header flags (smb2.h:789) */
#define SMB2_FLAGS_SERVER_TO_REDIR        0x00000001
#define SMB2_FLAGS_ASYNC_COMMAND          0x00000002
#define SMB2_FLAGS_RELATED_OPERATIONS     0x00000004

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
};

#define SMB2C_MAX_CONNS 8

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
    struct evpl_bind *bind;
    int               connected;
    int               disconnected;

    uint64_t          session_id;
    uint32_t          tree_id;
    uint64_t          msg_id;
    uint16_t          dialect;

    uint8_t          *sbuf;       /* request scratch */
    uint8_t          *rbuf;       /* last reply (framed, NetBIOS included) */
    int               rlen;
    int               reply_ready;

    /* unsolicited oplock/lease break notifications, oldest first */
    struct smb2_break brk[SMB2C_MAX_BREAKS];
    int               nbrk;
};

/* Parsed CREATE reply. */
struct smb2_create_out {
    uint32_t status;
    uint8_t  oplock;         /* reply OplockLevel byte */
    uint32_t action;         /* CreateAction */
    uint64_t end_of_file;
    uint64_t change_time;
    uint8_t  file_id[16];
    int      has_lease;      /* an RqLs response context was present */
    uint32_t lease_state;    /* granted SMB lease bits */
    uint32_t lease_flags;
    uint16_t lease_epoch;
};

/* An oplock/lease request attached to a CREATE. */
struct smb2_oplock_req {
    uint8_t  level;          /* legacy: SMB2_OPLOCK_LEVEL_*; 0 = none */
    int      is_lease;       /* request an RqLs lease instead */
    uint8_t  lease_key[16];
    uint32_t lease_state;    /* requested SMB lease bits (R/H/W) */
    uint16_t lease_epoch;    /* v2 request epoch */
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
            for (unsigned int i = 0; i < notify->recv_msg.niov; i++) {
                void *d   = evpl_iovec_data(&notify->recv_msg.iovec[i]);
                int   len = evpl_iovec_length(&notify->recv_msg.iovec[i]);
                if (off + len <= SMB2C_BUFSZ) {
                    memcpy(c->rbuf + off, d, len);
                }
                off += len;
            }
            for (unsigned int i = 0; i < notify->recv_msg.niov; i++) {
                evpl_iovec_release(evpl, &notify->recv_msg.iovec[i]);
            }
            if (off >= 4 + SMB2_HDR_SIZE) {
                uint16_t cmd    = g16(c->rbuf + 4, 12);
                uint64_t mid    = g64(c->rbuf + 4, 24);
                uint32_t status = g32(c->rbuf + 4, 8);
                uint32_t hflags = g32(c->rbuf + 4, 16);
                /* Unsolicited break: command 18, MessageId all-ones
                 * (smb_proc_oplock_break.c:58). */
                if (cmd == SMB2_OPLOCK_BREAK &&
                    mid == 0xFFFFFFFFFFFFFFFFull) {
                    smb2c_record_break(c, c->rbuf + 4 + SMB2_HDR_SIZE);
                    break;
                }
                /* Async interim: a parked op (e.g. a CREATE deferred behind
                 * an oplock/lease break) gets a STATUS_PENDING response with
                 * the ASYNC flag first, then the real reply.  Skip the
                 * interim -- the final response follows (smb_async_interim.c). */
                if (status == ST_PENDING &&
                    (hflags & SMB2_FLAGS_ASYNC_COMMAND)) {
                    break;
                }
            }
            c->rlen        = off;
            c->reply_ready = 1;
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

    env->server = chimera_server_init(config, env->metrics);
    chimera_server_start(env->server);

    env->evpl = evpl_create(NULL);
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
    chimera_server_create_share(env->server, "share", "share", 0);
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
    struct smb2_conn     *c  = calloc(1, sizeof(*c));
    struct evpl_endpoint *ep =
        chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                           "127.0.0.1", 445);

    c->env  = env;
    c->sbuf = malloc(SMB2C_BUFSZ);
    c->rbuf = malloc(SMB2C_BUFSZ);

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
        evpl_close(env->evpl, c->bind);
        while (!c->disconnected) {
            smb2_pump(env);
        }
    }
    for (int i = 0; i < env->nconns; i++) {
        free(env->conns[i]->sbuf);
        free(env->conns[i]->rbuf);
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
    uint8_t *h = c->sbuf + 4;   /* SMB2 header start */

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

    c->sbuf[0] = 0;
    c->sbuf[1] = (uint8_t) (payload >> 16);
    c->sbuf[2] = (uint8_t) (payload >> 8);
    c->sbuf[3] = (uint8_t) payload;

    c->reply_ready = 0;
    evpl_send(c->env->evpl, c->bind, c->sbuf, 4 + payload);
    c->msg_id++;
} /* smb2c_send */

/* Pump until THIS connection's reply lands.  Only safe when the reply does
 * not depend on another connection acting first (setup + quiescent ops);
 * break scenarios drive the loop explicitly instead. */
static inline uint32_t
smb2c_wait(struct smb2_conn *c)
{
    while (!c->reply_ready) {
        smb2_pump(c->env);
        if (c->disconnected) {
            fprintf(stderr, "SMB server dropped the connection\n");
            exit(3);
        }
    }
    return g32(c->rbuf + 4, 8);   /* reply header Status */
} /* smb2c_wait */

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
    memset(body + 12, 0x11, 16);      /* ClientGuid */
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
        if (next == 0) {
            break;
        }
        p += next;
    }
} /* smb2c_parse_create */

/* Build a CREATE into c->sbuf; returns the body length.  When req->is_lease
* an RqLs context is appended (v2 with epoch on dialect >= 3.0, else v1). */
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
    int      b        = smb2c_begin(c, SMB2_CREATE, 0);
    uint8_t *body     = c->sbuf + b;
    int      name_off = SMB2_HDR_SIZE + 56;
    int      nlen     = utf16le(name, body + 56);
    uint8_t  oplevel  =
        req ? (req->is_lease ? SMB2_OPLOCK_LEVEL_LEASE : req->level)
            : SMB2_OPLOCK_LEVEL_NONE;

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

    if (!req || !req->is_lease) {
        p32(body, 48, 0);             /* CreateContextsOffset */
        p32(body, 52, 0);             /* CreateContextsLength */
        return 56 + nlen;
    }

    /* Append one RqLs create context, 8-aligned after the name.  Offsets are
     * measured from the SMB2 header start. */
    int      cc_hoff = (SMB2_HDR_SIZE + 56 + nlen + 7) & ~7; /* from header */
    int      v2      = c->dialect >= SMB2_DIALECT_0300;
    int      dlen    = v2 ? 52 : 32;
    uint8_t *cc      = c->sbuf + 4 + cc_hoff;

    /* zero the pad between name-end and the context, plus the context */
    memset(c->sbuf + 4 + SMB2_HDR_SIZE + 56 + nlen, 0,
           (cc_hoff - (SMB2_HDR_SIZE + 56 + nlen)) + 24 + dlen);

    p32(cc, 0, 0);                    /* Next = 0 (only context) */
    p16(cc, 4, 16);                   /* NameOffset */
    p16(cc, 6, 4);                    /* NameLength */
    p16(cc, 10, 24);                  /* DataOffset */
    p32(cc, 12, dlen);                /* DataLength */
    memcpy(cc + 16, "RqLs", 4);       /* context name */
    /* data at +24 */
    memcpy(cc + 24 + 0, req->lease_key, 16);
    p32(cc + 24, 16, req->lease_state);
    p32(cc + 24, 20, 0);              /* LeaseFlags */
    p64(cc + 24, 24, 0);              /* LeaseDuration */
    if (v2) {
        /* ParentLeaseKey (16, zero) + Epoch (2) + Reserved (2) */
        p16(cc + 24, 48, req->lease_epoch);
    }

    p32(body, 48, cc_hoff);           /* CreateContextsOffset */
    p32(body, 52, (uint32_t) (24 + dlen)); /* CreateContextsLength */

    return (cc_hoff - SMB2_HDR_SIZE) + 24 + dlen;
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

#endif /* SMB2_MBT_COMMON_H */
