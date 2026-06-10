// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <time.h>
#include <utlist.h>
#include <netinet/in.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
#include "evpl/evpl.h"
#include "common/logging.h"
#include "common/macros.h"
#include "common/misc.h"
#include "smb2.h"
#include "smb1.h"
#include "smb_attr.h"
#include "smb_session.h"
#include "smb_string.h"
#include "smb_ntlm.h"
#include "smb_gssapi.h"
#include "smb_sharemode.h"
#include "smb_notify.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_idmap.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_notify.h"

#define SMB2_MAX_DIALECTS           16
#define SMB2_MAX_NEGOTIATE_CONTEXTS 16

#define chimera_smb_debug(...) chimera_debug("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_smb_info(...)  chimera_info("smb", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_smb_error(...) chimera_error("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_smb_fatal(...) chimera_fatal("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_smb_abort(...) chimera_abort("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)

#define chimera_smb_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "smb", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

#define chimera_smb_abort_if(cond, ...) \
        chimera_abort_if(cond, "smb", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

/*
 * Canonical mapping between the SMB caching-grant encodings and vfs_state's RWH
 * lease mode -- one source of truth shared by the create grant path and the
 * oplock/lease break-notification builder, which previously re-derived these
 * inline (and could drift).  The SMB2 lease bit layout (R=0x01, H=0x02, W=0x04)
 * differs from vfs_state's CHIMERA_VFS_LEASE_MODE_{R,W,H}, so map field by field.
 */
static inline uint8_t
chimera_smb_vfs_to_lease_bits(uint8_t vfs_mode)
{
    uint8_t s = 0;

    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_R) {
        s |= SMB2_LEASE_READ_CACHING;
    }
    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_H) {
        s |= SMB2_LEASE_HANDLE_CACHING;
    }
    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_W) {
        s |= SMB2_LEASE_WRITE_CACHING;
    }
    return s;
} /* chimera_smb_vfs_to_lease_bits */

static inline uint8_t
chimera_smb_lease_bits_to_vfs(uint8_t smb_bits)
{
    uint8_t v = 0;

    if (smb_bits & SMB2_LEASE_READ_CACHING) {
        v |= CHIMERA_VFS_LEASE_MODE_R;
    }
    if (smb_bits & SMB2_LEASE_HANDLE_CACHING) {
        v |= CHIMERA_VFS_LEASE_MODE_H;
    }
    if (smb_bits & SMB2_LEASE_WRITE_CACHING) {
        v |= CHIMERA_VFS_LEASE_MODE_W;
    }
    return v;
} /* chimera_smb_lease_bits_to_vfs */

/* Collapse a granted RWH mask to the closest legacy oplock level (used when an
 * open did not request an RqLs lease).  Legacy oplocks have no separate W/H, so
 * RWH -> BATCH, RW (no H) -> EXCLUSIVE, R-only -> LEVEL_II, none -> NONE. */
static inline uint8_t
chimera_smb_vfs_to_oplock_level(uint8_t vfs_mode)
{
    uint8_t rwh = vfs_mode & (CHIMERA_VFS_LEASE_MODE_R |
                              CHIMERA_VFS_LEASE_MODE_W |
                              CHIMERA_VFS_LEASE_MODE_H);

    if (rwh == (CHIMERA_VFS_LEASE_MODE_R | CHIMERA_VFS_LEASE_MODE_W |
                CHIMERA_VFS_LEASE_MODE_H)) {
        return SMB2_OPLOCK_LEVEL_BATCH;
    }
    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_W) {
        return SMB2_OPLOCK_LEVEL_EXCLUSIVE;
    }
    if (vfs_mode & CHIMERA_VFS_LEASE_MODE_R) {
        return SMB2_OPLOCK_LEVEL_II;
    }
    return SMB2_OPLOCK_LEVEL_NONE;
} /* chimera_smb_vfs_to_oplock_level */

/* Little-endian decoders for SMB2 wire fields. Used by the negotiate-context
 * and CREATE-context parsers; defined here so all SMB protocol code shares one
 * implementation. The SMB2 wire format is little-endian throughout. */
static inline uint16_t
smb_wire_le16(const uint8_t *p)
{
    return (uint16_t) p[0] | ((uint16_t) p[1] << 8);
} /* smb_wire_le16 */

static inline uint32_t
smb_wire_le32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* smb_wire_le32 */

static inline uint64_t
smb_wire_le64(const uint8_t *p)
{
    return (uint64_t) smb_wire_le32(p) | ((uint64_t) smb_wire_le32(p + 4) << 32);
} /* smb_wire_le64 */

struct chimera_smb_nic_info {
    struct sockaddr_storage addr;
    uint64_t                speed;
    uint8_t                 rdma;
};

struct chimera_smb_rdma_element {
    uint32_t token;
    uint32_t length;
    uint64_t offset;
};

struct chimera_smb_auth_config {
    int  winbind_enabled;
    int  kerberos_enabled;
    char winbind_domain[256];
    char kerberos_keytab[256];
    char kerberos_realm[256];
};

struct chimera_smb_config {
    char                           identity[80];
    int                            port;
    int                            rdma_port;
    int                            num_dialects;
    int                            num_nic_info;
    int                            soft_fail_bad_req;
    int                            persistent_handles;
    int                            named_streams;
    /* When set, the server advertises signing as mandatory
     * (SMB2_SIGNING_REQUIRED) in NEGOTIATE and FSCTL_VALIDATE_NEGOTIATE_INFO,
     * so conforming clients sign every request. */
    int                            signing_required;
    /* SMB3 transport encryption policy: 0 = off (no encryption advertised),
     * 1 = enabled (offered, used when the client opts in), 2 = required. */
    int                            encryption;
    /* SMB3 transport compression: 0 = off (no compression advertised),
     * 1 = enabled (offered, used when the client opts in). */
    int                            compression;
    /* SMB2 leases (RqLs) and legacy SMB oplocks: 0 = the server grants none
     * (clients run uncached, so no caching break can stall a conflicting open),
     * 1 = grant on request.  Both default off; opt in via smb_leases/smb_oplocks. */
    int                            leases;
    int                            oplocks;
    /* When set, CHANGE_NOTIFY is disabled for the server's shares: the
     * handler returns STATUS_NOT_IMPLEMENTED immediately instead of arming
     * a watch (the "change notify = no" share behaviour Windows exposes;
     * smb2.change_notify_disabled expects this). */
    int                            notify_disabled;
    /* Windows-style canonicalisation of the DACL_AUTO_INHERITED bit on
     * SET_SECURITY; see chimera_server_config_set_smb_acl_inherited_canonicalize
     * for semantics.  Default 1 (canonical). */
    int                            acl_inherited_canonicalize;
    uint32_t                       capabilities;
    uint32_t                       dialects[16];
    struct chimera_smb_nic_info    nic_info[16];
    struct chimera_smb_auth_config auth;
};

struct netbios_header {
    uint32_t word;
} __attribute__((packed));

struct chimera_smb_share {
    char                               name[81];
    char                               path[CHIMERA_VFS_PATH_MAX];
    /* Access-based directory enumeration: hide directory entries the caller
     * cannot read.  Advertised to clients via SMB2_SHAREFLAG_ACCESS_BASED_
     * DIRECTORY_ENUM and enforced in QUERY_DIRECTORY. */
    int                                access_based_enum;
    struct chimera_smb_share          *prev;
    struct chimera_smb_share          *next;
    struct chimera_smb_sharemode_table sharemode;
    /* Continuous-availability share: advertised to clients (SMB2_SHARE_CAP_
     * CONTINUOUS_AVAILABILITY) and required before a persistent (as opposed to
     * merely durable) handle may be granted.  For this initial pass it is set
     * from the global smb_persistent_handles flag; a per-share config knob
     * will replace that later. */
    bool                               continuous_availability;
    /* Per-share SMB3 encryption (MS-SMB2 §2.2.10 SMB2_SHAREFLAG_ENCRYPT_DATA):
     * advertised at TREE_CONNECT and enforced so all traffic on this tree must
     * be encrypted (an unsigned, unencrypted access is rejected).  Independent
     * of the global smb_encryption knob. */
    bool                               encrypt_data;
    /* Set once the share's backend has been scanned for persisted handle
     * records at first use (best-effort, idempotent recovery). */
    bool                               durable_recovered;
};

struct chimera_smb_conn;

#define CHIMERA_SMB_REQUEST_FLAG_SIGN         0x01
/* Set during compound parsing when a request cannot be set up (e.g. its header
* names an invalid session id).  The request is still added to the compound so
* the dispatcher completes it -- in order -- with request->status, instead of
* the parser completing it out of order and corrupting the compound counter. */
#define CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED 0x02

/* MaxTransactSize advertised in NEGOTIATE.  QUERY_DIRECTORY / QUERY_INFO
 * output is bounded by this per MS-SMB2, so it doubles as the cap on the
 * single contiguous reply buffer the server allocates for a FIND.  It must
 * stay <= the libevpl per-buffer size (2 MiB) because that reply buffer is
 * allocated as a single iovec (max_iovecs == 1); a request larger than one
 * evpl buffer cannot be satisfied with one iovec. */
#define CHIMERA_SMB_MAX_TRANSACT_SIZE         (1 * 1024 * 1024)

struct chimera_smb_rename_info {
    uint8_t                         replace_if_exist;
    struct chimera_vfs_open_handle *new_parent_handle;
    char                            new_parent[SMB_FILENAME_MAX];
    int                             new_parent_len;
    char                           *new_name;
    int                             new_name_len;
};

int
chimera_smb_parse_rename_info(
    struct evpl_iovec_cursor   *cursor,
    struct chimera_smb_request *request);

void
chimera_smb_set_info_rename_process(
    struct chimera_smb_request *request);

/* SMB3 persistent-handle backend record.  Key is CHIMERA_SMB_DURABLE_KEY_PREFIX
 * + persistent_id; the value is the serialized chimera_smb_durable_record (see
 * below).  Defined here because the CREATE request carries scratch buffers
 * sized by these macros. */
#define CHIMERA_SMB_DURABLE_KEY_PREFIX     "smbdh"
#define CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN 5
#define CHIMERA_SMB_DURABLE_KEY_LEN        (CHIMERA_SMB_DURABLE_KEY_PREFIX_LEN + 8)
#define CHIMERA_SMB_DURABLE_RECORD_MAGIC   0x31484453  /* 'SDH1' LE */
/* Fixed header bytes before the variable-length name in a serialized record:
 * magic(4) pid(8) create_guid(16) client_guid(16) session(8) durable_flags(4)
 * timeout(8) desired(4) share(4) name_len(4). */
#define CHIMERA_SMB_DURABLE_REC_HDR_LEN    (4 + 8 + 16 + 16 + 8 + 4 + 8 + 4 + 4 + 4)
#define CHIMERA_SMB_DURABLE_VALUE_MAX      (CHIMERA_SMB_DURABLE_REC_HDR_LEN + SMB_FILENAME_MAX)

struct chimera_smb_request {
    uint32_t                           status;
    uint16_t                           request_struct_size;
    uint16_t                           flags;
    uint64_t                           async_id; /* non-zero if async/parked */
    union {
        struct smb1_header smb1_hdr;
        struct smb2_header smb2_hdr;
    };
    /* MS-SMB2 §3.2.4.1.5 / §3.3.5.2.10: on a request the 4 bytes the header
     * struct calls "status" are ChannelSequence(2)+Reserved(2); captured here
     * (the reply emits the separate request->status field, so reading them off
     * smb2_hdr.status never corrupts the response Status).  is_replay mirrors
     * the SMB2_FLAGS_REPLAY_OPERATION header flag. */
    uint16_t                           channel_sequence;
    uint8_t                            is_replay;
    struct chimera_smb_session_handle *session_handle;
    struct chimera_smb_tree           *tree;
    struct chimera_smb_compound       *compound;
    struct chimera_smb_request        *next;

    /* Generic async-interim (STATUS_PENDING) state, managed by
     * smb_async_interim.c.  Populated when a handler decides it must block on an
     * external event and calls chimera_smb_async_interim_begin; cleared by
     * chimera_smb_complete_request / _cancel.  armed == 0 outside a pending
     * window.  The timer field is reserved for a future block deadline driver
     * (begin does not arm it today; cancel/drain remove it as a safe no-op). */
    struct {
        struct evpl_timer           timer;
        struct chimera_smb_request *park_next;
        uint8_t                     signing_key[16];
        uint64_t                    session_id;
        uint16_t                    dialect;
        uint16_t                    credit_charge;
        uint16_t                    credit_request;
        uint8_t                     signed_session;
        uint8_t                     armed;
    } async;

    union {

        struct {
            uint16_t dialect_count;
            uint8_t  security_mode;
            uint32_t capabilities;
            uint8_t  client_guid[16];
            uint32_t negotiate_context_offset;
            uint16_t negotiate_context_count;
            uint16_t r_dialect;
            uint16_t r_security_mode;
            uint8_t  r_server_guid[16];
            uint32_t r_capabilities;
            uint32_t r_max_transact_size;
            uint32_t r_max_read_size;
            uint32_t r_max_write_size;
            uint64_t r_system_time;
            uint64_t r_server_start_time;
            uint16_t dialects[SMB2_MAX_DIALECTS];
            /* Typed parse outputs for 3.1.1 negotiate contexts. The ctx_present_mask
             * is the canonical "client sent this context" record; the typed structs
             * carry the parsed fields. Phase 0 stores everything; Phase 2/4/5
             * consumes the chosen algorithms via conn->negotiated.*. */
            uint32_t ctx_present_mask;
            struct {
                uint16_t hash_alg_count;
                uint16_t salt_length;
                uint16_t hash_algs[8];
                uint8_t  salt[32];
            } preauth_in;
            struct {
                uint16_t cipher_count;
                uint16_t ciphers[8];
            } encryption_in;
            struct {
                uint16_t alg_count;
                uint32_t flags;
                uint16_t algs[8];
            } compression_in;
            struct {
                uint16_t alg_count;
                uint16_t algs[4];
            } signing_in;
            struct {
                uint16_t transform_count;
                uint16_t transforms[4];
            } rdma_transform_in;
            struct {
                uint32_t flags;
            } transport_in;
            struct {
                /* Wire length in bytes (not in UTF-16 code units). May be less
                 * than the on-wire NetnameNegotiateContextId if it exceeded
                 * sizeof(utf16le) — the parser truncates silently. Informational
                 * only in Phase 0; consumed by Phase 4 for virtual hosting. */
                uint16_t length_bytes;
                uint8_t  utf16le[256];
            } netname_in;
        } negotiate;

        struct {
            uint8_t           flags;
            uint8_t           security_mode;
            uint16_t          input_niov;
            uint32_t          capabilities;
            uint32_t          channel;
            uint64_t          prev_session_id;
            uint16_t          blob_offset;
            uint16_t          blob_length;
            struct evpl_iovec input_iov[64];
        } session_setup;

        struct {
            uint16_t flags;
            uint16_t path_offset;
            uint16_t path_length;
            uint8_t  is_ipc;
            char     path[SMB_FILENAME_MAX];
        } tree_connect;

        struct {
            uint64_t                        flags;
            uint8_t                         requested_oplock_level;
            uint32_t                        impersonation_level;
            uint32_t                        desired_access;
            uint32_t                        file_attributes;
            uint32_t                        share_access;
            uint32_t                        create_disposition;
            uint32_t                        create_options;
            uint16_t                        parent_path_len;
            uint16_t                        name_len;
            struct chimera_vfs_open_handle *parent_handle;
            struct chimera_smb_open_file   *r_open_file;
            struct chimera_smb_attrs        r_attrs;
            struct chimera_vfs_attrs        set_attr;
            /* Set by the open/mkdir callbacks when this CREATE actually created
             * the file/dir (vs opened an existing one) — drives the OPENED vs
             * CREATED Create Action in the reply. */
            uint8_t                         r_created;
            /* Set by the durable-reconnect path: this CREATE is reclaiming a
            * surviving open, not opening a new one.  The access was granted at
            * the original open, and MS-SMB2 has the reconnect ignore the
            * DesiredAccess / CreateOptions / etc. fields entirely, so the
            * getattr-reply callback must not re-run the ACL access check. */
            uint8_t                         reconnect;
            /* Set by the DH2Q create_guid replay path (MS-SMB2 §3.3.5.9.7): this
             * CREATE re-presents an already-completed create, so the reply must
             * echo the ORIGINAL handle's create_action (stored on the open) and,
             * for an oplock (non-lease) handle, the REQUESTED oplock level rather
             * than the granted one. */
            uint8_t                         replay;
            /* CREATE contexts the client sent (CHIMERA_SMB_CREATE_CTX_* bits).
             * Phase-0 stubs set the bit and capture a minimum set of fields needed
             * by the response emit; Phase 1/3 will populate the rest. */
            uint32_t                        ctx_present_mask;
            struct {
                uint64_t persistent;
                uint64_t volatile_id;
            } dhnc;
            struct {
                uint32_t timeout_ms;
                uint32_t flags;
                uint8_t  create_guid[16];
            } dh2q;
            struct {
                uint64_t persistent;
                uint64_t volatile_id;
                uint8_t  create_guid[16];
                uint32_t flags;
            } dh2c;
            struct {
                uint8_t  key[16];
                uint32_t state;
                uint32_t flags;
                uint8_t  parent_key[16];
                uint16_t epoch;
                int      is_v2;
            } rqls;
            uint64_t                           alsi_alloc_size;
            uint64_t                           twrp_timestamp;
            /* SMB2_CREATE_APP_INSTANCE_ID / *_VERSION (MS-SMB2 2.2.13.2.13/14).
             * app_instance_id is the 16-byte GUID value; the AppInstanceVersion
             * (VersionHigh/Low) is present only when the version context was
             * also supplied (app_version_present). */
            uint8_t                            app_instance_id[16];
            uint64_t                           app_version_high;
            uint64_t                           app_version_low;
            uint8_t                            app_version_present;
            /* Status to return when gen_open_file refuses an open due to a
             * share conflict: defaults to SHARING_VIOLATION, but an
             * app-instance version reject overrides it with FILE_FORCED_CLOSED. */
            uint32_t                           force_close_status;
            /* Backing storage for a canonical ACL decoded from a SecD create
             * context (SMB2_CREATE_SD_BUFFER); set_attr.va_acl points here. */
            uint8_t                            acl_storage[sizeof(struct chimera_acl) +
                                                           64 * sizeof(struct chimera_ace)];
            /* SMB3 persistent-handle write-through.  persist_pid != 0 marks this
             * open as a persistent grant (fresh or cold reclaim): the record is
             * persisted atomically with the VFS open via persist_hs, and
             * gen_open_file adopts persist_pid as the file's persistent id. */
            uint64_t                           persist_pid;
            struct chimera_vfs_handle_state    persist_hs;
            uint8_t                            persist_key[CHIMERA_SMB_DURABLE_KEY_LEN];
            uint8_t                            persist_value[CHIMERA_SMB_DURABLE_VALUE_MAX];
            char                               parent_path[SMB_FILENAME_MAX];
            char                              *name;
            /* Named-stream (ADS) suffix parsed off the final path component.
             * has_stream is set when the create targets "file:stream[:$DATA]";
             * stream_name holds the bare stream name and `name`/name_len are
             * trimmed to the base file.  base_oh is the base file's open handle
             * held across the chained chimera_vfs_open_stream. */
            uint8_t                            has_stream;
            uint16_t                           stream_name_len;
            char                               stream_name[SMB_FILENAME_MAX];
            struct chimera_vfs_open_handle    *base_oh;
            /* Async share-acquire park (Phase 2 oplock).  A regular-file open
             * whose share reservation hard-conflicts with a batch-oplock holder
             * parks here until the holder closes (then granted) or merely acks
             * (then SHARING_VIOLATION).  gen_finish_cb resumes the open's tail on
             * a synchronous OR parked grant; the gen_* fields carry the parked
             * open's state across the wait. */
            void                               (*gen_finish_cb)(
                struct chimera_smb_request   *request,
                struct chimera_smb_open_file *open_file);
            struct chimera_smb_open_file      *gen_parked_open;
            struct chimera_vfs_file_state     *gen_parked_fs;
            struct chimera_vfs_pending_acquire gen_ticket;
            uint8_t                            gen_held_granted;
            uint8_t                            gen_parked;
            uint8_t                            r_is_directory;
            uint32_t                           r_granted_access;
            uint32_t                           r_maximal_access;
            /* Deferred-response park (MS-SMB2 3.3.5.9 pending-open): when this open
             * triggered an ack-required lease break, its SUCCESS response is held
             * (via the async-interim path; the request links on conn->parked_
             * requests) until the holder acknowledges.  park_fh identifies the file
             * whose break it awaits (resumed when that file's caching leases
             * settle -- chimera_smb_create_resume_parked). */
            uint8_t                            park_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                            park_fh_len;
            uint64_t                           park_fh_hash;
        } create;

        struct  {
            uint16_t                        flags;
            struct chimera_smb_file_id      file_id;
            struct chimera_smb_open_file   *open_file;
            struct chimera_vfs_open_handle *parent_handle;
            struct chimera_vfs_doc_info     doc_info;
            struct chimera_smb_attrs        r_attrs;
        } close;

        struct {
            uint64_t                        offset;
            uint32_t                        length;
            uint32_t                        channel;
            uint32_t                        remaining;
            uint32_t                        flags;
            uint32_t                        niov;
            uint32_t                        num_rdma_elements;
            uint32_t                        pending_rdma_reads;
            uint32_t                        r_rdma_status;
            struct chimera_smb_file_id      file_id;
            struct chimera_smb_open_file   *open_file;
            /* Holds the pre-write mtime restored after a write through a
             * write-time-sticky handle (chimera_vfs_setattr keeps this pointer
             * across the async call, so it must live in the request). */
            struct chimera_vfs_attrs        restore_attrs;
            struct chimera_smb_rdma_element rdma_elements[8];
            struct evpl_iovec               iov[256];
            struct evpl_iovec               chunk_iov[256];

        } write;

        struct {
            uint8_t                         flags;
            uint32_t                        length;
            uint32_t                        niov;
            uint64_t                        offset;
            uint32_t                        minimum;
            uint32_t                        channel;
            uint32_t                        remaining;
            uint32_t                        r_length;
            uint32_t                        num_rdma_elements;
            uint32_t                        pending_rdma_writes;
            uint32_t                        r_rdma_status;
            struct chimera_smb_file_id      file_id;
            struct chimera_smb_open_file   *open_file;
            struct chimera_smb_rdma_element rdma_elements[8];
            struct evpl_iovec               iov[256];
            struct evpl_iovec               chunk_iov[256];
        } read;

        struct {
            struct chimera_smb_file_id    file_id;
            struct chimera_smb_open_file *open_file;
        } flush;

        struct {
            struct chimera_smb_file_id     file_id;
            struct chimera_smb_open_file  *open_file;
            uint16_t                       lock_count;
            uint32_t                       lock_sequence;
            /* First lock element, kept for the common LockCount==1 path. */
            uint64_t                       l_offset;
            uint64_t                       l_length;
            uint32_t                       l_flags;
            /* All elements of a multi-element (LockCount>1) request.  A request
             * carrying more than CHIMERA_SMB_LOCK_MAX_ELEMENTS is rejected with
             * INVALID_PARAMETER (lock_too_many set by the parser). */
#define CHIMERA_SMB_LOCK_MAX_ELEMENTS 16
            struct {
                uint64_t offset;
                uint64_t length;
                uint32_t flags;
            }                              elements[CHIMERA_SMB_LOCK_MAX_ELEMENTS];
            bool                           lock_too_many;
            struct chimera_smb_lock_entry *entry;  /* live for the in-flight acquire */
        } lock;

        struct {
            /* Discriminated by request_struct_size: 24 = legacy oplock
             * ack (§2.2.24.1); 36 = lease ack (§2.2.24.2). */
            bool                       is_lease;
            /* Legacy oplock ack fields. */
            uint8_t                    oplock_level;
            struct chimera_smb_file_id file_id;
            /* Lease ack fields. */
            uint8_t                    lease_key[16];
            uint8_t                    lease_state;
            uint32_t                   lease_flags;
        } oplock_break;

        struct {
            uint32_t                        ctl_code;
            struct chimera_smb_file_id      file_id;
            uint32_t                        input_offset;
            uint32_t                        input_count;
            uint32_t                        max_input_response;
            uint32_t                        output_offset;
            uint32_t                        output_count;
            uint32_t                        max_output_response;
            uint32_t                        input_niov;
            uint32_t                        flags;
            /* Validate Negotiate Info fields */
            uint32_t                        vni_capabilities;
            uint8_t                         vni_guid[16];
            uint8_t                         vni_security_mode;
            uint16_t                        vni_dialect_count;
            uint16_t                        vni_dialects[SMB2_MAX_DIALECTS];
            /* Response fields */
            uint32_t                        r_capabilities;
            uint8_t                         r_guid[16];
            uint8_t                         r_security_mode;

            uint16_t                        r_dialect;
            struct evpl_iovec               input_iov[64];
            struct evpl_iovec               output_iov;
            /* Reparse point fields */
            struct chimera_smb_open_file   *rp_open_file;
            struct chimera_vfs_open_handle *rp_parent_handle;
            uint32_t                        rp_reparse_tag;
            uint64_t                        rp_nfs_type;
            uint32_t                        rp_device_major;
            uint32_t                        rp_device_minor;
            int                             rp_target_len;
            char                            rp_target[CHIMERA_VFS_PATH_MAX];
            struct chimera_vfs_attrs        rp_set_attr;
            /* GET response buffer */
            uint8_t                         rp_response[16 + (CHIMERA_VFS_PATH_MAX - 1) * 2];
            int                             rp_response_len;
            /* SET_SPARSE / SET_ZERO_DATA / QUERY_ALLOCATED_RANGES fields */
            struct chimera_smb_open_file   *sp_open_file;
            uint8_t                         sp_set_sparse;
            uint64_t                        sp_zero_offset;
            uint64_t                        sp_zero_beyond;
            uint64_t                        sp_qar_offset;
            uint64_t                        sp_qar_length;
            uint64_t                        sp_qar_end;
            uint64_t                        sp_qar_cursor;
            uint64_t                        sp_qar_data_start;
            uint32_t                        sp_qar_count;
            struct chimera_vfs_attrs        sp_set_attr;
#define CHIMERA_SMB_QAR_MAX 256
            struct {
                uint64_t offset;
                uint64_t length;
            }                               sp_qar_ranges[CHIMERA_SMB_QAR_MAX];
            /* SRV_REQUEST_RESUME_KEY / SRV_COPYCHUNK fields */
            struct chimera_smb_open_file   *cc_src_open_file;
            struct chimera_smb_open_file   *cc_dst_open_file;
            struct chimera_smb_file_id      cc_src_file_id;
            uint32_t                        cc_chunk_count;
            uint32_t                        cc_chunk_idx;
            uint32_t                        cc_chunks_written;
            uint64_t                        cc_total_written;
#define CHIMERA_SMB_COPYCHUNK_MAX 16
            struct {
                uint64_t src_offset;
                uint64_t dst_offset;
                uint32_t length;
            }                               cc_chunks[CHIMERA_SMB_COPYCHUNK_MAX];
            /* FSCTL_CREATE_OR_GET_OBJECT_ID response buffer (MS-FSCC 2.4.28
             * FILE_OBJECTID_BUFFER, type 1): ObjectId(16) + BirthVolumeId(16)
             * + BirthObjectId(16) + DomainId(16) = 64 bytes. */
            uint8_t                         oid_buffer[64];
            /* FSCTL_LMR_REQUEST_RESILIENCY (NETWORK_RESILIENCY_REQUEST,
             * MS-SMB2 2.2.31.3): requested resiliency Timeout in milliseconds. */
            uint32_t                        rr_timeout_ms;
        } ioctl;
        struct {
            uint8_t                         info_type;
            uint8_t                         info_class;
            uint32_t                        addl_info;
            uint32_t                        flags;
            uint32_t                        output_length;
            struct chimera_smb_file_id      file_id;
            struct chimera_smb_attrs        r_attrs;
            struct chimera_smb_fs_attrs     r_fs_attrs;
            struct chimera_smb_open_file   *open_file;
            /* Security descriptor built in the getattr callback and emitted by
             * the reply builder (SMB2_INFO_SECURITY). */
            uint8_t                         sec_buf[2048];
            uint32_t                        sec_buf_len;
            /* When the SD references identities not yet in the cache, the
             * getattr'd owner/group/mode + ACL are copied here so the SD can be
             * built after an async identity resolve completes (the live attrs
             * are only valid during the getattr callback). */
            uint32_t                        sd_uid;
            uint32_t                        sd_gid;
            uint32_t                        sd_mode;
            int                             sd_has_acl;
            int                             sd_pending;
            uint8_t                         sd_acl_storage[sizeof(struct chimera_acl) +
                                                           64 * sizeof(struct chimera_ace)];
            /* FileStreamInformation: the packed VFS list_streams records are
             * held here from the list_streams callback until the reply builder
             * emits them as MS-FSCC FILE_STREAM_INFORMATION entries.
             * stream_base_handle is a temporary handle on the base file. */
            struct chimera_vfs_open_handle *stream_base_handle;
            uint32_t                        stream_record_len;
            uint32_t                        stream_record_count;
            uint8_t                         stream_records[4096];
        } query_info;

        struct {
            uint8_t                         info_type;
            uint8_t                         info_class;
            uint32_t                        buffer_length;
            uint16_t                        buffer_offset;
            uint32_t                        addl_info;
            uint32_t                        flags;
            struct chimera_smb_open_file   *open_file;
            struct chimera_vfs_open_handle *parent_handle;
            struct chimera_smb_file_id      file_id;
            struct chimera_smb_attrs        attrs;
            struct chimera_vfs_attrs        vfs_attrs;
            /* VFS notify event(s) the set_info callback fires on the parent on
             * success.  0 => default to ATTRS_CHANGED; EndOfFile/Allocation set
             * SIZE_CHANGED|FILE_MODIFIED so a FILE_NOTIFY_CHANGE_SIZE watch
             * fires (smb2.change_notify ChangeSize). */
            uint32_t                        notify_mask;
            /* Rename information */
            struct chimera_smb_rename_info  rename_info;
            /* Security descriptor buffer for SMB2_INFO_SECURITY */
            uint8_t                         sec_buf[2048];
            uint32_t                        sec_buf_len;
            /* Outstanding async identity resolves before the SD is decoded for
             * the final time (fan-out join guard). */
            int                             sd_pending;
            /* Backing storage for the canonical ACL decoded from the incoming
             * security descriptor; vfs_attrs.va_acl points here. */
            uint8_t                         acl_storage[sizeof(struct chimera_acl) +
                                                        64 * sizeof(struct chimera_ace)];
        } set_info;

        struct {
            uint8_t                       info_class;
            uint8_t                       flags;
            uint32_t                      file_index;
            uint8_t                       eof;
            uint16_t                      pattern_len;
            struct chimera_smb_file_id    file_id;
            uint16_t                      pattern_length;
            uint32_t                      output_length;
            uint32_t                      max_output_length;
            struct evpl_iovec             iov;
            struct chimera_smb_open_file *open_file;
            uint32_t                     *last_file_offset;
            char                          pattern[SMB_FILENAME_MAX];
        } query_directory;

        struct {
            uint32_t                        completion_filter;
            uint16_t                        flags;
            uint32_t                        output_buffer_length;
            int                             watch_tree;
            struct chimera_smb_file_id      file_id;
            struct chimera_smb_open_file   *open_file;
            int                             nevents;
            int                             overflowed;
            struct chimera_vfs_notify_event events[16];
        } change_notify;
    };
};

#define CHIMERA_SMB_COMPOUND_MAX_REQUESTS 64
struct chimera_smb_compound {
    int                                num_requests;
    int                                complete_requests;
    /* Set when this compound arrived wrapped in a TRANSFORM header; its reply
     * MUST then be encrypted (MS-SMB2 §3.3.4.1.4). */
    int                                received_encrypted;
    uint64_t                           saved_session_id;
    uint64_t                           saved_tree_id;
    struct chimera_smb_file_id         saved_file_id;
    struct chimera_smb_session_handle *saved_session_handle;
    struct chimera_smb_tree           *saved_tree;
    struct chimera_server_smb_thread  *thread;
    struct chimera_smb_conn           *conn;
    struct chimera_smb_compound       *next;
    struct chimera_smb_request        *requests[CHIMERA_SMB_COMPOUND_MAX_REQUESTS];
};

struct chimera_smb_session_handle {
    uint64_t                           session_id;
    struct chimera_smb_session        *session;
    uint8_t                            signing_key[16];
    uint8_t                            enc_key[32];
    uint8_t                            dec_key[32];
    size_t                             enc_key_len;
    uint16_t                           cipher_id;
    /* Set when this handle represents an additional channel bound to an
     * existing session (a successful SMB2_SESSION_FLAG_BINDING session setup),
     * so its teardown decrements session->num_channels. */
    uint8_t                            bound_channel;
    struct UT_hash_handle              hh;
    struct chimera_smb_session_handle *next;
    gss_ctx_id_t                       ctx;
};

#define CHIMERA_SMB_CONN_FLAG_SIGNING_REQUIRED      0x01
#define CHIMERA_SMB_CONN_FLAG_SMB_DIRECT_NEGOTIATED 0x02

/* Bits identifying which negotiate contexts the client sent (and that we
 * accepted on the wire). Phase 0 records this so unit tests can assert
 * dispatch behavior; Phase 2/4/5 will consume conn->negotiated.* values. */
#define CHIMERA_SMB_NEGOTIATE_CTX_PREAUTH           (1u << 0)
#define CHIMERA_SMB_NEGOTIATE_CTX_ENCRYPTION        (1u << 1)
#define CHIMERA_SMB_NEGOTIATE_CTX_COMPRESSION       (1u << 2)
#define CHIMERA_SMB_NEGOTIATE_CTX_NETNAME           (1u << 3)
#define CHIMERA_SMB_NEGOTIATE_CTX_TRANSPORT         (1u << 4)
#define CHIMERA_SMB_NEGOTIATE_CTX_RDMA_TRANSFORM    (1u << 5)
#define CHIMERA_SMB_NEGOTIATE_CTX_SIGNING           (1u << 6)

struct chimera_smb_notify_request;

struct chimera_smb_conn {
    OM_uint32                          gss_major;
    OM_uint32                          gss_minor;
    OM_uint32                          gss_flags;
    gss_ctx_id_t                       nascent_ctx;
    gss_buffer_desc                    gss_output;
    struct smb_ntlm_ctx                ntlm_ctx;
    struct smb_gssapi_ctx              gssapi_ctx;
    uint8_t                           *ntlm_output;
    size_t                             ntlm_output_len;
    unsigned int                       flags;
    /* Set under the owning thread's lease_break_lock while conn_free drains
     * this connection's queued lease-break notifications, so a break_cb racing
     * on another thread does not enqueue a send for a connection whose bind is
     * being torn down.  Reset on reuse in chimera_smb_conn_alloc. */
    uint8_t                            lease_break_tearing_down;
    enum evpl_protocol_id              protocol;
    uint16_t                           dialect;
    uint16_t                           smbvers;
    uint32_t                           capabilities;
    /* Client values captured at NEGOTIATE, validated later by
     * FSCTL_VALIDATE_NEGOTIATE_INFO (MS-SMB2 3.3.5.15.12). */
    uint8_t                            client_guid[16];
    uint8_t                            client_security_mode;
    uint32_t                           client_capabilities;
    /* Connection.SupportsNotifications (MS-SMB2 3.3.5.4): set when the
     * negotiated dialect is 3.1.1 and the client advertised
     * SMB2_GLOBAL_CAP_NOTIFICATIONS in its NEGOTIATE capabilities.  A binding
     * SESSION_SETUP whose connection's value differs from the bound session's
     * is rejected with STATUS_INVALID_PARAMETER (MS-SMB2 3.3.5.5). */
    uint8_t                            supports_notifications;
    /* SMB 3.1.1 preauth-integrity running hash (SHA-512).  preauth_hash is the
     * per-session-setup running value: it is reset to negotiate_preauth_hash at
     * the start of every new authentication (a SESSION_SETUP whose header
     * SessionId is 0) and then extended over that session's SESSION_SETUP
     * request/response chain, so the signing key derives over the per-session
     * hash MS-SMB2 3.3.5.5.3 mandates -- not a value that keeps accumulating
     * across multiple sessions / logoff-reauth on the same connection.
     * negotiate_preauth_hash is the post-NEGOTIATE baseline (MS-SMB2
     * Connection.PreauthIntegrityHashValue).  Both reset per connection at
     * accept; only consumed when the negotiated dialect is 3.1.1. */
    uint8_t                            preauth_hash[SMB2_PREAUTH_HASH_SIZE];
    uint8_t                            negotiate_preauth_hash[SMB2_PREAUTH_HASH_SIZE];
    /* Snapshot of preauth_hash taken just before a SESSION_SETUP request is
     * folded in.  A SESSION_SETUP that completes with a hard error (anything
     * other than SUCCESS / MORE_PROCESSING_REQUIRED) must leave no trace in the
     * preauth-integrity hash -- the client only folds requests whose response
     * is SUCCESS or MORE_PROCESSING (MS-SMB2 3.3.5.5.3), so a failed
     * authentication leg (e.g. the deliberately-invalid first bind in
     * MultipleChannel_SecondChannelSessionSetupFailAtFirstTime) is rolled back
     * to this snapshot so the next, successful leg derives its signing key over
     * the same hash the client used. */
    uint8_t                            preauth_hash_presession[SMB2_PREAUTH_HASH_SIZE];
    uint32_t                           requests_completed;
    int                                rdma_max_send;
    int                                rdma_niov;
    int                                rdma_length;
    /* Algorithms selected at NEGOTIATE time from the client's offer; consumed
     * by Phase 2 (preauth/encryption/signing), Phase 4 (RDMA transform), and
     * Phase 5 (RDMA path). Zero means "not selected / not supported". */
    struct {
        uint32_t ctx_present_mask;          /* CHIMERA_SMB_NEGOTIATE_CTX_* */
        uint16_t preauth_hash_alg;          /* 0 or SMB2_PREAUTH_HASH_SHA_512 */
        uint16_t cipher_id;                 /* 0 or SMB2_ENCRYPTION_* */
        uint16_t signing_alg;               /* SMB2_SIGNING_* */
        uint16_t compression_flags;         /* SMB2_COMPRESSION_FLAG_* */
        uint8_t  preauth_salt[32];          /* server-generated; Phase 2 will hash */
        uint8_t  compression_alg_count;
        uint8_t  rdma_transform_count;
        uint16_t compression_algs[8];
        uint16_t rdma_transforms[4];
    } negotiated;
    struct chimera_smb_session_handle *last_session_handle;
    struct chimera_smb_tree           *last_tree;
    struct chimera_smb_session_handle *session_handles;
    struct chimera_smb_notify_request *parked_notifies;  /* parked CHANGE_NOTIFY requests */
    /* Requests pending an async-interim (STATUS_PENDING already sent): a create
     * blocked on a lease break, a blocking lock, etc. (smb_async_interim.c).
     * SMB2_CANCEL walks this list by AsyncId; conn_free drains it before tearing
     * down the bind. */
    struct chimera_smb_request        *parked_requests;
    struct chimera_server_smb_thread  *thread;
    struct evpl_bind                  *bind;
    struct chimera_smb_conn           *prev;
    struct chimera_smb_conn           *next;
    /* Active-connection list links (thread->active_conns).  Distinct from
     * prev/next (the pooled free-list reuse) so an active conn can be walked by
     * its owning thread without disturbing free-list bookkeeping. */
    struct chimera_smb_conn           *active_prev;
    struct chimera_smb_conn           *active_next;
    struct evpl_iovec                  rdma_iov[256];
    char                               local_addr[128];
    char                               remote_addr[128];
};

/* Default and ceiling for a durable handle's reconnect grace window.  A v2
 * client may request a timeout; we honor it up to the ceiling, falling back to
 * the default when the client requests 0.  v1 handles have no timeout field on
 * the wire and always use the default. */
#define CHIMERA_SMB_DURABLE_TIMEOUT_DEFAULT_MS 60000
#define CHIMERA_SMB_DURABLE_TIMEOUT_MAX_MS     300000

/* One parked-or-live durable open, indexed by its (now globally unique)
 * persistent id.  For an initial in-memory pass this object IS the durable
 * state; the future persistence layer will serialize these fields. */
struct chimera_smb_durable_entry {
    uint64_t                          persistent_id; /* hash key == open_file->file_id.pid */
    uint8_t                           create_guid[16];
    /* Reconnect is matched on the client GUID (the connection's negotiated
     * identity), not the session id: a durable reconnect lands on a brand-new
     * session after the original connection dropped. */
    uint8_t                           client_guid[16];
    uint64_t                          session_id; /* original owning session (informational) */
    struct timespec                   deadline;  /* reconnect grace; valid only while parked */
    bool                              parked;    /* true => disconnected, awaiting reconnect */
    /* persistent: the record is also persisted in the share's backend (survives
     * server restart).  cold: recovered from the backend at startup — the open
     * is not in memory (open_file == NULL) and must be re-opened on reclaim. */
    bool                              persistent;
    bool                              cold;
    struct chimera_smb_open_file     *open_file;
    uint32_t                          name_len;
    char                              name[SMB_FILENAME_MAX];
    /* Transient worklist link used by the sweeper after the entry has been
     * removed from the hash; not valid while the entry is in the table. */
    struct chimera_smb_durable_entry *reap_next;
    struct UT_hash_handle             hh;
};

struct chimera_smb_durable_table {
    pthread_mutex_t                   lock;
    struct chimera_smb_durable_entry *by_pid;
};

struct chimera_smb_durable_record {
    uint64_t persistent_id;
    uint8_t  create_guid[16];
    uint8_t  client_guid[16];
    uint64_t session_id;
    uint32_t durable_flags;
    uint64_t durable_timeout_ms;
    uint32_t desired_access;
    uint32_t share_access;
    uint32_t name_len;
    char     name[SMB_FILENAME_MAX];
};

struct chimera_server_smb_shared {
    struct chimera_smb_config         config;
    int                               rdma;
    enum evpl_protocol_id             tcp_protocol;
    uint8_t                           guid[SMB2_GUID_SIZE];
    gss_name_t                        svc;
    gss_cred_id_t                     srv_cred;
    struct chimera_vfs               *vfs;
    struct prometheus_metrics        *metrics;
    struct evpl_endpoint             *endpoint;
    struct evpl_endpoint             *endpoint_rdma;
    struct evpl_listener             *listener;
    struct chimera_smb_session       *sessions;
    struct chimera_smb_session       *free_sessions;
    pthread_mutex_t                   sessions_lock;
    struct chimera_smb_share         *shares;
    pthread_mutex_t                   shares_lock;
    /* Set when any share has encrypt_data enabled.  Used by SESSION_SETUP to
     * decide whether to derive per-session encryption keys even when the global
     * smb_encryption knob is off (a client may still tree-connect to a
     * per-share-encrypted share). */
    int                               any_share_encrypt;
    struct chimera_smb_tree          *free_trees;
    pthread_mutex_t                   trees_lock;
    /* Monotonic, process-global allocator for file persistent ids.  Replaces
     * the old per-tree counter so persistent ids stay unique across tree
     * teardowns — a precondition for durable-handle reconnect lookup. */
    _Atomic uint64_t                  next_persistent_id;
    /* In-memory durable/persistent handle registry (see struct above). */
    struct chimera_smb_durable_table  durable;
    /* Registry of all live SMB threads (chained on thread->next_thread).  Used
     * to broadcast a lease-break *resume* doorbell to peer threads when an
     * OPLOCK_BREAK ack settles a lease that a CREATE parked on another thread is
     * waiting for. */
    struct chimera_server_smb_thread *threads;
    pthread_mutex_t                   threads_lock;
};

/* Forward decl so the inline open_file release paths can call into
 * smb_proc_lock.c without including smb_procs.h here. */
void
chimera_smb_open_file_drain_locks(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file);

/* Durable/persistent handle registry (smb_durable.c). */
void
chimera_smb_durable_table_init(
    struct chimera_smb_durable_table *table);
void
chimera_smb_durable_table_destroy(
    struct chimera_smb_durable_table *table);
void
chimera_smb_durable_register(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_open_file     *open_file,
    uint64_t                          session_id,
    const uint8_t                    *client_guid,
    const char                       *name,
    uint32_t                          name_len,
    bool                              persistent);
void
chimera_smb_durable_forget(
    struct chimera_server_smb_shared *shared,
    uint64_t                          persistent_id);
void
chimera_smb_durable_park(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_open_file     *open_file);
struct chimera_smb_open_file *
chimera_smb_durable_claim(
    struct chimera_server_smb_shared *shared,
    uint64_t                          persistent_id,
    const uint8_t                    *create_guid,
    const uint8_t                    *client_guid,
    const char                       *name,
    uint32_t                          name_len,
    bool                              has_lease_ctx,
    const uint8_t                    *lease_key,
    bool                             *r_cold,
    uint32_t                         *status);
void
chimera_smb_durable_sweep(
    struct chimera_server_smb_thread *thread);

/* Release every registry entry's live open at thread shutdown so the VFS
 * close thread can drain.  Without this a parked durable handle's VFS open
 * handle leaks and chimera_vfs_destroy hangs waiting for it. */
void
chimera_smb_durable_drain_all(
    struct chimera_server_smb_thread *thread);

/* Purge a parked (disconnected) durable open by persistent id when a new
 * conflicting open arrives.  Returns true iff found+purged.  No-op for live,
 * persistent, cold, or unknown ids. */
bool
chimera_smb_durable_purge_parked(
    struct chimera_server_smb_thread *thread,
    uint64_t                          persistent_id);

/* Scan a share's backend (routed via `fh`) for persisted handle records and
 * rebuild cold registry entries.  Best-effort, idempotent. */
void
chimera_smb_durable_recover_share(
    struct chimera_server_smb_thread *thread,
    const void                       *fh,
    int                               fh_len);

/* Add a cold (recovered-from-backend, not-yet-reopened) entry from a record
 * read off the backend at startup.  Idempotent on persistent_id. */
void
chimera_smb_durable_recover_entry(
    struct chimera_server_smb_shared        *shared,
    const struct chimera_smb_durable_record *record);

/* Build the backend KV key for a persistent id into buf (>= CHIMERA_SMB_DURABLE_KEY_LEN). */
uint32_t
chimera_smb_durable_key(
    uint8_t *buf,
    uint64_t persistent_id);

/* Serialize a record into buf; returns bytes written (0 if buf too small). */
uint32_t
chimera_smb_durable_serialize(
    uint8_t                                 *buf,
    uint32_t                                 buf_size,
    const struct chimera_smb_durable_record *record);

/* Parse a serialized record; returns 0 on success, -1 on malformed input. */
int
chimera_smb_durable_deserialize(
    const uint8_t                     *buf,
    uint32_t                           buf_len,
    struct chimera_smb_durable_record *record);

/* A queued, self-contained SMB2 OPLOCK_BREAK notification, addressed to the
 * holder connection's owning thread. Carries values only (no pointers into
 * lease/open state that could be freed) plus the target conn; conn_free drains
 * any messages still queued for a connection being torn down. */
struct chimera_smb_lease_break_msg {
    struct chimera_smb_conn            *conn;
    bool                                is_lease;
    uint8_t                             lease_key[16];
    uint8_t                             current_state;
    uint8_t                             new_state;
    bool                                ack_required;
    uint16_t                            new_epoch;
    uint64_t                            file_id_pid;
    uint64_t                            file_id_vid;
    uint8_t                             new_oplock_level;
    struct chimera_smb_lease_break_msg *next;
};

struct chimera_server_smb_thread {
    struct evpl                        *evpl;
    struct chimera_vfs_thread          *vfs_thread;
    struct evpl_listener_binding       *binding;
    struct chimera_server_smb_shared   *shared;
    struct chimera_smb_request         *free_requests;
    struct chimera_smb_compound        *free_compounds;
    struct chimera_smb_conn            *free_conns;
    struct chimera_smb_session_handle  *free_session_handles;
    struct chimera_smb_open_file       *free_open_files;
    struct chimera_smb_signing_ctx     *signing_ctx;
    struct chimera_smb_encrypt_ctx     *encrypt_ctx;
    struct chimera_smb_compress_ctx    *compress_ctx;
    struct chimera_smb_iconv_ctx        iconv_ctx;

    /* Notify doorbell: VFS callbacks push ready notify requests here,
     * then ring the doorbell so the SMB thread processes them. */
    struct evpl_doorbell                notify_doorbell;
    struct chimera_smb_notify_request  *notify_ready;
    pthread_mutex_t                     notify_ready_lock;

    /* Lease-break doorbell: a lease break_cb may fire on any thread (the
     * breaker's), but the OPLOCK_BREAK notification must be sent on the holder
     * connection's owning thread (evpl iovec pools and binds are thread-local).
     * The break_cb enqueues a self-contained message addressed to the holder
     * thread and rings this doorbell; the handler sends it on the right thread.
     * Enqueue is cross-thread (lock-protected); the handler and conn_free both
     * run on this thread, so draining on disconnect needs no extra sync. */
    struct evpl_doorbell                lease_break_doorbell;
    struct chimera_smb_lease_break_msg *lease_break_ready;
    pthread_mutex_t                     lease_break_lock;

    /* Lease-break *resume* doorbell: when an inbound OPLOCK_BREAK ack settles a
     * file's caching lease, CREATEs parked waiting on that break may live on a
     * different connection owned by a different thread (the two-client lease
     * break: A's ack arrives on A's thread, B's parked CREATE sits on B's
     * thread).  The deferred CREATE response's iovecs are thread-local, so the
     * ack handler cannot complete them; instead it rings every other thread's
     * resume doorbell, and each thread re-scans its own connections' parked
     * CREATEs and completes those whose break has now settled
     * (chimera_vfs_state_caching_breaking() is false).  The re-check makes a
     * parameterless broadcast safe -- only genuinely-settled creates complete. */
    struct evpl_doorbell                lease_resume_doorbell;

    /* Active (non-pooled) connections owned by this thread, threaded on
     * conn->active_prev / conn->active_next.  Lets a thread walk every
     * connection it owns to find parked CREATEs to resume.  Only touched on the
     * owning thread (accept / conn_free / resume doorbell all run here), so no
     * lock is needed. */
    struct chimera_smb_conn            *active_conns;

    /* Process-global registry link: every SMB thread is chained here under
    * shared->threads_lock so the resume broadcast can reach every peer. */
    struct chimera_server_smb_thread   *next_thread;

    /* Periodic sweep of the shared durable-handle registry for parked opens
     * whose reconnect grace window has expired.  Each thread sweeps the shared
     * table; entries are claimed under the registry lock so peers never race. */
    struct evpl_timer                   durable_sweeper;
};


static inline void
chimera_smb_tree_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_tree          *tree,
    bool                              preserve_durable);

/*
 * Open-file lifetime model (current; Phase 3 will change this):
 *
 *   - Allocation: thread-local LIFO free list per chimera_server_smb_thread;
 *     a freshly-popped open_file is NOT zeroed. Every caller (today only
 *     chimera_smb_create_gen_open_file) is responsible for explicitly setting
 *     every field that matters for its lifetime — see the explicit init block
 *     in smb_proc_create.c.
 *
 *   - Ownership: an open is owned by exactly one chimera_smb_tree at a time,
 *     hashed into the tree's open_files[] buckets by file_id (pid+vid). The
 *     tree's lifetime is bounded by the connection that established it; when
 *     the connection drops, the tree is torn down and every open it owns is
 *     freed back to its allocating thread's free list.
 *
 *   - Cross-references: the share-mode table and the change-notify state
 *     hold raw pointers into open_file objects. These are *only* safe while
 *     the owning tree is alive — there is currently no global open-instance
 *     registry, no disconnect-survival, and no cross-thread migration.
 *
 *   - Reconnect/durable: NOT supported. The ctx_present_mask / lease_* /
 *     durable_* fields added in Phase 0 are inert state slots; the durable
 *     handle lifecycle (which lets opens outlive their connection) lands in
 *     Phase 3 and will almost certainly need to lift these fields out of
 *     this struct into separate lease/durable objects with their own
 *     lifetimes.
 *
 *   - Phase-3 reviewer's checklist: before merging persistent-handle work,
 *     confirm (a) file_id no longer collides across tree teardowns, (b) lease
 *     and durable objects own their own backing store, and (c) the share-mode
 *     table either takes a strong reference or learns about open-instance
 *     migration.
 */
static inline struct chimera_smb_open_file *
chimera_smb_open_file_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_open_file *open_file = thread->free_open_files;

    if (open_file) {
        LL_DELETE(thread->free_open_files, open_file);
    } else {
        open_file = calloc(1, sizeof(*open_file));
    }

    return open_file;
} /* chimera_smb_open_file_alloc */

static inline void
chimera_smb_open_file_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    LL_PREPEND(thread->free_open_files, open_file);
} /* chimera_smb_open_file_free */

static inline struct chimera_smb_request *
chimera_smb_request_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_request *request = thread->free_requests;

    if (request) {
        LL_DELETE(thread->free_requests, request);
    } else {
        request = calloc(1, sizeof(*request));
    }

    request->status          = SMB2_STATUS_SUCCESS;
    request->flags           = 0;
    request->async_id        = 0;
    request->tree            = NULL;
    request->async.armed     = 0;
    request->async.park_next = NULL;

    return request;
} /* chimera_smb_request_alloc */

static inline void
chimera_smb_request_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request)
{
    LL_PREPEND(thread->free_requests, request);
} /* chimera_smb_request_free */

static inline struct chimera_smb_session *
chimera_smb_session_alloc(struct chimera_server_smb_shared *shared)
{
    struct chimera_smb_session *session;

    pthread_mutex_lock(&shared->sessions_lock);

    session = shared->free_sessions;

    if (session) {
        LL_DELETE(shared->free_sessions, session);
    } else {
        session = chimera_smb_session_create();
        pthread_mutex_init(&session->lock, NULL);
    }

    /* Keep the session id within 32 bits (non-zero).  The wire field is 64
     * bits, but Windows/Samba hand out small ids and some clients (and the
     * smb2.session-id torture test) round-trip the id through a uint32_t.  A
     * full 64-bit random id would be silently truncated by such clients and
     * then fail to match on the next request. */
    do {
        session->session_id = chimera_rand64() & 0xFFFFFFFFULL;
    } while (session->session_id == 0);
    session->flags        = 0;
    session->refcnt       = 1;
    session->num_channels = 0;

    pthread_mutex_unlock(&shared->sessions_lock);


    return session;
} /* chimera_smb_session_alloc */

static inline void
chimera_smb_session_authorize(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_session       *session)
{
    pthread_mutex_lock(&shared->sessions_lock);

    session->flags |= CHIMERA_SMB_SESSION_AUTHORIZED;

    HASH_ADD(hh, shared->sessions, session_id, sizeof(uint64_t), session);

    pthread_mutex_unlock(&shared->sessions_lock);


} // chimera_smb_session_authorize

static inline struct chimera_smb_session *
chimera_smb_session_lookup(
    struct chimera_server_smb_shared *shared,
    uint64_t                          session_id)
{
    struct chimera_smb_session *session;

    pthread_mutex_lock(&shared->sessions_lock);
    HASH_FIND(hh, shared->sessions, &session_id, sizeof(uint64_t), session);

    if (session) {
        session->refcnt++;
    }

    pthread_mutex_unlock(&shared->sessions_lock);

    return session;
} /* chimera_smb_session_lookup */


static inline void
chimera_smb_session_release(
    struct chimera_server_smb_thread *thread,
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_session       *session,
    bool                              preserve_durable)
{
    struct chimera_smb_tree *tree;
    int                      destroy = 0;

    /* refcnt and shared->sessions membership are guarded by sessions_lock,
     * the same lock chimera_smb_session_alloc/lookup/authorize hold when they
     * set, increment, or HASH_ADD.  Releasing under session->lock instead would
     * race the lookup refcnt++ (lost update -> premature free) and modify the
     * shared hash unprotected (double HASH_DEL on an emptied table crashes). */
    pthread_mutex_lock(&shared->sessions_lock);

    chimera_smb_abort_if(session->refcnt == 0, "session refcnt is 0 at release");

    session->refcnt--;

    if (session->refcnt == 0) {
        destroy = 1;
        if (session->flags & CHIMERA_SMB_SESSION_AUTHORIZED) {
            HASH_DEL(shared->sessions, session);
        }
    }

    pthread_mutex_unlock(&shared->sessions_lock);

    if (destroy) {

        for (int i = 0; i < session->max_trees; i++) {
            tree = session->trees[i];

            if (tree) {
                session->trees[i] = NULL;
                chimera_smb_tree_free(thread, shared, tree, preserve_durable);
            }
        }

        pthread_mutex_lock(&shared->sessions_lock);

        LL_PREPEND(shared->free_sessions, session);

        pthread_mutex_unlock(&shared->sessions_lock);
    }
} /* chimera_smb_session_free */

/* MS-SMB2 3.3.4.4 open-preservation rule: a durable open holding byte-range
 * locks survives a disconnect only if it also holds a batch oplock or a
 * WRITE-caching lease -- otherwise the locks cannot be safely maintained across
 * the gap, so the open is closed rather than parked (a later reconnect then gets
 * OBJECT_NAME_NOT_FOUND).  Every other durable open is preservable. */
static inline bool
chimera_smb_durable_open_preservable(const struct chimera_smb_open_file *open_file)
{
    if (open_file->lock_entries) {
        bool write_caching =
            (open_file->oplock_level == SMB2_OPLOCK_LEVEL_BATCH) ||
            (open_file->lease_state & SMB2_LEASE_WRITE_CACHING);

        if (!write_caching) {
            return false;
        }
    }
    return true;
} /* chimera_smb_durable_open_preservable */

/* Park every durable/persistent open held by `session` for reconnect, leaving
 * the rest of the session in place.  Used when a PreviousSessionId reconnect
 * closes this session out from under its still-live connection (MS-SMB2
 * 3.3.5.5.3): the durable handles must be parked immediately so the new
 * connection can reclaim them, while the session's remaining (non-durable) opens
 * and trees are torn down normally when the old connection finally drops -- on
 * its own thread, avoiding a cross-thread VFS-handle release.  Parking is pure
 * shared-state manipulation under the bucket and registry locks, so it is safe
 * to run from the new connection's thread. */
static inline void
chimera_smb_session_park_durables(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_session       *session)
{
    int i, b;

    pthread_mutex_lock(&session->lock);

    for (i = 0; i < session->max_trees; i++) {
        struct chimera_smb_tree      *tree = session->trees[i];
        struct chimera_smb_open_file *open_file, *tmp;

        if (!tree) {
            continue;
        }

        for (b = 0; b < CHIMERA_SMB_OPEN_FILE_BUCKETS; b++) {
            pthread_mutex_lock(&tree->open_files_lock[b]);
            HASH_ITER(hh, tree->open_files[b], open_file, tmp)
            {
                if (!open_file->durable_flags ||
                    (open_file->flags & CHIMERA_SMB_OPEN_FILE_PARKED) ||
                    !chimera_smb_durable_open_preservable(open_file)) {
                    continue;
                }
                HASH_DELETE(hh, tree->open_files[b], open_file);
                open_file->flags      |= CHIMERA_SMB_OPEN_FILE_PARKED;
                open_file->create_conn = NULL;
                chimera_smb_durable_park(shared, open_file);
            }
            pthread_mutex_unlock(&tree->open_files_lock[b]);
        }
    }

    pthread_mutex_unlock(&session->lock);
} /* chimera_smb_session_park_durables */

/* MS-SMB2 3.3.5.5.3: a SESSION_SETUP carrying a non-zero PreviousSessionId asks
 * the server to close that earlier session of the same user once the new one is
 * established (a client reconnecting on a fresh transport, e.g. for durable-
 * handle reclaim).  Mark the prior session deleted and unlink it from the global
 * table without freeing it: its original connection still holds a reference, so
 * requests still arriving there get STATUS_USER_SESSION_DELETED.  Its durable
 * handles are parked immediately so the new connection can reclaim them.  Never
 * invalidate the new session itself, and only act when the requesting user
 * matches the prior session's owner. */
static inline int
chimera_smb_session_invalidate_previous(
    struct chimera_server_smb_thread *thread,
    struct chimera_server_smb_shared *shared,
    uint64_t                          prev_session_id,
    struct chimera_smb_session       *new_session,
    uint16_t                          cur_dialect)
{
    struct chimera_smb_session *prev;

    if (prev_session_id == 0 || prev_session_id == new_session->session_id) {
        return 0;
    }

    pthread_mutex_lock(&shared->sessions_lock);

    HASH_FIND(hh, shared->sessions, &prev_session_id, sizeof(uint64_t), prev);

    if (prev && prev != new_session &&
        (prev->flags & CHIMERA_SMB_SESSION_AUTHORIZED) &&
        prev->cred.uid == new_session->cred.uid) {
        /* MS-SMB2 3.3.5.5.1: a reconnect whose connection dialect differs from
         * the previous session's MUST be rejected.  Leave the previous session
         * intact and signal the caller to fail with USER_SESSION_DELETED. */
        if (prev->dialect != cur_dialect) {
            pthread_mutex_unlock(&shared->sessions_lock);
            return 1;
        }
        /* Clear AUTHORIZED and unlink here so the later refcnt-driven
         * chimera_smb_session_release() does not HASH_DEL a second time.  Hold a
         * reference across the park so a concurrent release cannot free it. */
        prev->flags |= CHIMERA_SMB_SESSION_DELETED;
        prev->flags &= ~CHIMERA_SMB_SESSION_AUTHORIZED;
        HASH_DEL(shared->sessions, prev);
        prev->refcnt++;
    } else {
        prev = NULL;
    }

    pthread_mutex_unlock(&shared->sessions_lock);

    if (prev) {
        chimera_smb_session_park_durables(shared, prev);
        chimera_smb_session_release(thread, shared, prev, true);
    }
    return 0;
} /* chimera_smb_session_invalidate_previous */


static inline struct chimera_smb_session_handle *
chimera_smb_session_handle_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_session_handle *session_handle = thread->free_session_handles;

    if (session_handle) {
        LL_DELETE(thread->free_session_handles, session_handle);
    } else {
        session_handle = calloc(1, sizeof(*session_handle));
    }

    session_handle->bound_channel = 0;

    return session_handle;
} /* chimera_smb_session_handle_alloc */

static inline void
chimera_smb_session_handle_free(
    struct chimera_server_smb_thread  *thread,
    struct chimera_smb_session_handle *session_handle)
{
    LL_PREPEND(thread->free_session_handles, session_handle);
} /* chimera_smb_session_handle_free */

static inline struct chimera_smb_conn *
chimera_smb_conn_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_conn *conn = thread->free_conns;

    if (conn) {
        LL_DELETE(thread->free_conns, conn);
        conn->lease_break_tearing_down = 0;
    } else {
        conn = calloc(1, sizeof(*conn));
    }

    return conn;
} /* chimera_smb_conn_alloc */

/* Defined in smb_notify.c */
void chimera_smb_notify_cancel(
    struct chimera_smb_notify_request *nr);
void chimera_smb_notify_drop(
    struct chimera_smb_notify_request *nr);

/* Defined in smb_async_interim.c -- forward-declared so the inline conn_free
 * below can drain without including the header (which would create a cycle
 * through smb_internal.h). */
void chimera_smb_async_interim_drain(
    struct chimera_smb_conn *conn);

static inline void
chimera_smb_conn_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn)
{
    struct chimera_smb_session_handle  *session_handle, *tmp;
    struct chimera_smb_lease_break_msg *bmsg, **bpp;

    /* Drop all parked CHANGE_NOTIFY requests silently — the bind is
     * being torn down so a CANCELLED reply would race the destroy and
     * the client is already gone. */
    while (conn->parked_notifies) {
        chimera_smb_notify_drop(conn->parked_notifies);
    }

    /* Unlink any requests pending an async-interim (their interim is already on
     * the wire).  They are not freed here -- they remain owned by their
     * compounds, which tear down through the normal path; a late VFS callback
     * reaching chimera_smb_complete_request sees async.armed == 0 and skips the
     * cancel. */
    chimera_smb_async_interim_drain(conn);

    /* Drain any lease-break notifications still queued for this connection and
     * mark it tearing-down so a break_cb racing on another thread won't enqueue
     * a send against the bind we're about to free.  This runs on conn->thread,
     * the same thread as the lease-break doorbell handler, so the two never
     * interleave; only the cross-thread enqueue needs the lock. */
    pthread_mutex_lock(&thread->lease_break_lock);
    conn->lease_break_tearing_down = 1;
    bpp                            = &thread->lease_break_ready;
    while (*bpp) {
        if ((*bpp)->conn == conn) {
            bmsg = *bpp;
            *bpp = bmsg->next;
            free(bmsg);
        } else {
            bpp = &(*bpp)->next;
        }
    }
    pthread_mutex_unlock(&thread->lease_break_lock);

    /* Clear create_conn pointers on every open_file that references
     * this conn.  When the session refcount drops to zero below, the
     * trees and their opens get torn down — but if multi-channel keeps
     * the session alive past this conn, the opens persist with stale
     * create_conn pointers that the OPLOCK_BREAK path would dereference. */
    HASH_ITER(hh, conn->session_handles, session_handle, tmp)
    {
        struct chimera_smb_session *s = session_handle->session;
        int                         i;

        if (!s) {
            continue;
        }
        for (i = 0; i < s->max_trees; i++) {
            struct chimera_smb_tree      *t = s->trees[i];
            struct chimera_smb_open_file *of;
            struct chimera_smb_open_file *tmp_of;
            int                           b;

            if (!t) {
                continue;
            }
            for (b = 0; b < CHIMERA_SMB_OPEN_FILE_BUCKETS; b++) {
                pthread_mutex_lock(&t->open_files_lock[b]);
                HASH_ITER(hh, t->open_files[b], of, tmp_of)
                {
                    if (of->create_conn == conn) {
                        of->create_conn = NULL;
                    }
                }
                pthread_mutex_unlock(&t->open_files_lock[b]);
            }
        }
    }

    HASH_ITER(hh, conn->session_handles, session_handle, tmp)
    {
        HASH_DELETE(hh, conn->session_handles, session_handle);

        if (session_handle->ctx != GSS_C_NO_CONTEXT) {

            chimera_smb_debug("chimera_smb_conn_free freeing context for "
                              "session_handle %p\n", session_handle);

            gss_delete_sec_context(&conn->gss_minor,
                                   &session_handle->ctx, NULL);
            session_handle->ctx = GSS_C_NO_CONTEXT;
        }

        if (session_handle->session) {
            /* A bound additional channel is going away; free its slot so the
             * session can accept another channel later (MS-SMB2 §3.3.5.5.3). */
            if (session_handle->bound_channel) {
                pthread_mutex_lock(&thread->shared->sessions_lock);
                if (session_handle->session->num_channels > 0) {
                    session_handle->session->num_channels--;
                }
                pthread_mutex_unlock(&thread->shared->sessions_lock);
                session_handle->bound_channel = 0;
            }

            /* The transport for this connection has dropped: any durable handle
             * left open on a session whose last channel this was must be
             * preserved for reconnect (preserve_durable = true). */
            chimera_smb_session_release(thread, thread->shared, session_handle->session, true);
        }

        chimera_smb_session_handle_free(thread, session_handle);
    }

    /* last_session_handle is a raw cache into session_handles (above); the
     * loop just freed every entry, so drop the dangling pointer before this
     * conn struct is returned to the free list.  Otherwise a later reuse of
     * the pooled conn (chimera_smb_conn_alloc) inherits a pointer to a freed,
     * recycled handle, and the session-resolution fast path hands it out for
     * a request whose session id happens to match the recycled handle's
     * current session -- which then aborts in chimera_smb_logoff. */
    conn->last_session_handle = NULL;

    if (conn->nascent_ctx != GSS_C_NO_CONTEXT) {
        gss_delete_sec_context(&conn->gss_minor, &conn->nascent_ctx, NULL);
        conn->nascent_ctx = GSS_C_NO_CONTEXT;
    }

    if (conn->gss_output.value) {
        gss_release_buffer(&conn->gss_minor, &conn->gss_output);
        conn->gss_output.value  = NULL;
        conn->gss_output.length = 0;
    }

    if (conn->ntlm_output) {
        free(conn->ntlm_output);
        conn->ntlm_output     = NULL;
        conn->ntlm_output_len = 0;
    }

    // Cleanup GSSAPI context if initialized
    smb_gssapi_cleanup(&conn->gssapi_ctx);

    /* Drop from the owning thread's active-connection list before returning the
     * conn to the pool (the resume doorbell walks active_conns). */
    DL_DELETE2(thread->active_conns, conn, active_prev, active_next);

    LL_PREPEND(thread->free_conns, conn);
} /* chimera_smb_conn_free */


static inline struct chimera_smb_tree *
chimera_smb_tree_alloc(struct chimera_server_smb_shared *shared)
{
    struct chimera_smb_tree *tree;

    pthread_mutex_lock(&shared->trees_lock);

    tree = shared->free_trees;

    if (tree) {
        LL_DELETE(shared->free_trees, tree);
    } else {
        tree = calloc(1, sizeof(*tree));

        for (int i = 0; i < CHIMERA_SMB_OPEN_FILE_BUCKETS; i++) {
            pthread_mutex_init(&tree->open_files_lock[i], NULL);
        }
    }

    tree->fh_expiration.tv_sec  = 0;
    tree->fh_expiration.tv_nsec = 0;
    tree->refcnt                = 1;

    pthread_mutex_unlock(&shared->trees_lock);
    return tree;
} /* chimera_smb_tree_alloc */

static inline void
chimera_smb_tree_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_tree          *tree,
    bool                              preserve_durable)
{
    struct chimera_smb_open_file    *open_file, *tmp;
    struct chimera_smb_notify_state *gc_states = NULL;
    struct chimera_smb_notify_state *nstate;
    int                              i;

    for (i = 0; i < CHIMERA_SMB_OPEN_FILE_BUCKETS; i++) {

        pthread_mutex_lock(&tree->open_files_lock[i]);

        HASH_ITER(hh, tree->open_files[i], open_file, tmp)
        {
            chimera_smb_abort_if(open_file->refcnt == 0, "open file refcnt is 0 at tree destruction");
            HASH_DELETE(hh, tree->open_files[i], open_file);

            /* A durable/persistent open survives the teardown of its session
             * (connection loss or a graceful LOGOFF, MS-SMB2 3.3.5.6): keep the
             * object allocated with its leases, share reservation, byte-range
             * locks and VFS handle intact, and hand ownership to the durable
             * registry with a reconnect deadline.  Do NOT mark CLOSED (a
             * reconnect must be able to re-home it), drain locks, release
             * sharemode, or free.  The single tree reference becomes the
             * registry's reference, so refcnt is left untouched (steady-state 1).
             *
             * A TREE_DISCONNECT (preserve_durable == false) is the client
             * explicitly relinquishing every open on that tree (MS-SMB2
             * 3.3.5.7); the durable property does not apply, so fall through to
             * the normal close path, which also forgets the registry entry --
             * a later durable reconnect with the stale FileId then correctly
             * returns OBJECT_NAME_NOT_FOUND. */
            if (open_file->durable_flags && preserve_durable &&
                chimera_smb_durable_open_preservable(open_file)) {
                open_file->flags      |= CHIMERA_SMB_OPEN_FILE_PARKED;
                open_file->create_conn = NULL;
                /* park acquires the registry lock under this bucket lock —
                 * the bucket -> registry order is observed everywhere. */
                chimera_smb_durable_park(shared, open_file);
                continue;
            }

            /* Detach any CHANGE_NOTIFY watch/parked request from this open and
             * defer its teardown until after the bucket lock is dropped:
             * completing a parked request releases the open_file reference it
             * holds, which re-takes this same bucket lock.  A parked request
             * keeps refcnt > 0, so the open_file is NOT freed in this loop —
             * its final release happens in the deferred chimera_smb_notify_close
             * below.  A watch with no parked request holds no reference, so the
             * open_file is freed here and the deferred close just frees the
             * detached state + destroys the VFS watch. */
            nstate = open_file->notify_state;
            if (nstate) {
                open_file->notify_state = NULL;
                nstate->gc_next         = gc_states;
                gc_states               = nstate;
            }

            open_file->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;

            if (open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
                tree->share) {
                chimera_smb_sharemode_release(&tree->share->sharemode,
                                              open_file);
            }

            open_file->refcnt--;

            if (open_file->refcnt == 0) {
                /* Final teardown of a live durable open on a graceful
                 * tree/session teardown: drop its registry entry so a later
                 * reconnect with the stale FileId is not matched against a
                 * freed open_file. */
                if (open_file->durable_flags) {
                    chimera_smb_durable_forget(shared, open_file->file_id.pid);
                }
                chimera_smb_open_file_drain_locks(thread, open_file);
                if (open_file->handle) {
                    chimera_vfs_release(thread->vfs_thread, open_file->handle);
                }
                chimera_smb_open_file_free(thread, open_file);
            }
        }

        pthread_mutex_unlock(&tree->open_files_lock[i]);
    }

    /* Tear down notify state detached above, now that no bucket lock is held.
     * For a still-parked request this sends STATUS_NOTIFY_CLEANUP (the
     * connection is alive on a TREE_DISCONNECT / LOGOFF) and releases the
     * open_file reference it held — its final reference, so the open is freed
     * here.  During hard connection teardown chimera_smb_conn_free has already
     * dropped the parked requests silently, so there is nothing to send and
     * only the watch + state are freed. */
    while (gc_states) {
        nstate    = gc_states;
        gc_states = nstate->gc_next;
        chimera_smb_notify_close(shared->vfs->vfs_notify, nstate);
    }

    pthread_mutex_lock(&shared->trees_lock);

    LL_PREPEND(shared->free_trees, tree);

    pthread_mutex_unlock(&shared->trees_lock);
} /* chimera_smb_tree_free */

static inline struct chimera_smb_compound *
chimera_smb_compound_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_compound *compound = thread->free_compounds;

    if (compound) {
        LL_DELETE(thread->free_compounds, compound);
    } else {
        compound = calloc(1, sizeof(*compound));
    }

    return compound;
} /* chimera_smb_compound_alloc */

static inline void
chimera_smb_compound_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_compound      *compound)
{
    for (int i = 0; i < compound->num_requests; i++) {
        chimera_smb_request_free(thread, compound->requests[i]);
    }

    LL_PREPEND(thread->free_compounds, compound);
} /* chimera_smb_compound_free */

static inline struct chimera_smb_open_file *
chimera_smb_open_file_resolve(
    struct chimera_smb_request *request,
    struct chimera_smb_file_id *file_id)
{
    struct chimera_smb_open_file *open_file;
    struct chimera_smb_tree      *tree = request->tree;
    int                           open_file_bucket;

    chimera_smb_abort_if(!tree, "tree is NULL");

    /* A UINT64_MAX FileId inherits the previous request's FileId, but only in a
     * related compound (MS-SMB2 3.3.5.2.7.2).  For an unrelated request it is
     * just an invalid handle and must resolve to FILE_CLOSED, not the prior
     * request's file. */
    if (unlikely(file_id->pid == UINT64_MAX)) {
        if (!(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) ||
            request->compound->saved_file_id.pid == UINT64_MAX) {
            return NULL;
        }
        file_id->pid = request->compound->saved_file_id.pid;
    }

    if (unlikely(file_id->vid == UINT64_MAX)) {
        if (!(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) ||
            request->compound->saved_file_id.vid == UINT64_MAX) {
            return NULL;
        }
        file_id->vid = request->compound->saved_file_id.vid;
    }

    open_file_bucket = file_id->vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_FIND(hh, tree->open_files[open_file_bucket], file_id, sizeof(*file_id), open_file);

    if (likely(open_file)) {
        if (likely(!(open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED))) {
            open_file->refcnt++;
        } else {
            open_file = NULL;
        }
    }

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    return open_file;
} /* chimera_smb_open_file_resolve */

/*
 * MS-SMB2 §3.3.5.2.10 "Verifying the Channel Sequence Number".  Compare the
 * request's ChannelSequence against the highest seen on this Open.  Returns true
 * when the op must be rejected as stale (only mutating ops -- WRITE/SET_INFO/
 * IOCTL -- reject; READ tolerates a stale sequence).  An equal-or-ahead sequence
 * advances the Open's tracked value; a behind sequence (delta in [0x8000,0xFFFF]
 * mod 0x10000) is stale.  The first op on a not-yet-seeded Open just seeds it
 * (opens are normally seeded from the CREATE's sequence at grant time).
 */
static inline bool
chimera_smb_channel_sequence_stale(
    struct chimera_smb_open_file *open_file,
    uint16_t                      req_cs,
    int                           mutating)
{
    uint16_t delta;

    if (!open_file->channel_sequence_valid) {
        open_file->channel_sequence       = req_cs;
        open_file->channel_sequence_valid = 1;
        return false;
    }

    delta = (uint16_t) (req_cs - open_file->channel_sequence);

    if (delta >= 0x8000) {
        /* Stale (behind).  Mutating ops are rejected; reads proceed without
         * advancing the tracked sequence. */
        return mutating ? true : false;
    }

    /* Equal or ahead: this becomes the new high-water sequence. */
    open_file->channel_sequence = req_cs;
    return false;
} /* chimera_smb_channel_sequence_stale */

/*
 * Resolve the open_file that owns the caching lease registered under `lease_key`
 * (an SMB2 RqLs lease).  Unlike file_id, lease keys are not hashed, so this scans
 * the tree's open_files buckets -- a lease-break ack is rare relative to I/O, so
 * the linear scan is acceptable; a dedicated lease-key index is a later
 * optimization.  Multiple opens may share one lease key (coalesced), but only the
 * one that inserted the lease carries caching_lease_inserted, so match on that.
 * On success the returned open_file has had its refcnt bumped (release with
 * chimera_smb_open_file_release).
 */
static inline struct chimera_smb_open_file *
chimera_smb_open_file_resolve_by_lease_key(
    struct chimera_smb_request *request,
    const uint8_t              *lease_key)
{
    struct chimera_smb_open_file *open_file, *tmp, *found = NULL;
    struct chimera_smb_tree      *tree = request->tree;
    int                           b;

    chimera_smb_abort_if(!tree, "tree is NULL");

    for (b = 0; b < CHIMERA_SMB_OPEN_FILE_BUCKETS && !found; b++) {
        pthread_mutex_lock(&tree->open_files_lock[b]);
        HASH_ITER(hh, tree->open_files[b], open_file, tmp)
        {
            if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) &&
                open_file->caching_lease_inserted &&
                open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE &&
                memcmp(open_file->lease_key, lease_key, 16) == 0) {
                open_file->refcnt++;
                found = open_file;
                break;
            }
        }
        pthread_mutex_unlock(&tree->open_files_lock[b]);
    }

    return found;
} /* chimera_smb_open_file_resolve_by_lease_key */

/*
 * A lease key is bound to exactly one file per client (MS-SMB2 3.3.5.9.8): a
 * client may not use the same lease key on two different files.  Returns true if
 * any live open in the session already holds this lease key on a DIFFERENT file
 * than (fh, fh_len) -- in which case the create must be rejected with
 * STATUS_INVALID_PARAMETER.  A re-open of the SAME file under the key (coalesce)
 * is not a conflict.  Scans the session's trees' open_files (lease creates are
 * rare relative to I/O, so the linear scan is acceptable).
 */
static inline bool
chimera_smb_session_lease_key_conflict(
    struct chimera_smb_session *session,
    const uint8_t              *key,
    const uint8_t              *fh,
    uint32_t                    fh_len)
{
    bool conflict = false;
    int  t;

    pthread_mutex_lock(&session->lock);
    for (t = 0; t < session->max_trees && !conflict; t++) {
        struct chimera_smb_tree      *tree = session->trees[t];
        struct chimera_smb_open_file *of, *tmp;
        int                           b;

        if (!tree) {
            continue;
        }
        for (b = 0; b < CHIMERA_SMB_OPEN_FILE_BUCKETS && !conflict; b++) {
            pthread_mutex_lock(&tree->open_files_lock[b]);
            HASH_ITER(hh, tree->open_files[b], of, tmp)
            {
                if (!(of->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) &&
                    of->oplock_level == SMB2_OPLOCK_LEVEL_LEASE &&
                    of->handle &&
                    memcmp(of->lease_key, key, 16) == 0 &&
                    (of->handle->fh_len != fh_len ||
                     memcmp(of->handle->fh, fh, fh_len) != 0)) {
                    conflict = true;
                    break;
                }
            }
            pthread_mutex_unlock(&tree->open_files_lock[b]);
        }
    }
    pthread_mutex_unlock(&session->lock);
    return conflict;
} /* chimera_smb_session_lease_key_conflict */

/*
 * Caching-grant membership.  A VFS-owned caching grant (chimera_vfs_caching_grant)
 * may be shared by several opens under one (client, lease key); each such open is
 * threaded onto grant->holders so a break callback running on an arbitrary thread
 * can pick a still-connected member to deliver the OPLOCK_BREAK on (and revoke the
 * lease when no member is live).  The list is guarded by the grant's file->lock.
 */
static inline void
chimera_smb_grant_add_member(
    struct chimera_vfs_caching_grant *grant,
    struct chimera_smb_open_file     *open_file)
{
    pthread_mutex_lock(&grant->file->lock);
    open_file->grant_member_next = grant->holders;
    grant->holders               = open_file;
    pthread_mutex_unlock(&grant->file->lock);
} /* chimera_smb_grant_add_member */

static inline void
chimera_smb_grant_remove_member(
    struct chimera_vfs_caching_grant *grant,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_smb_open_file **pp;

    pthread_mutex_lock(&grant->file->lock);
    for (pp = (struct chimera_smb_open_file **) &grant->holders; *pp;
         pp = &(*pp)->grant_member_next) {
        if (*pp == open_file) {
            *pp = open_file->grant_member_next;
            break;
        }
    }
    open_file->grant_member_next = NULL;
    pthread_mutex_unlock(&grant->file->lock);
} /* chimera_smb_grant_remove_member */

static inline void
chimera_smb_open_file_release(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_smb_tree      *tree              = request->tree;
    struct chimera_smb_open_file *open_file_to_free = NULL;
    int                           open_file_bucket;

    chimera_smb_abort_if(!tree, "tree is NULL");

    open_file_bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    chimera_smb_abort_if(open_file->refcnt == 0, "open file refcnt is 0 at release");

    open_file->refcnt--;

    if (open_file->refcnt == 0) {
        /* Final teardown of a live durable open (e.g. an explicit CLOSE):
         * drop its registry entry before the object is recycled. */
        if (open_file->durable_flags) {
            chimera_smb_durable_forget(request->compound->thread->shared,
                                       open_file->file_id.pid);
        }
        chimera_smb_open_file_drain_locks(request->compound->thread, open_file);
        if (open_file->handle) {
            chimera_vfs_release(request->compound->thread->vfs_thread, open_file->handle);
        }

        open_file_to_free = open_file;
    }

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    if (open_file_to_free) {
        chimera_smb_open_file_free(request->compound->thread, open_file_to_free);
    }
} // chimera_smb_open_file_release

/*
 * Release an open_file reference without a request context.
 * Used by parked notify requests which outlive their original request.
 */
static inline void
chimera_smb_open_file_release_nr(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_tree          *tree,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_smb_open_file *open_file_to_free = NULL;
    int                           open_file_bucket;

    open_file_bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    chimera_smb_abort_if(open_file->refcnt == 0, "open file refcnt is 0 at release");

    open_file->refcnt--;

    if (open_file->refcnt == 0) {
        if (open_file->durable_flags) {
            chimera_smb_durable_forget(thread->shared, open_file->file_id.pid);
        }
        chimera_smb_open_file_drain_locks(thread, open_file);
        if (open_file->handle) {
            chimera_vfs_release(thread->vfs_thread, open_file->handle);
        }

        open_file_to_free = open_file;
    }

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    if (open_file_to_free) {
        chimera_smb_open_file_free(thread, open_file_to_free);
    }
} // chimera_smb_open_file_release_nr

static inline struct chimera_smb_open_file *
chimera_smb_open_file_close(
    struct chimera_smb_request *request,
    struct chimera_smb_file_id *file_id)
{
    struct chimera_smb_open_file *open_file;
    struct chimera_smb_tree      *tree = request->tree;
    int                           open_file_bucket;

    chimera_smb_abort_if(!tree, "tree is NULL");

    /* Only a related compound request inherits the previous request's FileId
     * from a UINT64_MAX placeholder; for an unrelated request it is an invalid
     * handle and must report FILE_CLOSED (MS-SMB2 3.3.5.2.7.2). */
    if (unlikely(file_id->pid == UINT64_MAX)) {
        if (!(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) ||
            request->compound->saved_file_id.pid == UINT64_MAX) {
            return NULL;
        }
        file_id->pid = request->compound->saved_file_id.pid;
    }

    if (unlikely(file_id->vid == UINT64_MAX)) {
        if (!(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) ||
            request->compound->saved_file_id.vid == UINT64_MAX) {
            return NULL;
        }
        file_id->vid = request->compound->saved_file_id.vid;
    }

    open_file_bucket = file_id->vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_FIND(hh, tree->open_files[open_file_bucket], file_id, sizeof(*file_id), open_file);

    if (open_file) {

        chimera_smb_abort_if(open_file->refcnt == 0, "open file refcnt is 0 at close");

        if (open_file->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) {
            chimera_smb_error("Attempted to close already closed file id %lx.%lx",
                              file_id->pid, file_id->vid);
            open_file = NULL;
        } else {
            open_file->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
            HASH_DELETE(hh, tree->open_files[open_file_bucket], open_file);
        }
    }

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);


    return open_file;
} /* chimera_smb_open_file_remove */
