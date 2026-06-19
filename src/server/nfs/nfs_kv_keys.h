// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

#include "nfs4_xdr.h"

/*
 * Binary key encoding for chimera state persisted in the global VFS key-value
 * store (the chimera_vfs_*_key API, routed to the configured server.kv_module).
 *
 * Many subsystems share one KV namespace, and -- when N chimera instances drive
 * one shared backing store -- so do many server instances.  Every key starts
 * with a 3-byte header that partitions the space by record type:
 *
 *     offset 0: magic   = CHIMERA_KV_MAGIC   (0xC4)
 *     offset 1: version = CHIMERA_KV_VERSION (0x02)
 *     offset 2: type    = enum chimera_kv_record_type
 *     offset 3+: type-specific key bytes
 *
 * Byte 0 = 0xC4 is non-ASCII, so this band can never collide with the SMB
 * durable-handle keys (ASCII "smbdh"...).  search_keys returns keys in byte
 * order, so the 3-byte [magic,version,type] prefix scans exactly one record
 * type.
 *
 * Within a type, the tail is keyed one of two ways, by what the record must
 * survive:
 *
 *   - NODE-scoped (recovery, epoch): the tail begins with the 2-byte minting
 *     node_id (big-endian, so keys sort by node) -- giving a 5-byte
 *     [magic,version,type,node] scan prefix.  These back a node's OWN reboot:
 *     each instance reloads only the records it minted (it never reconstructs a
 *     live peer's grace state), and the boot epoch is inherently per-node.  See
 *     nfs_kv_node_prefix and server.nfs4_node_id.
 *
 *   - CLIENT-scoped (session, reply-4.1, NFSv3/NFSv4.0 DRC): the tail begins
 *     with the client's own stable identity -- the globally-unique sessionid
 *     for 4.1, or the source address for the connectionless v3/v4.0 caches --
 *     and carries NO node_id.  These follow the client across a failover: a
 *     given client is served by one node at a time, so two nodes never write
 *     the same key, and whichever node the client reconnects to hydrates that
 *     client's band lazily on first contact (see nfs4_drc_session_hydrate /
 *     nfs3_drc.c).  See nfs_kv_type_prefix.
 *
 * Values are little-endian throughout (see the nfs_kv_put_le* / nfs_kv_le*
 * helpers) and each begins with a 4-byte record magic that the deserializer
 * validates before trusting any bytes.
 */

#define CHIMERA_KV_MAGIC      0xC4u
#define CHIMERA_KV_VERSION    0x02u
#define CHIMERA_KV_HDR_LEN    3u
#define CHIMERA_KV_NODE_LEN   2u
/* type header + node_id == the per-node scan prefix. */
#define CHIMERA_KV_PREFIX_LEN (CHIMERA_KV_HDR_LEN + CHIMERA_KV_NODE_LEN)

enum chimera_kv_record_type {
    CHIMERA_KV_TYPE_NFS4_RECOVERY  = 0x01, /* confirmed-client identity (node)   */
    CHIMERA_KV_TYPE_NFS4_EPOCH     = 0x02, /* per-node server boot-epoch (node)  */
    CHIMERA_KV_TYPE_NFS4_SESSION   = 0x03, /* 4.1 session metadata (by sessionid)*/
    CHIMERA_KV_TYPE_NFS4_REPLY     = 0x04, /* 4.1 reply slot entry (by sessionid)*/
    CHIMERA_KV_TYPE_NFS3_REPLY     = 0x05, /* NFSv3 DRC reply entry (by client)  */
    CHIMERA_KV_TYPE_NSM_STATE      = 0x06, /* singleton NSM/statd state number   */
    CHIMERA_KV_TYPE_NSM_MONITOR    = 0x07, /* per-host NSM monitor (nfs_nsm)     */
    CHIMERA_KV_TYPE_NFS4_V40_REPLY = 0x08, /* NFSv4.0 DRC reply entry (by client)*/
    /* 0x09 .. 0xFE reserved for future global record types */
};

/* Longest normalized client address string an NFSv3 DRC key embeds (IPv6
 * literal, no port -- comfortably inside INET6_ADDRSTRLEN). */
#define CHIMERA_KV_NFS3_ADDR_MAX 48u

/* Largest key any NFS record type produces: prefix + a full client owner. */
#define CHIMERA_KV_NFS_KEY_MAX   (CHIMERA_KV_PREFIX_LEN + NFS4_OPAQUE_LIMIT)

/* ----------------------------------------------------------------------- *
*  Little-endian value (de)serialization helpers                          *
* ----------------------------------------------------------------------- */

static inline void
nfs_kv_put_le32(
    uint8_t  *b,
    uint32_t *p,
    uint32_t  v)
{
    b[*p]     = v & 0xff;
    b[*p + 1] = (v >> 8) & 0xff;
    b[*p + 2] = (v >> 16) & 0xff;
    b[*p + 3] = (v >> 24) & 0xff;
    *p       += 4;
} /* nfs_kv_put_le32 */

static inline void
nfs_kv_put_le64(
    uint8_t  *b,
    uint32_t *p,
    uint64_t  v)
{
    nfs_kv_put_le32(b, p, (uint32_t) v);
    nfs_kv_put_le32(b, p, (uint32_t) (v >> 32));
} /* nfs_kv_put_le64 */

static inline uint32_t
nfs_kv_le32(const uint8_t *p)
{
    return (uint32_t) p[0] |
           ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) |
           ((uint32_t) p[3] << 24);
} /* nfs_kv_le32 */

static inline uint64_t
nfs_kv_le64(const uint8_t *p)
{
    return (uint64_t) nfs_kv_le32(p) | ((uint64_t) nfs_kv_le32(p + 4) << 32);
} /* nfs_kv_le64 */

/* ----------------------------------------------------------------------- *
*  Key builders.  Each returns the total key length written into buf.     *
*  buf must hold at least CHIMERA_KV_HDR_LEN + the type-specific tail.     *
* ----------------------------------------------------------------------- */

/* The 5-byte per-node scan prefix: [magic,version,type,node_hi,node_lo].
 * Every key builder below starts here; the reload scans use exactly this as the
 * search prefix so a node only sees its own records. */
static inline uint32_t
nfs_kv_node_prefix(
    uint8_t *buf,
    uint8_t  type,
    uint16_t node_id)
{
    buf[0] = CHIMERA_KV_MAGIC;
    buf[1] = CHIMERA_KV_VERSION;
    buf[2] = type;
    buf[3] = (uint8_t) (node_id >> 8);
    buf[4] = (uint8_t) (node_id & 0xff);
    return CHIMERA_KV_PREFIX_LEN;
} /* nfs_kv_node_prefix */

/* Bare 3-byte type prefix spanning every node's records of one type.  Reserved
 * for cross-node enumeration; the per-node cold-start reloads use the 5-byte
 * nfs_kv_node_prefix instead. */
static inline uint32_t
nfs_kv_type_prefix(
    uint8_t *buf,
    uint8_t  type)
{
    buf[0] = CHIMERA_KV_MAGIC;
    buf[1] = CHIMERA_KV_VERSION;
    buf[2] = type;
    return CHIMERA_KV_HDR_LEN;
} /* nfs_kv_type_prefix */

/* Recovery record key = prefix + client owner bytes (the stable identity the
 * client re-presents at EXCHANGE_ID/SETCLIENTID, so re-confirm is idempotent). */
static inline uint32_t
nfs_kv_recovery_key(
    uint8_t       *buf,
    uint16_t       node_id,
    const uint8_t *owner,
    uint32_t       owner_len)
{
    uint32_t p = nfs_kv_node_prefix(buf, CHIMERA_KV_TYPE_NFS4_RECOVERY, node_id);

    memcpy(buf + p, owner, owner_len);
    return p + owner_len;
} /* nfs_kv_recovery_key */

/* Per-node boot-epoch key = the 5-byte prefix (node_id is the whole sub-id). */
static inline uint32_t
nfs_kv_epoch_key(
    uint8_t *buf,
    uint16_t node_id)
{
    return nfs_kv_node_prefix(buf, CHIMERA_KV_TYPE_NFS4_EPOCH, node_id);
} /* nfs_kv_epoch_key */

/* 4.1 session metadata key = [hdr] + sessionid.  CLIENT-scoped (the sessionid
 * is globally unique and follows the client across failover); no node_id, so
 * whatever node the client reconnects to can find + hydrate the record. */
static inline uint32_t
nfs_kv_session_key(
    uint8_t       *buf,
    const uint8_t *sessionid)
{
    uint32_t p = nfs_kv_type_prefix(buf, CHIMERA_KV_TYPE_NFS4_SESSION);

    memcpy(buf + p, sessionid, NFS4_SESSIONID_SIZE);
    return p + NFS4_SESSIONID_SIZE;
} /* nfs_kv_session_key */

/* 4.1 reply-cache slot key = [hdr] + sessionid + slotid + seqid.  CLIENT-scoped
 * like the session key; the [hdr] + sessionid prefix scans one client's whole
 * reply band for the lazy hydrate. */
static inline uint32_t
nfs_kv_reply_key(
    uint8_t       *buf,
    const uint8_t *sessionid,
    uint32_t       slotid,
    uint32_t       seqid)
{
    uint32_t p = nfs_kv_type_prefix(buf, CHIMERA_KV_TYPE_NFS4_REPLY);

    memcpy(buf + p, sessionid, NFS4_SESSIONID_SIZE);
    p += NFS4_SESSIONID_SIZE;
    nfs_kv_put_le32(buf, &p, slotid);
    nfs_kv_put_le32(buf, &p, seqid);
    return p;
} /* nfs_kv_reply_key */

/* The [hdr] + sessionid prefix that selects one session's records (the session
 * metadata record, or that session's whole reply band). */
#define CHIMERA_KV_SESSION_PREFIX_LEN (CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE)
#define CHIMERA_KV_REPLY_KEY_LEN \
        (CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE + 4 + 4)
#define CHIMERA_KV_SESSION_KEY_LEN    (CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE)

/* Connectionless-DRC reply key (NFSv3 and NFSv4.0 share this shape; `type`
 * selects the band) = [hdr] + addr_len(1) + client addr + proc(LE32) +
 * xid(LE32) + checksum(LE64).  Keyed by CLIENT identity, NOT node: the client
 * address is the request's source IP with the ephemeral port stripped, so the
 * record follows the client across a reconnect to whatever node it lands on
 * (that node hydrates the client's band on first contact -- see nfs3_drc.c).
 * proc + xid + checksum disambiguate a reused xid: a hit means the same client
 * re-presenting an identical call.  A given client is served by one node at a
 * time, so two nodes never write the same key.  (NFSv4.0 has one proc --
 * COMPOUND -- so its proc field is constant and the checksum carries the
 * disambiguation.) */
static inline uint32_t
nfs_kv_conn_reply_key(
    uint8_t       *buf,
    uint8_t        type,
    const uint8_t *addr,
    uint8_t        addr_len,
    uint32_t       proc,
    uint32_t       xid,
    uint64_t       cksum)
{
    uint32_t p = nfs_kv_type_prefix(buf, type);

    buf[p++] = addr_len;
    memcpy(buf + p, addr, addr_len);
    p += addr_len;
    nfs_kv_put_le32(buf, &p, proc);
    nfs_kv_put_le32(buf, &p, xid);
    nfs_kv_put_le64(buf, &p, cksum);
    return p;
} /* nfs_kv_conn_reply_key */

/* Per-client scan prefix = [hdr] + addr_len + addr.  Selects every reply record
 * one client address owns (all proc/xid/cksum), used to hydrate that client's
 * DRC on first contact.  The leading addr_len byte stops one address from
 * being a byte-prefix of another. */
static inline uint32_t
nfs_kv_conn_addr_prefix(
    uint8_t       *buf,
    uint8_t        type,
    const uint8_t *addr,
    uint8_t        addr_len)
{
    uint32_t p = nfs_kv_type_prefix(buf, type);

    buf[p++] = addr_len;
    memcpy(buf + p, addr, addr_len);
    return p + addr_len;
} /* nfs_kv_conn_addr_prefix */

#define CHIMERA_KV_CONN_ADDR_PREFIX_MAX (CHIMERA_KV_HDR_LEN + 1 + CHIMERA_KV_NFS3_ADDR_MAX)
#define CHIMERA_KV_CONN_REPLY_KEY_MAX \
        (CHIMERA_KV_HDR_LEN + 1 + CHIMERA_KV_NFS3_ADDR_MAX + 4 + 4 + 8)

/* NSM singleton state-number key = header + a fixed 0x00 sub-id (mirrors the
 * NFSv4 epoch key).  Value = magic(LE32) + version(LE32) + state(LE32). */
static inline uint32_t
nfs_kv_nsm_state_key(uint8_t *buf)
{
    uint32_t p = nfs_kv_type_prefix(buf, CHIMERA_KV_TYPE_NSM_STATE);

    buf[p] = 0x00;
    return p + 1;
} /* nfs_kv_nsm_state_key */

#define CHIMERA_KV_NSM_STATE_KEY_LEN (CHIMERA_KV_HDR_LEN + 1)

/* NSM monitor key = header + host name bytes (the NLM caller_name / mon_name,
 * the same stable identity a peer's statd presents in SM_NOTIFY).  Value =
 * magic(LE32) + flags(LE32) + addr_len(1) + addr[addr_len] (the peer IP, no
 * port -- how we reach its statd on our reboot). */
static inline uint32_t
nfs_kv_nsm_monitor_key(
    uint8_t       *buf,
    const uint8_t *host,
    uint32_t       host_len)
{
    uint32_t p = nfs_kv_type_prefix(buf, CHIMERA_KV_TYPE_NSM_MONITOR);

    memcpy(buf + p, host, host_len);
    return p + host_len;
} /* nfs_kv_nsm_monitor_key */
