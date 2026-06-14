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
 * Many subsystems share one KV namespace, so every key carries a 3-byte header
 * that partitions the space:
 *
 *     offset 0: magic   = CHIMERA_KV_MAGIC   (0xC4)
 *     offset 1: version = CHIMERA_KV_VERSION (0x01)
 *     offset 2: type    = enum chimera_kv_record_type
 *     offset 3+: type-specific key bytes
 *
 * Byte 0 = 0xC4 is non-ASCII, so this band can never collide with the SMB
 * durable-handle keys (ASCII "smbdh"...).  search_keys returns keys in byte
 * order, so a [magic,version,type] .. [magic,version,type+1] range scans
 * exactly one record type.  Values are little-endian throughout (see the
 * nfs_kv_put_le* / nfs_kv_le* helpers) and each begins with a 4-byte record
 * magic that the deserializer validates before trusting any bytes.
 */

#define CHIMERA_KV_MAGIC   0xC4u
#define CHIMERA_KV_VERSION 0x01u
#define CHIMERA_KV_HDR_LEN 3u

enum chimera_kv_record_type {
    CHIMERA_KV_TYPE_NFS4_RECOVERY = 0x01, /* confirmed-client identity (always) */
    CHIMERA_KV_TYPE_NFS4_EPOCH    = 0x02, /* singleton server boot-epoch marker */
    CHIMERA_KV_TYPE_NFS4_SESSION  = 0x03, /* session metadata (nfs4_drc only)   */
    CHIMERA_KV_TYPE_NFS4_REPLY    = 0x04, /* reply-cache slot entry (nfs4_drc)  */
    CHIMERA_KV_TYPE_NFS3_REPLY    = 0x05, /* NFSv3 DRC reply entry (nfs3_drc)   */
    CHIMERA_KV_TYPE_NSM_STATE     = 0x06, /* singleton NSM/statd state number   */
    CHIMERA_KV_TYPE_NSM_MONITOR   = 0x07, /* per-host NSM monitor (nfs_nsm)     */
    /* 0x08 .. 0xFE reserved for future global record types */
};

/* Longest normalized client address string an NFSv3 DRC key embeds (IPv6
 * literal, no port -- comfortably inside INET6_ADDRSTRLEN). */
#define CHIMERA_KV_NFS3_ADDR_MAX 48u

/* Largest key any NFS record type produces: header + a full client owner. */
#define CHIMERA_KV_NFS_KEY_MAX   (CHIMERA_KV_HDR_LEN + NFS4_OPAQUE_LIMIT)

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

static inline uint32_t
nfs_kv_hdr(
    uint8_t *buf,
    uint8_t  type)
{
    buf[0] = CHIMERA_KV_MAGIC;
    buf[1] = CHIMERA_KV_VERSION;
    buf[2] = type;
    return CHIMERA_KV_HDR_LEN;
} /* nfs_kv_hdr */

/* Bounded [start,end) prefix for a search_keys scan over one record type. */
static inline uint32_t
nfs_kv_type_prefix(
    uint8_t *buf,
    uint8_t  type)
{
    return nfs_kv_hdr(buf, type);
} /* nfs_kv_type_prefix */

/* Recovery record key = header + client owner bytes (the stable identity the
 * client re-presents at EXCHANGE_ID/SETCLIENTID, so re-confirm is idempotent). */
static inline uint32_t
nfs_kv_recovery_key(
    uint8_t       *buf,
    const uint8_t *owner,
    uint32_t       owner_len)
{
    uint32_t p = nfs_kv_hdr(buf, CHIMERA_KV_TYPE_NFS4_RECOVERY);

    memcpy(buf + p, owner, owner_len);
    return p + owner_len;
} /* nfs_kv_recovery_key */

/* Singleton boot-epoch key = header + a fixed 0x00 sub-id. */
static inline uint32_t
nfs_kv_epoch_key(uint8_t *buf)
{
    uint32_t p = nfs_kv_hdr(buf, CHIMERA_KV_TYPE_NFS4_EPOCH);

    buf[p] = 0x00;
    return p + 1;
} /* nfs_kv_epoch_key */

/* Session metadata key = header + sessionid. */
static inline uint32_t
nfs_kv_session_key(
    uint8_t       *buf,
    const uint8_t *sessionid)
{
    uint32_t p = nfs_kv_hdr(buf, CHIMERA_KV_TYPE_NFS4_SESSION);

    memcpy(buf + p, sessionid, NFS4_SESSIONID_SIZE);
    return p + NFS4_SESSIONID_SIZE;
} /* nfs_kv_session_key */

/* Reply-cache slot key = header + sessionid + slotid + seqid. */
static inline uint32_t
nfs_kv_reply_key(
    uint8_t       *buf,
    const uint8_t *sessionid,
    uint32_t       slotid,
    uint32_t       seqid)
{
    uint32_t p = nfs_kv_hdr(buf, CHIMERA_KV_TYPE_NFS4_REPLY);

    memcpy(buf + p, sessionid, NFS4_SESSIONID_SIZE);
    p += NFS4_SESSIONID_SIZE;
    nfs_kv_put_le32(buf, &p, slotid);
    nfs_kv_put_le32(buf, &p, seqid);
    return p;
} /* nfs_kv_reply_key */

#define CHIMERA_KV_REPLY_KEY_LEN \
        (CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE + 4 + 4)
#define CHIMERA_KV_SESSION_KEY_LEN (CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE)

/* NFSv3 DRC key = header + addr_len(1) + client addr bytes + proc(LE32) +
 * xid(LE32) + checksum(LE64).  The client address is the request's source IP
 * with the ephemeral port stripped, so it stays stable across the reconnect a
 * retransmit rides in on.  proc + xid + checksum disambiguate a reused xid:
 * a hit means the same client re-presenting an identical call. */
static inline uint32_t
nfs_kv_nfs3_reply_key(
    uint8_t       *buf,
    const uint8_t *addr,
    uint8_t        addr_len,
    uint32_t       proc,
    uint32_t       xid,
    uint64_t       cksum)
{
    uint32_t p = nfs_kv_hdr(buf, CHIMERA_KV_TYPE_NFS3_REPLY);

    buf[p++] = addr_len;
    memcpy(buf + p, addr, addr_len);
    p += addr_len;
    nfs_kv_put_le32(buf, &p, proc);
    nfs_kv_put_le32(buf, &p, xid);
    nfs_kv_put_le64(buf, &p, cksum);
    return p;
} /* nfs_kv_nfs3_reply_key */

#define CHIMERA_KV_NFS3_REPLY_KEY_MAX \
        (CHIMERA_KV_HDR_LEN + 1 + CHIMERA_KV_NFS3_ADDR_MAX + 4 + 4 + 8)

/* NSM singleton state-number key = header + a fixed 0x00 sub-id (mirrors the
 * NFSv4 epoch key).  Value = magic(LE32) + version(LE32) + state(LE32). */
static inline uint32_t
nfs_kv_nsm_state_key(uint8_t *buf)
{
    uint32_t p = nfs_kv_hdr(buf, CHIMERA_KV_TYPE_NSM_STATE);

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
    uint32_t p = nfs_kv_hdr(buf, CHIMERA_KV_TYPE_NSM_MONITOR);

    memcpy(buf + p, host, host_len);
    return p + host_len;
} /* nfs_kv_nsm_monitor_key */
