// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

#include "vfs/vfs_attrs.h"
#include "vfs/vfs_lease_types.h"

/*
 * Binary key/value encoding for VFS leases persisted to a KV store on behalf of
 * backends that do NOT advertise CHIMERA_VFS_CAP_LEASE (so the core cannot
 * project the lease into an authoritative backend).  Records are written via
 * chimera_vfs_put_key_at(fh, ...) -- routed to the backend's own KV when it has
 * CHIMERA_VFS_CAP_KV, else to the default KV module with a 1-byte fh_magic
 * namespace prefix (chimera_vfs_kv_route_fh).  This is fire-and-forget and NOT
 * coherent across nodes; it exists for crash recovery / restart reclaim only.
 *
 * Keys carry a 3-byte header that partitions the shared KV namespace:
 *     [0] magic   = 0xC5  (non-ASCII; != NFS 0xC4, != SMB ASCII "smbdh")
 *     [1] version = 0x01
 *     [2] type    = 0x01  (lease)
 *     [3] fh_len
 *     [4 .. 4+fh_len)  fh
 *     kind(1) owner_lo(LE64) owner_hi(LE64) offset(LE64)
 * Including the fh groups a file's leases together; including kind+owner+offset
 * makes the key unique per (file, lease kind, owner, byte-range start) so two
 * byte-range locks from one owner do not collide.  search_keys returns keys in
 * byte order, so the {magic,version,type} prefix scans exactly the lease band.
 */

#define CHIMERA_VFS_LEASE_KV_MAGIC     0xC5u
#define CHIMERA_VFS_LEASE_KV_VERSION   0x01u
#define CHIMERA_VFS_LEASE_KV_TYPE      0x01u
#define CHIMERA_VFS_LEASE_KV_HDR_LEN   3u

/* header + fh_len(1) + fh + kind(1) + owner_lo(8) + owner_hi(8) + offset(8) */
#define CHIMERA_VFS_LEASE_KV_KEY_MAX   (CHIMERA_VFS_LEASE_KV_HDR_LEN + 1 + \
                                        CHIMERA_VFS_FH_SIZE + 1 + 8 + 8 + 8)

/* record magic(4) + kind/granted/denied(3) + protocol(4) + client_key(8) +
 * owner_lo/hi(16) + offset(8) + length(8) + fh_len(1) + fh */
#define CHIMERA_VFS_LEASE_KV_VALUE_MAX (4 + 3 + 4 + 8 + 16 + 8 + 8 + 1 + \
                                        CHIMERA_VFS_FH_SIZE)

#define CHIMERA_VFS_LEASE_KV_REC_MAGIC 0x3156454Cu /* "LEV1" little-endian */

/* ----------------------------------------------------------------------- *
*  Little-endian (de)serialization helpers                                *
* ----------------------------------------------------------------------- */

static inline void
chimera_vfs_lease_kv_put_le32(
    uint8_t  *b,
    uint32_t *p,
    uint32_t  v)
{
    b[*p]     = v & 0xff;
    b[*p + 1] = (v >> 8) & 0xff;
    b[*p + 2] = (v >> 16) & 0xff;
    b[*p + 3] = (v >> 24) & 0xff;
    *p       += 4;
} /* chimera_vfs_lease_kv_put_le32 */

static inline void
chimera_vfs_lease_kv_put_le64(
    uint8_t  *b,
    uint32_t *p,
    uint64_t  v)
{
    chimera_vfs_lease_kv_put_le32(b, p, (uint32_t) v);
    chimera_vfs_lease_kv_put_le32(b, p, (uint32_t) (v >> 32));
} /* chimera_vfs_lease_kv_put_le64 */

static inline uint32_t
chimera_vfs_lease_kv_le32(const uint8_t *p)
{
    return (uint32_t) p[0] |
           ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) |
           ((uint32_t) p[3] << 24);
} /* chimera_vfs_lease_kv_le32 */

static inline uint64_t
chimera_vfs_lease_kv_le64(const uint8_t *p)
{
    return (uint64_t) chimera_vfs_lease_kv_le32(p) |
           ((uint64_t) chimera_vfs_lease_kv_le32(p + 4) << 32);
} /* chimera_vfs_lease_kv_le64 */

/* Bounded prefix for a search_keys scan over the whole lease band. */
static inline uint32_t
chimera_vfs_lease_kv_prefix(uint8_t *buf)
{
    buf[0] = CHIMERA_VFS_LEASE_KV_MAGIC;
    buf[1] = CHIMERA_VFS_LEASE_KV_VERSION;
    buf[2] = CHIMERA_VFS_LEASE_KV_TYPE;
    return CHIMERA_VFS_LEASE_KV_HDR_LEN;
} /* chimera_vfs_lease_kv_prefix */

/* Build the key for one lease.  Returns the key length written into buf. */
static inline uint32_t
chimera_vfs_lease_kv_key(
    uint8_t                        *buf,
    const uint8_t                  *fh,
    uint8_t                         fh_len,
    const struct chimera_vfs_lease *lease)
{
    uint32_t p = chimera_vfs_lease_kv_prefix(buf);

    buf[p++] = fh_len;
    memcpy(buf + p, fh, fh_len);
    p       += fh_len;
    buf[p++] = (uint8_t) lease->kind;
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->owner.owner_lo);
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->owner.owner_hi);
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->offset);
    return p;
} /* chimera_vfs_lease_kv_key */

/* Serialize a lease record value.  Returns the value length written into buf. */
static inline uint32_t
chimera_vfs_lease_kv_value(
    uint8_t                        *buf,
    const uint8_t                  *fh,
    uint8_t                         fh_len,
    const struct chimera_vfs_lease *lease)
{
    uint32_t p = 0;

    chimera_vfs_lease_kv_put_le32(buf, &p, CHIMERA_VFS_LEASE_KV_REC_MAGIC);
    buf[p++] = (uint8_t) lease->kind;
    buf[p++] = lease->mode.granted;
    buf[p++] = lease->mode.denied;
    chimera_vfs_lease_kv_put_le32(buf, &p, lease->owner.protocol);
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->owner.client_key);
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->owner.owner_lo);
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->owner.owner_hi);
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->offset);
    chimera_vfs_lease_kv_put_le64(buf, &p, lease->length);
    buf[p++] = fh_len;
    memcpy(buf + p, fh, fh_len);
    p += fh_len;
    return p;
} /* chimera_vfs_lease_kv_value */

/* Deserialize a lease record into `lease` and (optionally) the file handle.
 * Returns 0 on success, -1 if the value is malformed / wrong magic. */
static inline int
chimera_vfs_lease_kv_parse(
    const uint8_t            *value,
    uint32_t                  value_len,
    struct chimera_vfs_lease *lease,
    uint8_t                  *out_fh,
    uint8_t                  *out_fh_len)
{
    uint32_t p = 0;
    uint8_t  fh_len;

    if (value_len < 4 + 3 + 4 + 8 + 16 + 8 + 8 + 1) {
        return -1;
    }
    if (chimera_vfs_lease_kv_le32(value) != CHIMERA_VFS_LEASE_KV_REC_MAGIC) {
        return -1;
    }
    p = 4;

    memset(lease, 0, sizeof(*lease));
    lease->kind             = value[p++];
    lease->mode.granted     = value[p++];
    lease->mode.denied      = value[p++];
    lease->owner.protocol   = chimera_vfs_lease_kv_le32(value + p); p += 4;
    lease->owner.client_key = chimera_vfs_lease_kv_le64(value + p); p += 8;
    lease->owner.owner_lo   = chimera_vfs_lease_kv_le64(value + p); p += 8;
    lease->owner.owner_hi   = chimera_vfs_lease_kv_le64(value + p); p += 8;
    lease->offset           = chimera_vfs_lease_kv_le64(value + p); p += 8;
    lease->length           = chimera_vfs_lease_kv_le64(value + p); p += 8;
    fh_len                  = value[p++];

    if (fh_len > CHIMERA_VFS_FH_SIZE || p + fh_len > value_len) {
        return -1;
    }
    if (out_fh) {
        memcpy(out_fh, value + p, fh_len);
    }
    if (out_fh_len) {
        *out_fh_len = fh_len;
    }
    return 0;
} /* chimera_vfs_lease_kv_parse */
