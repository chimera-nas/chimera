// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#include "nfs_fh_wrap.h"

struct chimera_vfs;

/*
 * Fleet-wide FH signing keyring.
 *
 * The wire-format side lives in nfs_fh_wrap.h (`struct nfs_fh_keyring`,
 * `chimera_nfs_fh_wrap_v2`, `chimera_nfs_fh_unwrap_ring`).  This file loads the
 * ring from the KV store (record type CHIMERA_KV_TYPE_NFS_FH_SIGNING, with no
 * node_id in the key), so every node sharing the store holds the same keys and
 * a V2 handle minted on one node verifies on any other.
 *
 *   - nfs_server_start loads the ring before any listener opens, the same way
 *     it reloads recovery records, so every handle a client receives is V2.
 *   - An empty ring (first start of a fleet) makes each node mint a candidate
 *     kid=1 and store it with put-if-absent: exactly one candidate is stored,
 *     and every other node reads it back and adopts it.
 *   - On a KV backend that does not survive a restart (memkv, diskfs), kid=1
 *     is derived from the node's persisted V1 key instead, so handles still
 *     survive a restart of that node (but are not verifiable on other nodes).
 *   - Verification accepts any ACTIVE or RETIRED kid in the ring and signing
 *     uses the newest ACTIVE one, so a new key can be introduced without
 *     invalidating handles minted under the old one.
 */

enum nfs_fh_keyring_load_state {
    NFS_FH_KR_LOAD_IDLE  = 0,
    NFS_FH_KR_LOAD_READY = 1,
};

struct nfs_fh_keyring_state {
    /* Read by the sign and verify hot paths. */
    struct nfs_fh_keyring ring;
    _Atomic int           load_state;
    /* KV store does not survive a restart: derive kid=1 from the node key. */
    bool                  persistence_disabled;
    uint16_t              node_id;
};

/* Value layout:
 *   magic(LE32=NFS_FH_KEYRING_RECORD_MAGIC) state(1) kid(1)
 *   key_len(1) key_bytes(key_len) created_ns(LE64)
 * Returns bytes written, or 0 if the slot is not persistable or buf is short. */
uint32_t
nfs_fh_keyring_serialize(
    uint8_t                      *buf,
    uint32_t                      buf_size,
    const struct nfs_fh_key_slot *slot);

/* Returns 0 on success, -1 on truncation, unknown magic/state or bad key_len. */
int
nfs_fh_keyring_deserialize(
    const uint8_t          *buf,
    uint32_t                len,
    struct nfs_fh_key_slot *out);

void
nfs_fh_keyring_init(
    struct nfs_fh_keyring_state *kr,
    uint16_t                     node_id,
    bool                         persistence_disabled);

/* Zero the key material. */
void
nfs_fh_keyring_free(
    struct nfs_fh_keyring_state *kr);

/* Fill a fresh 16-byte key from the system CSPRNG; aborts on failure. */
void
nfs_fh_keyring_random_key(
    uint8_t out[16]);

/* Derive kid=1 from the node's persisted V1 key (non-persistent KV store). */
void
nfs_fh_keyring_derive_key(
    const uint8_t node_key[16],
    uint8_t       out[16]);

/* Load the ring from the KV store (or mint the fleet's first key), blocking
 * until it is READY.  Aborts if the store cannot be read: signing with a key
 * peers do not hold would hand out handles that fail after a failover. */
void
nfs_fh_keyring_load(
    struct nfs_fh_keyring_state *kr,
    struct chimera_vfs          *vfs,
    const uint8_t                node_key[16]);

static inline bool
nfs_fh_keyring_ready(const struct nfs_fh_keyring_state *kr)
{
    return atomic_load_explicit(&kr->load_state, memory_order_acquire) ==
           NFS_FH_KR_LOAD_READY;
} /* nfs_fh_keyring_ready */
