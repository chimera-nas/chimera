// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "common/platform.h"
#include "evpl/evpl.h"
#include "nfs_fh_keyring.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_kv_keys.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

/* Record magic ("KRF1" = KeyRing Format 1, LE32 first word). */
#define NFS_FH_KEYRING_RECORD_MAGIC 0x3146524Bu

/* Fixed-layout portion: magic(4) state(1) kid(1) key_len(1) + variable
 * key(key_len) + created_ns(8). */
#define NFS_FH_KEYRING_HDR_LEN      7u
#define NFS_FH_KEYRING_KEYLEN_MAX   32u /* room for a future 32-byte primitive */
#define NFS_FH_KEYRING_VALUE_MAX    (NFS_FH_KEYRING_HDR_LEN + NFS_FH_KEYRING_KEYLEN_MAX + 8u)

uint32_t
nfs_fh_keyring_serialize(
    uint8_t                      *buf,
    uint32_t                      buf_size,
    const struct nfs_fh_key_slot *slot)
{
    uint32_t p = 0;

    /* Never persist an UNUSED slot, the kid=0 sentinel, or a kid no V2 tag
     * can carry. */
    if (!slot || slot->kid == 0 || slot->kid > CHIMERA_NFS_FH_V2_KID_MAX ||
        (slot->state != NFS_FH_KEY_ACTIVE && slot->state != NFS_FH_KEY_RETIRED)) {
        return 0;
    }
    if (buf_size < NFS_FH_KEYRING_HDR_LEN + 16u + 8u) {
        return 0;
    }

    nfs_kv_put_le32(buf, &p, NFS_FH_KEYRING_RECORD_MAGIC);
    buf[p++] = slot->state;
    buf[p++] = slot->kid;
    buf[p++] = 16u;                       /* key_len -- SipHash-128 */
    memcpy(buf + p, slot->key, 16);
    p += 16;
    nfs_kv_put_le64(buf, &p, slot->created_ns);
    return p;
} /* nfs_fh_keyring_serialize */

int
nfs_fh_keyring_deserialize(
    const uint8_t          *buf,
    uint32_t                len,
    struct nfs_fh_key_slot *out)
{
    uint32_t p = 0;
    uint8_t  state;
    uint8_t  kid;
    uint8_t  key_len;

    if (len < NFS_FH_KEYRING_HDR_LEN + 8u) {
        return -1;
    }
    if (nfs_kv_le32(buf) != NFS_FH_KEYRING_RECORD_MAGIC) {
        return -1;
    }
    p      += 4;
    state   = buf[p++];
    kid     = buf[p++];
    key_len = buf[p++];

    if (kid == 0 || kid > CHIMERA_NFS_FH_V2_KID_MAX) {
        return -1;
    }
    /* An UNUSED or unknown state must not hydrate: it would widen the set of
     * keys a forged handle is checked against. */
    if (state != NFS_FH_KEY_ACTIVE && state != NFS_FH_KEY_RETIRED) {
        return -1;
    }
    if (key_len != 16u) {
        return -1;
    }
    if (p + key_len + 8u > len) {
        return -1;
    }

    out->kid   = kid;
    out->state = state;
    memcpy(out->key, buf + p, 16);
    p              += 16;
    out->created_ns = nfs_kv_le64(buf + p);
    return 0;
} /* nfs_fh_keyring_deserialize */

void
nfs_fh_keyring_init(
    struct nfs_fh_keyring_state *kr,
    uint16_t                     node_id,
    bool                         persistence_disabled)
{
    memset(kr, 0, sizeof(*kr));
    atomic_store(&kr->load_state, NFS_FH_KR_LOAD_IDLE);
    kr->node_id              = node_id;
    kr->persistence_disabled = persistence_disabled;
} /* nfs_fh_keyring_init */

void
nfs_fh_keyring_free(struct nfs_fh_keyring_state *kr)
{
    volatile uint8_t *p = (volatile uint8_t *) kr->ring.slots;
    size_t            n = sizeof(kr->ring.slots);

    while (n--) {
        *p++ = 0;
    }
} /* nfs_fh_keyring_free */

void
nfs_fh_keyring_random_key(uint8_t out[16])
{
    if (chimera_getrandom(out, 16) != 0) {
        chimera_nfs_abort("NFS FH keyring: cannot read random bytes for a new key");
    }
} /* nfs_fh_keyring_random_key */

void
nfs_fh_keyring_derive_key(
    const uint8_t node_key[16],
    uint8_t       out[16])
{
    static const char lo_label[] = "chimera-fh-ring-kid1-lo";
    static const char hi_label[] = "chimera-fh-ring-kid1-hi";
    uint64_t          lo         = chimera_nfs_siphash(node_key, lo_label, sizeof(lo_label) - 1);
    uint64_t          hi         = chimera_nfs_siphash(node_key, hi_label, sizeof(hi_label) - 1);

    memcpy(out, &lo, sizeof(lo));
    memcpy(out + sizeof(lo), &hi, sizeof(hi));
} /* nfs_fh_keyring_derive_key */

static uint64_t
nfs_fh_keyring_now_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
} /* nfs_fh_keyring_now_ns */

/* Install a set of slots, sign with the newest ACTIVE kid, and publish READY. */
static void
nfs_fh_keyring_finalize(
    struct nfs_fh_keyring_state  *kr,
    const struct nfs_fh_key_slot *slots,
    int                           n_slots)
{
    uint8_t  signing_kid      = 0;
    uint64_t newest_active_ns = 0;
    int      i;

    memset(kr->ring.slots, 0, sizeof(kr->ring.slots));

    for (i = 0; i < n_slots && i < NFS_FH_KEYRING_MAX_SLOTS; i++) {
        kr->ring.slots[i] = slots[i];
        if (slots[i].state == NFS_FH_KEY_ACTIVE &&
            slots[i].created_ns >= newest_active_ns) {
            newest_active_ns = slots[i].created_ns;
            signing_kid      = slots[i].kid;
        }
    }
    kr->ring.signing_kid = signing_kid;

    if (signing_kid == 0) {
        chimera_nfs_abort(
            "NFS FH keyring: loaded %d key(s) but none ACTIVE; a new ACTIVE key "
            "must be added before this server can serve NFS", n_slots);
    }

    atomic_store_explicit(&kr->load_state, NFS_FH_KR_LOAD_READY,
                          memory_order_release);

    chimera_nfs_info("NFS FH keyring: ready with %d key(s), signing kid=%u",
                     n_slots, signing_kid);
} /* nfs_fh_keyring_finalize */

struct nfs_fh_keyring_load_ctx {
    struct nfs_fh_keyring_state *kr;
    struct chimera_vfs_thread   *vfs_thread;
    struct nfs_fh_key_slot       found_slots[NFS_FH_KEYRING_MAX_SLOTS];
    int                          found_count;
    struct nfs_fh_key_slot       local_slot;
    uint8_t                      key[CHIMERA_KV_FH_SIGNING_KEY_LEN];
    uint32_t                     key_len;
    uint8_t                      value[NFS_FH_KEYRING_VALUE_MAX];
    uint32_t                     value_len;
};

static void
nfs_fh_keyring_mint_adopt_cb(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs_fh_keyring_load_ctx *ctx = private_data;
    struct nfs_fh_key_slot          stored;

    if (error_code != CHIMERA_VFS_OK || !value ||
        nfs_fh_keyring_deserialize(value, value_len, &stored) != 0 ||
        stored.kid != ctx->local_slot.kid) {
        chimera_nfs_abort(
            "NFS FH keyring: kid=1 already exists but could not be read back "
            "(%d); refusing to sign with a key peers may not hold",
            (int) error_code);
    }

    chimera_nfs_info("NFS FH keyring: kid=1 already minted, adopting the stored key");
    nfs_fh_keyring_finalize(ctx->kr, &stored, 1);
} /* nfs_fh_keyring_mint_adopt_cb */

static void
nfs_fh_keyring_mint_put_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_fh_keyring_load_ctx *ctx = private_data;

    if (error_code == CHIMERA_VFS_EEXIST) {
        chimera_vfs_get_key(ctx->vfs_thread, ctx->key, ctx->key_len,
                            nfs_fh_keyring_mint_adopt_cb, ctx);
        return;
    }
    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_abort("NFS FH keyring: storing the first key failed (%d)",
                          (int) error_code);
    }

    chimera_nfs_info("NFS FH keyring: minted first kid=1");
    nfs_fh_keyring_finalize(ctx->kr, &ctx->local_slot, 1);
} /* nfs_fh_keyring_mint_put_cb */

/* Every node that finds an empty ring stores a candidate kid=1 with
 * put-if-absent; exactly one is stored and the others adopt it. */
static void
nfs_fh_keyring_mint_first(struct nfs_fh_keyring_load_ctx *ctx)
{
    memset(&ctx->local_slot, 0, sizeof(ctx->local_slot));
    ctx->local_slot.kid        = 1;
    ctx->local_slot.state      = NFS_FH_KEY_ACTIVE;
    ctx->local_slot.created_ns = nfs_fh_keyring_now_ns();
    nfs_fh_keyring_random_key(ctx->local_slot.key);

    ctx->key_len   = nfs_kv_fh_signing_key(ctx->key, ctx->local_slot.kid);
    ctx->value_len = nfs_fh_keyring_serialize(ctx->value, sizeof(ctx->value),
                                              &ctx->local_slot);

    chimera_vfs_put_key_if_absent(ctx->vfs_thread, ctx->key, ctx->key_len,
                                  ctx->value, ctx->value_len,
                                  nfs_fh_keyring_mint_put_cb, ctx);
} /* nfs_fh_keyring_mint_first */

static int
nfs_fh_keyring_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs_fh_keyring_load_ctx *ctx = private_data;
    uint8_t                         prefix[CHIMERA_KV_FH_SIGNING_KEY_LEN];
    struct nfs_fh_key_slot          s;
    int                             i;

    nfs_kv_fh_signing_key(prefix, 0);
    if (key_len < CHIMERA_KV_FH_SIGNING_PREFIX_LEN ||
        memcmp(key, prefix, CHIMERA_KV_FH_SIGNING_PREFIX_LEN) != 0) {
        return 1;  /* past the ring band */
    }

    if (ctx->found_count >= NFS_FH_KEYRING_MAX_SLOTS) {
        chimera_nfs_info("NFS FH keyring: ignoring keys beyond the first %d",
                         NFS_FH_KEYRING_MAX_SLOTS);
        return 1;
    }
    if (nfs_fh_keyring_deserialize(value, value_len, &s) != 0) {
        return 0;  /* skip a malformed entry */
    }
    for (i = 0; i < ctx->found_count; i++) {
        if (ctx->found_slots[i].kid == s.kid) {
            return 0;
        }
    }
    ctx->found_slots[ctx->found_count++] = s;
    return 0;
} /* nfs_fh_keyring_scan_cb */

static void
nfs_fh_keyring_scan_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_fh_keyring_load_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_ENOENT) {
        chimera_nfs_abort("NFS FH keyring: KV scan failed (%d); refusing to serve "
                          "NFS with keys peers may not hold", (int) error_code);
    }

    if (ctx->found_count == 0) {
        nfs_fh_keyring_mint_first(ctx);
    } else {
        nfs_fh_keyring_finalize(ctx->kr, ctx->found_slots, ctx->found_count);
    }
} /* nfs_fh_keyring_scan_complete */

void
nfs_fh_keyring_load(
    struct nfs_fh_keyring_state *kr,
    struct chimera_vfs          *vfs,
    const uint8_t                node_key[16])
{
    struct nfs_fh_keyring_load_ctx *ctx;
    struct evpl                    *evpl;

    if (nfs_fh_keyring_ready(kr)) {
        return;
    }

    /* diskfs keeps its KV entries in memory only, like memkv. */
    if (kr->persistence_disabled || !vfs ||
        strcmp(vfs->kv_module->name, "diskfs") == 0) {
        struct nfs_fh_key_slot slot;

        /* Nothing to share with peers; derive kid=1 from the node's persisted
         * key so the ring is the same after a restart. */
        memset(&slot, 0, sizeof(slot));
        slot.kid   = 1;
        slot.state = NFS_FH_KEY_ACTIVE;
        nfs_fh_keyring_derive_key(node_key, slot.key);
        nfs_fh_keyring_finalize(kr, &slot, 1);
        return;
    }

    chimera_nfs_info("NFS FH keyring: loading from KV backend '%s'",
                     vfs->kv_module->name);

    evpl = evpl_create(NULL);
    ctx  = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(ctx == NULL,
                         "NFS FH keyring: failed to allocate load context");
    ctx->kr         = kr;
    ctx->vfs_thread = chimera_vfs_thread_init(evpl, vfs);

    nfs_kv_fh_signing_key(ctx->key, 0);
    chimera_vfs_search_keys(ctx->vfs_thread, ctx->key,
                            CHIMERA_KV_FH_SIGNING_PREFIX_LEN,
                            NULL, 0, 0,
                            nfs_fh_keyring_scan_cb,
                            nfs_fh_keyring_scan_complete,
                            ctx);

    /* Same drive loop as nfs_recovery_cold_start: the KV operations complete
     * on this private loop. */
    while (!nfs_fh_keyring_ready(kr)) {
        evpl_continue(evpl);
    }

    chimera_vfs_thread_destroy(ctx->vfs_thread);
    evpl_destroy(evpl);
    free(ctx);
} /* nfs_fh_keyring_load */
