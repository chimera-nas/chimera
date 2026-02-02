// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <rocksdb/c.h>

#ifdef ROCKSDB_LEGACY_COMPAT

/*
 * Compatibility shims for older RocksDB versions (< 7.x) that lack
 * rocksdb_transaction_get_pinned and rocksdb_transaction_get_pinned_for_update.
 *
 * These wrappers call the non-pinned equivalents (which return a malloc'd copy)
 * and wrap the result in a rocksdb_pinnableslice_t via rocksdb_get_pinned on a
 * dummy path.  Since we can't construct a pinnableslice directly from the C API,
 * we instead change the calling code to use our own thin wrapper type.
 */

struct cairn_compat_slice {
    char  *data;
    size_t len;
};

static inline struct cairn_compat_slice *
cairn_compat_transaction_get_pinned(
    rocksdb_transaction_t       *txn,
    const rocksdb_readoptions_t *options,
    const char                  *key,
    size_t                       klen,
    char                       **errptr)
{
    struct cairn_compat_slice *s;
    size_t                     vlen;
    char                      *val;

    val = rocksdb_transaction_get(txn, options, key, klen, &vlen, errptr);

    if (!val) {
        return NULL;
    }

    s       = malloc(sizeof(*s));
    s->data = val;
    s->len  = vlen;
    return s;
} // cairn_compat_transaction_get_pinned

static inline struct cairn_compat_slice *
cairn_compat_transaction_get_pinned_for_update(
    rocksdb_transaction_t       *txn,
    const rocksdb_readoptions_t *options,
    const char                  *key,
    size_t                       klen,
    unsigned char                exclusive,
    char                       **errptr)
{
    struct cairn_compat_slice *s;
    size_t                     vlen;
    char                      *val;

    val = rocksdb_transaction_get_for_update(txn, options, key, klen, &vlen,
                                             exclusive, errptr);

    if (!val) {
        return NULL;
    }

    s       = malloc(sizeof(*s));
    s->data = val;
    s->len  = vlen;
    return s;
} // cairn_compat_transaction_get_pinned_for_update

static inline const char *
cairn_compat_slice_value(
    const struct cairn_compat_slice *s,
    size_t                          *vlen)
{
    *vlen = s->len;
    return s->data;
} // cairn_compat_slice_value

static inline void
cairn_compat_slice_destroy(struct cairn_compat_slice *s)
{
    if (s) {
        free(s->data);
        free(s);
    }
} // cairn_compat_slice_destroy

#define rocksdb_transaction_get_pinned(txn, opts, key, klen, errptr) \
        ((rocksdb_pinnableslice_t *) cairn_compat_transaction_get_pinned((txn), (opts), (key), (klen), (errptr)))

#define rocksdb_transaction_get_pinned_for_update(txn, opts, key, klen, excl, errptr) \
        ((rocksdb_pinnableslice_t *) cairn_compat_transaction_get_pinned_for_update((txn), (opts), (key), (klen), (excl) \
                                                                                    , (errptr)))

#define rocksdb_pinnableslice_value(s, vlen) \
        cairn_compat_slice_value((const struct cairn_compat_slice *) (s), (vlen))

#define rocksdb_pinnableslice_destroy(s) \
        cairn_compat_slice_destroy((struct cairn_compat_slice *) (s))

#endif /* ROCKSDB_LEGACY_COMPAT */
