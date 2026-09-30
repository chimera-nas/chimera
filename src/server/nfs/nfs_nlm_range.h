// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

/*
 * NLM byte-range length conventions.
 *
 * Three conventions meet in the NLM server and every to-EOF length crosses
 * at least one boundary between them:
 *
 *   wire   nlm4_lock.l_len, as clients send it and as the server sends it
 *          back in TEST holders and NLM_GRANTED callbacks.  0 means to end
 *          of file (RFC 1813 Appendix II, the only spelling the RFC
 *          defines).  Linux lockd also tolerates 0xffffffffffffffff on
 *          input (its decoder maps a negative end to EOF), and so does
 *          this server, on INPUT only.  On OUTPUT the server always uses
 *          0: Linux lockd decodes a received range as end = offset + len
 *          - 1 and only turns len == 0 (or a negative end) into "to end of
 *          file", so 0xffffffffffffffff from a nonzero offset wraps to
 *          offset - 2 and no longer describes the lock the client is
 *          waiting for.  Linux knfsd sends 0 for the same reason
 *          (fs/lockd/xdr4.c, svcxdr_encode_holder).
 *   posix  nlm_lock_entry.length, what UNLOCK and CANCEL matching compare:
 *          0 means to end of file.
 *   vfs    chimera_vfs_claim.length: UINT64_MAX means to end of file and 0
 *          is a real zero-byte range.
 */

/* Wire -> stored: both wire spellings of to-EOF become POSIX 0. */
static inline uint64_t
nlm_wire_len_to_posix(uint64_t wire_len)
{
    return wire_len == UINT64_MAX ? 0 : wire_len;
} /* nlm_wire_len_to_posix */

/* Stored -> claim core: POSIX 0 becomes the VFS to-EOF sentinel. */
static inline uint64_t
nlm_posix_len_to_vfs(uint64_t posix_len)
{
    return posix_len == 0 ? UINT64_MAX : posix_len;
} /* nlm_posix_len_to_vfs */

/* Stored -> wire: the conventions coincide (0 is to-EOF in both), spelled
 * out so the GRANTED callback names the boundary it crosses. */
static inline uint64_t
nlm_posix_len_to_wire(uint64_t posix_len)
{
    return posix_len;
} /* nlm_posix_len_to_wire */

/* Claim core -> wire: the VFS sentinel becomes wire 0. */
static inline uint64_t
nlm_vfs_len_to_wire(uint64_t vfs_len)
{
    return vfs_len == UINT64_MAX ? 0 : vfs_len;
} /* nlm_vfs_len_to_wire */
