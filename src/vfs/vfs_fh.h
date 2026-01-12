// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>
#include <xxhash.h>
#include "common/varint.h"

#define CHIMERA_VFS_MOUNT_ID_SIZE 16
#define CHIMERA_VFS_FSID_SIZE     16

/*
 * Encode a file handle for a mount root or cross-mount reference.
 *
 * This function computes the mount_id by hashing the concatenation of
 * the FSID and the fh_fragment, then constructs the file handle from
 * the 16-byte mount_id followed by the fh_fragment.
 *
 * Use this function when:
 * - Generating a file handle for the root of a mount (no parent available)
 * - In vfs_root where parent may be from a different FSID
 *
 * @param fsid          16-byte filesystem identifier
 * @param fh_fragment   Backend-specific portion of the file handle
 * @param fh_fragment_len Length of fh_fragment
 * @param out_fh        Output buffer (must be at least 16 + fh_fragment_len bytes)
 * @return              Total file handle length (16 + fh_fragment_len)
 */
static inline uint32_t
chimera_vfs_encode_fh_mount(
    const void *fsid,
    const void *fh_fragment,
    int         fh_fragment_len,
    void       *out_fh)
{
    uint8_t       concat_buf[CHIMERA_VFS_FSID_SIZE + 32];
    XXH128_hash_t hash;
    uint8_t      *fh = out_fh;

    /* Build concatenation buffer: fsid || fh_fragment */
    memcpy(concat_buf, fsid, CHIMERA_VFS_FSID_SIZE);
    memcpy(concat_buf + CHIMERA_VFS_FSID_SIZE, fh_fragment, fh_fragment_len);

    /* Compute 128-bit hash to get mount_id */
    hash = XXH3_128bits(concat_buf, CHIMERA_VFS_FSID_SIZE + fh_fragment_len);

    /* Write mount_id (XXH128 hash) as first 16 bytes */
    memcpy(fh, &hash, CHIMERA_VFS_MOUNT_ID_SIZE);

    /* Append fh_fragment */
    memcpy(fh + CHIMERA_VFS_MOUNT_ID_SIZE, fh_fragment, fh_fragment_len);

    return CHIMERA_VFS_MOUNT_ID_SIZE + fh_fragment_len;
} /* chimera_vfs_encode_fh_mount */

/*
 * Encode a file handle using the mount_id from a parent file handle.
 *
 * This function copies the mount_id from the parent file handle and
 * appends the new fh_fragment. Use this function when generating
 * file handles for children where the parent is known.
 *
 * @param parent_fh     Parent file handle (must start with 16-byte mount_id)
 * @param fh_fragment   Backend-specific portion of the new file handle
 * @param fh_fragment_len Length of fh_fragment
 * @param out_fh        Output buffer (must be at least 16 + fh_fragment_len bytes)
 * @return              Total file handle length (16 + fh_fragment_len)
 */
static inline uint32_t
chimera_vfs_encode_fh_parent(
    const void *parent_fh,
    const void *fh_fragment,
    int         fh_fragment_len,
    void       *out_fh)
{
    uint8_t *fh = out_fh;

    /* Copy mount_id from parent */
    memcpy(fh, parent_fh, CHIMERA_VFS_MOUNT_ID_SIZE);

    /* Append fh_fragment */
    memcpy(fh + CHIMERA_VFS_MOUNT_ID_SIZE, fh_fragment, fh_fragment_len);

    return CHIMERA_VFS_MOUNT_ID_SIZE + fh_fragment_len;
} /* chimera_vfs_encode_fh_parent */

/*
 * Encode a file handle for a mount root using inum+gen as the fragment.
 *
 * Convenience function that varint-encodes inum and gen, then calls
 * chimera_vfs_encode_fh_mount. This is useful for backends that use
 * inum+gen as their file handle fragment (memfs, demofs, cairn).
 *
 * @param fsid      16-byte filesystem identifier
 * @param inum      Inode number
 * @param gen       Generation number
 * @param out_fh    Output buffer (must be at least 16 + 15 bytes)
 * @return          Total file handle length
 */
static inline uint32_t
chimera_vfs_encode_fh_inum_mount(
    const void *fsid,
    uint64_t    inum,
    uint32_t    gen,
    void       *out_fh)
{
    uint8_t  fragment[15];  /* Max: 10 bytes for uint64 + 5 bytes for uint32 */
    uint8_t *ptr = fragment;

    ptr += chimera_encode_uint64(inum, ptr);
    ptr += chimera_encode_uint32(gen, ptr);

    return chimera_vfs_encode_fh_mount(fsid, fragment, ptr - fragment, out_fh);
} /* chimera_vfs_encode_fh_inum_mount */

/*
 * Encode a file handle using parent's mount_id and inum+gen as the fragment.
 *
 * Convenience function that varint-encodes inum and gen directly into
 * the output buffer after the mount_id. This avoids an intermediate
 * buffer and memcpy for backends that use inum+gen as their file handle
 * fragment.
 *
 * @param parent_fh Parent file handle (must start with 16-byte mount_id)
 * @param inum      Inode number
 * @param gen       Generation number
 * @param out_fh    Output buffer (must be at least 16 + 15 bytes)
 * @return          Total file handle length
 */
static inline uint32_t
chimera_vfs_encode_fh_inum_parent(
    const void *parent_fh,
    uint64_t    inum,
    uint32_t    gen,
    void       *out_fh)
{
    uint8_t *fh  = out_fh;
    uint8_t *ptr = fh + CHIMERA_VFS_MOUNT_ID_SIZE;

    /* Copy mount_id from parent */
    memcpy(fh, parent_fh, CHIMERA_VFS_MOUNT_ID_SIZE);

    /* Encode inum and gen directly into output buffer */
    ptr += chimera_encode_uint64(inum, ptr);
    ptr += chimera_encode_uint32(gen, ptr);

    return ptr - fh;
} /* chimera_vfs_encode_fh_inum_parent */

/*
 * Decode inum and gen from a file handle that uses inum+gen format.
 *
 * This function skips the 16-byte mount_id prefix and decodes the
 * varint-encoded inum and gen values.
 *
 * @param fh        File handle buffer
 * @param fhlen     Length of file handle
 * @param inum      Output: inode number
 * @param gen       Output: generation number
 */
static inline void
chimera_vfs_decode_fh_inum(
    const void *fh,
    int         fhlen,
    uint64_t   *inum,
    uint32_t   *gen)
{
    const uint8_t *ptr = (const uint8_t *) fh + CHIMERA_VFS_MOUNT_ID_SIZE;

    ptr += chimera_decode_uint64(ptr, inum);
    chimera_decode_uint32(ptr, gen);
} /* chimera_vfs_decode_fh_inum */

/*
 * Get the fh_fragment (backend-specific portion) from a file handle.
 *
 * @param fh        File handle buffer
 * @param fhlen     Total file handle length
 * @return          Pointer to fh_fragment (starts after 16-byte mount_id)
 */
static inline const void *
chimera_vfs_fh_fragment(
    const void *fh,
    int         fhlen)
{
    return (const uint8_t *) fh + CHIMERA_VFS_MOUNT_ID_SIZE;
} /* chimera_vfs_fh_fragment */

/*
 * Get the length of the fh_fragment from a file handle.
 *
 * @param fhlen     Total file handle length
 * @return          Length of fh_fragment
 */
static inline int
chimera_vfs_fh_fragment_len(int fhlen)
{
    return fhlen - CHIMERA_VFS_MOUNT_ID_SIZE;
} /* chimera_vfs_fh_fragment_len */

/*
 * Get the mount_id from a file handle.
 *
 * @param fh        File handle buffer
 * @return          Pointer to 16-byte mount_id
 */
static inline const void *
chimera_vfs_fh_mount_id(const void *fh)
{
    return fh;
} /* chimera_vfs_fh_mount_id */

