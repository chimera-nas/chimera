// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>
#include <xxhash.h>
#include "vfs_varint.h"
#include "vfs_attrs.h"     /* CHIMERA_VFS_FH_SIZE */

/*
 * FILE HANDLE ROUTING CONTRACT
 *
 * Core routes every handle-addressed operation by looking the handle up in
 * the mount table (chimera_vfs_get_module): the lookup key is the leading
 * CHIMERA_VFS_MOUNT_ID_SIZE bytes, and the mount's entry is registered from
 * the root handle the backend returned out of its mount op.  A backend that
 * does not satisfy the invariant below is unroutable -- core resolves no
 * module for its handles and every operation fails ESTALE/BADHANDLE.
 *
 * A backend must therefore guarantee:
 *
 *   1. Every handle it emits for an object in a mount begins with the same
 *      16 bytes as that mount's root handle.
 *   2. Those 16 bytes are unique across all mounts live in the process --
 *      including mounts served by other backends.  A collision misroutes
 *      operations to the wrong mount.
 *   3. CHIMERA_VFS_MOUNT_ID_SIZE <= va_fh_len <= CHIMERA_VFS_FH_SIZE.
 *
 * Everything after the mount_id is the backend's own fh_fragment: core never
 * interprets it, so its layout, and whether it is stable across restarts, is
 * the backend's business.  (Note that NFS clients expect handles to survive a
 * server restart; a backend that mints volatile fragments will hand out stale
 * handles across one.)
 *
 * The encoders below are the supported way to satisfy 1 and 2:
 * chimera_vfs_encode_fh_mount derives the mount_id as
 * XXH3_128(fsid || fh_fragment), which gets uniqueness from the 16-byte fsid,
 * and chimera_vfs_encode_fh_parent copies a parent's mount_id onto a child.
 * A backend may compute the 16 bytes some other way, but then it owns the
 * uniqueness argument in 2.
 */
#define CHIMERA_VFS_MOUNT_ID_SIZE 16
#define CHIMERA_VFS_FSID_SIZE     16

/*
 * A chimera file handle is [mount_id : 16][fh_fragment : N], and the whole
 * thing has to fit CHIMERA_VFS_FH_SIZE, so this is all the room a backend has
 * for its fragment.
 */
#define CHIMERA_VFS_FH_FRAGMENT_MAX \
        (CHIMERA_VFS_FH_SIZE - CHIMERA_VFS_MOUNT_ID_SIZE)

/*
 * The longest fragment, and hence the longest handle, the *inum-varint* family
 * of backends mints: memfs, diskfs and cairn encode inum then gen as varints,
 * and memfs's named-stream handles (memfs_encode_stream_fh) append a third
 * varint for the stream id, which is the longest fragment that family composes.
 *
 * Scoped to that family on purpose -- it is NOT the maximum over all backends,
 * and the name says INUM so no caller mistakes it for one.  The others compose
 * fragments this does not describe:
 *
 *   smb   1 + sizeof(chimera_smb_client_file_id) = 17  (vfs/smb/smb_internal.h)
 *   linux varint(mount_id) + varint(type) + f_handle, up to 32
 *                                                    (vfs/linux/linux_common.h)
 *   nfs   1 + remote handle, up to 48                 (vfs/nfs/nfs_internal.h)
 *
 * All of them are bounded by CHIMERA_VFS_FH_FRAGMENT_MAX above, which is what
 * keeps a chimera handle a chimera handle.  The narrower question -- which of
 * them are small enough to survive being re-embedded by a chimera NFS *client*
 * proxying this server -- is answered at CHIMERA_NFS_PROXY_REMOTE_FH_MAX in
 * nfs_common/nfs_fh_limits.h, where the family below is the one asserted to fit.
 */
#define CHIMERA_VFS_FH_INUM_FRAGMENT_MAX \
        (CHIMERA_VARINT_UINT64_MAX_BYTES + CHIMERA_VARINT_UINT32_MAX_BYTES)
#define CHIMERA_VFS_FH_STREAM_FRAGMENT_MAX \
        (CHIMERA_VFS_FH_INUM_FRAGMENT_MAX + CHIMERA_VARINT_UINT32_MAX_BYTES)
#define CHIMERA_VFS_FH_INUM_EMITTED_MAX \
        (CHIMERA_VFS_MOUNT_ID_SIZE + CHIMERA_VFS_FH_STREAM_FRAGMENT_MAX)

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
    uint8_t       concat_buf[CHIMERA_VFS_FSID_SIZE + CHIMERA_VFS_FH_SIZE];
    XXH128_hash_t hash;
    uint8_t      *fh = out_fh;

    /* Build concatenation buffer: fsid || fh_fragment */
    memcpy(concat_buf, fsid, CHIMERA_VFS_FSID_SIZE);

    if (fh_fragment_len > 0) {
        memcpy(concat_buf + CHIMERA_VFS_FSID_SIZE, fh_fragment, fh_fragment_len);
    }

    /* Compute 128-bit hash to get mount_id */
    hash = XXH3_128bits(concat_buf, CHIMERA_VFS_FSID_SIZE + fh_fragment_len);

    /* Write mount_id (XXH128 hash) as first 16 bytes */
    memcpy(fh, &hash, CHIMERA_VFS_MOUNT_ID_SIZE);

    /* Append fh_fragment */
    if (fh_fragment_len > 0) {
        memcpy(fh + CHIMERA_VFS_MOUNT_ID_SIZE, fh_fragment, fh_fragment_len);
    }

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
 * inum+gen as their file handle fragment (memfs, diskfs, cairn).
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
    uint8_t  fragment[CHIMERA_VFS_FH_INUM_FRAGMENT_MAX];
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

    (void) fhlen;

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
    (void) fhlen;

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

