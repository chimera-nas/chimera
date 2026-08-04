// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs_attrs.h"
#include "vfs/vfs_fh.h"

/*
 * How large an upstream file handle the NFS proxy client can re-encode.
 *
 * The nfs VFS module is an NFS *client*: it dials an upstream server and
 * re-encodes the handles that server hands back (GETFH, LOOKUP, MOUNT) into
 * chimera handles, as
 *
 *   [ mount_id : 16 ][ server_index : 1 ][ remote_fh : N ]
 *
 * so N is bounded by what is left of CHIMERA_VFS_FH_SIZE once the mount_id and
 * the server index are paid for.  Nothing bounds N on the wire: nfs3.x and
 * nfs4.x declare nfs_fh3<NFS3_FHSIZE> and nfs_fh4<NFS4_FHSIZE>, but the
 * generated decoders ignore the declared bound, so N is an arbitrary uint32_t
 * chosen by the peer.  Every site that re-encodes one must therefore check it
 * itself:
 *
 *   chimera_nfs3_unmarshall_fh        (nfs_common/nfs3_attr.h)
 *   chimera_nfs4_unmarshall_fh        (vfs/nfs/nfs_internal.h)
 *   chimera_nfs4_readdir_parse_attrs  (vfs/nfs/nfs4_readdir_attr.h)
 *
 * and so must the two mount paths that open-code the same fragment build
 * (vfs/nfs/nfs3_mount.c, vfs/nfs/nfs4_mount.c).  The three above are covered by
 * vfs/nfs/tests/nfs_fh_bounds_test.c; the mount paths need a live upstream, so
 * they are covered by the pynfs and kvm proxy suites instead.
 *
 * NFSv3 permits 64-byte handles and NFSv4 permits 128, both above this ceiling,
 * so a general upstream can legitimately hand us a handle we cannot represent.
 * Such a server is refused (EOVERFLOW, or EIO at mount) rather than proxied;
 * embedding it would need a handle indirection table instead of the in-place
 * re-encoding done here.
 */
#define CHIMERA_NFS_PROXY_FH_SERVER_IDX_SIZE 1

/*
 * Margin against a chimera upstream.  Proxying one chimera through another is a
 * shipped, CI-exercised topology (kvm/kvm_pnfs_proxy_test_wrapper.sh), and it
 * works because a share on an inum-varint backend -- memfs, diskfs or cairn,
 * which is what that wrapper mounts -- fits the ceiling defined just below:
 *
 *   mount_id                                      16
 *   inum + gen varints                            15   (CHIMERA_VFS_FH_INUM_EMITTED_MAX
 *   stream id varint                               5     covers all three)
 *   wire wrap: tag + export_id                     3   (CHIMERA_NFS_FH_HDR)
 *   SipHash MAC, when fh_sign is enabled           8   (CHIMERA_NFS_FH_MAC)
 *                                                 --
 *                                                  47  == the ceiling, exactly
 *
 * An inum+gen handle (no stream id) comes to 42 and leaves 5 bytes spare; a
 * named-stream handle consumes the margin exactly.  Because the fit is exact,
 * any future change to handle composition -- a wider mount_id, a fourth varint
 * in a fragment, a longer wire header or MAC -- would turn chimera-to-chimera
 * proxying from "fits" into "rejected at the ceiling", and before the
 * re-encoders named above started checking, into memory corruption.  The
 * _Static_assert beside CHIMERA_NFS_FH_HDR in server/nfs/nfs_fh_wrap.h makes
 * that a build failure instead.  It lives there because that is where the wire
 * wrap is defined; this is the client side of the same invariant.
 *
 * That sum is the inum-varint family only, and deliberately so: an upstream
 * chimera serving a linux-backed (59) or nfs-backed (75) share exceeds the
 * ceiling and is refused here rather than proxied, exactly like any other
 * upstream whose handles are too large.  smb fits with three bytes spare.  The
 * per-backend numbers and why only one family is asserted are at the
 * _Static_assert itself.
 */
#define CHIMERA_NFS_PROXY_REMOTE_FH_MAX \
        (CHIMERA_VFS_FH_FRAGMENT_MAX - CHIMERA_NFS_PROXY_FH_SERVER_IDX_SIZE)
