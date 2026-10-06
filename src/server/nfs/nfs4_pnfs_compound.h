// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef CHIMERA_NFS4_PNFS_COMPOUND_H
#define CHIMERA_NFS4_PNFS_COMPOUND_H

#include "nfs4_state.h"
#include "vfs/vfs_compound.h"

#define LAYOUT4_FLEX_FILES   0x4         /* RFC 8435; not in the generated XDR */
#define LAYOUT4_BLOCK_VOLUME 0x3         /* RFC 5663; not in the generated XDR */
#define LAYOUT4_SCSI         0x5         /* RFC 8154; not in the generated XDR */

struct nfs_request;
struct nfs4_layoutget;

/* Appends the complete layout path to the caller's compound. All conditional
 * branches are described before submission; callbacks only choose/bind them.
 * The shared encoder owns authorization, identity and journal lifetime. */
struct nfs4_layoutget * nfs4_layoutget_alloc(
    struct nfs_request        *req,
    uint32_t                   wire_index,
    struct nfs_layout_journal *journal,
    nfsstat4                  *status);
void nfs4_layoutget_free(
    struct nfs4_layoutget *ctx);
int nfs4_layoutget_append(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx);
nfsstat4 nfs4_layoutget_admit(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx,
    struct nfs_client           *client,
    uint16_t                     export_id,
    const struct stateid4       *input,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    uint32_t                     own_barriers);
void nfs4_layoutget_reset(
    struct nfs4_layoutget *ctx);
const struct LAYOUTGET4res * nfs4_layoutget_result(
    struct nfs4_layoutget *ctx);
bool nfs4_layoutget_device(
    struct nfs4_layoutget            *ctx,
    struct chimera_vfs_compound      *compound,
    const uint8_t                     deviceid[16],
    struct chimera_vfs_layout_device *out);
void nfs4_layoutget_publish(
    struct chimera_vfs_compound *compound,
    struct nfs4_layoutget       *ctx);

#endif // ifndef CHIMERA_NFS4_PNFS_COMPOUND_H
