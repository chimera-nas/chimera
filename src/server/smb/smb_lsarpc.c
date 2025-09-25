// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_lsarpc.h"
#include "smb_dcerpc.h"

static const dce_if_uuid_t LSA_INTERFACE = {
    .if_uuid       = { 0x01, 0x00, 0x78, 0x57, 0x34, 0x12, 0x34, 0x12, 0xCD, 0xAB, 0xEF, 0x00, 0x01, 0x23, 0x45, 0x67 },
    .if_vers_major = 0,
    .if_vers_minor = 0,
};

int
chimera_smb_lsarpc_transceive(
    struct evpl_iovec *input_iov,
    int                input_niov,
    struct evpl_iovec *output_iov)
{
    int status;

    status = dce_rpc(&LSA_INTERFACE, input_iov, input_niov, output_iov);

    if (status != 0) {
        chimera_smb_error("LSA RPC transceive failed");
        return status;
    }

    return 0;



} /* chimera_smb_lsarpc_transceive */
