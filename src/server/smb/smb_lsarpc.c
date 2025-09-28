// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_lsarpc.h"
#include "smb_dcerpc.h"
#include <complex.h>
#include <time.h>

static const dce_if_uuid_t LSA_INTERFACE = {
    .if_uuid       = { 0x78, 0x57, 0x34, 0x12, 0x34, 0x12, 0xCD, 0xAB, 0xEF, 0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab },
    .if_vers_major = 0,
    .if_vers_minor = 0,
};

#define LSA_OP_CLOSE        0
#define LSA_OP_LOOKUPNAMES  14
#define LSA_OP_OPENPOLICY2  44
#define LSA_OP_GETUSERNAME  45

#define LSA_REFID_USERNAME  0x00020000
#define LSA_REFID_AUTHORITY 0x00020010

static int
chimera_smb_lsarpc_handler(
    int                       opnum,
    struct evpl_iovec_cursor *cursor,
    void                     *output,
    void                     *private_data)
{
    struct chimera_smb_request   *request = private_data;
    struct chimera_smb_iconv_ctx *ctx     = &request->compound->thread->iconv_ctx;
    void                         *outputp = output;

    switch (opnum) {
        case LSA_OP_CLOSE:
            /* dummy context flags */
            memset(outputp, 0, 4);
            outputp += 4;

            /* dummy context uuid */
            memset(outputp, 0xaa, 16);
            outputp += 16;

            *(uint32_t *) outputp = 0; // STATUS_SUCCESS
            outputp              += 4;

            return outputp - output;

        case LSA_OP_LOOKUPNAMES:
            outputp += dce_append_ref_id(outputp, 0x00020000);

            *(uint32_t *) outputp = 1;
            outputp              += 4;

            outputp += dce_append_ref_id(outputp, 0x00020010);

            *(uint32_t *) outputp = 1;
            outputp              += 4;

            *(uint32_t *) outputp = 1;
            outputp              += 4;

            outputp += dce_append_string_array(ctx, outputp, 0x00030000, 0x00030010, "WORKGROUP");

            *(uint32_t *) outputp = 1;
            outputp              += 4;

            /* SID */
            *(uint8_t *) outputp = 1; /* Revision */
            outputp             += 1;

            *(uint8_t *) outputp = 4; /* SubAuthorityCount */
            outputp             += 1;

            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x05; /* IdentifierAuthority */
            outputp             += 1;

            *(uint32_t *) outputp = 21; /* SubAuthority */
            outputp              += 4;
            *(uint32_t *) outputp = 1111; /* SubAuthority */
            outputp              += 4;
            *(uint32_t *) outputp = 2222; /* SubAuthority */
            outputp              += 4;
            *(uint32_t *) outputp = 3333; /* SubAuthority */
            outputp              += 4;

            *(uint32_t *) outputp = 1; /* Entries */
            outputp              += 4;

            outputp += dce_append_ref_id(outputp, 0x00040000);

            *(uint32_t *) outputp = 1; /* MaxCount */
            outputp              += 4;

            *(uint32_t *) outputp = 1; /* SidTypeUser */
            outputp              += 4;

            *(uint32_t *) outputp = 1001; /* Ref Id */
            outputp              += 4;

            *(uint32_t *) outputp = 0; /* Domain Index */
            outputp              += 4;

            #if 0
            /* SID */
            *(uint8_t *) outputp = 1; /* Revision */
            outputp             += 1;

            *(uint8_t *) outputp = 5; /* SubAuthorityCount */
            outputp             += 1;

            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x00; /* IdentifierAuthority */
            outputp             += 1;
            *(uint8_t *) outputp = 0x05; /* IdentifierAuthority */
            outputp             += 1;

            *(uint32_t *) outputp = 21; /* SubAuthority */
            outputp              += 4;
            *(uint32_t *) outputp = 1111; /* SubAuthority */
            outputp              += 4;
            *(uint32_t *) outputp = 2222; /* SubAuthority */
            outputp              += 4;
            *(uint32_t *) outputp = 3333; /* SubAuthority */
            outputp              += 4;
            *(uint32_t *) outputp = 1001; /* SubAuthority */
            outputp              += 4;

            #endif /* if 0 */

            *(uint32_t *) outputp = 1; // Count
            outputp              += 4;

            *(uint32_t *) outputp = 0; // STATUS_SUCCESS
            outputp              += 4;

            return outputp - output;

        case LSA_OP_OPENPOLICY2:
            /* dummy context flags */
            memset(outputp, 0, 4);
            outputp += 4;

            /* dummy context uuid */
            memset(outputp, 0xaa, 16);
            outputp += 16;

            *(uint32_t *) outputp = 0; // STATUS_SUCCESS
            outputp              += 4;


            return outputp - output;

        case LSA_OP_GETUSERNAME:
            outputp += dce_append_ref_id(outputp, LSA_REFID_USERNAME);
            outputp += dce_append_string(ctx, outputp, LSA_REFID_USERNAME, "myuser");
            outputp += dce_append_ref_id(outputp, LSA_REFID_AUTHORITY);
            outputp += dce_append_ref_id(outputp, LSA_REFID_AUTHORITY);
            outputp += dce_append_string(ctx, outputp, LSA_REFID_AUTHORITY, "WORKGROUP");

            *(uint32_t *) outputp = 0; // STATUS_SUCCESS
            outputp              += 4;

            return outputp - output;
        default:
            return -1;
    } /* switch */
} /* chimera_smb_lsarpc_handler */

int
chimera_smb_lsarpc_transceive(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov)
{
    int status;

    status = dce_rpc(&LSA_INTERFACE, input_iov, input_niov, output_iov, chimera_smb_lsarpc_handler, request);

    if (status != 0) {
        chimera_smb_error("LSA RPC transceive failed");
        return status;
    }

    return 0;



} /* chimera_smb_lsarpc_transceive */
