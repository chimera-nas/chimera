// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "smb_internal.h"

/*
 * One request PDU through a named pipe's transceive, shared by the two SMB2
 * transports: FSCTL_PIPE_TRANSCEIVE (smb_proc_ioctl.c) and the WRITE half of
 * WRITE/READ (smb_proc_write.c).  Most transceives answer inline; one that
 * must wait (an LSARPC lookup resolving names) parks the call and finishes it
 * later, on the same thread, through chimera_smb_pipe_call_finish.
 */

/* A pipe_transceive that parked returns this; it then owns finishing the call. */
#define CHIMERA_SMB_PIPE_ASYNC 1

/* Run one request PDU through open_file->pipe_transceive.  Takes the
 * open_file reference and the input iovecs; `done` runs exactly once, inline
 * or later on this thread, and must release request->pipe.open_file and
 * complete the request. */
void
chimera_smb_pipe_call(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file,
    struct evpl_iovec            *input_iov,
    int                           input_niov,
    struct evpl_iovec            *output_iov,
    chimera_smb_pipe_done_t       done);

/* Finish a call whose transceive returned CHIMERA_SMB_PIPE_ASYNC: release the
 * input iovecs, then run `done`. */
void
chimera_smb_pipe_call_finish(
    struct chimera_smb_request *request,
    int                         status);
