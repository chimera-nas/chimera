// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_pipe.h"

void
chimera_smb_pipe_call(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file,
    struct evpl_iovec            *input_iov,
    int                           input_niov,
    struct evpl_iovec            *output_iov,
    chimera_smb_pipe_done_t       done)
{
    int status;

    /* Everything finishing the call needs, set before the transceive runs: one
     * that parks finishes it from here once the handler has returned. */
    request->pipe.open_file  = open_file;
    request->pipe.input_iov  = input_iov;
    request->pipe.input_niov = input_niov;
    request->pipe.output_iov = output_iov;
    request->pipe.done       = done;

    status = open_file->pipe_transceive(request, input_iov, input_niov,
                                        output_iov);

    if (status == CHIMERA_SMB_PIPE_ASYNC) {
        return;
    }

    chimera_smb_pipe_call_finish(request, status);
} /* chimera_smb_pipe_call */

void
chimera_smb_pipe_call_finish(
    struct chimera_smb_request *request,
    int                         status)
{
    evpl_iovecs_release(request->compound->thread->evpl,
                        request->pipe.input_iov, request->pipe.input_niov);

    request->pipe.done(request, status);
} /* chimera_smb_pipe_call_finish */
