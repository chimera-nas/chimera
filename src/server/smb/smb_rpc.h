// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * DCE/RPC over SMB2 named pipes (ncacn_np), served by libdcerpc's server
 * runtime (ext/libsmb2/libdcerpc, <dcerpc/server.h>).
 *
 * The SMB2 transport moves the bytes: every WRITE (and the input of an
 * FSCTL_PIPE_TRANSCEIVE) is handed to chimera_smb_rpc_pipe_write(), and every
 * READ (and the output of a TRANSCEIVE) is answered from
 * chimera_smb_rpc_pipe_read().  libdcerpc does BIND / ALTER_CONTEXT,
 * reassembles fragmented requests, decodes them, calls the stubs in
 * smb_srvsvc.c / smb_wkssvc.c / smb_lsarpc.c / smb_samr.c, and fragments the
 * replies to the size the client negotiated.
 *
 * This header is deliberately free of libdcerpc types so the SMB2 protocol
 * files can include it; the service files use smb_rpc_service.h.
 */

#include <stddef.h>
#include <stdint.h>

#include "common/export.h"

struct evpl_iovec;
struct chimera_server_smb_shared;
struct chimera_vfs_cred;
struct chimera_smb_rpc_pipe;

/* Called once, single-threaded, at SMB server start: libdcerpc registers its
 * coders in an unlocked global list the first time a context is created, so
 * that has to happen before any SMB thread can open a pipe. */
SYMBOL_EXPORT int
chimera_smb_rpc_init(
    void);

/* A client's open of the named pipe `name` (e.g. "lsarpc").  Returns NULL if
 * no service is offered under that name or on allocation failure. */
SYMBOL_EXPORT struct chimera_smb_rpc_pipe *
chimera_smb_rpc_pipe_open(
    const char                       *name,
    struct chimera_server_smb_shared *shared,
    const struct chimera_vfs_cred    *cred);

SYMBOL_EXPORT void
chimera_smb_rpc_pipe_close(
    struct chimera_smb_rpc_pipe *pipe);

/* Bytes the client wrote.  Every complete PDU is processed before this returns
 * (a partial one is kept for the next write).  Returns 0, or -1 if the stream
 * is not DCE/RPC. */
SYMBOL_EXPORT int
chimera_smb_rpc_pipe_write(
    struct chimera_smb_rpc_pipe *pipe,
    const struct evpl_iovec     *iov,
    int                          niov);

/* The next pipe message for a client read, up to max bytes.  Returns the bytes
 * copied (0 when there is no output); *more is set when the current fragment
 * did not fit and the rest is returned by the next read, which SMB2 reports as
 * STATUS_BUFFER_OVERFLOW. */
SYMBOL_EXPORT uint32_t
chimera_smb_rpc_pipe_read(
    struct chimera_smb_rpc_pipe *pipe,
    void                        *buf,
    uint32_t                     max,
    int                         *more);

SYMBOL_EXPORT int
chimera_smb_rpc_pipe_has_output(
    struct chimera_smb_rpc_pipe *pipe);
