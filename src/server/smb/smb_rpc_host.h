// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * The seam between the SMB server and the libdcerpc services.
 *
 * The DCE/RPC side (smb_rpc.c and the smb_srvsvc.c / smb_wkssvc.c /
 * smb_lsarpc.c / smb_samr.c stubs) includes libdcerpc's headers; the SMB side
 * (smb_rpc_host.c) includes chimera's.  Neither includes the other's: on
 * Windows chimera's platform headers bring in <windows.h>, whose MS-DTYP
 * security types (ACL, SECURITY_DESCRIPTOR, ACCESS_MASK, ...) share their
 * names with libdcerpc's <dcerpc/dcerpc-dtyp.h>.  Everything that crosses
 * between the two halves goes through this header, which is plain C.
 */

#include <stddef.h>
#include <stdint.h>

/* ---- the SMB server, as the services see it (smb_rpc_host.c) ---- */

struct chimera_smb_rpc_host;

/* An account a name or SID resolved to. */
struct chimera_smb_rpc_account {
    int      is_group;
    uint32_t id;           /* uid or gid */
    char     name[256];
    char     sid[96];      /* "S-1-..." */
};

/* The server's name, which is also its account domain's. */
const char *
chimera_smb_rpc_host_identity(
    const struct chimera_smb_rpc_host *host);

/* The workgroup (or winbind domain) the server is in. */
const char *
chimera_smb_rpc_host_workgroup(
    const struct chimera_smb_rpc_host *host);

/* Sub-authorities 1-3 of the machine SID S-1-5-21-X-Y-Z. */
void
chimera_smb_rpc_host_machine_sid(
    const struct chimera_smb_rpc_host *host,
    uint32_t                           sub[3]);

/* The uid the pipe was opened as. */
uint32_t
chimera_smb_rpc_host_uid(
    const struct chimera_smb_rpc_host *host);

/* Called once per configured share, under the share list lock; a non-zero
 * return stops the walk and is returned. */
typedef int (*chimera_smb_rpc_share_cb)(
    void       *arg,
    const char *name,
    int         access_based_enum);

int
chimera_smb_rpc_host_shares(
    const struct chimera_smb_rpc_host *host,
    chimera_smb_rpc_share_cb           cb,
    void                              *arg);

/* Account lookups; 0 if found (and *out filled), -1 if not. */
int
chimera_smb_rpc_host_lookup_name(
    const struct chimera_smb_rpc_host *host,
    const char                        *name,
    struct chimera_smb_rpc_account    *out);

int
chimera_smb_rpc_host_lookup_sid(
    const struct chimera_smb_rpc_host *host,
    const char                        *sid,
    struct chimera_smb_rpc_account    *out);

int
chimera_smb_rpc_host_lookup_uid(
    const struct chimera_smb_rpc_host *host,
    uint32_t                           uid,
    struct chimera_smb_rpc_account    *out);

uint64_t
chimera_smb_rpc_host_random(
    void);

#define CHIMERA_SMB_RPC_LOG_ERROR 0
#define CHIMERA_SMB_RPC_LOG_INFO  1
#define CHIMERA_SMB_RPC_LOG_DEBUG 2

/* The most verbose CHIMERA_SMB_RPC_LOG_* level currently logged. */
int
chimera_smb_rpc_host_log_level(
    void);

void
chimera_smb_rpc_host_log(
    int         level,
    const char *msg);

/* ---- the DCE/RPC side, as the SMB server sees it (smb_rpc.c) ---- */

/* One client's open of a named pipe.  Not thread-safe: the SMB side
 * serializes every call on a state. */
struct chimera_smb_rpc_state;

int
chimera_smb_rpc_state_init(
    void);

/* NULL if no service is offered on pipe `name`. */
struct chimera_smb_rpc_state *
chimera_smb_rpc_state_open(
    const char                  *name,
    struct chimera_smb_rpc_host *host);

void
chimera_smb_rpc_state_close(
    struct chimera_smb_rpc_state *state);

int
chimera_smb_rpc_state_write(
    struct chimera_smb_rpc_state *state,
    const void                   *buf,
    size_t                        len);

size_t
chimera_smb_rpc_state_read(
    struct chimera_smb_rpc_state *state,
    void                         *buf,
    size_t                        max,
    int                          *more);

int
chimera_smb_rpc_state_has_output(
    struct chimera_smb_rpc_state *state);
