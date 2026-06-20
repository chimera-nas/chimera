// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "evpl/evpl_rpc2_gss.h"

struct chimera_vfs_cred;

/*
 * Chimera's RPCSEC_GSS acceptor, backed by GSSAPI/Kerberos.  This implements
 * the provider vtable that libevpl's rpc2 layer drives (see evpl_rpc2_gss.h):
 * libevpl owns all RPC-level framing and the handshake/replay state machine,
 * while this module wraps the actual Kerberos primitives.
 */

/*
 * Register the keytab to accept security contexts against (process-global,
 * via gsskrb5_register_acceptor_identity).  keytab may be NULL/empty to fall
 * back to the KRB5_KTNAME environment variable.  Returns 0 on success.
 * Idempotent; safe to call once per server start.
 */
int
chimera_nfs_gss_init(
    const char *keytab);

/* The GSSAPI-backed RPCSEC_GSS provider vtable. */
extern const struct evpl_rpc2_gss_provider chimera_nfs_gss_provider;

/*
 * Map an authenticated GSS principal ("user@REALM") to a VFS credential.
 * A user principal resolves via the local password database to its uid/gid;
 * a service/machine principal (one containing '/') or an unresolvable name
 * is squashed to the anonymous credential.
 */
void
chimera_nfs_gss_map_principal(
    const char              *principal,
    struct chimera_vfs_cred *cred);
