// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The SMB server's half of the named-pipe DCE/RPC services (see
 * smb_rpc_host.h): the public smb_rpc.h API the SMB2 READ / WRITE /
 * FSCTL_PIPE_TRANSCEIVE paths call, and the server state the libdcerpc stubs
 * read through smb_rpc_host.h.  No libdcerpc header is included here.
 */

#include <stdio.h>
#include <string.h>

#include "smb_internal.h"
#include "smb_rpc.h"
#include "smb_rpc_host.h"
#include "vfs/vfs.h"
#include "vfs/vfs_user_cache.h"
#include "vfs/vfs_idmap.h"

struct chimera_smb_rpc_host {
    struct chimera_server_smb_shared *shared;
    struct chimera_vfs_cred           cred;
};

struct chimera_smb_rpc_pipe {
    /* libdcerpc is not thread-safe.  A pipe open is used from whichever
     * thread runs the SMB2 request (more than one with multichannel), so
     * every call into its state holds this. */
    evpl_mutex_t                  lock;
    struct chimera_smb_rpc_host   host;
    struct chimera_smb_rpc_state *state;
};

/* ---- smb_rpc.h ---- */

SYMBOL_EXPORT int
chimera_smb_rpc_init(void)
{
    return chimera_smb_rpc_state_init();
} /* chimera_smb_rpc_init */

SYMBOL_EXPORT struct chimera_smb_rpc_pipe *
chimera_smb_rpc_pipe_open(
    const char                       *name,
    struct chimera_server_smb_shared *shared,
    const struct chimera_vfs_cred    *cred)
{
    struct chimera_smb_rpc_pipe *pipe = calloc(1, sizeof(*pipe));

    if (!pipe) {
        return NULL;
    }

    pipe->host.shared = shared;

    if (cred) {
        pipe->host.cred = *cred;
    }

    pipe->state = chimera_smb_rpc_state_open(name, &pipe->host);

    if (!pipe->state) {
        free(pipe);
        return NULL;
    }

    evpl_mutex_init(&pipe->lock, NULL);
    return pipe;
} /* chimera_smb_rpc_pipe_open */

SYMBOL_EXPORT void
chimera_smb_rpc_pipe_close(struct chimera_smb_rpc_pipe *pipe)
{
    if (!pipe) {
        return;
    }

    chimera_smb_rpc_state_close(pipe->state);
    evpl_mutex_destroy(&pipe->lock);
    free(pipe);
} /* chimera_smb_rpc_pipe_close */

SYMBOL_EXPORT int
chimera_smb_rpc_pipe_write(
    struct chimera_smb_rpc_pipe *pipe,
    const struct evpl_iovec     *iov,
    int                          niov)
{
    int i, rc = 0;

    evpl_mutex_lock(&pipe->lock);

    /* libdcerpc keeps a partial PDU between writes, so a PDU split across
     * iovecs (or across SMB2 WRITEs) is reassembled without copying here. */
    for (i = 0; i < niov && rc == 0; i++) {
        if (iov[i].length) {
            rc = chimera_smb_rpc_state_write(pipe->state, iov[i].data, iov[i].length);
        }
    }

    evpl_mutex_unlock(&pipe->lock);

    return rc ? -1 : 0;
} /* chimera_smb_rpc_pipe_write */

SYMBOL_EXPORT uint32_t
chimera_smb_rpc_pipe_read(
    struct chimera_smb_rpc_pipe *pipe,
    void                        *buf,
    uint32_t                     max,
    int                         *more)
{
    size_t n;

    evpl_mutex_lock(&pipe->lock);
    n = chimera_smb_rpc_state_read(pipe->state, buf, max, more);
    evpl_mutex_unlock(&pipe->lock);

    return (uint32_t) n;
} /* chimera_smb_rpc_pipe_read */

SYMBOL_EXPORT int
chimera_smb_rpc_pipe_has_output(struct chimera_smb_rpc_pipe *pipe)
{
    int rc;

    evpl_mutex_lock(&pipe->lock);
    rc = chimera_smb_rpc_state_has_output(pipe->state);
    evpl_mutex_unlock(&pipe->lock);

    return rc;
} /* chimera_smb_rpc_pipe_has_output */

/* ---- smb_rpc_host.h (called with the pipe's lock held) ---- */

const char *
chimera_smb_rpc_host_identity(const struct chimera_smb_rpc_host *host)
{
    return host->shared->config.identity[0] ? host->shared->config.identity : "CHIMERA";
} /* chimera_smb_rpc_host_identity */

const char *
chimera_smb_rpc_host_workgroup(const struct chimera_smb_rpc_host *host)
{
    const char *domain = host->shared->config.auth.winbind_domain;

    return domain[0] ? domain : "WORKGROUP";
} /* chimera_smb_rpc_host_workgroup */

void
chimera_smb_rpc_host_machine_sid(
    const struct chimera_smb_rpc_host *host,
    uint32_t                           sub[3])
{
    sub[0] = host->shared->machine_domain_sub[0];
    sub[1] = host->shared->machine_domain_sub[1];
    sub[2] = host->shared->machine_domain_sub[2];
} /* chimera_smb_rpc_host_machine_sid */

uint32_t
chimera_smb_rpc_host_uid(const struct chimera_smb_rpc_host *host)
{
    return host->cred.uid;
} /* chimera_smb_rpc_host_uid */

int
chimera_smb_rpc_host_shares(
    const struct chimera_smb_rpc_host *host,
    chimera_smb_rpc_share_cb           cb,
    void                              *arg)
{
    struct chimera_server_smb_shared *shared = host->shared;
    struct chimera_smb_share         *cur;
    int                               rc = 0;

    evpl_mutex_lock(&shared->shares_lock);

    LL_FOREACH(shared->shares, cur)
    {
        rc = cb(arg, cur->name, cur->access_based_enum);

        if (rc) {
            break;
        }
    }

    evpl_mutex_unlock(&shared->shares_lock);

    return rc;
} /* chimera_smb_rpc_host_shares */

static struct chimera_vfs_user_cache *
chimera_smb_rpc_host_cache(const struct chimera_smb_rpc_host *host)
{
    return host->shared->vfs->vfs_user_cache;
} /* chimera_smb_rpc_host_cache */

static void
chimera_smb_rpc_host_set_user(
    const struct chimera_vfs_user  *u,
    struct chimera_smb_rpc_account *out)
{
    memset(out, 0, sizeof(*out));
    out->is_group = 0;
    out->id       = u->uid;
    snprintf(out->name, sizeof(out->name), "%s", u->username);

    if (u->sid[0]) {
        snprintf(out->sid, sizeof(out->sid), "%s", u->sid);
    } else {
        snprintf(out->sid, sizeof(out->sid), "S-1-22-1-%u", u->uid);
    }
} /* chimera_smb_rpc_host_set_user */

static void
chimera_smb_rpc_host_set_group(
    const struct chimera_vfs_group *g,
    struct chimera_smb_rpc_account *out)
{
    memset(out, 0, sizeof(*out));
    out->is_group = 1;
    out->id       = g->gid;
    snprintf(out->name, sizeof(out->name), "%s", g->groupname);

    if (g->sid[0]) {
        snprintf(out->sid, sizeof(out->sid), "%s", g->sid);
    } else {
        snprintf(out->sid, sizeof(out->sid), "S-1-22-2-%u", g->gid);
    }
} /* chimera_smb_rpc_host_set_group */

int
chimera_smb_rpc_host_lookup_name(
    const struct chimera_smb_rpc_host *host,
    const char                        *name,
    struct chimera_smb_rpc_account    *out)
{
    const struct chimera_vfs_user *u;

    u = chimera_vfs_user_cache_lookup_by_name(chimera_smb_rpc_host_cache(host), name);

    if (!u) {
        return -1;
    }

    chimera_smb_rpc_host_set_user(u, out);
    return 0;
} /* chimera_smb_rpc_host_lookup_name */

int
chimera_smb_rpc_host_lookup_sid(
    const struct chimera_smb_rpc_host *host,
    const char                        *sid,
    struct chimera_smb_rpc_account    *out)
{
    struct chimera_vfs_user_cache  *cache = chimera_smb_rpc_host_cache(host);
    const struct chimera_vfs_user  *u;
    const struct chimera_vfs_group *g = NULL;
    struct chimera_principal        pr;

    u = chimera_vfs_user_cache_lookup_by_sid(cache, sid);

    if (!u) {
        g = chimera_vfs_group_cache_lookup_by_sid(cache, sid);
    }

    /* The unix-id SIDs (S-1-22-1-uid, S-1-22-2-gid) via the idmap. */
    if (!u && !g && chimera_idmap_sid_to_principal(sid, &pr) == 0) {
        if (pr.type == CHIMERA_PRINCIPAL_USER) {
            u = chimera_vfs_user_cache_lookup_by_uid(cache, pr.id);
        } else if (pr.type == CHIMERA_PRINCIPAL_GROUP) {
            g = chimera_vfs_group_cache_lookup_by_gid(cache, pr.id);
        }
    }

    if (u) {
        chimera_smb_rpc_host_set_user(u, out);
    } else if (g) {
        chimera_smb_rpc_host_set_group(g, out);
    } else {
        return -1;
    }

    /* Report the SID asked about, which the idmap path may spell
     * differently from the record's own. */
    snprintf(out->sid, sizeof(out->sid), "%s", sid);
    return 0;
} /* chimera_smb_rpc_host_lookup_sid */

int
chimera_smb_rpc_host_lookup_uid(
    const struct chimera_smb_rpc_host *host,
    uint32_t                           uid,
    struct chimera_smb_rpc_account    *out)
{
    const struct chimera_vfs_user *u;

    u = chimera_vfs_user_cache_lookup_by_uid(chimera_smb_rpc_host_cache(host), uid);

    if (!u) {
        return -1;
    }

    chimera_smb_rpc_host_set_user(u, out);
    return 0;
} /* chimera_smb_rpc_host_lookup_uid */

uint64_t
chimera_smb_rpc_host_random(void)
{
    return chimera_rand64();
} /* chimera_smb_rpc_host_random */

int
chimera_smb_rpc_host_log_level(void)
{
    return ChimeraLogLevel >= CHIMERA_LOG_DEBUG ? CHIMERA_SMB_RPC_LOG_DEBUG :
           ChimeraLogLevel >= CHIMERA_LOG_INFO ? CHIMERA_SMB_RPC_LOG_INFO :
           CHIMERA_SMB_RPC_LOG_ERROR;
} /* chimera_smb_rpc_host_log_level */

void
chimera_smb_rpc_host_log(
    int         level,
    const char *msg)
{
    switch (level) {
        case CHIMERA_SMB_RPC_LOG_ERROR:
            chimera_smb_error("rpc: %s", msg);
            break;
        case CHIMERA_SMB_RPC_LOG_INFO:
            chimera_smb_info("rpc: %s", msg);
            break;
        default:
            chimera_smb_debug("rpc: %s", msg);
            break;
    } /* switch */
} /* chimera_smb_rpc_host_log */
