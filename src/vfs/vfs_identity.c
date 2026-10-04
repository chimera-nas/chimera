// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Identity engine -- see vfs_identity.h.
 *
 * A cache hit resolves synchronously on the calling thread (lock-free RCU
 * read).  A miss is queued to a small pool of worker pthreads that walk the
 * registered identity modules (sdk/vfs_identity_module.h), populate the user
 * cache, then hand the result back to the originating evpl thread by appending
 * it to that thread's pending-identity queue and ringing its doorbell.
 */

#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include "common/thread.h"
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <strings.h>
#include <unistd.h>
#include <dlfcn.h>
#endif /* ifdef _WIN32 */

#include "vfs.h"
#include "vfs_internal.h"
#include "vfs_user_cache.h"
#include "vfs_identity.h"
#include "common/macros.h"
#include "common/pthread_util.h"

/* The in-tree modules, referenced directly so the symbols are retained
 * regardless of library type and linker --as-needed, and found without a
 * dlopen.  An out-of-tree module arrives via module_path instead. */
#ifndef _WIN32
extern struct chimera_vfs_identity_module        identity_nss;
#endif /* ifndef _WIN32 */
#ifdef HAVE_WBCLIENT
extern struct chimera_vfs_identity_module        identity_winbind;
#endif /* ifdef HAVE_WBCLIENT */

static const struct chimera_vfs_identity_module *chimera_vfs_identity_builtins[] = {
#ifndef _WIN32
    &identity_nss,
#endif /* ifndef _WIN32 */
#ifdef HAVE_WBCLIENT
    &identity_winbind,
#endif /* ifdef HAVE_WBCLIENT */
    NULL
};

/* Longest key string a request carries: a SID, a username, or a principal,
 * which is a username plus its domain or realm. */
#define CHIMERA_VFS_IDENTITY_PRINCIPAL_MAX_LEN \
        (2 * CHIMERA_VFS_IDENTITY_NAME_MAX_LEN)

struct chimera_vfs_identity_module_entry {
    const struct chimera_vfs_identity_module *module;
    void                                     *private_data;
    /* The domains this module serves principals for; none means every
     * principal.  See chimera_vfs_identity_add_domain. */
    char                                    **domains;
    int                                       num_domains;
    struct chimera_vfs_identity_module_entry *next;
};

/* One realm map entry: principals qualified by `realm` belong to `domain`. */
struct chimera_vfs_identity_realm {
    char realm[CHIMERA_VFS_IDENTITY_NAME_MAX_LEN];
    char domain[CHIMERA_VFS_IDENTITY_NAME_MAX_LEN];
};

struct chimera_vfs_identity_request {
    struct chimera_vfs_identity_request *prev;
    struct chimera_vfs_identity_request *next;
    struct chimera_vfs_thread           *origin;
    enum chimera_vfs_identity_key        key;
    uint32_t                             id;
    char                                 name[CHIMERA_VFS_IDENTITY_PRINCIPAL_MAX_LEN];
    int                                  found;
    struct chimera_vfs_identity_result   result;
    chimera_vfs_identity_callback        callback;
    void                                *private_data;
};

struct chimera_vfs_identity {
    struct chimera_vfs                       *vfs;
    int                                       num_workers;
    evpl_native_thread_t                     *workers;
    evpl_mutex_t                              lock;
    evpl_cond_t                               cond;
    struct chimera_vfs_identity_request      *queue;
    int                                       shutdown;
    evpl_mutex_t                              module_lock;
    struct chimera_vfs_identity_module_entry *modules;
    struct chimera_vfs_identity_realm        *realms;
    int                                       num_realms;
};

/* Copy a cached user into a standalone result (so callbacks never hold an RCU
 * pointer). */
static inline void
chimera_vfs_identity_copy_user(
    struct chimera_vfs_identity_user *dst,
    const struct chimera_vfs_user    *src)
{
    dst->uid   = src->uid;
    dst->gid   = src->gid;
    dst->ngids = src->ngids;
    memcpy(dst->gids, src->gids, src->ngids * sizeof(uint32_t));
    memcpy(dst->username, src->username, sizeof(dst->username));
    memcpy(dst->sid, src->sid, sizeof(dst->sid));
    dst->username_len = src->username_len;
} /* chimera_vfs_identity_copy_user */

/* Copy a cached group into a standalone result. */
static inline void
chimera_vfs_identity_copy_group(
    struct chimera_vfs_identity_group *dst,
    const struct chimera_vfs_group    *src)
{
    dst->gid = src->gid;
    memcpy(dst->groupname, src->groupname, sizeof(dst->groupname));
    memcpy(dst->sid, src->sid, sizeof(dst->sid));
    dst->groupname_len = src->groupname_len;
} /* chimera_vfs_identity_copy_group */

/*
 * Synchronous cache probe.  Returns 1 and fills `out` on a hit, 0 on a miss.
 * Caller need not hold the RCU read lock -- it is taken here.
 */
static int
chimera_vfs_identity_cache_probe(
    struct chimera_vfs_user_cache      *cache,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    const struct chimera_vfs_user  *user  = NULL;
    const struct chimera_vfs_group *group = NULL;
    int                             found = 0;

    chimera_rcu_read_lock(&cache->rcu);

    switch (key) {
        case CHIMERA_VFS_IDENTITY_BY_UID:
            user = chimera_vfs_user_cache_lookup_by_uid(cache, id);
            break;
        case CHIMERA_VFS_IDENTITY_BY_NAME:
            user = chimera_vfs_user_cache_lookup_by_name(cache, name);
            break;
        case CHIMERA_VFS_IDENTITY_BY_SID:
            /* A SID is ambiguous: try users first (preserving the previous
             * behaviour), then groups. */
            user = chimera_vfs_user_cache_lookup_by_sid(cache, name);
            if (!user) {
                group = chimera_vfs_group_cache_lookup_by_sid(cache, name);
            }
            break;
        case CHIMERA_VFS_IDENTITY_BY_GID:
            group = chimera_vfs_group_cache_lookup_by_gid(cache, id);
            break;
        default:
            break;
    } /* switch */

    if (user) {
        out->is_group = 0;
        chimera_vfs_identity_copy_user(&out->user, user);
        found = 1;
    } else if (group) {
        out->is_group = 1;
        chimera_vfs_identity_copy_group(&out->group, group);
        found = 1;
    }

    chimera_rcu_read_unlock(&cache->rcu);

    return found;
} /* chimera_vfs_identity_cache_probe */

static inline int
chimera_vfs_identity_result_has_sid(const struct chimera_vfs_identity_result *result)
{
    return result->is_group ? result->group.sid[0] != '\0'
           : result->user.sid[0] != '\0';
} /* chimera_vfs_identity_result_has_sid */

/*
 * Rewrite a principal as DOMAIN\user, the one form modules see, and report the
 * domain it belongs to.  A Kerberos client name arrives as user@REALM; a
 * down-level logon name as DOMAIN\user; a bare name has no domain.  Whatever
 * qualifies the name is looked up in the realm map, so a principal from a
 * realm whose name differs from its domain's still reaches the module that
 * serves that domain.  Returns 0, or -1 when the principal does not fit.
 */
static int
chimera_vfs_identity_normalize_principal(
    const struct chimera_vfs_identity *identity,
    const char                        *principal,
    char                              *domain,
    size_t                             domain_len,
    char                              *out,
    size_t                             out_len)
{
    const char *at  = strrchr(principal, '@');
    const char *sep = strchr(principal, '\\');
    const char *qual = "", *user = principal;
    size_t      qual_len = 0, user_len = strlen(principal);
    int         i, n;

    if (at && at != principal && at[1] != '\0') {
        qual     = at + 1;
        qual_len = strlen(qual);
        user_len = (size_t) (at - principal);
    } else if (sep && sep != principal && sep[1] != '\0') {
        qual_len = (size_t) (sep - principal);
        qual     = principal;
        user     = sep + 1;
        user_len = strlen(user);
    }

    if (qual_len >= domain_len) {
        return -1;
    }
    memcpy(domain, qual, qual_len);
    domain[qual_len] = '\0';

    for (i = 0; i < identity->num_realms; i++) {
        if (strcasecmp(domain, identity->realms[i].realm) == 0) {
            snprintf(domain, domain_len, "%s", identity->realms[i].domain);
            break;
        }
    }

    if (domain[0]) {
        n = snprintf(out, out_len, "%s\\%.*s", domain, (int) user_len, user);
    } else {
        n = snprintf(out, out_len, "%.*s", (int) user_len, user);
    }

    return (n < 0 || (size_t) n >= out_len) ? -1 : 0;
} /* chimera_vfs_identity_normalize_principal */

/* Non-zero if `entry` serves principals of `domain`: it lists that domain, or
 * lists none and so serves every principal.  A module that lists domains never
 * sees an unqualified principal. */
static int
chimera_vfs_identity_serves_domain(
    const struct chimera_vfs_identity_module_entry *entry,
    const char                                     *domain)
{
    int i;

    if (entry->num_domains == 0) {
        return 1;
    }

    for (i = 0; i < entry->num_domains; i++) {
        if (strcasecmp(domain, entry->domains[i]) == 0) {
            return 1;
        }
    }

    return 0;
} /* chimera_vfs_identity_serves_domain */

/*
 * Walk the registered modules in order until one resolves the key.
 *
 * For the numeric keys (BY_UID / BY_GID) a plain first-wins walk is not enough.
 * NSS is registered first and, on a host whose nsswitch routes passwd/group
 * through winbind, it answers those keys itself -- with a name but never a SID,
 * because NSS has no notion of one.  That would short-circuit the winbind
 * module, which is the only one that can supply the real SID, and the identity
 * would be cached SID-less; the cache hit then suppresses any further resolve,
 * so the SID never arrives and the marshaller falls back to the algorithmic
 * S-1-5-88 form for good.
 *
 * So for those two keys a SID-less success is only provisional: keep walking in
 * case a later module can name the same identity properly, and settle for the
 * provisional answer only if none can.  This is safe precisely because the key
 * is numeric -- every module is being asked about the same uid/gid, so they
 * can only disagree about the SID, never about which identity it is.  BY_NAME,
 * BY_SID and BY_PRINCIPAL stay first-wins: there the key is a string that two
 * modules could legitimately resolve to different identities (a local "alice"
 * and a domain "alice"), and preferring the SID-bearing answer would silently
 * change which account wins.
 *
 * BY_PRINCIPAL goes only to modules that declare CAP_PRINCIPAL.  A principal
 * is a name some authority has already vouched for, and mapping it is a policy
 * decision a module opts into -- NSS in particular never sees one, so a
 * domain principal can never land on a same-named local account.  Among those
 * modules it is routed by domain: one configured with a domain list sees only
 * the principals of those domains, so a module for one forest is never asked
 * about another's.
 *
 * Returns OK with *out filled; otherwise UNAVAILABLE when no module answered
 * and at least one was down, else NOT_MINE.
 */
static enum chimera_vfs_identity_status
chimera_vfs_identity_run_lookup(
    struct chimera_vfs_identity        *identity,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    struct chimera_vfs_identity_module_entry *entry;
    struct chimera_vfs_identity_result        provisional;
    char principal[CHIMERA_VFS_IDENTITY_PRINCIPAL_MAX_LEN];
    char domain[CHIMERA_VFS_IDENTITY_NAME_MAX_LEN];
    int have_provisional = 0;
    int any_unavailable  = 0;
    int prefer_sid;
    uint32_t required = CHIMERA_VFS_IDENTITY_CAP_LOOKUP;
    enum chimera_vfs_identity_status          status;

    prefer_sid = (key == CHIMERA_VFS_IDENTITY_BY_UID ||
                  key == CHIMERA_VFS_IDENTITY_BY_GID);

    if (key == CHIMERA_VFS_IDENTITY_BY_PRINCIPAL) {
        if (!name || name[0] == '\0' ||
            chimera_vfs_identity_normalize_principal(identity, name, domain,
                                                     sizeof(domain), principal,
                                                     sizeof(principal)) != 0) {
            return CHIMERA_VFS_IDENTITY_NOT_MINE;
        }
        name      = principal;
        required |= CHIMERA_VFS_IDENTITY_CAP_PRINCIPAL;
    }

    evpl_mutex_lock(&identity->module_lock);
    entry = identity->modules;
    evpl_mutex_unlock(&identity->module_lock);

    /* The module list is only appended to at startup, so it is safe to walk
     * after grabbing the head. */
    for (; entry; entry = entry->next) {
        if ((entry->module->capabilities & required) != required) {
            continue;
        }
        if (key == CHIMERA_VFS_IDENTITY_BY_PRINCIPAL &&
            !chimera_vfs_identity_serves_domain(entry, domain)) {
            continue;
        }

        memset(out, 0, sizeof(*out));
        status = entry->module->lookup(entry->private_data, key, id, name, out);

        if (status == CHIMERA_VFS_IDENTITY_UNAVAILABLE) {
            chimera_vfs_error("identity module %s unavailable during lookup",
                              entry->module->name);
            any_unavailable = 1;
        } else if (status == CHIMERA_VFS_IDENTITY_OK) {
            if (!prefer_sid || chimera_vfs_identity_result_has_sid(out)) {
                return CHIMERA_VFS_IDENTITY_OK;
            }
            /* Resolved, but with no SID.  Hold it and keep looking. */
            if (!have_provisional) {
                provisional      = *out;
                have_provisional = 1;
            }
        }
    }

    if (have_provisional) {
        *out = provisional;
        return CHIMERA_VFS_IDENTITY_OK;
    }

    return any_unavailable ? CHIMERA_VFS_IDENTITY_UNAVAILABLE :
           CHIMERA_VFS_IDENTITY_NOT_MINE;
} /* chimera_vfs_identity_run_lookup */

static void *
chimera_vfs_identity_worker(void *arg)
{
    struct chimera_vfs_identity         *identity = arg;
    struct chimera_vfs_identity_request *req;
    struct chimera_vfs_thread           *origin;

    /* Pure writer: this worker only runs module lookups and populates the
     * cache (retire + publish), never taking the read side -- the read-side
     * probe runs on the evpl threads.  So it is not registered as a QSBR
     * reader; registering it would put a thread that blocks in cond_wait and in
     * NSS/winbind resolution into the grace-period quorum and stall reclamation
     * process-wide. */
    evpl_mutex_lock(&identity->lock);

    while (1) {
        while (!identity->queue && !identity->shutdown) {
            evpl_cond_wait(&identity->cond, &identity->lock);
        }

        if (identity->shutdown && !identity->queue) {
            break;
        }

        req = identity->queue;
        DL_DELETE(identity->queue, req);

        evpl_mutex_unlock(&identity->lock);

        /* Blocking resolution happens here, off the event loop. */
        if (chimera_vfs_identity_run_lookup(identity, req->key, req->id,
                                            req->name, &req->result) ==
            CHIMERA_VFS_IDENTITY_OK) {
            req->found = 1;
            /* Populate the cache so future lookups for this identity are
             * synchronous (TTL-expiring, non-pinned).  A group goes to the
             * group chains, never the user ones. */
            if (req->result.is_group) {
                chimera_vfs_group_cache_add(
                    identity->vfs->vfs_user_cache,
                    req->result.group.groupname,
                    req->result.group.sid[0] ? req->result.group.sid : NULL,
                    req->result.group.gid, 0);
            } else {
                chimera_vfs_user_cache_add(
                    identity->vfs->vfs_user_cache,
                    req->result.user.username[0] ? req->result.user.username : "",
                    NULL, NULL,
                    req->result.user.sid[0] ? req->result.user.sid : NULL,
                    req->result.user.uid, req->result.user.gid,
                    req->result.user.ngids, req->result.user.gids, 0);
            }
        } else {
            req->found = 0;
        }

        /* Hand the completed job back to the originating evpl thread.
         *
         * Read the origin out of the job FIRST.  Appending it publishes it:
         * from the moment the origin's lock is dropped that thread may drain
         * the list, run the callback and free the job, so any later reach
         * through `req` -- including for the doorbell to wake it with -- is a
         * use-after-free.  The window is one instruction wide and the
         * identity path is a cache miss, so it takes an unlucky Debug run to
         * catch it, which is how it survived. */
        origin = req->origin;

        evpl_mutex_lock(&origin->lock);
        DL_APPEND(origin->pending_identity, req);
        evpl_mutex_unlock(&origin->lock);

        evpl_ring_doorbell(&origin->doorbell);

        evpl_mutex_lock(&identity->lock);
    }

    evpl_mutex_unlock(&identity->lock);

    return NULL;
} /* chimera_vfs_identity_worker */

/* ---- module registration ----------------------------------------------- */

static void
chimera_vfs_identity_add_module(
    struct chimera_vfs_identity              *identity,
    const struct chimera_vfs_identity_module *module,
    const char                               *cfgdata)
{
    struct chimera_vfs_identity_module_entry *entry, **pp;

    /* A module built against a different SDK contract would misinterpret the
     * result structures; refuse it at load time rather than corrupt memory.
     * Out-of-tree modules arrive via dlopen (module_path), so this is their
     * only compatibility gate. */
    chimera_vfs_abort_if(module->sdk_version != CHIMERA_VFS_IDENTITY_SDK_VERSION,
                         "identity module %s was built against identity SDK version %u; "
                         "this chimera provides version %u",
                         module->name, module->sdk_version,
                         CHIMERA_VFS_IDENTITY_SDK_VERSION);

    chimera_vfs_abort_if((module->capabilities & CHIMERA_VFS_IDENTITY_CAP_LOOKUP) &&
                         !module->lookup,
                         "identity module %s claims CAP_LOOKUP without a lookup op",
                         module->name);
    chimera_vfs_abort_if((module->capabilities & CHIMERA_VFS_IDENTITY_CAP_PRINCIPAL) &&
                         !(module->capabilities & CHIMERA_VFS_IDENTITY_CAP_LOOKUP),
                         "identity module %s claims CAP_PRINCIPAL without CAP_LOOKUP",
                         module->name);
    chimera_vfs_abort_if((module->capabilities & CHIMERA_VFS_IDENTITY_CAP_DOMAIN_INFO) &&
                         !module->domain_info,
                         "identity module %s claims CAP_DOMAIN_INFO without a domain_info op",
                         module->name);

    entry         = calloc(1, sizeof(*entry));
    entry->module = module;

    if (module->init) {
        entry->private_data = module->init(cfgdata ? cfgdata : "",
                                           identity->vfs->metrics.metrics);
    }

    evpl_mutex_lock(&identity->module_lock);
    /* Append to preserve registration order (NSS first, then the configured
     * modules in configuration order). */
    pp = &identity->modules;
    while (*pp) {
        pp = &(*pp)->next;
    }
    *pp = entry;
    evpl_mutex_unlock(&identity->module_lock);

    chimera_vfs_info("identity module %s registered (capabilities 0x%x)",
                     module->name, module->capabilities);
} /* chimera_vfs_identity_add_module */

/* Find a module by its symbol name: the built-in table first, then whatever
 * the process (or a dlopen'ed shared object) exports. */
static const struct chimera_vfs_identity_module *
chimera_vfs_identity_find_module(const char *symbol)
{
    int i;

    for (i = 0; chimera_vfs_identity_builtins[i]; i++) {
        if (!strncmp(symbol, "identity_", 9) &&
            !strcmp(symbol + 9, chimera_vfs_identity_builtins[i]->name)) {
            return chimera_vfs_identity_builtins[i];
        }
    }
#ifdef _WIN32
    return NULL;
#else  /* ifdef _WIN32 */
    return dlsym(RTLD_DEFAULT, symbol);
#endif /* ifdef _WIN32 */
} /* chimera_vfs_identity_find_module */

/* ---- lifecycle --------------------------------------------------------- */

SYMBOL_EXPORT struct chimera_vfs_identity *
chimera_vfs_identity_create(
    struct chimera_vfs *vfs,
    int                 num_workers)
{
    struct chimera_vfs_identity *identity;
    int                          i;

    if (num_workers < 1) {
        num_workers = 1;
    }

    identity              = calloc(1, sizeof(*identity));
    identity->vfs         = vfs;
    identity->num_workers = num_workers;
    identity->workers     = calloc(num_workers, sizeof(evpl_native_thread_t));

    evpl_mutex_init(&identity->lock, NULL);
    evpl_cond_init(&identity->cond, NULL);
    evpl_mutex_init(&identity->module_lock, NULL);

    /* The default local/NSS module is always present and tried first.
     * (Registered directly on `identity`: vfs->identity is assigned only after
     * this function returns.) */
#ifndef _WIN32
    chimera_vfs_identity_add_module(identity, &identity_nss, "");
#endif /* ifndef _WIN32 */

    for (i = 0; i < num_workers; i++) {
        int rc = chimera_pthread_create(&identity->workers[i], NULL,
                                        chimera_vfs_identity_worker, identity);

        /* A missing worker would leave queued identity jobs that nothing
         * services and destroy() joining a garbage evpl_native_thread_t. */
        chimera_vfs_abort_if(rc,
                             "chimera_vfs_identity_create: evpl_native_thread_create failed: %s",
                             strerror(rc));
    }

    return identity;
} /* chimera_vfs_identity_create */

SYMBOL_EXPORT void
chimera_vfs_identity_destroy(struct chimera_vfs_identity *identity)
{
    struct chimera_vfs_identity_module_entry *entry, *next;
    struct chimera_vfs_identity_request      *req;
    int                                       i;

    evpl_mutex_lock(&identity->lock);
    identity->shutdown = 1;
    evpl_cond_broadcast(&identity->cond);
    evpl_mutex_unlock(&identity->lock);

    for (i = 0; i < identity->num_workers; i++) {
        evpl_native_thread_join(identity->workers[i], NULL);
    }

    /* Any jobs still queued at shutdown are dropped (their callers are gone). */
    while (identity->queue) {
        req = identity->queue;
        DL_DELETE(identity->queue, req);
        free(req);
    }

    entry = identity->modules;
    while (entry) {
        next = entry->next;
        if (entry->module->destroy) {
            entry->module->destroy(entry->private_data);
        }
        for (i = 0; i < entry->num_domains; i++) {
            free(entry->domains[i]);
        }
        free(entry->domains);
        free(entry);
        entry = next;
    }

    evpl_mutex_destroy(&identity->lock);
    evpl_cond_destroy(&identity->cond);
    evpl_mutex_destroy(&identity->module_lock);

    free(identity->realms);
    free(identity->workers);
    free(identity);
} /* chimera_vfs_identity_destroy */

SYMBOL_EXPORT void
chimera_vfs_identity_register_module(
    struct chimera_vfs                       *vfs,
    const struct chimera_vfs_identity_module *module,
    const char                               *cfgdata)
{
    chimera_vfs_identity_add_module(vfs->identity, module, cfgdata);
} /* chimera_vfs_identity_register_module */

SYMBOL_EXPORT void
chimera_vfs_identity_load_modules(
    struct chimera_vfs                  *vfs,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules)
{
    const struct chimera_vfs_identity_module *module;
    char                                      modsym[80];
    int                                       i;

    for (i = 0; i < num_modules; i++) {
        snprintf(modsym, sizeof(modsym), "identity_%s", module_cfgs[i].module_name);

        /* NSS is registered unconditionally at create time; a configured
         * entry for it only carries configuration, never a second instance. */
        if (strcmp(module_cfgs[i].module_name, "nss") == 0) {
            continue;
        }

        if (module_cfgs[i].module_path[0] != '\0') {
            if (chimera_vfs_identity_find_module(modsym) != NULL) {
                chimera_vfs_error("Identity module %s already loaded, skipping dlopen of %s",
                                  module_cfgs[i].module_name, module_cfgs[i].module_path);
            } else {
#ifdef _WIN32
                chimera_vfs_abort_if(1, "External identity modules require a shared-library build: %s",
                                     module_cfgs[i].module_path);
#else  /* ifdef _WIN32 */
                void *handle = dlopen(module_cfgs[i].module_path, RTLD_NOW | RTLD_GLOBAL);

                chimera_vfs_abort_if(!handle, "Failed to load identity module %s from %s: %s",
                                     module_cfgs[i].module_name,
                                     module_cfgs[i].module_path,
                                     dlerror());
                chimera_vfs_info("Identity module %s loaded from %s",
                                 module_cfgs[i].module_name, module_cfgs[i].module_path);
#endif /* ifdef _WIN32 */
            }
        }

        module = chimera_vfs_identity_find_module(modsym);
        chimera_vfs_abort_if(!module,
                             "Identity module %s symbol %s not found (is this build missing its dependency?)",
                             module_cfgs[i].module_name, modsym);

        chimera_vfs_identity_add_module(vfs->identity, module, module_cfgs[i].config_data);
    }
} /* chimera_vfs_identity_load_modules */

SYMBOL_EXPORT int
chimera_vfs_identity_has_capability(
    struct chimera_vfs *vfs,
    uint32_t            caps)
{
    struct chimera_vfs_identity              *identity = vfs->identity;
    struct chimera_vfs_identity_module_entry *entry;
    int                                       found = 0;

    evpl_mutex_lock(&identity->module_lock);
    for (entry = identity->modules; entry; entry = entry->next) {
        if ((entry->module->capabilities & caps) == caps) {
            found = 1;
            break;
        }
    }
    evpl_mutex_unlock(&identity->module_lock);

    return found;
} /* chimera_vfs_identity_has_capability */

SYMBOL_EXPORT void
chimera_vfs_identity_add_domain(
    struct chimera_vfs *vfs,
    const char         *module_name,
    const char         *domain)
{
    struct chimera_vfs_identity              *identity = vfs->identity;
    struct chimera_vfs_identity_module_entry *entry;

    evpl_mutex_lock(&identity->module_lock);
    for (entry = identity->modules; entry; entry = entry->next) {
        if (strcmp(entry->module->name, module_name) == 0) {
            break;
        }
    }
    evpl_mutex_unlock(&identity->module_lock);

    /* A route to a module that is not loaded would silently strand that
     * domain's principals; the configuration is wrong, so say so. */
    chimera_vfs_abort_if(!entry, "identity domain %s routed to module %s, which is not loaded",
                         domain, module_name);
    chimera_vfs_abort_if(!(entry->module->capabilities & CHIMERA_VFS_IDENTITY_CAP_PRINCIPAL),
                         "identity domain %s routed to module %s, which does not map principals",
                         domain, module_name);

    entry->domains = realloc(entry->domains,
                             (entry->num_domains + 1) * sizeof(*entry->domains));
    chimera_vfs_abort_if(!entry->domains, "out of memory routing identity domain %s", domain);
    entry->domains[entry->num_domains] = strdup(domain);
    chimera_vfs_abort_if(!entry->domains[entry->num_domains],
                         "out of memory routing identity domain %s", domain);
    entry->num_domains++;

    chimera_vfs_info("identity module %s serves principals of domain %s",
                     module_name, domain);
} /* chimera_vfs_identity_add_domain */

SYMBOL_EXPORT void
chimera_vfs_identity_add_realm(
    struct chimera_vfs *vfs,
    const char         *realm,
    const char         *domain)
{
    struct chimera_vfs_identity       *identity = vfs->identity;
    struct chimera_vfs_identity_realm *entry;

    identity->realms = realloc(identity->realms,
                               (identity->num_realms + 1) * sizeof(*identity->realms));
    chimera_vfs_abort_if(!identity->realms, "out of memory mapping identity realm %s", realm);

    entry = &identity->realms[identity->num_realms++];
    snprintf(entry->realm, sizeof(entry->realm), "%s", realm);
    snprintf(entry->domain, sizeof(entry->domain), "%s", domain);

    chimera_vfs_info("identity realm %s maps to domain %s", realm, domain);
} /* chimera_vfs_identity_add_realm */

SYMBOL_EXPORT enum chimera_vfs_identity_status
chimera_vfs_identity_domain_info(
    struct chimera_vfs                      *vfs,
    struct chimera_vfs_identity_domain_info *out)
{
    struct chimera_vfs_identity              *identity = vfs->identity;
    struct chimera_vfs_identity_module_entry *entry;
    enum chimera_vfs_identity_status          status = CHIMERA_VFS_IDENTITY_NOT_MINE;

    evpl_mutex_lock(&identity->module_lock);
    entry = identity->modules;
    evpl_mutex_unlock(&identity->module_lock);

    for (; entry; entry = entry->next) {
        if (!(entry->module->capabilities & CHIMERA_VFS_IDENTITY_CAP_DOMAIN_INFO)) {
            continue;
        }
        memset(out, 0, sizeof(*out));
        status = entry->module->domain_info(entry->private_data, out);
        if (status == CHIMERA_VFS_IDENTITY_OK) {
            break;
        }
    }

    return status;
} /* chimera_vfs_identity_domain_info */

SYMBOL_EXPORT void
chimera_vfs_identity_resolve(
    struct chimera_vfs_thread    *thread,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name,
    chimera_vfs_identity_callback callback,
    void                         *private_data)
{
    struct chimera_vfs_identity         *identity = thread->vfs->identity;
    struct chimera_vfs_identity_result   hit;
    struct chimera_vfs_identity_request *req;

    memset(&hit, 0, sizeof(hit));

    /* Fast path: a cache hit resolves inline on this thread. */
    if (chimera_vfs_identity_cache_probe(thread->vfs->vfs_user_cache,
                                         key, id, name, &hit)) {
        callback(&hit, private_data);
        return;
    }

    /* Miss: dispatch to a worker and park (the callback fires on `thread`'s
     * evpl loop once the worker has resolved and cached the identity). */
    req               = calloc(1, sizeof(*req));
    req->origin       = thread;
    req->key          = key;
    req->id           = id;
    req->callback     = callback;
    req->private_data = private_data;
    if (name) {
        strncpy(req->name, name, sizeof(req->name) - 1);
    }

    evpl_mutex_lock(&identity->lock);
    DL_APPEND(identity->queue, req);
    evpl_cond_signal(&identity->cond);
    evpl_mutex_unlock(&identity->lock);
} /* chimera_vfs_identity_resolve */

SYMBOL_EXPORT enum chimera_vfs_identity_status
chimera_vfs_identity_lookup(
    struct chimera_vfs                 *vfs,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    memset(out, 0, sizeof(*out));

    return chimera_vfs_identity_run_lookup(vfs->identity, key, id, name, out);
} /* chimera_vfs_identity_lookup */

SYMBOL_EXPORT int
chimera_vfs_identity_cached(
    struct chimera_vfs           *vfs,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name)
{
    struct chimera_vfs_identity_result scratch;

    memset(&scratch, 0, sizeof(scratch));

    return chimera_vfs_identity_cache_probe(vfs->vfs_user_cache, key, id, name,
                                            &scratch);
} /* chimera_vfs_identity_cached */

SYMBOL_EXPORT void
chimera_vfs_identity_thread_complete(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_identity_request *jobs, *req;

    evpl_mutex_lock(&thread->lock);
    jobs                     = thread->pending_identity;
    thread->pending_identity = NULL;
    evpl_mutex_unlock(&thread->lock);

    while (jobs) {
        req = jobs;
        DL_DELETE(jobs, req);
        req->callback(req->found ? &req->result : NULL, req->private_data);
        free(req);
    }
} /* chimera_vfs_identity_thread_complete */
