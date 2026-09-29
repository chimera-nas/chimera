// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Winbind identity module: resolves uids, gids, names and real Active
 * Directory SIDs through winbindd (libwbclient), and reports the identity the
 * host is joined to its domain with.  Configured behind NSS; the engine
 * prefers this module's SID-bearing answer for a numeric key over NSS's
 * SID-less one.
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>
#include <wbclient.h>

#include "common/macros.h"
#include "vfs/sdk/vfs_identity_module.h"
#include "vfs/sdk/vfs_log.h"

#define chimera_winbind_debug(...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { \
                __chimera_debug("identity/winbind", __FILE__, __LINE__, __VA_ARGS__); \
            } \
}

#define chimera_winbind_error(...) { \
            if (ChimeraLogLevel >= CHIMERA_LOG_ERROR) { \
                __chimera_error("identity/winbind", __FILE__, __LINE__, __VA_ARGS__); \
            } \
}

/* Bounded copy that always terminates.  Deliberately not strncpy: gcc's
 * -Wstringop-truncation fires on a constant-bounded strncpy whose source may
 * fill the destination, and this file is built with -Werror. */
static void
wbc_copy_string(
    char       *dst,
    size_t      dst_len,
    const char *src)
{
    size_t len = 0;

    if (!dst || dst_len == 0) {
        return;
    }

    if (src) {
        len = strnlen(src, dst_len - 1);
        memcpy(dst, src, len);
    }
    dst[len] = '\0';
} /* wbc_copy_string */

/* Convert wbcDomainSid to string format */
static int
wbc_sid_to_string(
    const struct wbcDomainSid *sid,
    char                      *buf,
    size_t                     buf_len)
{
    wbcErr wbc_err;
    char  *sid_str = NULL;

    wbc_err = wbcSidToString(sid, &sid_str);
    if (wbc_err != WBC_ERR_SUCCESS || !sid_str) {
        return -1;
    }

    wbc_copy_string(buf, buf_len, sid_str);
    wbcFreeMemory(sid_str);
    return 0;
} /* wbc_sid_to_string */

/* A winbindd that is not answering is UNAVAILABLE; any other failure means
 * winbind does not know the key. */
static inline enum chimera_vfs_identity_status
wbc_miss_status(wbcErr wbc_err)
{
    return wbc_err == WBC_ERR_WINBIND_NOT_AVAILABLE ?
           CHIMERA_VFS_IDENTITY_UNAVAILABLE : CHIMERA_VFS_IDENTITY_NOT_MINE;
} /* wbc_miss_status */

/* Fill a user record (uid/gid/groups/name + real SID) from a resolved user
* SID.  Returns 0 on success, -1 if the SID does not map to a Unix user. */
static int
wbc_fill_user(
    const struct wbcDomainSid        *user_sid,
    const char                       *sidstr,
    struct chimera_vfs_identity_user *out)
{
    wbcErr               wbc_err;
    uint32_t             unix_uid;
    struct passwd       *pwd         = NULL;
    struct wbcDomainSid *groups_sids = NULL;
    uint32_t             num_groups  = 0;
    uint32_t             i;

    wbc_err = wbcSidToUid(user_sid, &unix_uid);
    if (wbc_err != WBC_ERR_SUCCESS) {
        return -1;
    }

    out->uid = unix_uid;
    if (sidstr) {
        wbc_copy_string(out->sid, sizeof(out->sid), sidstr);
    }

    /* Primary gid + name from the winbind passwd entry. */
    wbc_err = wbcGetpwuid(unix_uid, &pwd);
    if (wbc_err == WBC_ERR_SUCCESS && pwd) {
        out->gid = pwd->pw_gid;
        wbc_copy_string(out->username, sizeof(out->username), pwd->pw_name);
        out->username_len = (int) strlen(out->username);
        wbcFreeMemory(pwd);
    } else {
        out->gid = unix_uid;
    }

    /* Supplementary groups. */
    out->ngids = 0;
    wbc_err    = wbcLookupUserSids(user_sid, 0, &num_groups, &groups_sids);
    if (wbc_err == WBC_ERR_SUCCESS && num_groups > 0) {
        for (i = 0; i < num_groups && out->ngids < CHIMERA_VFS_CRED_MAX_GIDS; i++) {
            uint32_t group_gid;
            if (wbcSidToGid(&groups_sids[i], &group_gid) == WBC_ERR_SUCCESS) {
                out->gids[out->ngids++] = group_gid;
            }
        }
        wbcFreeMemory(groups_sids);
    }

    return 0;
} /* wbc_fill_user */

/* Fill a group record (gid/name + real SID) from a resolved group SID.
 * Returns 0 on success, -1 if the SID does not map to a Unix group. */
static int
wbc_fill_group(
    const struct wbcDomainSid         *group_sid,
    const char                        *sidstr,
    struct chimera_vfs_identity_group *out)
{
    wbcErr        wbc_err;
    uint32_t      unix_gid;
    struct group *grp = NULL;

    wbc_err = wbcSidToGid(group_sid, &unix_gid);
    if (wbc_err != WBC_ERR_SUCCESS) {
        return -1;
    }

    out->gid = unix_gid;
    if (sidstr) {
        wbc_copy_string(out->sid, sizeof(out->sid), sidstr);
    }

    /* Name from the winbind group entry (best effort -- the gid<->SID mapping
     * is what callers need). */
    wbc_err = wbcGetgrgid(unix_gid, &grp);
    if (wbc_err == WBC_ERR_SUCCESS && grp) {
        wbc_copy_string(out->groupname, sizeof(out->groupname), grp->gr_name);
        out->groupname_len = (int) strlen(out->groupname);
        wbcFreeMemory(grp);
    }

    return 0;
} /* wbc_fill_group */

/* Non-zero if `type` names a group rather than a user. */
static int
wbc_sid_type_is_group(enum wbcSidType type)
{
    return type == WBC_SID_NAME_DOM_GRP ||
           type == WBC_SID_NAME_ALIAS ||
           type == WBC_SID_NAME_WKN_GRP;
} /* wbc_sid_type_is_group */

static inline enum chimera_vfs_identity_status
wbc_fill_status(int rc)
{
    return rc == 0 ? CHIMERA_VFS_IDENTITY_OK : CHIMERA_VFS_IDENTITY_NOT_MINE;
} /* wbc_fill_status */

static enum chimera_vfs_identity_status
chimera_identity_winbind_lookup(
    void                               *private_data,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    wbcErr wbc_err;
    struct wbcDomainSid sid;
    enum wbcSidType     sid_type;
    char sidbuf[CHIMERA_VFS_IDENTITY_SID_MAX_LEN];
    char *lookup_domain = NULL, *lookup_name = NULL;
    int rc;

    (void) private_data;

    switch (key) {
        case CHIMERA_VFS_IDENTITY_BY_UID:
            wbc_err = wbcUidToSid(id, &sid);
            if (wbc_err != WBC_ERR_SUCCESS) {
                return wbc_miss_status(wbc_err);
            }
            if (wbc_sid_to_string(&sid, sidbuf, sizeof(sidbuf)) < 0) {
                return CHIMERA_VFS_IDENTITY_NOT_MINE;
            }
            return wbc_fill_status(wbc_fill_user(&sid, sidbuf, &out->user));

        case CHIMERA_VFS_IDENTITY_BY_GID:
            wbc_err = wbcGidToSid(id, &sid);
            if (wbc_err != WBC_ERR_SUCCESS) {
                return wbc_miss_status(wbc_err);
            }
            if (wbc_sid_to_string(&sid, sidbuf, sizeof(sidbuf)) < 0) {
                return CHIMERA_VFS_IDENTITY_NOT_MINE;
            }
            out->is_group = 1;
            return wbc_fill_status(wbc_fill_group(&sid, sidbuf, &out->group));

        case CHIMERA_VFS_IDENTITY_BY_SID:
            if (!name) {
                return CHIMERA_VFS_IDENTITY_NOT_MINE;
            }
            wbc_err = wbcStringToSid(name, &sid);
            if (wbc_err != WBC_ERR_SUCCESS) {
                return CHIMERA_VFS_IDENTITY_NOT_MINE;
            }
            /* A SID is ambiguous.  Ask winbind what it names before choosing
             * the fill: a group SID put through the user-shaped fill fails at
             * wbcSidToUid, which is why domain-group ACEs used to be dropped. */
            wbc_err = wbcLookupSid(&sid, &lookup_domain, &lookup_name,
                                   &sid_type);
            if (wbc_err == WBC_ERR_WINBIND_NOT_AVAILABLE) {
                return CHIMERA_VFS_IDENTITY_UNAVAILABLE;
            }
            if (wbc_err != WBC_ERR_SUCCESS) {
                /* Unknown to the DC: fall back to the historical user fill. */
                return wbc_fill_status(wbc_fill_user(&sid, name, &out->user));
            }
            if (lookup_domain) {
                wbcFreeMemory(lookup_domain);
            }
            if (lookup_name) {
                wbcFreeMemory(lookup_name);
            }
            if (wbc_sid_type_is_group(sid_type)) {
                out->is_group = 1;
                return wbc_fill_status(wbc_fill_group(&sid, name, &out->group));
            }
            return wbc_fill_status(wbc_fill_user(&sid, name, &out->user));

        case CHIMERA_VFS_IDENTITY_BY_NAME:
            if (!name) {
                return CHIMERA_VFS_IDENTITY_NOT_MINE;
            }
            wbc_err = wbcLookupName("", name, &sid, &sid_type);
            if (wbc_err != WBC_ERR_SUCCESS) {
                return wbc_miss_status(wbc_err);
            }
            if (wbc_sid_to_string(&sid, sidbuf, sizeof(sidbuf)) < 0) {
                sidbuf[0] = '\0';
            }
            if (wbc_sid_type_is_group(sid_type)) {
                out->is_group = 1;
                rc            = wbc_fill_group(&sid, sidbuf[0] ? sidbuf : NULL,
                                               &out->group);
                if (rc == 0 && !out->group.groupname[0]) {
                    wbc_copy_string(out->group.groupname,
                                    sizeof(out->group.groupname), name);
                    out->group.groupname_len = (int) strlen(out->group.groupname);
                }
                return wbc_fill_status(rc);
            }
            if (sid_type != WBC_SID_NAME_USER) {
                return CHIMERA_VFS_IDENTITY_NOT_MINE;
            }
            rc = wbc_fill_user(&sid, sidbuf[0] ? sidbuf : NULL, &out->user);
            if (rc == 0 && !out->user.username[0]) {
                wbc_copy_string(out->user.username,
                                sizeof(out->user.username), name);
                out->user.username_len = (int) strlen(out->user.username);
            }
            return wbc_fill_status(rc);

        default:
            return CHIMERA_VFS_IDENTITY_NOT_MINE;
    } /* switch */
} /* chimera_identity_winbind_lookup */

/* The NetBIOS identity winbind is joined with (name of the machine account,
 * short domain and DNS domain).  Domain controllers validate the NTLMv2
 * target info a pass-through logon carries against the machine account on the
 * netlogon channel, so the SMB CHALLENGE must advertise these names. */
static enum chimera_vfs_identity_status
chimera_identity_winbind_domain_info(
    void                                    *private_data,
    struct chimera_vfs_identity_domain_info *out)
{
    wbcErr wbc_err;
    struct wbcInterfaceDetails *details = NULL;

    (void) private_data;

    wbc_err = wbcInterfaceDetails(&details);
    if (wbc_err != WBC_ERR_SUCCESS || !details) {
        chimera_winbind_error("wbcInterfaceDetails failed: %s",
                              wbcErrorString(wbc_err));
        return wbc_miss_status(wbc_err);
    }

    wbc_copy_string(out->netbios_name, sizeof(out->netbios_name), details->netbios_name);
    wbc_copy_string(out->netbios_domain, sizeof(out->netbios_domain), details->netbios_domain);
    wbc_copy_string(out->dns_domain, sizeof(out->dns_domain), details->dns_domain);

    wbcFreeMemory(details);
    return CHIMERA_VFS_IDENTITY_OK;
} /* chimera_identity_winbind_domain_info */

SYMBOL_EXPORT struct chimera_vfs_identity_module identity_winbind = {
    .sdk_version  = CHIMERA_VFS_IDENTITY_SDK_VERSION,
    .name         = "winbind",
    .capabilities = CHIMERA_VFS_IDENTITY_CAP_LOOKUP |
        CHIMERA_VFS_IDENTITY_CAP_DOMAIN_INFO,
    .lookup      = chimera_identity_winbind_lookup,
    .domain_info = chimera_identity_winbind_domain_info,
};
