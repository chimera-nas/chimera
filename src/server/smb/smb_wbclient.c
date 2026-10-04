// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_wbclient.h"
#include "smb_internal.h"

#ifdef HAVE_WBCLIENT

#include <wbclient.h>
#include <string.h>

SYMBOL_EXPORT int
smb_wbclient_available(void)
{
    wbcErr wbc_err;

    // Try pinging winbind to check availability
    wbc_err = wbcPing();

    return (wbc_err == WBC_ERR_SUCCESS) ? 1 : 0;
} // smb_wbclient_available

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
} // wbc_copy_string

// Convert wbcDomainSid to string format
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
} // wbc_sid_to_string

int
smb_wbclient_auth_ntlm(
    const char    *username,
    const char    *domain,
    const char    *workstation,
    const uint8_t *challenge,
    const uint8_t *lm_response,
    size_t         lm_response_len,
    const uint8_t *nt_response,
    size_t         nt_response_len,
    uint32_t      *uid,
    uint32_t      *gid,
    uint32_t      *ngids,
    uint32_t      *gids,
    char          *sid_out,
    char          *group_sid_out,
    uint8_t       *session_key)
{
    wbcErr                   wbc_err;
    struct wbcAuthUserParams params;
    struct wbcAuthUserInfo  *info  = NULL;
    struct wbcAuthErrorInfo *error = NULL;
    struct wbcDomainSid      user_sid;
    uint32_t                 unix_uid, unix_gid;
    struct wbcDomainSid     *groups_sids = NULL;
    uint32_t                 num_groups  = 0;
    uint32_t                 i;
    const uint8_t           *lm_data;
    uint32_t                 lm_len;

    memset(&params, 0, sizeof(params));

    params.level            = WBC_AUTH_USER_LEVEL_RESPONSE;
    params.account_name     = username;
    params.domain_name      = domain;
    params.workstation_name = workstation ? workstation : "UNKNOWN";

    /* Same MSV1_0 parameter control smbd passes for network logons; some
     * domain controllers reject requests without these bits. */
    params.parameter_control = WBC_MSV1_0_ALLOW_SERVER_TRUST_ACCOUNT |
        WBC_MSV1_0_ALLOW_WORKSTATION_TRUST_ACCOUNT;

    // Set up challenge/response authentication
    memcpy(params.password.response.challenge, challenge, 8);

    smb_wbclient_lm_response(lm_response, lm_response_len, &lm_data, &lm_len);

    params.password.response.lm_length = lm_len;
    params.password.response.lm_data   = (uint8_t *) lm_data;

    params.password.response.nt_length = nt_response_len;
    params.password.response.nt_data   = (uint8_t *) nt_response;

    wbc_err = wbcAuthenticateUserEx(&params, &info, &error);

    if (wbc_err != WBC_ERR_SUCCESS) {
        chimera_smb_error("wbcAuthenticateUserEx failed: %s",
                          error ? error->display_string : wbcErrorString(wbc_err));
        if (error) {
            wbcFreeMemory(error);
        }
        return -1;
    }

    // Get the user SID from info and convert to Unix UID
    user_sid = info->sids[0].sid;

    // Output the SID string if requested
    if (sid_out) {
        if (wbc_sid_to_string(&user_sid, sid_out, SMB_WBCLIENT_SID_MAX_LEN) < 0) {
            sid_out[0] = '\0';
        }
    }

    wbc_err = wbcSidToUid(&user_sid, &unix_uid);
    if (wbc_err != WBC_ERR_SUCCESS) {
        chimera_smb_error("wbcSidToUid failed: %s", wbcErrorString(wbc_err));
        wbcFreeMemory(info);
        return -1;
    }

    // Get primary group SID and convert to Unix GID.  The SID goes out with
    // the gid it mapped to, and only then: a SID beside a gid it does not
    // describe is worse than none.
    if (group_sid_out) {
        group_sid_out[0] = '\0';
    }
    if (info->num_sids > 1) {
        // The second SID is typically the primary group
        wbc_err = wbcSidToGid(&info->sids[1].sid, &unix_gid);
        if (wbc_err != WBC_ERR_SUCCESS) {
            // Fall back to the user's UID as GID
            unix_gid = unix_uid;
        } else if (group_sid_out &&
                   wbc_sid_to_string(&info->sids[1].sid, group_sid_out,
                                     SMB_WBCLIENT_SID_MAX_LEN) < 0) {
            group_sid_out[0] = '\0';
        }
    } else {
        unix_gid = unix_uid;
    }

    *uid = unix_uid;
    *gid = unix_gid;

    // Get supplementary groups
    wbc_err = wbcLookupUserSids(&user_sid, 0, &num_groups, &groups_sids);
    if (wbc_err == WBC_ERR_SUCCESS && num_groups > 0) {
        *ngids = 0;
        for (i = 0; i < num_groups && *ngids < SMB_WBCLIENT_MAX_GROUPS; i++) {
            uint32_t group_gid;
            wbc_err = wbcSidToGid(&groups_sids[i], &group_gid);
            if (wbc_err == WBC_ERR_SUCCESS) {
                gids[(*ngids)++] = group_gid;
            }
        }
        wbcFreeMemory(groups_sids);
    } else {
        *ngids = 0;
    }

    // Copy session key if requested
    if (session_key) {
        memcpy(session_key, info->user_session_key, 16);
    }

    chimera_smb_info("wbclient auth success: user=%s\\%s uid=%u gid=%u ngids=%u",
                     domain, username, *uid, *gid, *ngids);

    wbcFreeMemory(info);
    return 0;
} // smb_wbclient_auth_ntlm

int
smb_wbclient_auth_password(
    const char *username,
    const char *domain,
    const char *password,
    uint32_t   *uid,
    uint32_t   *gid,
    uint32_t   *ngids,
    uint32_t   *gids,
    char       *sid_out)
{
    wbcErr                   wbc_err;
    struct wbcAuthUserParams params;
    struct wbcAuthUserInfo  *info  = NULL;
    struct wbcAuthErrorInfo *error = NULL;
    struct wbcDomainSid      user_sid;
    uint32_t                 unix_uid, unix_gid;
    struct wbcDomainSid     *groups_sids = NULL;
    uint32_t                 num_groups  = 0;
    uint32_t                 i;

    memset(&params, 0, sizeof(params));

    params.level              = WBC_AUTH_USER_LEVEL_PLAIN;
    params.account_name       = username;
    params.domain_name        = domain ? domain : "";
    params.password.plaintext = password;

    wbc_err = wbcAuthenticateUserEx(&params, &info, &error);

    if (wbc_err != WBC_ERR_SUCCESS) {
        chimera_smb_error("wbcAuthenticateUserEx (plain) failed: %s",
                          error ? error->display_string : wbcErrorString(
                              wbc_err));
        if (error) {
            wbcFreeMemory(error);
        }
        return -1;
    }

    user_sid = info->sids[0].sid;

    if (sid_out) {
        if (wbc_sid_to_string(&user_sid, sid_out,
                              SMB_WBCLIENT_SID_MAX_LEN) < 0) {
            sid_out[0] = '\0';
        }
    }

    wbc_err = wbcSidToUid(&user_sid, &unix_uid);
    if (wbc_err != WBC_ERR_SUCCESS) {
        chimera_smb_error("wbcSidToUid failed: %s", wbcErrorString(wbc_err));
        wbcFreeMemory(info);
        return -1;
    }

    if (info->num_sids > 1) {
        wbc_err = wbcSidToGid(&info->sids[1].sid, &unix_gid);
        if (wbc_err != WBC_ERR_SUCCESS) {
            unix_gid = unix_uid;
        }
    } else {
        unix_gid = unix_uid;
    }

    *uid = unix_uid;
    *gid = unix_gid;

    wbc_err = wbcLookupUserSids(&user_sid, 0, &num_groups, &groups_sids);
    if (wbc_err == WBC_ERR_SUCCESS && num_groups > 0) {
        *ngids = 0;
        for (i = 0; i < num_groups && *ngids < SMB_WBCLIENT_MAX_GROUPS; i++) {
            uint32_t group_gid;
            wbc_err = wbcSidToGid(&groups_sids[i], &group_gid);
            if (wbc_err == WBC_ERR_SUCCESS) {
                gids[(*ngids)++] = group_gid;
            }
        }
        wbcFreeMemory(groups_sids);
    } else {
        *ngids = 0;
    }

    chimera_smb_info(
        "wbclient plain auth success: user=%s\\%s uid=%u gid=%u ngids=%u",
        domain ? domain : "", username, *uid, *gid, *ngids);

    wbcFreeMemory(info);
    return 0;
} // smb_wbclient_auth_password

#else // HAVE_WBCLIENT

// Stub implementations when libwbclient is not available

SYMBOL_EXPORT int
smb_wbclient_available(void)
{
    return 0;
} // smb_wbclient_available

int
smb_wbclient_auth_ntlm(
    const char    *username,
    const char    *domain,
    const char    *workstation,
    const uint8_t *challenge,
    const uint8_t *lm_response,
    size_t         lm_response_len,
    const uint8_t *nt_response,
    size_t         nt_response_len,
    uint32_t      *uid,
    uint32_t      *gid,
    uint32_t      *ngids,
    uint32_t      *gids,
    char          *sid_out,
    char          *group_sid_out,
    uint8_t       *session_key)
{
    (void) username;
    (void) domain;
    (void) workstation;
    (void) challenge;
    (void) lm_response;
    (void) lm_response_len;
    (void) nt_response;
    (void) nt_response_len;
    (void) uid;
    (void) gid;
    (void) ngids;
    (void) gids;
    (void) sid_out;
    (void) group_sid_out;
    (void) session_key;

    return -1;
} // smb_wbclient_auth_ntlm

int
smb_wbclient_auth_password(
    const char *username,
    const char *domain,
    const char *password,
    uint32_t   *uid,
    uint32_t   *gid,
    uint32_t   *ngids,
    uint32_t   *gids,
    char       *sid_out)
{
    (void) username;
    (void) domain;
    (void) password;
    (void) uid;
    (void) gid;
    (void) ngids;
    (void) gids;
    (void) sid_out;

    return -1;
} // smb_wbclient_auth_password

#endif // HAVE_WBCLIENT
