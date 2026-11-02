// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* A stand-in for libwbclient, linked into smb2_wire_probe_wbstub.  The
 * executable exports these definitions, so the server's calls into
 * libwbclient bind here instead of to the real library.  That takes both
 * halves: SYMBOL_EXPORT on each function, since the tree builds with
 * -fvisibility=hidden, and ENABLE_EXPORTS on the executable (CMakeLists.txt),
 * which puts them in its dynamic symbol table.
 *
 * It defines the calls smb_wbclient.c makes, and knows the one account in
 * wbclient_stub.h.  It answers for that account as a domain controller would:
 * the logon, and its user and primary group SIDs to and from their ids.
 * Every other lookup is not found. */

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
/* wbclient.h uses uint8_t, bool and uid_t without including their headers. */
#include <wbclient.h>

#include "common/crypto.h"
#include "common/export.h"
#include "wbclient_stub.h"
#include "smb2_mbt_wire.h"

SYMBOL_EXPORT wbcErr
wbcPing(void)
{
    return WBC_ERR_SUCCESS;
} /* wbcPing */

unsigned int wbstub_auth_calls;

/* The errors the stub returns, named, so a red transcript says which. */
SYMBOL_EXPORT const char *
wbcErrorString(wbcErr error)
{
    switch (error) {
        case WBC_ERR_SUCCESS:
            return "WBC_ERR_SUCCESS";
        case WBC_ERR_NO_MEMORY:
            return "wbclient stub: WBC_ERR_NO_MEMORY";
        case WBC_ERR_INVALID_SID:
            return "wbclient stub: WBC_ERR_INVALID_SID";
        case WBC_ERR_INVALID_PARAM:
            return "wbclient stub: WBC_ERR_INVALID_PARAM";
        case WBC_ERR_DOMAIN_NOT_FOUND:
            return "wbclient stub: WBC_ERR_DOMAIN_NOT_FOUND (not the stub's account)";
        case WBC_ERR_AUTH_ERROR:
            return "wbclient stub: WBC_ERR_AUTH_ERROR";
        default:
            return "wbclient stub: unexpected error";
    } /* switch */
} /* wbcErrorString */

SYMBOL_EXPORT void
wbcFreeMemory(void *p)
{
    free(p);
} /* wbcFreeMemory */

/* A logon refusal: WBC_ERR_AUTH_ERROR with an error record, one block, as the
 * caller frees it with wbcFreeMemory. */
static wbcErr
stub_auth_error(
    struct wbcAuthErrorInfo **error,
    const char               *msg)
{
    size_t                   len = strlen(msg) + 1;
    struct wbcAuthErrorInfo *e;

    if (error) {
        e = calloc(1, sizeof(*e) + len);
        if (e) {
            e->nt_status      = 0xc000006du; /* STATUS_LOGON_FAILURE */
            e->display_string = memcpy(e + 1, msg, len);
        }
        *error = e;
    }
    return WBC_ERR_AUTH_ERROR;
} /* stub_auth_error */

/* Defined below; wbcAuthenticateUserEx builds its SIDs with it. */
SYMBOL_EXPORT wbcErr wbcStringToSid(
    const char          *sid_string,
    struct wbcDomainSid *sid);

/* What the domain controller does for a network logon (MS-NLMP 3.3.2):
 * recompute NTProofStr = HMAC-MD5(NTOWFv2, ServerChallenge || blob) for the
 * one account the stub knows, and on a match hand back the session base key
 * HMAC-MD5(NTOWFv2, NTProofStr) as the user session key.  Only that: like a
 * real DC, winbind never sees the AUTHENTICATE's EncryptedRandomSessionKey, so
 * the KEY_EXCH unwrap stays the server's job.
 *
 * The check is not independent of the client: it uses the same smb2w_ntowfv2
 * and chimera_crypto_hmac the probe's client signs on with, so a bug in those
 * would cancel out here.  The extended tier's run against a real DC is what
 * checks them. */
SYMBOL_EXPORT wbcErr
wbcAuthenticateUserEx(
    const struct wbcAuthUserParams *params,
    struct wbcAuthUserInfo        **info,
    struct wbcAuthErrorInfo       **error)
{
    const uint8_t          *nt;
    uint32_t                nt_len;
    uint8_t                 ntowf[16], proof[16], hmac_in[8 + 1024];
    struct wbcAuthUserInfo *u;
    char                   *strings;
    size_t                  ulen, dlen;

    __atomic_add_fetch(&wbstub_auth_calls, 1, __ATOMIC_RELAXED);

    if (error) {
        *error = NULL;
    }
    if (!params || !info) {
        return WBC_ERR_INVALID_PARAM;
    }
    *info = NULL;

    if (params->level != WBC_AUTH_USER_LEVEL_RESPONSE ||
        !params->account_name || !params->domain_name ||
        strcasecmp(params->account_name, WBSTUB_USER) != 0 ||
        strcasecmp(params->domain_name, WBSTUB_DOMAIN) != 0) {
        return stub_auth_error(error, "wbclient stub: no such user");
    }

    nt     = params->password.response.nt_data;
    nt_len = params->password.response.nt_length;
    if (!nt || nt_len <= 16) {
        return stub_auth_error(error, "wbclient stub: not an NTLMv2 response");
    }
    if (nt_len - 16 > sizeof(hmac_in) - 8) {
        return stub_auth_error(error, "wbclient stub: NTLMv2 response too long");
    }

    smb2w_ntowfv2(params->account_name, WBSTUB_PASSWORD, params->domain_name,
                  ntowf);

    memcpy(hmac_in, params->password.response.challenge, 8);
    memcpy(hmac_in + 8, nt + 16, nt_len - 16);
    if (!chimera_crypto_hmac(CHIMERA_CRYPTO_HMAC_MD5, ntowf, 16, hmac_in,
                             8 + (nt_len - 16), proof, 16)) {
        return stub_auth_error(error, "wbclient stub: HMAC-MD5 failed");
    }
    if (chimera_crypto_compare(proof, nt, 16) != 0) {
        return stub_auth_error(error, "wbclient stub: wrong password");
    }

    /* One block: the struct, its two SIDs, then the strings. */
    ulen = strlen(params->account_name) + 1;
    dlen = strlen(params->domain_name) + 1;
    u    = calloc(1, sizeof(*u) + 2 * sizeof(struct wbcSidWithAttr) + ulen + dlen);
    if (!u) {
        return WBC_ERR_NO_MEMORY;
    }
    u->num_sids = 2;
    u->sids     = (struct wbcSidWithAttr *) (u + 1);
    if (wbcStringToSid(WBSTUB_USER_SID, &u->sids[0].sid) != WBC_ERR_SUCCESS ||
        wbcStringToSid(WBSTUB_GROUP_SID, &u->sids[1].sid) != WBC_ERR_SUCCESS) {
        free(u);
        return WBC_ERR_INVALID_SID;
    }
    strings         = (char *) (u->sids + 2);
    u->account_name = memcpy(strings, params->account_name, ulen);
    u->domain_name  = memcpy(strings + ulen, params->domain_name, dlen);

    if (!chimera_crypto_hmac(CHIMERA_CRYPTO_HMAC_MD5, ntowf, 16, proof, 16,
                             u->user_session_key, 16)) {
        free(u);
        return stub_auth_error(error, "wbclient stub: HMAC-MD5 failed");
    }

    *info = u;
    return WBC_ERR_SUCCESS;
} /* wbcAuthenticateUserEx */

SYMBOL_EXPORT wbcErr
wbcInterfaceDetails(struct wbcInterfaceDetails **details)
{
    static const char           name[] = WBSTUB_NETBIOS_NAME;
    static const char           dom[]  = WBSTUB_DOMAIN;
    static const char           dns[]  = WBSTUB_DNS_DOMAIN;
    struct wbcInterfaceDetails *d;
    char                       *p;

    d = calloc(1, sizeof(*d) + sizeof(name) + sizeof(dom) + sizeof(dns));
    if (!d) {
        *details = NULL;
        return WBC_ERR_NO_MEMORY;
    }
    p                    = (char *) (d + 1);
    d->netbios_name      = memcpy(p, name, sizeof(name));
    d->netbios_domain    = memcpy(p + sizeof(name), dom, sizeof(dom));
    d->dns_domain        = memcpy(p + sizeof(name) + sizeof(dom), dns, sizeof(dns));
    d->winbind_separator = '\\';
    *details             = d;
    return WBC_ERR_SUCCESS;
} /* wbcInterfaceDetails */

/* S-1-<authority>-<sub>... (MS-DTYP 2.4.2.1); authorities above 2^32 do not
 * occur here. */
SYMBOL_EXPORT wbcErr
wbcSidToString(
    const struct wbcDomainSid *sid,
    char                     **sid_string)
{
    /* "S-255-" + a 48-bit authority + 15 "-4294967295": 15 * 11 + 25, as
     * Samba sizes it (WBC_SID_STRING_BUFLEN). */
    char     buf[WBC_MAXSUBAUTHS * 11 + 25];
    uint64_t auth = 0;
    int      n, i;

    if (!sid_string) {
        return WBC_ERR_INVALID_PARAM;
    }
    *sid_string = NULL;
    if (!sid || sid->num_auths > WBC_MAXSUBAUTHS) {
        return WBC_ERR_INVALID_SID;
    }
    for (i = 0; i < 6; i++) {
        auth = (auth << 8) | sid->id_auth[i];
    }
    n = snprintf(buf, sizeof(buf), "S-%u-%llu", sid->sid_rev_num,
                 (unsigned long long) auth);
    for (i = 0; i < sid->num_auths && n > 0 && (size_t) n < sizeof(buf); i++) {
        n += snprintf(buf + n, sizeof(buf) - n, "-%u", sid->sub_auths[i]);
    }
    if (n < 0 || (size_t) n >= sizeof(buf)) {
        return WBC_ERR_INVALID_SID;
    }
    *sid_string = strdup(buf);
    return *sid_string ? WBC_ERR_SUCCESS : WBC_ERR_NO_MEMORY;
} /* wbcSidToString */

SYMBOL_EXPORT wbcErr
wbcStringToSid(
    const char          *sid_string,
    struct wbcDomainSid *sid)
{
    const char        *p;
    char              *end;
    unsigned long long v;
    int                i;

    if (!sid_string || strncmp(sid_string, "S-1-", 4) != 0) {
        return WBC_ERR_INVALID_SID;
    }
    memset(sid, 0, sizeof(*sid));
    sid->sid_rev_num = 1;

    /* Each field must start with a digit: strtoull alone would also take
     * leading space, a sign, or an empty field before a '-'. */
    if (!isdigit((unsigned char) sid_string[4])) {
        return WBC_ERR_INVALID_SID;
    }
    v = strtoull(sid_string + 4, &end, 10);
    if (end == sid_string + 4 || v > 0xffffffffull) {
        return WBC_ERR_INVALID_SID;
    }
    for (i = 0; i < 6; i++) {
        sid->id_auth[5 - i] = (uint8_t) (v >> (8 * i));
    }

    for (p = end; *p == '-'; p = end) {
        if (sid->num_auths == WBC_MAXSUBAUTHS || !isdigit((unsigned char) p[1])) {
            return WBC_ERR_INVALID_SID;
        }
        v = strtoull(p + 1, &end, 10);
        if (end == p + 1 || v > 0xffffffffull) {
            return WBC_ERR_INVALID_SID;
        }
        sid->sub_auths[sid->num_auths++] = (uint32_t) v;
    }
    return *p == '\0' ? WBC_ERR_SUCCESS : WBC_ERR_INVALID_SID;
} /* wbcStringToSid */

static int
stub_sid_is(
    const struct wbcDomainSid *sid,
    const char                *str)
{
    struct wbcDomainSid want;

    return sid && wbcStringToSid(str, &want) == WBC_ERR_SUCCESS &&
           memcmp(sid, &want, sizeof(want)) == 0;
} /* stub_sid_is */

SYMBOL_EXPORT wbcErr
wbcSidToUid(
    const struct wbcDomainSid *sid,
    uid_t                     *puid)
{
    if (!stub_sid_is(sid, WBSTUB_USER_SID)) {
        return WBC_ERR_DOMAIN_NOT_FOUND;
    }
    *puid = WBSTUB_UID;
    return WBC_ERR_SUCCESS;
} /* wbcSidToUid */

SYMBOL_EXPORT wbcErr
wbcSidToGid(
    const struct wbcDomainSid *sid,
    gid_t                     *pgid)
{
    if (!stub_sid_is(sid, WBSTUB_GROUP_SID)) {
        return WBC_ERR_DOMAIN_NOT_FOUND;
    }
    *pgid = WBSTUB_GID;
    return WBC_ERR_SUCCESS;
} /* wbcSidToGid */

SYMBOL_EXPORT wbcErr
wbcUidToSid(
    uid_t                uid,
    struct wbcDomainSid *sid)
{
    if (uid != WBSTUB_UID) {
        return WBC_ERR_DOMAIN_NOT_FOUND;
    }
    return wbcStringToSid(WBSTUB_USER_SID, sid);
} /* wbcUidToSid */

SYMBOL_EXPORT wbcErr
wbcGidToSid(
    gid_t                gid,
    struct wbcDomainSid *sid)
{
    if (gid != WBSTUB_GID) {
        return WBC_ERR_DOMAIN_NOT_FOUND;
    }
    return wbcStringToSid(WBSTUB_GROUP_SID, sid);
} /* wbcGidToSid */

SYMBOL_EXPORT wbcErr
wbcLookupUserSids(
    const struct wbcDomainSid *user_sid,
    bool                       domain_groups_only,
    uint32_t                  *num_sids,
    struct wbcDomainSid      **sids)
{
    (void) domain_groups_only;
    *num_sids = 0;
    *sids     = NULL;
    if (!stub_sid_is(user_sid, WBSTUB_USER_SID)) {
        return WBC_ERR_DOMAIN_NOT_FOUND;
    }
    *sids = malloc(sizeof(**sids));
    if (!*sids) {
        return WBC_ERR_NO_MEMORY;
    }
    if (wbcStringToSid(WBSTUB_GROUP_SID, *sids) != WBC_ERR_SUCCESS) {
        free(*sids);
        *sids = NULL;
        return WBC_ERR_INVALID_SID;
    }
    *num_sids = 1;
    return WBC_ERR_SUCCESS;
} /* wbcLookupUserSids */

SYMBOL_EXPORT wbcErr
wbcLookupName(
    const char          *dom_name,
    const char          *name,
    struct wbcDomainSid *sid,
    enum wbcSidType     *name_type)
{
    (void) dom_name;
    (void) name;
    (void) sid;
    (void) name_type;
    return WBC_ERR_DOMAIN_NOT_FOUND;
} /* wbcLookupName */

SYMBOL_EXPORT wbcErr
wbcLookupSid(
    const struct wbcDomainSid *sid,
    char                     **domain,
    char                     **name,
    enum wbcSidType           *name_type)
{
    (void) sid;
    (void) name_type;
    *domain = NULL;
    *name   = NULL;
    return WBC_ERR_DOMAIN_NOT_FOUND;
} /* wbcLookupSid */

SYMBOL_EXPORT wbcErr
wbcGetpwuid(
    uid_t           uid,
    struct passwd **pwd)
{
    (void) uid;
    if (!pwd) {
        return WBC_ERR_INVALID_PARAM;
    }
    *pwd = NULL;
    return WBC_ERR_DOMAIN_NOT_FOUND;
} /* wbcGetpwuid */

SYMBOL_EXPORT wbcErr
wbcGetgrgid(
    gid_t          gid,
    struct group **grp)
{
    (void) gid;
    if (!grp) {
        return WBC_ERR_INVALID_PARAM;
    }
    *grp = NULL;
    return WBC_ERR_DOMAIN_NOT_FOUND;
} /* wbcGetgrgid */
