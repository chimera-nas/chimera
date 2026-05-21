// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>

#include "vfs_idmap.h"
#include "common/macros.h"

/* Well-known SID strings used for the special-whos. */
#define SID_EVERYONE      "S-1-1-0"  /* World            */
#define SID_CREATOR_OWNER "S-1-3-0"  /* CREATOR OWNER    */
#define SID_CREATOR_GROUP "S-1-3-1"  /* CREATOR GROUP    */
#define SID_OWNER_RIGHTS  "S-1-3-4"  /* OWNER RIGHTS     */
#define SID_SYSTEM        "S-1-5-18" /* NT AUTHORITY\SYSTEM */
#define SID_AUTHENTICATED "S-1-5-11" /* Authenticated    */
#define SID_NETWORK       "S-1-5-2"  /* Network          */
#define SID_INTERACTIVE   "S-1-5-4"  /* Interactive      */
#define SID_ANONYMOUS     "S-1-5-7"  /* Anonymous        */

static const char *
special_to_who_string(uint8_t special)
{
    switch (special) {
        case CHIMERA_WHO_OWNER:         return "OWNER@";
        case CHIMERA_WHO_GROUP:         return "GROUP@";
        case CHIMERA_WHO_EVERYONE:      return "EVERYONE@";
        case CHIMERA_WHO_INTERACTIVE:   return "INTERACTIVE@";
        case CHIMERA_WHO_NETWORK:       return "NETWORK@";
        case CHIMERA_WHO_AUTHENTICATED: return "AUTHENTICATED@";
        case CHIMERA_WHO_ANONYMOUS:     return "ANONYMOUS@";
        default:                        return NULL;
    } /* switch */
} /* special_to_who_string */

/* Match a NUL-terminated special who string; returns special id or -1. */
static int
who_string_to_special(
    const char *who,
    int         len)
{
    static const struct {
        const char *name;
        uint8_t     special;
    } table[] = {
        { "OWNER@",         CHIMERA_WHO_OWNER                                                 },
        { "GROUP@",         CHIMERA_WHO_GROUP                                                 },
        { "EVERYONE@",      CHIMERA_WHO_EVERYONE                                              },
        { "INTERACTIVE@",   CHIMERA_WHO_INTERACTIVE                                           },
        { "NETWORK@",       CHIMERA_WHO_NETWORK                                               },
        { "AUTHENTICATED@", CHIMERA_WHO_AUTHENTICATED                                         },
        { "ANONYMOUS@",     CHIMERA_WHO_ANONYMOUS                                             },
    };

    for (unsigned i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
        int nlen = (int) strlen(table[i].name);

        if (nlen == len && memcmp(who, table[i].name, len) == 0) {
            return table[i].special;
        }
    }
    return -1;
} /* who_string_to_special */

SYMBOL_EXPORT int
chimera_idmap_principal_to_who(
    const struct chimera_principal *p,
    const char                     *domain,
    char                           *buf,
    int                             buflen)
{
    int len;

    if (p->type == CHIMERA_PRINCIPAL_SPECIAL) {
        const char *s = special_to_who_string(p->special);

        if (!s) {
            return -1;
        }
        len = (int) strlen(s);
        if (len + 1 > buflen) {
            return -1;
        }
        memcpy(buf, s, len + 1);
        return len;
    }

    /* Numeric user/group.  Resolve to name@domain when a domain is configured
     * and nsswitch knows the id; otherwise emit the numeric form, which is a
     * valid NFSv4 who string (RFC 8881 section 5.9). */
    if (domain && domain[0]) {
        char tmp[CHIMERA_IDMAP_WHO_MAX];
        char namebuf[512];

        if (p->type == CHIMERA_PRINCIPAL_USER) {
            struct passwd pw, *res = NULL;

            if (getpwuid_r(p->id, &pw, namebuf, sizeof(namebuf), &res) == 0 &&
                res) {
                len = snprintf(tmp, sizeof(tmp), "%s@%s", pw.pw_name, domain);
                if (len > 0 && len + 1 <= buflen) {
                    memcpy(buf, tmp, len + 1);
                    return len;
                }
            }
        } else {
            struct group gr, *res = NULL;

            if (getgrgid_r(p->id, &gr, namebuf, sizeof(namebuf), &res) == 0 &&
                res) {
                len = snprintf(tmp, sizeof(tmp), "%s@%s", gr.gr_name, domain);
                if (len > 0 && len + 1 <= buflen) {
                    memcpy(buf, tmp, len + 1);
                    return len;
                }
            }
        }
    }

    len = snprintf(buf, buflen, "%u", p->id);
    if (len < 0 || len + 1 > buflen) {
        return -1;
    }
    return len;
} /* chimera_idmap_principal_to_who */

/* All-decimal check over [s, s+len). */
static int
all_digits(
    const char *s,
    int         len)
{
    if (len <= 0) {
        return 0;
    }
    for (int i = 0; i < len; i++) {
        if (s[i] < '0' || s[i] > '9') {
            return 0;
        }
    }
    return 1;
} /* all_digits */

SYMBOL_EXPORT int
chimera_idmap_who_to_principal(
    const char               *who,
    int                       len,
    int                       is_group,
    const char               *domain,
    struct chimera_principal *p)
{
    int         special;
    const char *at;

    if (len <= 0) {
        return -1;
    }

    special = who_string_to_special(who, len);
    if (special >= 0) {
        p->type    = CHIMERA_PRINCIPAL_SPECIAL;
        p->special = (uint8_t) special;
        p->id      = 0;
        return 0;
    }

    /* Pure numeric id. */
    if (all_digits(who, len)) {
        p->type    = is_group ? CHIMERA_PRINCIPAL_GROUP : CHIMERA_PRINCIPAL_USER;
        p->special = 0;
        p->id      = (uint32_t) strtoul(who, NULL, 10);
        return 0;
    }

    /* name@domain: resolve the local part via nsswitch. */
    at = memchr(who, '@', len);
    if (at) {
        char name[256];
        int  nlen = (int) (at - who);
        char namebuf[1024];

        (void) domain;
        if (nlen <= 0 || nlen >= (int) sizeof(name)) {
            return -1;
        }
        memcpy(name, who, nlen);
        name[nlen] = '\0';

        if (is_group) {
            struct group gr, *res = NULL;

            if (getgrnam_r(name, &gr, namebuf, sizeof(namebuf), &res) == 0 &&
                res) {
                p->type    = CHIMERA_PRINCIPAL_GROUP;
                p->special = 0;
                p->id      = gr.gr_gid;
                return 0;
            }
        } else {
            struct passwd pw, *res = NULL;

            if (getpwnam_r(name, &pw, namebuf, sizeof(namebuf), &res) == 0 &&
                res) {
                p->type    = CHIMERA_PRINCIPAL_USER;
                p->special = 0;
                p->id      = pw.pw_uid;
                return 0;
            }
        }
    }

    return -1;
} /* chimera_idmap_who_to_principal */

SYMBOL_EXPORT int
chimera_idmap_principal_to_sid(
    const struct chimera_principal *p,
    char                           *buf,
    int                             buflen)
{
    const char *s = NULL;
    int         len;

    if (p->type == CHIMERA_PRINCIPAL_SPECIAL) {
        switch (p->special) {
            /* OWNER@/GROUP@ have no standalone SID -- they denote the object's
             * current owner/owning-group and the SD emitter substitutes the
             * concrete owner SID.  Only the CREATOR placeholders map to the
             * S-1-3 well-known SIDs. */
            case CHIMERA_WHO_CREATOR_OWNER: s = SID_CREATOR_OWNER; break;
            case CHIMERA_WHO_CREATOR_GROUP: s = SID_CREATOR_GROUP; break;
            case CHIMERA_WHO_OWNER_RIGHTS:  s = SID_OWNER_RIGHTS; break;
            case CHIMERA_WHO_SYSTEM:        s = SID_SYSTEM; break;
            case CHIMERA_WHO_EVERYONE:      s = SID_EVERYONE; break;
            case CHIMERA_WHO_INTERACTIVE:   s = SID_INTERACTIVE; break;
            case CHIMERA_WHO_NETWORK:       s = SID_NETWORK; break;
            case CHIMERA_WHO_AUTHENTICATED: s = SID_AUTHENTICATED; break;
            case CHIMERA_WHO_ANONYMOUS:     s = SID_ANONYMOUS; break;
            default:                        return -1;
        } /* switch */
        len = (int) strlen(s);
        if (len + 1 > buflen) {
            return -1;
        }
        memcpy(buf, s, len + 1);
        return len;
    }

    /* Unix uid/gid SIDs in the modefromsid scheme (S-1-5-88-1/2-<id>), matching
     * the owner/group SIDs the SMB security-descriptor emitter writes for the
     * SD owner and group fields, so a uid appears as the same SID throughout a
     * descriptor.  sid_to_principal parses both this and the S-1-22 form. */
    if (p->type == CHIMERA_PRINCIPAL_USER) {
        len = snprintf(buf, buflen, "S-1-5-88-1-%u", p->id);
    } else {
        len = snprintf(buf, buflen, "S-1-5-88-2-%u", p->id);
    }

    if (len < 0 || len + 1 > buflen) {
        return -1;
    }
    return len;
} /* chimera_idmap_principal_to_sid */

SYMBOL_EXPORT int
chimera_idmap_sid_to_principal(
    const char               *sid,
    struct chimera_principal *p)
{
    /* Well-known special SIDs. */
    if (strcmp(sid, SID_EVERYONE) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_EVERYONE);
        return 0;
    }
    if (strcmp(sid, SID_CREATOR_OWNER) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_CREATOR_OWNER);
        return 0;
    }
    if (strcmp(sid, SID_CREATOR_GROUP) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_CREATOR_GROUP);
        return 0;
    }
    if (strcmp(sid, SID_OWNER_RIGHTS) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_OWNER_RIGHTS);
        return 0;
    }
    if (strcmp(sid, SID_SYSTEM) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_SYSTEM);
        return 0;
    }
    if (strcmp(sid, SID_AUTHENTICATED) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_AUTHENTICATED);
        return 0;
    }
    if (strcmp(sid, SID_NETWORK) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_NETWORK);
        return 0;
    }
    if (strcmp(sid, SID_INTERACTIVE) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_INTERACTIVE);
        return 0;
    }
    if (strcmp(sid, SID_ANONYMOUS) == 0) {
        *p = chimera_idmap_special_principal(CHIMERA_WHO_ANONYMOUS);
        return 0;
    }

    /* Samba unix-id SIDs and modefromsid (S-1-5-88-1/2) SIDs. */
    {
        unsigned long id;

        if (sscanf(sid, "S-1-22-1-%lu", &id) == 1) {
            *p = chimera_idmap_uid_principal((uint32_t) id);
            return 0;
        }
        if (sscanf(sid, "S-1-22-2-%lu", &id) == 1) {
            *p = chimera_idmap_gid_principal((uint32_t) id);
            return 0;
        }
        if (sscanf(sid, "S-1-5-88-1-%lu", &id) == 1) {
            *p = chimera_idmap_uid_principal((uint32_t) id);
            return 0;
        }
        if (sscanf(sid, "S-1-5-88-2-%lu", &id) == 1) {
            *p = chimera_idmap_gid_principal((uint32_t) id);
            return 0;
        }
    }

    return -1;
} /* chimera_idmap_sid_to_principal */
