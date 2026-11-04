// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "vfs_acl_windows.h"
#include "sdk/vfs_sid.h"

/*
 * The algorithmic modefromsid SIDs are S-1-5-88-<kind>-<id>: revision 1, three
 * sub-authorities (88, kind, id), identifier authority 5.  They are encoded and
 * decoded here in binary form, which is the layout chimera_sid carries.
 */
#define ALGO_SID_SUBAUTHS 3
#define ALGO_SID_LEN      (CHIMERA_SID_MIN_LEN + 4 * ALGO_SID_SUBAUTHS)
#define ALGO_SID_PREFIX   88
#define ALGO_SID_KIND_UID 1
#define ALGO_SID_KIND_GID 2

static inline void
put_le32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = (uint8_t) v;
    p[1] = (uint8_t) (v >> 8);
    p[2] = (uint8_t) (v >> 16);
    p[3] = (uint8_t) (v >> 24);
} /* put_le32 */

static inline uint32_t
get_le32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* get_le32 */

/* Rebuild `p` as a PRINCIPAL_SID carrying the algorithmic SID of kind/id. */
static void
algo_sid_principal(
    struct chimera_principal *p,
    uint32_t                  kind,
    uint32_t                  id)
{
    memset(p, 0, sizeof(*p));
    p->type        = CHIMERA_PRINCIPAL_SID;
    p->sid.len     = ALGO_SID_LEN;
    p->sid.data[0] = 1;
    p->sid.data[1] = ALGO_SID_SUBAUTHS;
    p->sid.data[7] = 5;
    put_le32(&p->sid.data[8], ALGO_SID_PREFIX);
    put_le32(&p->sid.data[12], kind);
    put_le32(&p->sid.data[16], id);
} /* algo_sid_principal */

/* Decode an algorithmic SID; returns 1 and sets kind/id, or 0 if it is not one. */
static int
algo_sid_decode(
    const struct chimera_sid *sid,
    uint32_t                 *kind,
    uint32_t                 *id)
{
    static const uint8_t authority[6] = { 0, 0, 0, 0, 0, 5 };

    if (sid->len != ALGO_SID_LEN ||
        sid->data[0] != 1 ||
        sid->data[1] != ALGO_SID_SUBAUTHS ||
        memcmp(&sid->data[2], authority, sizeof(authority)) != 0 ||
        get_le32(&sid->data[8]) != ALGO_SID_PREFIX) {
        return 0;
    }

    *kind = get_le32(&sid->data[12]);
    *id   = get_le32(&sid->data[16]);

    return *kind == ALGO_SID_KIND_UID || *kind == ALGO_SID_KIND_GID;
} /* algo_sid_decode */

SYMBOL_EXPORT int
chimera_vfs_acl_windows_strip(struct chimera_acl *acl)
{
    int numeric_only = 0;

    for (unsigned i = 0; i < acl->num_aces; i++) {
        struct chimera_principal *who = &acl->aces[i].who;

        if (who->type != CHIMERA_PRINCIPAL_USER &&
            who->type != CHIMERA_PRINCIPAL_GROUP) {
            continue;
        }

        if (chimera_sid_present(&who->sid)) {
            struct chimera_sid sid = who->sid;

            memset(who, 0, sizeof(*who));
            who->type = CHIMERA_PRINCIPAL_SID;
            who->sid  = sid;
        } else {
            algo_sid_principal(who,
                               who->type == CHIMERA_PRINCIPAL_USER ?
                               ALGO_SID_KIND_UID : ALGO_SID_KIND_GID,
                               who->id);
            numeric_only++;
        }
    }

    return numeric_only;
} /* chimera_vfs_acl_windows_strip */

SYMBOL_EXPORT int
chimera_vfs_acl_windows_enrich_sync(struct chimera_acl *acl)
{
    int remaining = 0;

    for (unsigned i = 0; i < acl->num_aces; i++) {
        struct chimera_ace *ace = &acl->aces[i];
        uint32_t            kind, id;

        if (ace->who.type != CHIMERA_PRINCIPAL_SID) {
            continue;
        }

        if (!algo_sid_decode(&ace->who.sid, &kind, &id)) {
            remaining++;
            continue;
        }

        memset(&ace->who, 0, sizeof(ace->who));
        ace->who.id = id;
        if (kind == ALGO_SID_KIND_GID) {
            ace->who.type = CHIMERA_PRINCIPAL_GROUP;
            ace->flags   |= CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
        } else {
            ace->who.type = CHIMERA_PRINCIPAL_USER;
            ace->flags   &= ~CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
        }
    }

    return remaining;
} /* chimera_vfs_acl_windows_enrich_sync */
