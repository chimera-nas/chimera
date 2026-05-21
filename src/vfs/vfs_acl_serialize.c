// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include "vfs_acl_serialize.h"
#include "common/macros.h"

static inline void
put_u8(
    uint8_t **p,
    uint8_t   v)
{
    *(*p)++ = v;
} /* put_u8 */

static inline void
put_u16(
    uint8_t **p,
    uint16_t  v)
{
    (*p)[0] = (uint8_t) (v & 0xff);
    (*p)[1] = (uint8_t) ((v >> 8) & 0xff);
    *p     += 2;
} /* put_u16 */

static inline void
put_u32(
    uint8_t **p,
    uint32_t  v)
{
    (*p)[0] = (uint8_t) (v & 0xff);
    (*p)[1] = (uint8_t) ((v >> 8) & 0xff);
    (*p)[2] = (uint8_t) ((v >> 16) & 0xff);
    (*p)[3] = (uint8_t) ((v >> 24) & 0xff);
    *p     += 4;
} /* put_u32 */

static inline uint8_t
get_u8(const uint8_t **p)
{
    return *(*p)++;
} /* get_u8 */

static inline uint16_t
get_u16(const uint8_t **p)
{
    uint16_t v = (uint16_t) ((*p)[0] | ((*p)[1] << 8));

    *p += 2;
    return v;
} /* get_u16 */

static inline uint32_t
get_u32(const uint8_t **p)
{
    uint32_t v = (uint32_t) (*p)[0] |
        ((uint32_t) (*p)[1] << 8) |
        ((uint32_t) (*p)[2] << 16) |
        ((uint32_t) (*p)[3] << 24);

    *p += 4;
    return v;
} /* get_u32 */

SYMBOL_EXPORT int
chimera_acl_serialize(
    const struct chimera_acl *acl,
    void                     *buf,
    size_t                    buflen)
{
    uint8_t *p = buf;

    if (buflen < chimera_acl_serialized_size(acl)) {
        return -1;
    }

    put_u8(&p, CHIMERA_ACL_SERIAL_VERSION);
    put_u16(&p, acl->ctrl_flags);
    put_u16(&p, acl->num_aces);

    for (unsigned i = 0; i < acl->num_aces; i++) {
        const struct chimera_ace *ace = &acl->aces[i];

        put_u16(&p, ace->type);
        put_u16(&p, ace->flags);
        put_u32(&p, ace->access_mask);
        put_u8(&p, ace->who.type);
        put_u8(&p, ace->who.special);
        put_u32(&p, ace->who.id);
    }

    return (int) (p - (uint8_t *) buf);
} /* chimera_acl_serialize */

SYMBOL_EXPORT int
chimera_acl_deserialize(
    const void         *buf,
    size_t              buflen,
    struct chimera_acl *out,
    unsigned            max_aces)
{
    const uint8_t *p = buf;
    uint8_t        version;
    uint16_t       num_aces;

    if (buflen < CHIMERA_ACL_SERIAL_HDR) {
        return -1;
    }

    version = get_u8(&p);
    if (version != CHIMERA_ACL_SERIAL_VERSION) {
        return -1;
    }

    out->ctrl_flags = get_u16(&p);
    num_aces        = get_u16(&p);

    if (num_aces > max_aces || num_aces > CHIMERA_ACL_MAX_ACES) {
        return -1;
    }

    if (buflen < CHIMERA_ACL_SERIAL_HDR +
        (size_t) num_aces * CHIMERA_ACL_SERIAL_ACE) {
        return -1;
    }

    for (unsigned i = 0; i < num_aces; i++) {
        struct chimera_ace *ace = &out->aces[i];

        ace->type        = get_u16(&p);
        ace->flags       = get_u16(&p);
        ace->access_mask = get_u32(&p);
        ace->who.type    = get_u8(&p);
        ace->who.special = get_u8(&p);
        ace->who.id      = get_u32(&p);
    }

    out->num_aces = num_aces;
    return num_aces;
} /* chimera_acl_deserialize */
