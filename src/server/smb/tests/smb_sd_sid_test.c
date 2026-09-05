// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB security-descriptor native-SID round trip.
 *
 * Drives the SD decoder (chimera_smb_sd_to_acl / chimera_smb_parse_sd_to_acl)
 * and the SD emitter (chimera_smb_acl_to_sd) directly, without a server:
 *
 *   - a real SID the identity authority cannot map is kept as an opaque
 *     CHIMERA_PRINCIPAL_SID and re-emitted byte-for-byte instead of dropped;
 *   - a real SID the identity authority knows resolves to its uid AND keeps
 *     the SID on the principal (and as the owner's native-SID companion);
 *   - algorithmic (S-1-5-88) and well-known SIDs stay numeric/special;
 *   - the emitter prefers stored SIDs (owner/group companions, ACE SIDs,
 *     OWNER@ substitution) over the identity cache and the algorithmic form;
 *   - the first decode pass records uncached real SIDs for async resolution
 *     rather than storing them opaque.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_idmap.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_sid.h"
#include "common/logging.h"
#include "prometheus-c.h"

#include "server/smb/smb_internal.h"
#include "server/smb/smb_procs.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define SID_OWNER_REAL   "S-1-5-21-1-2-3-500"
#define SID_GROUP_REAL   "S-1-5-21-1-2-3-513"
#define SID_OPAQUE       "S-1-5-21-1-2-3-1001"
#define SID_OPAQUE2      "S-1-5-21-1-2-3-9999"
#define SID_ALICE        "S-1-5-21-1-2-3-1105"
#define SID_EVERYONE     "S-1-1-0"
#define SID_UNIX_UID1000 "S-1-5-88-1-1000"
#define SID_UNIX_GID1000 "S-1-5-88-2-1000"

/* SD control bits (mirrors smb_proc_security.c). */
#define SE_SELF_RELATIVE  0x8000
#define SE_DACL_PRESENT   0x0004
#define SE_DACL_PROTECTED 0x1000

#define ACE_ALLOWED 0
#define MASK_READ   0x00000001
#define MASK_RW     0x00000003

#define MAX_ACES 8
#define ACL_BUF_SIZE (sizeof(struct chimera_acl) + MAX_ACES * sizeof(struct chimera_ace))

static void
put_le16(
    uint8_t *b,
    uint16_t v)
{
    b[0] = v & 0xff;
    b[1] = (v >> 8) & 0xff;
} /* put_le16 */

static void
put_le32(
    uint8_t *b,
    uint32_t v)
{
    b[0] = v & 0xff;
    b[1] = (v >> 8) & 0xff;
    b[2] = (v >> 16) & 0xff;
    b[3] = (v >> 24) & 0xff;
} /* put_le32 */

static uint32_t
get_le32(const uint8_t *b)
{
    return (uint32_t) b[0] | ((uint32_t) b[1] << 8) |
           ((uint32_t) b[2] << 16) | ((uint32_t) b[3] << 24);
} /* get_le32 */

/*
 * Assemble a self-relative SD: owner/group (either may be NULL), then a DACL
 * of `nace` ALLOW ACEs with the given masks and SID strings.  Returns the SD
 * length.
 */
static uint32_t
build_sd(
    uint8_t     *sd,
    uint32_t     cap,
    uint16_t     control,
    const char  *owner,
    const char  *group,
    int          nace,
    const uint32_t *masks,
    const char **sids)
{
    uint32_t off = 20;
    uint32_t owner_off = 0, group_off = 0, dacl_off = 0;
    int      n;

    memset(sd, 0, cap);
    sd[0] = 1; /* revision */
    put_le16(sd + 2, control);

    if (owner) {
        n = chimera_sid_str_to_bin(owner, sd + off, cap - off);
        assert(n > 0);
        owner_off = off;
        off      += n;
    }
    if (group) {
        n = chimera_sid_str_to_bin(group, sd + off, cap - off);
        assert(n > 0);
        group_off = off;
        off      += n;
    }
    if (nace) {
        uint32_t acl_hdr = off;
        uint32_t ace_pos = off + 8;

        for (int i = 0; i < nace; i++) {
            n = chimera_sid_str_to_bin(sids[i], sd + ace_pos + 8, cap - ace_pos - 8);
            assert(n > 0);
            sd[ace_pos]     = ACE_ALLOWED;
            sd[ace_pos + 1] = 0;
            put_le16(sd + ace_pos + 2, 8 + n);
            put_le32(sd + ace_pos + 4, masks[i]);
            ace_pos += 8 + n;
        }
        sd[acl_hdr] = 2; /* ACL revision */
        put_le16(sd + acl_hdr + 2, ace_pos - acl_hdr);
        put_le16(sd + acl_hdr + 4, nace);
        dacl_off = acl_hdr;
        off      = ace_pos;
    }

    put_le32(sd + 4, owner_off);
    put_le32(sd + 8, group_off);
    put_le32(sd + 12, 0);
    put_le32(sd + 16, dacl_off);
    return off;
} /* build_sd */

/* Walk an emitted SD's DACL and return the i-th ACE's SID as a string. */
static void
sd_ace_sid(
    const uint8_t *sd,
    int            index,
    char          *out,
    int            outlen,
    uint32_t      *mask)
{
    uint32_t dacl = get_le32(sd + 16);
    uint16_t nace = sd[dacl + 4] | (sd[dacl + 5] << 8);
    uint32_t pos  = dacl + 8;

    assert(index < nace);
    for (int i = 0; i < index; i++) {
        pos += sd[pos + 2] | (sd[pos + 3] << 8);
    }
    *mask = get_le32(sd + pos + 4);
    assert(chimera_sid_bin_to_str(sd + pos + 8, 128, out, outlen) > 0);
} /* sd_ace_sid */

static uint16_t
sd_ace_count(const uint8_t *sd)
{
    uint32_t dacl = get_le32(sd + 16);

    return sd[dacl + 4] | (sd[dacl + 5] << 8);
} /* sd_ace_count */

static void
sd_owner_str(
    const uint8_t *sd,
    char          *out,
    int            outlen)
{
    assert(chimera_sid_bin_to_str(sd + get_le32(sd + 4), 128, out, outlen) > 0);
} /* sd_owner_str */

static void
sd_group_str(
    const uint8_t *sd,
    char          *out,
    int            outlen)
{
    assert(chimera_sid_bin_to_str(sd + get_le32(sd + 8), 128, out, outlen) > 0);
} /* sd_group_str */

/*
 * No identity authority (the create-time path): an unmappable real SID
 * becomes an opaque principal and survives decode -> emit -> decode intact,
 * while well-known and algorithmic SIDs stay special/numeric.
 */
static void
test_opaque_roundtrip_no_authority(void)
{
    uint8_t                  sd[512], out[1024];
    uint8_t                  acl_buf[ACL_BUF_SIZE], acl_buf2[ACL_BUF_SIZE];
    struct chimera_acl      *acl  = (struct chimera_acl *) acl_buf;
    struct chimera_acl      *acl2 = (struct chimera_acl *) acl_buf2;
    struct chimera_vfs_attrs attrs;
    struct chimera_sid       opaque, owner;
    const uint32_t           masks[3] = { MASK_READ, MASK_READ, MASK_RW };
    const char              *sids[3]  = { SID_OPAQUE, SID_EVERYONE, SID_UNIX_UID1000 };
    char                     str[CHIMERA_SID_STR_MAX];
    uint32_t                 sd_len, mask;
    int                      out_len;

    assert(chimera_sid_from_str(&opaque, SID_OPAQUE) == 0);
    assert(chimera_sid_from_str(&owner, SID_OWNER_REAL) == 0);

    sd_len = build_sd(sd, sizeof(sd),
                      SE_SELF_RELATIVE | SE_DACL_PRESENT | SE_DACL_PROTECTED,
                      SID_OWNER_REAL, SID_GROUP_REAL, 3, masks, sids);

    memset(&attrs, 0, sizeof(attrs));
    memset(acl_buf, 0, sizeof(acl_buf));
    chimera_smb_parse_sd_to_acl(sd, sd_len, &attrs, acl_buf, sizeof(acl_buf),
                                NULL, 1);

    /* A real owner/group SID with no authority to map it is left alone
     * (today's behaviour): no uid/gid and no companion is set. */
    assert(!(attrs.va_set_mask & CHIMERA_VFS_ATTR_UID));
    assert(!(attrs.va_set_mask & CHIMERA_VFS_ATTR_GID));
    assert(!(attrs.va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID));

    /* All three ACEs are kept -- the unmappable one as an opaque SID. */
    assert(attrs.va_set_mask & CHIMERA_VFS_ATTR_ACL);
    assert(attrs.va_acl == acl);
    assert(acl->num_aces == 3);
    assert(acl->ctrl_flags & CHIMERA_ACL_CTRL_PROTECTED);
    assert(acl->aces[0].who.type == CHIMERA_PRINCIPAL_SID);
    assert(chimera_sid_equal(&acl->aces[0].who.sid, &opaque));
    assert(acl->aces[1].who.type == CHIMERA_PRINCIPAL_SPECIAL);
    assert(acl->aces[1].who.special == CHIMERA_WHO_EVERYONE);
    assert(!chimera_sid_present(&acl->aces[1].who.sid));
    assert(acl->aces[2].who.type == CHIMERA_PRINCIPAL_USER);
    assert(acl->aces[2].who.id == 1000);
    assert(!chimera_sid_present(&acl->aces[2].who.sid));
    TEST_PASS("decode keeps an unmappable ACE as an opaque SID principal");

    /* Emit: the owner companion verbatim, the group from the algorithmic
     * fallback, the opaque ACE verbatim, the rest re-derived. */
    out_len = chimera_smb_acl_to_sd(1000, 1000, 0644, acl, &owner, NULL,
                                    1, 1, 1, out, sizeof(out), NULL);
    assert(out_len > 0);
    sd_owner_str(out, str, sizeof(str));
    assert(strcmp(str, SID_OWNER_REAL) == 0);
    sd_group_str(out, str, sizeof(str));
    assert(strcmp(str, SID_UNIX_GID1000) == 0);
    assert(sd_ace_count(out) == 3);
    sd_ace_sid(out, 0, str, sizeof(str), &mask);
    assert(strcmp(str, SID_OPAQUE) == 0 && mask == MASK_READ);
    sd_ace_sid(out, 1, str, sizeof(str), &mask);
    assert(strcmp(str, SID_EVERYONE) == 0);
    sd_ace_sid(out, 2, str, sizeof(str), &mask);
    assert(strcmp(str, SID_UNIX_UID1000) == 0 && mask == MASK_RW);
    assert((out[2] | (out[3] << 8)) & SE_DACL_PROTECTED);
    TEST_PASS("emit writes stored owner SID and opaque ACE SID verbatim");

    /* And the emitted descriptor decodes back to the same principals. */
    memset(&attrs, 0, sizeof(attrs));
    memset(acl_buf2, 0, sizeof(acl_buf2));
    chimera_smb_parse_sd_to_acl(out, out_len, &attrs, acl_buf2, sizeof(acl_buf2),
                                NULL, 1);
    assert(acl2->num_aces == 3);
    assert(memcmp(acl->aces, acl2->aces, 3 * sizeof(struct chimera_ace)) == 0);
    assert(acl2->ctrl_flags == acl->ctrl_flags);
    TEST_PASS("decode(emit(decode(sd))) is lossless");
} /* test_opaque_roundtrip_no_authority */

/*
 * With an identity authority: a cached real SID resolves to its uid and keeps
 * the SID; the owner companion is captured on the SET path; an uncached real
 * SID is recorded for async resolution on the first pass and kept opaque on
 * the final pass; the emitter prefers every stored SID.
 */
static void
test_with_identity_authority(void)
{
    struct chimera_vfs           *vfs;
    struct chimera_vfs_thread    *thread;
    struct evpl                  *evpl;
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    uint8_t                       sd[512], out[1024];
    uint8_t                       acl_buf[ACL_BUF_SIZE];
    struct chimera_acl           *acl = (struct chimera_acl *) acl_buf;
    struct chimera_vfs_attrs      attrs;
    struct chimera_sid            alice, opaque2, owner_out, group_out;
    struct smb_unres_sids         unres;
    const uint32_t                masks[2] = { MASK_RW, MASK_READ };
    const char                   *sids[2]  = { SID_ALICE, SID_OPAQUE2 };
    char                          str[CHIMERA_SID_STR_MAX];
    uint32_t                      sd_len, mask;
    int                           out_len;

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs",
            sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv",
            sizeof(module_cfgs[1].module_name) - 1);

    evpl = evpl_create(NULL);
    assert(evpl != NULL);
    vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(vfs != NULL);
    thread = chimera_vfs_thread_init(evpl, vfs);
    assert(thread != NULL);

    /* alice is a domain user the identity authority knows, with a real SID. */
    chimera_vfs_add_user(vfs, "alice", NULL, NULL, SID_ALICE, 4000, 4000, 0, NULL, 1);
    assert(chimera_sid_from_str(&alice, SID_ALICE) == 0);
    assert(chimera_sid_from_str(&opaque2, SID_OPAQUE2) == 0);

    sd_len = build_sd(sd, sizeof(sd), SE_SELF_RELATIVE | SE_DACL_PRESENT,
                      SID_ALICE, NULL, 2, masks, sids);

    /* --- first pass: the uncached SID is recorded, its ACE skipped --- */
    memset(&attrs, 0, sizeof(attrs));
    memset(acl_buf, 0, sizeof(acl_buf));
    memset(&unres, 0, sizeof(unres));
    owner_out.len = 0;
    group_out.len = 0;
    assert(chimera_smb_sd_to_acl(sd, sd_len, &attrs, acl, MAX_ACES, vfs, &unres,
                                 &owner_out, &group_out, 1) == 0);
    assert(unres.count == 1);
    assert(strcmp(unres.sids[0], SID_OPAQUE2) == 0);
    assert(acl->num_aces == 1);
    assert(acl->aces[0].who.type == CHIMERA_PRINCIPAL_USER);
    assert(acl->aces[0].who.id == 4000);
    assert(chimera_sid_equal(&acl->aces[0].who.sid, &alice));
    TEST_PASS("first pass resolves alice with her SID and parks the unknown SID");

    /* The owner resolved through the authority: uid plus the SID companion. */
    assert(attrs.va_set_mask & CHIMERA_VFS_ATTR_UID);
    assert(attrs.va_uid == 4000);
    assert(attrs.va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID);
    assert(attrs.va_owner_sid == &owner_out);
    assert(chimera_sid_equal(&owner_out, &alice));
    assert(!(attrs.va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID));
    TEST_PASS("owner SID resolved via the authority is kept as the companion");

    /* --- final pass (unres == NULL): the still-unknown SID is stored opaque --- */
    memset(&attrs, 0, sizeof(attrs));
    memset(acl_buf, 0, sizeof(acl_buf));
    owner_out.len = 0;
    assert(chimera_smb_sd_to_acl(sd, sd_len, &attrs, acl, MAX_ACES, vfs, NULL,
                                 &owner_out, &group_out, 1) == 0);
    assert(acl->num_aces == 2);
    assert(acl->aces[0].who.type == CHIMERA_PRINCIPAL_USER);
    assert(acl->aces[0].who.id == 4000);
    assert(acl->aces[1].who.type == CHIMERA_PRINCIPAL_SID);
    assert(chimera_sid_equal(&acl->aces[1].who.sid, &opaque2));
    TEST_PASS("final pass stores the unmappable ACE as an opaque SID");

    /* --- emit: alice's ACE from her stored SID, the opaque one verbatim, the
     *     owner from the companion, and an OWNER@ entry substituted with it --- */
    {
        uint8_t             acl3_buf[ACL_BUF_SIZE];
        struct chimera_acl *acl3 = (struct chimera_acl *) acl3_buf;

        memset(acl3_buf, 0, sizeof(acl3_buf));
        acl3->num_aces            = 3;
        acl3->aces[0]             = acl->aces[0];
        acl3->aces[1]             = acl->aces[1];
        acl3->aces[2].type        = CHIMERA_ACE_ALLOWED;
        acl3->aces[2].access_mask = MASK_READ;
        acl3->aces[2].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
        acl3->aces[2].who.special = CHIMERA_WHO_OWNER;

        out_len = chimera_smb_acl_to_sd(4000, 4000, 0644, acl3, &owner_out, NULL,
                                        1, 1, 1, out, sizeof(out), vfs);
        assert(out_len > 0);
        sd_owner_str(out, str, sizeof(str));
        assert(strcmp(str, SID_ALICE) == 0);
        assert(sd_ace_count(out) == 3);
        sd_ace_sid(out, 0, str, sizeof(str), &mask);
        assert(strcmp(str, SID_ALICE) == 0 && mask == MASK_RW);
        sd_ace_sid(out, 1, str, sizeof(str), &mask);
        assert(strcmp(str, SID_OPAQUE2) == 0 && mask == MASK_READ);
        sd_ace_sid(out, 2, str, sizeof(str), &mask);
        assert(strcmp(str, SID_ALICE) == 0);
        TEST_PASS("emit prefers stored SIDs, including for OWNER@ substitution");
    }

    /* Without a companion the owner still comes from the identity cache. */
    out_len = chimera_smb_acl_to_sd(4000, 4000, 0644, NULL, NULL, NULL,
                                    1, 1, 1, out, sizeof(out), vfs);
    assert(out_len > 0);
    sd_owner_str(out, str, sizeof(str));
    assert(strcmp(str, SID_ALICE) == 0);
    sd_group_str(out, str, sizeof(str));
    assert(strcmp(str, "S-1-5-88-2-4000") == 0);
    TEST_PASS("without a companion the cached real SID is still used");

    chimera_vfs_thread_destroy(thread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(evpl);
    prometheus_metrics_destroy(metrics);
} /* test_with_identity_authority */

int
main(
    int    argc,
    char **argv)
{
    chimera_log_init();

    test_opaque_roundtrip_no_authority();
    test_with_identity_authority();

    fprintf(stderr, "All SMB SD native-SID tests passed\n");
    return 0;
} /* main */
