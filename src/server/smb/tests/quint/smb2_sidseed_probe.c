/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Creator-SID seeding probe: whose native owner SID a CREATE stores, and --
 * just as load-bearing -- which CREATEs must store nothing at all.
 *
 * A CREATE carries no security descriptor unless the client sends one, so
 * without seeding a backend that persists native SIDs stores only the
 * algorithmic S-1-5-88-* modefromsid form.  Seeding the creator's own SID is
 * what puts a real principal on disk.
 *
 * The existing security coverage cannot reach any of this.  smb2_info_probe's
 * probe_security_sids runs an ANONYMOUS null session against a server with no
 * identity authority, so both identity probes miss and the seed never fires;
 * smb_sd_sid_test drives the SD codec and chimera_vfs_setattr directly and
 * never goes near CREATE.  The seed therefore only activates in an AD/winbind
 * deployment -- which is precisely where it is least observable.  This probe
 * registers accounts that carry real SIDs, so the identity cache HITS and the
 * seeding path runs in the quick tier.
 *
 * The group half has a twist the owner half does not.  A new object's gid is
 * not always the creator's: a set-group-ID parent hands down its own group, so
 * a group SID seeded from the session would sit beside a gid naming a
 * different group.  The probe therefore makes a set-group-ID directory owned
 * by a second group and creates under it: the children must carry that
 * group, never the creator's stored SID.
 *
 * The negative cases matter more than the positive one.  The attributes a
 * CREATE hands the backend also ride the deferred truncate that replaces an
 * EXISTING file's contents, and they are applied to the BASE file by a named
 * stream create; a seed that fires on those paths rewrites an object's stored
 * owner, and (on an engine-authoritative backend, which memfs is) trips the
 * setattr chown gate and refuses the overwrite outright.  Both are asserted
 * here against a second account, because a seed that is merely "usually right"
 * is a regression in the one case -- overwriting a file you do not own -- that
 * every SMB client performs constantly.
 */

#include "smb2_mbt_common.h"

static int failures = 0;

#define CHECK(cond, ...)                             \
        do {                                         \
            if (cond) {                              \
                printf("ok   - " __VA_ARGS__);       \
                printf("\n");                        \
            } else {                                 \
                printf("FAIL - " __VA_ARGS__);       \
                printf("\n");                        \
                failures++;                          \
            }                                        \
        } while (0)

/* Two accounts from a domain the server has no authority for: the SIDs are
 * supplied with the account, so uid -> SID resolves out of the identity cache
 * exactly as a winbind-resolved logon would. */
#define CREATOR_USER         "sidcreator"
#define CREATOR_PASSWORD     "Chimera!SID1"
#define CREATOR_UID          4001u
#define CREATOR_GID          4001u
#define CREATOR_SID          "S-1-5-21-77-88-99-1001"
#define CREATOR_GROUP        "sidgroup1"
#define CREATOR_GROUP_SID    "S-1-5-21-77-88-99-2001"

/* The second account belongs to a second group, so a group seed that leaks
 * onto a path that does not create the object (an overwrite, a stream create)
 * is observable as a change of group, exactly as the owner leak is. */
#define OTHER_USER           "sidother"
#define OTHER_PASSWORD       "Chimera!SID2"
#define OTHER_UID            4002u
#define OTHER_GID            4002u
#define OTHER_SID            "S-1-5-21-77-88-99-1002"
#define OTHER_GROUP          "sidgroup2"
#define OTHER_GROUP_SID      "S-1-5-21-77-88-99-2002"

/* What a gid reports once no identity authority knows it: the algorithmic
 * modefromsid form -- which is also what an object with NO stored group SID
 * reports after its group is evicted. */
#define OTHER_GID_UNIX_SID   "S-1-5-88-2-4002"

/* modefromsid mode ACE for 02770: set-group-ID, rwx for owner and group. */
#define SETGID_DIR_MODE_ACE  "S-1-5-88-3-1528"

/* The owner an SD create context names.  It is the SECOND account rather than
 * some third principal on purpose: a SID the server can resolve to a uid is
 * the only one whose ownership is observable afterwards.  An SD naming a
 * principal from a domain the server knows nothing about resolves to no uid,
 * and -- because chimera_smb_parse_sd_to_acl does not persist the SIDs it
 * parses -- leaves the object with no stored owner at all, so the query falls
 * back to resolving the creator's uid and the two outcomes are indistinguishable.
 * (That the SD path stores no native owner SID of its own is a pre-existing
 * gap, not something the creator seed introduced.) */
#define NAMED_OWNER_SID      OTHER_SID
#define NAMED_GROUP_SID      OTHER_SID

#define FILE_ALL_ACCESS_MASK 0x001f01ffu
#define READ_CONTROL_ACCESS  0x00020000u

/* Read the stored owner/group back out of a handle.  Returns 0 on success. */
static int
query_sd(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    struct smb2_sd   *out)
{
    uint8_t  sd[2048];
    uint32_t st, len = 0;

    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, file_id,
                         SMB2_SEC_OWNER | SMB2_SEC_GROUP | SMB2_SEC_DACL, sd, sizeof(sd),
                         &len);

    if (st != ST_SUCCESS) {
        return -1;
    }
    return smb2_sd_parse(sd, len, out);
} /* query_sd */

/* Open `name`, read its owner and group SIDs, close.  Returns 0 on success. */
static int
principals_of(
    struct smb2_conn *c,
    const char       *name,
    uint32_t          create_options,
    char             *owner,
    size_t            owner_cap,
    char             *group,
    size_t            group_cap)
{
    struct smb2_create_out co;
    struct smb2_sd         d;
    uint32_t               st;

    st = smb2_create_opts(c, name, MBT_FILE_OPEN, READ_CONTROL_ACCESS | MBT_FILE_READ_ATTRIBUTES,
                          MBT_FILE_SHARE_RWD, create_options, NULL, &co);

    if (st != ST_SUCCESS) {
        return -1;
    }

    if (query_sd(c, co.file_id, &d) != 0) {
        smb2_close(c, co.file_id);
        return -1;
    }

    snprintf(owner, owner_cap, "%s", d.owner);
    snprintf(group, group_cap, "%s", d.group);
    smb2_close(c, co.file_id);
    return 0;
} /* principals_of */

/* Hand the file a DACL that grants Everyone full control, so a second account
* can legitimately overwrite it.  Without this an overwrite by a non-owner is
* refused on ordinary POSIX mode grounds and proves nothing about the seed. */
static void
grant_everyone_full_control(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
    struct smb2_sd_ace ace[1];
    uint8_t            built[1024];
    uint32_t           st;
    int                nlen;

    memset(ace, 0, sizeof(ace));
    ace[0].type        = 0;                        /* ACCESS_ALLOWED */
    ace[0].access_mask = FILE_ALL_ACCESS_MASK;
    snprintf(ace[0].sid, sizeof(ace[0].sid), "S-1-1-0");   /* Everyone */

    nlen = smb2_sd_build(built, sizeof(built), NULL, NULL, ace, 1);
    CHECK(nlen > 0, "setup: built a full-control DACL (%d bytes)", nlen);

    if (nlen <= 0) {
        return;
    }

    st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, file_id, SMB2_SEC_DACL,
                            built, (uint32_t) nlen);
    CHECK(st == ST_SUCCESS, "setup: grant Everyone full control -> 0x%08x", st);
} /* grant_everyone_full_control */

/* ------------------------------------------------------------------------ */

/* Create the objects under test, as the creator.  Ownership is asserted later,
 * AFTER the account is evicted -- see probe_stored_not_resolved. */
static void
make_objects(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint32_t               st;

    printf("# --- the creator makes a file and a directory ---\n");

    st = smb2_create(c, "owned.bin", MBT_FILE_CREATE, FILE_ALL_ACCESS_MASK,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "CREATE owned.bin -> 0x%08x", st);

    if (st == ST_SUCCESS) {
        grant_everyone_full_control(c, co.file_id);
        smb2_close(c, co.file_id);
    }

    st = smb2_create_opts(c, "owneddir", MBT_FILE_CREATE, FILE_ALL_ACCESS_MASK,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &co);
    CHECK(st == ST_SUCCESS, "CREATE owneddir (directory) -> 0x%08x", st);

    if (st == ST_SUCCESS) {
        smb2_close(c, co.file_id);
    }

    /* A stream create can bring the base file into existence as well.  That
     * base is this session's creation and owes it a stored owner exactly as a
     * plain create would; it is only the STREAM half that must never touch
     * an existing base's ownership (probe_stream_create_leaves_base_owner). */
    st = smb2_create(c, "streamed.bin:meta", MBT_FILE_CREATE, FILE_ALL_ACCESS_MASK,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS,
          "CREATE streamed.bin:meta (the base file made by a stream create) "
          "-> 0x%08x", st);

    if (st == ST_SUCCESS) {
        smb2_close(c, co.file_id);
    }
} /* make_objects */

/* A set-group-ID directory owned by the OTHER group, and the creator's
 * children under it.  The creator is a member of that group, so the chgrp and
 * the set-group-ID bit are both allowed; the mode travels as a modefromsid
 * mode ACE, the one way an SMB client can name POSIX mode bits. */
static void
make_setgid_objects(struct smb2_conn *c)
{
    struct smb2_create_out co;
    struct smb2_sd_ace     ace[2];
    uint8_t                built[1024];
    uint32_t               st;
    int                    nlen;

    printf("# --- the creator makes a set-group-ID directory of another group ---\n");

    st = smb2_create_opts(c, "sgdir", MBT_FILE_CREATE, FILE_ALL_ACCESS_MASK,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &co);
    CHECK(st == ST_SUCCESS, "CREATE sgdir (directory) -> 0x%08x", st);

    if (st != ST_SUCCESS) {
        return;
    }

    memset(ace, 0, sizeof(ace));
    ace[0].type        = 0;                        /* ACCESS_ALLOWED */
    ace[0].access_mask = FILE_ALL_ACCESS_MASK;
    snprintf(ace[0].sid, sizeof(ace[0].sid), "%s", SETGID_DIR_MODE_ACE);
    ace[1].type        = 0;
    ace[1].access_mask = FILE_ALL_ACCESS_MASK;
    snprintf(ace[1].sid, sizeof(ace[1].sid), "S-1-1-0");   /* Everyone */

    nlen = smb2_sd_build(built, sizeof(built), NULL, OTHER_GROUP_SID, ace, 2);
    CHECK(nlen > 0, "setup: built an SD naming group %s and mode 02770 (%d bytes)",
          OTHER_GROUP_SID, nlen);

    if (nlen > 0) {
        st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                                SMB2_SEC_GROUP | SMB2_SEC_DACL, built,
                                (uint32_t) nlen);
        CHECK(st == ST_SUCCESS, "setup: sgdir -> group %s, mode 02770 -> 0x%08x",
              OTHER_GROUP_SID, st);
    }

    smb2_close(c, co.file_id);

    st = smb2_create(c, "sgdir\\child.bin", MBT_FILE_CREATE, FILE_ALL_ACCESS_MASK,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "CREATE sgdir\\child.bin -> 0x%08x", st);

    if (st == ST_SUCCESS) {
        smb2_close(c, co.file_id);
    }

    st = smb2_create_opts(c, "sgdir\\childdir", MBT_FILE_CREATE, FILE_ALL_ACCESS_MASK,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &co);
    CHECK(st == ST_SUCCESS, "CREATE sgdir\\childdir (directory) -> 0x%08x", st);

    if (st == ST_SUCCESS) {
        smb2_close(c, co.file_id);
    }
} /* make_setgid_objects */

/* Under a set-group-ID parent the child's group is the PARENT's, so the group
 * SID the create stores -- if any -- must name that group, never the creator's.
 * Asserted while every account still resolves, so a stored creator-group SID
 * is unambiguous: QUERY SECURITY prefers a stored SID over the live gid, so it
 * would show through here as the creator's group on an object whose gid says
 * otherwise. */
static void
probe_setgid_child_group_follows_parent(struct smb2_conn *c)
{
    char owner[SMB2_SID_STR_MAX];
    char group[SMB2_SID_STR_MAX];

    printf("# --- a set-group-ID parent's group wins over the creator's ---\n");

    if (principals_of(c, "sgdir\\child.bin", MBT_FILE_NON_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(group, OTHER_GROUP_SID) == 0,
              "the child file carries the parent's group (%s)", group);
        CHECK(strcmp(group, CREATOR_GROUP_SID) != 0,
              "  ... and not the creator's group");
    } else {
        CHECK(0, "the child file's principals can be read back");
    }

    if (principals_of(c, "sgdir\\childdir", MBT_FILE_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(group, OTHER_GROUP_SID) == 0,
              "the child directory carries the parent's group (%s)", group);
        CHECK(strcmp(group, CREATOR_GROUP_SID) != 0,
              "  ... and not the creator's group");
    } else {
        CHECK(0, "the child directory's principals can be read back");
    }
} /* probe_setgid_child_group_follows_parent */

/* The feature actually working, asserted so that only STORAGE can satisfy it.
 *
 * A QUERY cannot be taken at face value here: chimera_smb_emit_owner_sid falls
 * back to a live uid -> SID probe when the object carries no stored SID, so a
 * file that stored nothing still reports the creator's real SID for as long as
 * the account is in the identity cache.  Asserting the SID right after the
 * create therefore passes whether or not the seed did anything at all.
 *
 * So the account is EVICTED first.  With uid 4001 no longer resolvable, the
 * fallback can only produce the algorithmic S-1-5-88-1-4001 form, and the real
 * SID coming back proves it was persisted at create -- which is the whole
 * point of native-SID storage: an identity that outlives the cache that
 * resolved it.  A directory is checked alongside the file because it reaches
 * the backend by a different route (mkdir, not open). */
static void
probe_stored_not_resolved(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    char owner[SMB2_SID_STR_MAX];
    char group[SMB2_SID_STR_MAX];

    printf("# --- the stored SIDs outlive the identity cache ---\n");

    chimera_server_remove_user(env->server, CREATOR_USER);
    chimera_server_remove_group(env->server, CREATOR_GROUP);
    chimera_server_remove_group(env->server, OTHER_GROUP);

    if (principals_of(c, "owned.bin", MBT_FILE_NON_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(owner, CREATOR_SID) == 0,
              "with the account evicted, the file still reports the stored "
              "SID (%s)", owner);
        CHECK(strncmp(owner, "S-1-5-88-", 9) != 0,
              "  ... i.e. it did not fall back to the algorithmic form");
        CHECK(strcmp(group, CREATOR_GROUP_SID) == 0,
              "with the group evicted, the file still reports the stored "
              "group SID (%s)", group);
    } else {
        CHECK(0, "the file's principals can be read back after eviction");
    }

    if (principals_of(c, "owneddir", MBT_FILE_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(owner, CREATOR_SID) == 0,
              "with the account evicted, the directory still reports the "
              "stored SID (%s)", owner);
        CHECK(strcmp(group, CREATOR_GROUP_SID) == 0,
              "with the group evicted, the directory still reports the "
              "stored group SID (%s)", group);
    } else {
        CHECK(0, "the directory's principals can be read back after eviction");
    }

    if (principals_of(c, "streamed.bin", MBT_FILE_NON_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(owner, CREATOR_SID) == 0,
              "with the account evicted, the base file a stream create made "
              "still reports the stored SID (%s)", owner);
        CHECK(strcmp(group, CREATOR_GROUP_SID) == 0,
              "  ... and the stored group SID (%s)", group);
    } else {
        CHECK(0, "the stream-created base file's principals can be read back "
              "after eviction");
    }

    /* Under the set-group-ID parent nothing named the inherited group's SID
     * at create, so with that group evicted the algorithmic form is the only
     * correct answer: the creator's stored group SID here would mean the seed
     * ignored inheritance, and S-1-5-88-2-4001 would mean the set-group-ID bit
     * never took. */
    if (principals_of(c, "sgdir\\child.bin", MBT_FILE_NON_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(group, OTHER_GID_UNIX_SID) == 0,
              "with every group evicted, the set-group-ID child reports its "
              "inherited gid (%s)", group);
    } else {
        CHECK(0, "the set-group-ID child's principals can be read back after "
              "eviction");
    }
} /* probe_stored_not_resolved */

/* The regression that matters: a second account overwriting a file it does not
 * own.  The replacement is issued as a deferred truncate carrying the create's
 * attributes, so a seed that rides along names a new owner on an existing
 * object -- which the setattr chown gate refuses outright (ACCESS_DENIED), and
 * which would silently re-stamp the owner if it did not. */
static void
probe_overwrite_by_other_is_allowed(struct smb2_conn *other)
{
    struct smb2_create_out co;
    char                   owner[SMB2_SID_STR_MAX];
    char                   group[SMB2_SID_STR_MAX];
    uint32_t               st;

    printf("# --- a non-owner may still overwrite ---\n");

    st = smb2_create(other, "owned.bin", MBT_FILE_OVERWRITE_IF,
                     FILE_ALL_ACCESS_MASK, MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS,
          "a second account's MBT_FILE_OVERWRITE_IF on another user's file "
          "succeeds -> 0x%08x", st);

    if (st != ST_SUCCESS) {
        return;
    }

    smb2_close(other, co.file_id);

    if (principals_of(other, "owned.bin", MBT_FILE_NON_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(owner, CREATOR_SID) == 0,
              "  ... and the file still belongs to its creator (%s)", owner);
        CHECK(strcmp(group, CREATOR_GROUP_SID) == 0,
              "  ... and to its creator's group (%s)", group);
    } else {
        CHECK(0, "  ... and the overwritten file's principals can be read back");
    }
} /* probe_overwrite_by_other_is_allowed */

/* A named-stream create applies the create's attributes to the BASE file, and
 * does it through the open path where no chown gate runs at all.  Creating a
 * stream on someone else's file must not take the SID half of its ownership. */
static void
probe_stream_create_leaves_base_owner(struct smb2_conn *other)
{
    struct smb2_create_out co;
    char                   owner[SMB2_SID_STR_MAX];
    char                   group[SMB2_SID_STR_MAX];
    uint32_t               st;

    printf("# --- a stream create leaves the base file's owner alone ---\n");

    st = smb2_create(other, "owned.bin:meta", MBT_FILE_OVERWRITE_IF,
                     FILE_ALL_ACCESS_MASK, MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "a second account creates owned.bin:meta -> 0x%08x",
          st);

    if (st != ST_SUCCESS) {
        return;
    }

    smb2_close(other, co.file_id);

    if (principals_of(other, "owned.bin", MBT_FILE_NON_DIRECTORY_FILE, owner,
                      sizeof(owner), group, sizeof(group)) == 0) {
        CHECK(strcmp(owner, CREATOR_SID) == 0,
              "  ... the base file still belongs to its creator (%s)", owner);
        CHECK(strcmp(group, CREATOR_GROUP_SID) == 0,
              "  ... and to its creator's group (%s)", group);
    } else {
        CHECK(0, "  ... the base file's principals can be read back");
    }
} /* probe_stream_create_leaves_base_owner */

/* When the client supplies a security descriptor with the CREATE, the owner it
 * names is the owner it asked for.  The session's own identity must not
 * overwrite that choice, or the stored owner SID and the uid derived from the
 * same descriptor name two different principals -- and since QUERY SECURITY
 * prefers the stored SID, the file would report an owner its own uid
 * contradicts. */
static void
probe_sd_context_owner_wins(struct smb2_conn *c)
{
    struct smb2_create_out co;
    struct smb2_sd         d;
    struct smb2_sd_ace     ace[1];
    struct smb2_cctx       ctx;
    uint8_t                built[1024];
    static const uint8_t   secd_tag[4] = { 'S', 'e', 'c', 'D' };
    uint32_t               st;
    int                    nlen;

    printf("# --- an SD create context names the owner ---\n");

    memset(ace, 0, sizeof(ace));
    ace[0].type        = 0;                        /* ACCESS_ALLOWED */
    ace[0].access_mask = FILE_ALL_ACCESS_MASK;
    snprintf(ace[0].sid, sizeof(ace[0].sid), "S-1-1-0");

    nlen = smb2_sd_build(built, sizeof(built), NAMED_OWNER_SID,
                         NAMED_GROUP_SID, ace, 1);
    CHECK(nlen > 0, "built an SD naming a foreign owner (%d bytes)", nlen);

    if (nlen <= 0) {
        return;
    }

    ctx.name     = secd_tag;
    ctx.name_len = 4;
    ctx.data     = built;
    ctx.data_len = nlen;

    smb2c_send(c, smb2c_build_create_full(c, "sdowned.bin", MBT_FILE_CREATE,
                                          FILE_ALL_ACCESS_MASK, MBT_FILE_SHARE_RWD,
                                          MBT_FILE_NON_DIRECTORY_FILE, NULL,
                                          &ctx, 1));
    smb2c_wait(c);
    smb2c_parse_create(c, &co);
    st = co.status;

    CHECK(st == ST_SUCCESS, "CREATE sdowned.bin with a SecD context -> 0x%08x",
          st);

    if (st != ST_SUCCESS) {
        return;
    }

    if (query_sd(c, co.file_id, &d) == 0) {
        CHECK(strcmp(d.owner, NAMED_OWNER_SID) == 0,
              "the file belongs to the owner the descriptor named (%s)",
              d.owner);
        CHECK(strcmp(d.owner, CREATOR_SID) != 0,
              "  ... and not to the session that created it");
    } else {
        CHECK(0, "QUERY SECURITY on the SD-created file");
    }

    smb2_close(c, co.file_id);
} /* probe_sd_context_owner_wins */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env                       env;
    struct smb2_env_opts                  opts = { 0 };
    struct smb2_conn                     *creator, *other;
    /* The creator is a member of the other group too, so it may chgrp a
     * directory to it and set that directory's set-group-ID bit. */
    const uint32_t                        cgids[2] = { CREATOR_GID, OTHER_GID };
    const uint32_t                        ogids[1] = { OTHER_GID };
    static const struct smb2_wire_profile w        = {
        .name = "sidseed", .max_dialect = 0x0300, .ntlmv2 = 1
    };

    (void) argc;
    (void) argv;

    setvbuf(stdout, NULL, _IONBF, 0);

    /* Named streams are off by default and the base-file stamping case needs
     * them. */
    opts.named_streams = 1;

    smb2_env_open_wire(&env, &opts, &w);

    /* The harness registers its own account with no SID, which is the case
     * every other probe runs under.  These two carry real SIDs, so the
     * identity cache HITS and the seeding path is reachable at all. */
    if (chimera_server_add_user(env.server, CREATOR_USER, CREATOR_PASSWORD,
                                CREATOR_PASSWORD, CREATOR_SID,
                                CREATOR_UID, CREATOR_GID, 2, cgids, 1) != 0) {
        fprintf(stderr, "failed to register %s\n", CREATOR_USER);
        exit(1);
    }

    if (chimera_server_add_user(env.server, OTHER_USER, OTHER_PASSWORD,
                                OTHER_PASSWORD, OTHER_SID,
                                OTHER_UID, OTHER_GID, 1, ogids, 1) != 0) {
        fprintf(stderr, "failed to register %s\n", OTHER_USER);
        exit(1);
    }

    /* The groups carry real SIDs the same way, so a gid -> SID probe hits
     * for both and the group half of the seed is reachable. */
    if (chimera_server_add_group(env.server, CREATOR_GROUP, CREATOR_GROUP_SID,
                                 CREATOR_GID, 1) != 0) {
        fprintf(stderr, "failed to register %s\n", CREATOR_GROUP);
        exit(1);
    }

    if (chimera_server_add_group(env.server, OTHER_GROUP, OTHER_GROUP_SID,
                                 OTHER_GID, 1) != 0) {
        fprintf(stderr, "failed to register %s\n", OTHER_GROUP);
        exit(1);
    }

    smb2_env_fs_setup(&env, "fs0");

    creator = smb2_conn_open(&env);
    smb2_handshake_as(creator, CREATOR_USER, CREATOR_PASSWORD);

    other = smb2_conn_open(&env);
    smb2_handshake_as(other, OTHER_USER, OTHER_PASSWORD);

    make_objects(creator);
    make_setgid_objects(creator);

    /* The negative cases run while both accounts still resolve, so a failure
     * is unambiguously the seed firing where it must not. */
    probe_overwrite_by_other_is_allowed(other);
    probe_stream_create_leaves_base_owner(other);
    probe_sd_context_owner_wins(creator);
    probe_setgid_child_group_follows_parent(creator);

    /* Last: evicting the creator is destructive to every check above. */
    probe_stored_not_resolved(&env, other);

    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d creator-SID check(s) FAILED\n", failures);
        return 1;
    }

    printf("# all creator-SID checks passed\n");
    return 0;
} /* main */
