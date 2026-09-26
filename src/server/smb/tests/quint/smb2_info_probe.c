/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 QUERY_INFO / SET_INFO information-class probe.
 *
 * The trace corpus asks for exactly one information class (the model's
 * CQueryBasic) and sets three things (end-of-file, disposition, rename).  That
 * leaves most of query_info, set_info and the attribute marshallers in
 * smb_attr.h dark, along with the whole extended-attribute and named-stream
 * surface -- roughly 1,900 lines between them.
 *
 * Information classes are a bad fit for the model (each is a distinct
 * fixed-layout struct, and the model's file abstraction has no notion of an
 * alignment requirement or a normalized name) but an excellent fit for a
 * ground-truth probe, because the classes CONSTRAIN EACH OTHER: the size in
 * StandardInformation must equal the one in AllInformation and the one in
 * NetworkOpenInformation, the attributes in BasicInformation must match
 * AttributeTagInformation, and anything SET_INFO writes must come back out of
 * the corresponding QUERY.  Checking them against each other is far stronger
 * than checking each against a constant, and it is what catches a marshaller
 * that writes the right number of bytes into the wrong field.
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

/* Every FILE information class the server implements, with the output size it
 * must produce.  A size of 0 means "variable" and is only checked for being
 * non-empty. */
struct info_case {
    const char *name;
    uint8_t     cls;
    uint32_t    size;      /* expected OutputBufferLength, 0 = variable */
};

/* *INDENT-OFF* */
static const struct info_case file_classes[] = {
    { "FileBasicInformation",         SMB2_FILE_BASIC_INFO_T,        40 },
    { "FileStandardInformation",      SMB2_FILE_STANDARD_INFO_T,     24 },
    { "FileInternalInformation",      SMB2_FILE_INTERNAL_INFO_T,      8 },
    { "FileEaInformation",            SMB2_FILE_EA_INFO_T,            4 },
    { "FileAccessInformation",        SMB2_FILE_ACCESS_INFO_T,        4 },
    { "FilePositionInformation",      SMB2_FILE_POSITION_INFO_T,      8 },
    { "FileModeInformation",          SMB2_FILE_MODE_INFO_T,          4 },
    { "FileAlignmentInformation",     SMB2_FILE_ALIGNMENT_INFO_T,     4 },
    { "FileCompressionInformation",   SMB2_FILE_COMPRESSION_INFO_T,  16 },
    { "FileNetworkOpenInformation",   SMB2_FILE_NETWORK_OPEN_T,      56 },
    { "FileAttributeTagInformation",  SMB2_FILE_ATTRIBUTE_TAG_T,      8 },
    { "FileAllInformation",           SMB2_FILE_ALL_INFO_T,           0 },
    { "FileNormalizedNameInformation", SMB2_FILE_NORMALIZED_NAME_T,   0 },
    { "FileFullEaInformation",        SMB2_FILE_FULL_EA_INFO_T,       0 },
    { "FileStreamInformation",        SMB2_FILE_STREAM_INFO_T,        0 },
};

/* The FILESYSTEM classes are answered off the share, not the handle, so they
 * work on any open. */
static const struct info_case fs_classes[] = {
    { "FileFsVolumeInformation",     SMB2_FS_VOLUME_INFO_T,       0 },
    { "FileFsSizeInformation",       SMB2_FS_SIZE_INFO_T,        24 },
    { "FileFsDeviceInformation",     SMB2_FS_DEVICE_INFO_T,       8 },
    { "FileFsAttributeInformation",  SMB2_FS_ATTRIBUTE_INFO_T,    0 },
    { "FileFsControlInformation",    SMB2_FS_CONTROL_INFO_T,     48 },
    { "FileFsFullSizeInformation",   SMB2_FS_FULL_SIZE_INFO_T,   32 },
    { "FileFsObjectIdInformation",   SMB2_FS_OBJECTID_INFO_T,    64 },
    { "FileFsSectorSizeInformation", SMB2_FS_SECTOR_SIZE_INFO_T, 28 },
};
/* *INDENT-ON* */

/* Query one class into `out` and require it to have answered.
 *
 * The cross-class comparisons below are only meaningful if every class
 * actually replied: smb2_query_info fills the caller's buffer ONLY on success,
 * so comparing after an unchecked query would be comparing uninitialised stack
 * bytes -- which can agree with each other by luck and report a pass.  Zero the
 * buffer and make the failure loud instead. */
static int
query_ok(
    struct smb2_conn *c,
    uint8_t           info_type,
    uint8_t           info_class,
    const uint8_t     file_id[16],
    uint8_t          *out,
    uint32_t          cap,
    const char       *what)
{
    uint32_t st, len = 0;

    memset(out, 0, cap);
    st = smb2_query_info(c, info_type, info_class, file_id, 0, out, cap, &len);
    CHECK(st == ST_SUCCESS, "QUERY %s -> 0x%08x", what, st);
    return st == ST_SUCCESS;
} /* query_ok */

/* ---- the query sweep ---------------------------------------------------- */

static void
probe_query_sweep(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    const char       *what)
{
    uint8_t      buf[4096];
    uint32_t     st, len;
    unsigned int i;

    printf("# --- QUERY_INFO sweep on %s ---\n", what);

    for (i = 0; i < sizeof(file_classes) / sizeof(file_classes[0]); i++) {
        const struct info_case *ic = &file_classes[i];

        st = smb2_query_info(c, SMB2_INFO_FILE_T, ic->cls, file_id, 0,
                             buf, sizeof(buf), &len);
        if (ic->size) {
            CHECK(st == ST_SUCCESS && len == ic->size,
                  "%s: %s -> 0x%08x (%u bytes, want %u)", what, ic->name, st,
                  len, ic->size);
        } else {
            /* A variable-length class may legitimately answer with nothing --
             * an empty EA list, or a directory's (absent) data stream -- so
             * the sweep only requires that the class is answered.  The EA and
             * stream sections below check non-empty results with real
             * content. */
            CHECK(st == ST_SUCCESS, "%s: %s -> 0x%08x (%u bytes)", what,
                  ic->name, st, len);
        }
    }

    for (i = 0; i < sizeof(fs_classes) / sizeof(fs_classes[0]); i++) {
        const struct info_case *ic = &fs_classes[i];

        st = smb2_query_info(c, SMB2_INFO_FILESYSTEM_T, ic->cls, file_id, 0,
                             buf, sizeof(buf), &len);
        if (ic->size) {
            CHECK(st == ST_SUCCESS && len == ic->size,
                  "%s: %s -> 0x%08x (%u bytes, want %u)", what, ic->name, st,
                  len, ic->size);
        } else {
            CHECK(st == ST_SUCCESS, "%s: %s -> 0x%08x (%u bytes)", what,
                  ic->name, st, len);
        }
    }
} /* probe_query_sweep */

/* ---- FileFsAttributeInformation flags ------------------------------------
 *
 * MS-FSCC 2.5.1 FileSystemAttributes is what a client consults BEFORE trying a
 * feature: macOS writes AppleDouble sidecars instead of streams unless
 * FILE_NAMED_STREAMS is set, Windows Explorer hides the Security tab without
 * FILE_PERSISTENT_ACLS, and robocopy picks its stream strategy from the word.
 * smbtorture never reads it (it opens streams directly), so a wrong word is
 * invisible to the whole conformance suite -- only a real client notices.
 * The word must follow the serving module's capabilities and the
 * smb_named_streams knob, so the probe pins it under both knob settings.
 *
 * MS-FSCC names, spelled with an FSA_ prefix so they cannot collide with the
 * harness's own FILE_* constants. */
#define FSA_CASE_SENSITIVE_SEARCH      0x00000001
#define FSA_CASE_PRESERVED_NAMES       0x00000002
#define FSA_UNICODE_ON_DISK            0x00000004
#define FSA_PERSISTENT_ACLS            0x00000008
#define FSA_SUPPORTS_SPARSE_FILES      0x00000040
#define FSA_SUPPORTS_REPARSE_POINTS    0x00000080
#define FSA_NAMED_STREAMS              0x00040000
#define FSA_SUPPORTS_BLOCK_REFCOUNTING 0x08000000

/* memfs stores rich ACLs, punches holes, reflinks and keeps named streams, so
 * with the knob on it advertises everything chimera can derive. */
#define FSA_MEMFS_STREAMS_ON           (FSA_CASE_SENSITIVE_SEARCH |      \
                                        FSA_CASE_PRESERVED_NAMES |       \
                                        FSA_UNICODE_ON_DISK |            \
                                        FSA_PERSISTENT_ACLS |            \
                                        FSA_SUPPORTS_SPARSE_FILES |      \
                                        FSA_SUPPORTS_REPARSE_POINTS |    \
                                        FSA_NAMED_STREAMS |              \
                                        FSA_SUPPORTS_BLOCK_REFCOUNTING)

static void
probe_fs_attributes(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint32_t          want,
    const char       *what)
{
    uint8_t  buf[64];
    uint32_t st, len = 0, got;

    printf("# --- FileFsAttributeInformation (%s) ---\n", what);

    memset(buf, 0, sizeof(buf));
    st = smb2_query_info(c, SMB2_INFO_FILESYSTEM_T, SMB2_FS_ATTRIBUTE_INFO_T,
                         file_id, 0, buf, sizeof(buf), &len);
    CHECK(st == ST_SUCCESS && len >= 12,
          "%s: FileFsAttributeInformation -> 0x%08x (%u bytes)", what, st, len);
    if (st != ST_SUCCESS || len < 12) {
        return;
    }

    got = g32(buf, 0);
    CHECK(got == want,
          "%s: FileSystemAttributes 0x%08x (want 0x%08x; missing 0x%08x, extra 0x%08x)",
          what, got, want, want & ~got, got & ~want);
    CHECK(g32(buf, 4) == 255,
          "%s: MaximumComponentNameLength %u (want 255)", what, g32(buf, 4));
} /* probe_fs_attributes */

/* The knob half of the gate: with smb_named_streams off the same memfs share
 * must drop FILE_NAMED_STREAMS and nothing else.  Needs its own server. */
static void
probe_fs_attributes_streams_off(void)
{
    struct smb2_env        env;
    struct smb2_env_opts   opts = { .named_streams = 0 };
    struct smb2_conn      *c;
    struct smb2_create_out file;
    uint32_t               st;

    smb2_env_start_opts(&env, &opts);
    c = smb2_conn_open(&env);
    smb2_handshake(c);

    st = smb2_create(c, "fsattr.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &file);
    if (st == ST_SUCCESS) {
        probe_fs_attributes(c, file.file_id,
                            FSA_MEMFS_STREAMS_ON & ~FSA_NAMED_STREAMS,
                            "memfs, smb_named_streams off");
        smb2_close(c, file.file_id);
    } else {
        CHECK(0, "setup: CREATE fsattr.bin -> 0x%08x", st);
    }

    smb2_env_stop(&env);
} /* probe_fs_attributes_streams_off */

/* ---- extended attributes ------------------------------------------------
 *
 * FILE_FULL_EA_INFORMATION (MS-FSCC 2.4.15) is a chain of
 * { NextEntryOffset(4), Flags(1), EaNameLength(1), EaValueLength(2), Name,
 * NUL, Value }, the last entry carrying NextEntryOffset 0.  Setting a list and
 * reading it back is the only way to reach the EA marshaller, the name
 * validator, and the async list+get walk the query drives -- none of which the
 * corpus touches. */
static int
ea_put(
    uint8_t    *buf,
    int         off,
    const char *name,
    const char *value,
    int         last)
{
    int nlen = (int) strlen(name);
    int vlen = (int) strlen(value);
    int need = 8 + nlen + 1 + vlen;
    int adv  = last ? 0 : ((need + 3) & ~3);   /* entries are 4-byte aligned */

    p32(buf, off, (uint32_t) adv);
    buf[off + 4] = 0;                          /* Flags */
    buf[off + 5] = (uint8_t) nlen;             /* EaNameLength, excludes NUL */
    p16(buf, off + 6, (uint16_t) vlen);        /* EaValueLength */
    memcpy(buf + off + 8, name, nlen);
    buf[off + 8 + nlen] = '\0';
    memcpy(buf + off + 8 + nlen + 1, value, vlen);

    return off + (last ? need : adv);
} /* ea_put */

static void
probe_ea(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint8_t                in[512], out[1024];
    uint32_t               st, len = 0;
    int                    n, found_one = 0, found_two = 0;
    uint32_t               off;

    printf("# --- extended attributes ---\n");

    st = smb2_create(c, "ea.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "setup: CREATE ea.bin -> 0x%08x", st);

    /* An empty EA list is the honest starting state. */
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                         co.file_id, 0, out, sizeof(out), &len);
    CHECK(st == ST_SUCCESS && len == 0,
          "FullEaInformation on a fresh file is empty (0x%08x, %u bytes)", st,
          len);

    /* Two EAs in one SET, which is what exercises the chaining. */
    memset(in, 0, sizeof(in));
    n = ea_put(in, 0, "USER.ONE", "first-value", 0);
    n = ea_put(in, n, "USER.TWO", "second", 1);

    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                       co.file_id, in, (uint32_t) n);
    CHECK(st == ST_SUCCESS, "SET FullEaInformation with 2 entries -> 0x%08x",
          st);

    /* FileEaInformation reports the size the EA list would occupy.  Queried
     * through query_ok because the CHECK below reads the buffer in its message
     * argument, which is evaluated whether or not the condition short-circuits
     * -- an unchecked query would print uninitialised stack bytes. */
    if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_EA_INFO_T, co.file_id,
                 out, sizeof(out), "EaInformation")) {
        CHECK(g32(out, 0) > 0,
              "EaInformation reports a non-zero EaSize (%u)", g32(out, 0));
    }

    /* And the full list comes back with both entries and their values. */
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                         co.file_id, 0, out, sizeof(out), &len);
    CHECK(st == ST_SUCCESS && len > 0,
          "FullEaInformation returns the list (0x%08x, %u bytes)", st, len);

    off = 0;
    while (off + 8 <= len) {
        uint32_t next = g32(out, (int) off);
        uint8_t  nlen = out[off + 5];
        uint16_t vlen = g16(out, (int) off + 6);
        char     nm[64];
        char     vl[64];

        if (nlen >= sizeof(nm) || vlen >= sizeof(vl) ||
            off + 8 + nlen + 1 + vlen > len) {
            break;
        }
        memcpy(nm, out + off + 8, nlen);
        nm[nlen] = '\0';
        memcpy(vl, out + off + 8 + nlen + 1, vlen);
        vl[vlen] = '\0';

        if (strcmp(nm, "USER.ONE") == 0 && strcmp(vl, "first-value") == 0) {
            found_one = 1;
        }
        if (strcmp(nm, "USER.TWO") == 0 && strcmp(vl, "second") == 0) {
            found_two = 1;
        }
        if (next == 0) {
            break;
        }
        off += next;
    }
    CHECK(found_one, "  ... USER.ONE round-trips with its value");
    CHECK(found_two, "  ... USER.TWO round-trips with its value");

    /* An EA name carrying reserved punctuation is refused (Samba's
     * is_invalid_windows_ea_name rule). */
    memset(in, 0, sizeof(in));
    n  = ea_put(in, 0, "BAD=NAME", "x", 1);
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                       co.file_id, in, (uint32_t) n);
    CHECK(st != ST_SUCCESS, "SET FullEaInformation with an invalid name is "
          "refused (0x%08x)", st);

    /* Setting an EA to an empty value deletes it. */
    memset(in, 0, sizeof(in));
    n  = ea_put(in, 0, "USER.ONE", "", 1);
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                       co.file_id, in, (uint32_t) n);
    CHECK(st == ST_SUCCESS, "SET FullEaInformation with an empty value -> "
          "0x%08x", st);

    smb2_close(c, co.file_id);
} /* probe_ea */

/* ---- cross-class agreement ---------------------------------------------
 *
 * The same facts are reachable through several classes.  A marshaller that
 * writes the right byte count into the wrong field passes a size check and
 * fails this one. */
static void
probe_agreement(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint8_t                basic[64], stdinfo[64], all[512], netopen[64], tag[64];
    uint8_t                payload[300];
    uint32_t               st, count = 0;
    uint64_t               eof_std, eof_all, eof_net, alloc_std, alloc_net;
    uint32_t               attr_basic, attr_all, attr_net, attr_tag;
    int                    i;

    printf("# --- cross-class agreement ---\n");

    for (i = 0; i < (int) sizeof(payload); i++) {
        payload[i] = (uint8_t) i;
    }

    st = smb2_create(c, "info.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "setup: CREATE info.bin -> 0x%08x", st);
    st = smb2_write(c, co.file_id, 0, payload, sizeof(payload), &count);
    CHECK(st == ST_SUCCESS && count == sizeof(payload),
          "setup: WRITE %zu bytes -> 0x%08x", sizeof(payload), st);

    if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, co.file_id,
                  basic, sizeof(basic), "BasicInformation") ||
        !query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, co.file_id,
                  stdinfo, sizeof(stdinfo), "StandardInformation") ||
        !query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_ALL_INFO_T, co.file_id,
                  all, sizeof(all), "AllInformation") ||
        !query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_NETWORK_OPEN_T, co.file_id,
                  netopen, sizeof(netopen), "NetworkOpenInformation") ||
        !query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_ATTRIBUTE_TAG_T, co.file_id,
                  tag, sizeof(tag), "AttributeTagInformation")) {
        smb2_close(c, co.file_id);
        return;
    }

    /* FILE_STANDARD_INFORMATION: AllocationSize(8), EndOfFile(8), ... */
    alloc_std = g64(stdinfo, 0);
    eof_std   = g64(stdinfo, 8);

    /* FILE_ALL_INFORMATION embeds Basic(40) then Standard at +40, so its
     * AllocationSize is at +40 and its EndOfFile at +48. */
    eof_all = g64(all, 48);

    /* FILE_NETWORK_OPEN_INFORMATION: 4 timestamps (32), AllocationSize(8),
     * EndOfFile(8), FileAttributes(4). */
    alloc_net = g64(netopen, 32);
    eof_net   = g64(netopen, 40);

    CHECK(eof_std == sizeof(payload),
          "StandardInformation EndOfFile is the bytes written (%llu)",
          (unsigned long long) eof_std);
    CHECK(eof_all == eof_std,
          "AllInformation agrees on EndOfFile (%llu vs %llu)",
          (unsigned long long) eof_all, (unsigned long long) eof_std);
    CHECK(eof_net == eof_std,
          "NetworkOpenInformation agrees on EndOfFile (%llu vs %llu)",
          (unsigned long long) eof_net, (unsigned long long) eof_std);
    CHECK(alloc_net == alloc_std,
          "NetworkOpenInformation agrees on AllocationSize (%llu vs %llu)",
          (unsigned long long) alloc_net, (unsigned long long) alloc_std);

    /* FILE_BASIC_INFORMATION: 4 timestamps (32) then FileAttributes(4). */
    attr_basic = g32(basic, 32);
    attr_all   = g32(all, 32);
    attr_net   = g32(netopen, 48);
    attr_tag   = g32(tag, 0);

    CHECK(attr_all == attr_basic,
          "AllInformation agrees on FileAttributes (0x%08x vs 0x%08x)",
          attr_all, attr_basic);
    CHECK(attr_net == attr_basic,
          "NetworkOpenInformation agrees on FileAttributes (0x%08x vs 0x%08x)",
          attr_net, attr_basic);
    CHECK(attr_tag == attr_basic,
          "AttributeTagInformation agrees on FileAttributes (0x%08x vs 0x%08x)",
          attr_tag, attr_basic);
    CHECK(!(attr_basic & SMB2_FILE_ATTRIBUTE_DIRECTORY),
          "a regular file is not reported as a directory (0x%08x)", attr_basic);

    /* FILE_INTERNAL_INFORMATION's IndexNumber must match the one AllInformation
     * carries (Basic 40 + Standard 24 = 64). */
    {
        uint8_t internal[16];

        if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_INTERNAL_INFO_T,
                      co.file_id, internal, sizeof(internal),
                      "InternalInformation")) {
            smb2_close(c, co.file_id);
            return;
        }
        CHECK(g64(internal, 0) == g64(all, 64) && g64(internal, 0) != 0,
              "InternalInformation IndexNumber matches AllInformation (%llu)",
              (unsigned long long) g64(internal, 0));
    }

    /* FILE_ACCESS_INFORMATION reports THIS handle's granted access, so an
     * all-access open and a read-only open of the same file must differ. */
    {
        struct smb2_create_out ro;
        uint8_t                acc_all[8], acc_ro[8];

        if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_ACCESS_INFO_T,
                      co.file_id, acc_all, sizeof(acc_all),
                      "AccessInformation")) {
            smb2_close(c, co.file_id);
            return;
        }

        st = smb2_create(c, "info.bin", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &ro);
        if (st == ST_SUCCESS &&
            query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_ACCESS_INFO_T,
                     ro.file_id, acc_ro, sizeof(acc_ro),
                     "AccessInformation (read-only handle)")) {
            CHECK(g32(acc_all, 0) != g32(acc_ro, 0),
                  "AccessInformation is per-handle (0x%08x vs 0x%08x)",
                  g32(acc_all, 0), g32(acc_ro, 0));
            smb2_close(c, ro.file_id);
        }
    }

    smb2_close(c, co.file_id);
} /* probe_agreement */

/* ---- SET_INFO round trips ----------------------------------------------- */

static void
probe_set_info_access(struct smb2_conn *c)
{
    struct smb2_create_out owner, limited, lookup;
    const uint32_t         access[] = { MBT_FILE_READ_ACCESS, MBT_FILE_READ_ATTRIBUTES | 0x2u,
                                        MBT_FILE_READ_ATTRIBUTES | 0x40000u }; /* WRITE_DAC */
    uint8_t                before[64], after[512], input[128];
    uint32_t               len, st;

    printf("# --- SET_INFO granted-access checks ---\n");
    st = smb2_create(c, "setinfo-access.bin", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &owner);
    CHECK(st == ST_SUCCESS, "setup access-check file -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    memset(input, 0, 20);
    input[0] = 1;
    p16(input, 2, 0x8010); /* SELF_RELATIVE | SACL_PRESENT, NULL SACL */
    st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, owner.file_id,
                            0x8u, input, 20);
    CHECK(st == ST_ACCESS_DENIED, "full control without ACCESS_SYSTEM_SECURITY cannot set SACL");
    const uint32_t privileged[] = { 0x01000000u | MBT_FILE_READ_ATTRIBUTES,
                                    0x01000000u | 0x02000000u }; /* MAXIMUM_ALLOWED */
    for (size_t i = 0; i < sizeof(privileged) / sizeof(privileged[0]); i++) {
        st = smb2_create(c, "setinfo-access.bin", MBT_FILE_OPEN, privileged[i],
                         MBT_FILE_SHARE_RWD, NULL, &lookup);
        CHECK(st == 0xC0000061u,
              "guest cannot obtain ACCESS_SYSTEM_SECURITY, including with MAXIMUM_ALLOWED -> 0x%08x", st);
        if (st == ST_SUCCESS) {
            smb2_close(c, lookup.file_id);
        }
    }
    for (size_t i = 0; i < sizeof(access) / sizeof(access[0]); i++) {
        st = smb2_create(c, "setinfo-access.bin", MBT_FILE_OPEN, access[i],
                         MBT_FILE_SHARE_RWD, NULL, &limited);
        CHECK(st == ST_SUCCESS, "setup restricted open %zu -> 0x%08x", i, st);
        if (st != ST_SUCCESS) {
            continue;
        }
        memset(input, 0, 20);
        input[0] = 1;
        p16(input, 2, 0x8010);
        st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, limited.file_id,
                                0x8u, input, 20);
        CHECK(st == ST_ACCESS_DENIED, "restricted/WRITE_DAC open cannot set SACL");
        if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, owner.file_id,
                     before, sizeof(before), "BasicInformation before denial")) {
            memset(input, 0, 40);
            p64(input, 16, 133000000000000000ull);
            p32(input, 32, 0x2u); /* HIDDEN */
            st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                               limited.file_id, input, 40);
            CHECK(st == ST_ACCESS_DENIED, "BASIC without WRITE_ATTRIBUTES -> 0x%08x", st);
            if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, owner.file_id,
                         after, sizeof(after), "BasicInformation after denial")) {
                CHECK(g64(before, 16) == g64(after, 16) && g32(before, 32) == g32(after, 32),
                      "denied BASIC preserved modification time and attributes");
            }
        }
        st = smb2_set_disposition(c, limited.file_id, 1);
        CHECK(st == ST_ACCESS_DENIED, "DISPOSITION without DELETE -> 0x%08x", st);
        p32(input, 0, 1);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, 0x40, limited.file_id, input, 4);
        CHECK(st == ST_ACCESS_DENIED, "DISPOSITION_EX without DELETE -> 0x%08x", st);
        if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, owner.file_id,
                     after, sizeof(after), "StandardInformation after denial")) {
            CHECK(after[20] == 0, "denied disposition did not set delete-pending");
        }
        st = smb2_rename(c, limited.file_id, "setinfo-access-moved.bin", 0);
        CHECK(st == ST_ACCESS_DENIED, "RENAME without DELETE -> 0x%08x", st);
        int n = ea_put(input, 0, "USER.DENIED", "value", 1);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                           limited.file_id, input, (uint32_t) n);
        CHECK(st == ST_ACCESS_DENIED, "EA without WRITE_EA -> 0x%08x", st);
        len = 0;
        st  = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                              owner.file_id, 0, after, sizeof(after), &len);
        CHECK(st == ST_SUCCESS && len == 0, "denied EA left attribute list empty");
        smb2_close(c, limited.file_id);
    }
    smb2_close(c, owner.file_id);
    st = smb2_create(c, "setinfo-access.bin", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &lookup);
    CHECK(st == ST_SUCCESS, "denied rename/disposition preserved original name");
    if (st == ST_SUCCESS) {
        smb2_close(c, lookup.file_id);
    }
    st = smb2_create(c, "setinfo-access-moved.bin", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &lookup);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "denied rename did not create destination");
    if (st == ST_SUCCESS) {
        smb2_close(c, lookup.file_id);
    }
} /* probe_set_info_access */

static void
probe_set_info(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint8_t                buf[512], in[64];
    uint32_t               st;

    printf("# --- SET_INFO round trips ---\n");

    st = smb2_create(c, "setinfo.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "setup: CREATE setinfo.bin -> 0x%08x", st);

    /* FILE_BASIC_INFORMATION (MS-FSCC 2.4.7): 4 timestamps then attributes.
     * A zero timestamp means "leave unchanged", so set only LastWriteTime and
     * read it back. */
    {
        uint64_t want = 133000000000000000ull;   /* an arbitrary NT time */

        memset(in, 0, 40);
        p64(in, 16, want);                        /* LastWriteTime */
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                           co.file_id, in, 40);
        CHECK(st == ST_SUCCESS, "SET BasicInformation(LastWriteTime) -> 0x%08x",
              st);

        if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, co.file_id,
                      buf, sizeof(buf), "BasicInformation")) {
            smb2_close(c, co.file_id);
            return;
        }
        CHECK(g64(buf, 16) == want,
              "  ... LastWriteTime round-trips (%llu vs %llu)",
              (unsigned long long) g64(buf, 16), (unsigned long long) want);
    }

    /* FILE_BASIC_INFORMATION can also set the DOS attribute bits. */
    {
        memset(in, 0, 40);
        p32(in, 32, 0x00000020u);                 /* FILE_ATTRIBUTE_ARCHIVE */
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                           co.file_id, in, 40);
        CHECK(st == ST_SUCCESS, "SET BasicInformation(FileAttributes) -> 0x%08x",
              st);

        if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_ATTRIBUTE_TAG_T,
                      co.file_id, buf, sizeof(buf), "AttributeTagInformation")) {
            smb2_close(c, co.file_id);
            return;
        }
        CHECK((g32(buf, 0) & 0x00000020u) != 0,
              "  ... ARCHIVE shows up in AttributeTagInformation (0x%08x)",
              g32(buf, 0));
    }

    /* FILE_ALLOCATION_INFORMATION (MS-FSCC 2.4.4): a single AllocationSize.
     * Shrinking the allocation below EOF truncates the file. */
    {
        uint8_t  payload[256];
        uint32_t count = 0;

        memset(payload, 0xAB, sizeof(payload));
        smb2_write(c, co.file_id, 0, payload, sizeof(payload), &count);

        memset(in, 0, 8);
        p64(in, 0, 64);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_ALLOCATION_INFO_T,
                           co.file_id, in, 8);
        CHECK(st == ST_SUCCESS, "SET AllocationInformation(64) -> 0x%08x", st);

        if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T,
                      co.file_id, buf, sizeof(buf), "StandardInformation")) {
            smb2_close(c, co.file_id);
            return;
        }
        CHECK(g64(buf, 8) <= 64,
              "  ... EndOfFile is clamped to the new allocation (%llu)",
              (unsigned long long) g64(buf, 8));
    }

    /* FILE_END_OF_FILE_INFORMATION, the one SET the corpus already drives --
     * included so the round trip is checked against StandardInformation
     * rather than only against the model. */
    {
        st = smb2_set_eof(c, co.file_id, 4096);
        CHECK(st == ST_SUCCESS, "SET EndOfFileInformation(4096) -> 0x%08x", st);

        if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T,
                      co.file_id, buf, sizeof(buf), "StandardInformation")) {
            smb2_close(c, co.file_id);
            return;
        }
        CHECK(g64(buf, 8) == 4096,
              "  ... StandardInformation reports the new size (%llu)",
              (unsigned long long) g64(buf, 8));
    }

    /* FILE_POSITION_INFORMATION (MS-FSCC 2.4.32): per-handle byte offset. */
    {
        memset(in, 0, 8);
        p64(in, 0, 1234);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_POSITION_INFO_T,
                           co.file_id, in, 8);
        CHECK(st == ST_SUCCESS, "SET PositionInformation(1234) -> 0x%08x", st);

        if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_POSITION_INFO_T,
                      co.file_id, buf, sizeof(buf), "PositionInformation")) {
            smb2_close(c, co.file_id);
            return;
        }
        CHECK(g64(buf, 0) == 1234,
              "  ... CurrentByteOffset round-trips (%llu)",
              (unsigned long long) g64(buf, 0));
    }

    smb2_close(c, co.file_id);
} /* probe_set_info */

/* ---- named streams ------------------------------------------------------
 *
 * FILE_STREAM_INFORMATION (MS-FSCC 2.4.40) enumerates a file's data streams as
 * a chain of { NextEntryOffset(4), StreamNameLength(4), StreamSize(8),
 * StreamAllocationSize(8), StreamName }.  A file with no alternate streams
 * still reports its default "::$DATA" stream, so the interesting case -- the
 * one that walks the backend's stream list rather than synthesizing a single
 * entry -- needs a stream actually created. */
static void
probe_streams(struct smb2_conn *c)
{
    struct smb2_create_out base, strm;
    uint8_t                out[1024];
    uint32_t               st, len = 0, count = 0;
    uint32_t               off;
    int                    entries = 0, found_alt = 0;

    printf("# --- named streams ---\n");

    st = smb2_create(c, "streams.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "setup: CREATE streams.bin -> 0x%08x", st);
    smb2_write(c, base.file_id, 0, "base", 4, &count);

    /* The default data stream alone. */
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STREAM_INFO_T,
                         base.file_id, 0, out, sizeof(out), &len);
    CHECK(st == ST_SUCCESS && len > 0,
          "StreamInformation reports the default stream (0x%08x, %u bytes)",
          st, len);

    /* Create an alternate stream with the "file:stream" create syntax. */
    st = smb2_create(c, "streams.bin:alt", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &strm);
    CHECK(st == ST_SUCCESS, "CREATE streams.bin:alt -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        st = smb2_write(c, strm.file_id, 0, "alternate", 9, &count);
        CHECK(st == ST_SUCCESS && count == 9,
              "  ... WRITE 9 bytes to the alternate stream -> 0x%08x", st);
        smb2_close(c, strm.file_id);
    }

    /* Now the enumeration must report both. */
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STREAM_INFO_T,
                         base.file_id, 0, out, sizeof(out), &len);
    CHECK(st == ST_SUCCESS && len > 0,
          "StreamInformation after adding a stream (0x%08x, %u bytes)", st,
          len);

    off = 0;
    while (off + 24 <= len) {
        uint32_t next = g32(out, (int) off);
        uint32_t nlen = g32(out, (int) off + 4);
        uint64_t size = g64(out, (int) off + 8);
        char     nm[128];
        uint32_t i;

        entries++;
        if (nlen / 2 < sizeof(nm) - 1 && off + 24 + nlen <= len) {
            for (i = 0; i < nlen / 2; i++) {
                nm[i] = (char) out[off + 24 + i * 2];
            }
            nm[nlen / 2] = '\0';
            if (strstr(nm, "alt") && size == 9) {
                found_alt = 1;
            }
        }
        if (next == 0) {
            break;
        }
        off += next;
    }
    CHECK(entries >= 2,
          "  ... the enumeration lists both streams (%d entries)", entries);
    CHECK(found_alt,
          "  ... the alternate stream is named and carries its 9 bytes");

    smb2_close(c, base.file_id);
} /* probe_streams */

/* ---- hard links ---------------------------------------------------------
 *
 * FILE_LINK_INFORMATION (MS-FSCC 2.4.21): ReplaceIfExists(1), Reserved(7),
 * RootDirectory(8), FileNameLength(4), FileName (UTF-16LE).  This is the only
 * way to reach set_info's link chain, which the corpus never drives. */
static void
probe_link(struct smb2_conn *c)
{
    struct smb2_create_out co, lo;
    uint8_t                in[256], buf[64];
    uint32_t               st, count = 0;
    int                    nlen;

    printf("# --- hard links (FileLinkInformation) ---\n");

    st = smb2_create(c, "link_src.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "setup: CREATE link_src.bin -> 0x%08x", st);
    smb2_write(c, co.file_id, 0, "linked", 6, &count);

    memset(in, 0, sizeof(in));
    nlen  = utf16le("link_dst.bin", in + 20);
    in[0] = 0;                        /* ReplaceIfExists */
    p64(in, 8, 0);                    /* RootDirectory */
    p32(in, 16, (uint32_t) nlen);     /* FileNameLength */

    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_LINK_INFO_T, co.file_id,
                       in, (uint32_t) (20 + nlen));
    CHECK(st == ST_SUCCESS, "SET LinkInformation(link_dst.bin) -> 0x%08x", st);

    if (st == ST_SUCCESS) {
        /* The link is a second name for the same inode: opening it must give
         * the same IndexNumber and the same content. */
        st = smb2_create(c, "link_dst.bin", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &lo);
        CHECK(st == ST_SUCCESS, "  ... the link opens -> 0x%08x", st);
        if (st == ST_SUCCESS) {
            uint8_t  src_internal[16];
            uint32_t rlen = 0;
            uint8_t  rd[16];

            if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_INTERNAL_INFO_T,
                          co.file_id, src_internal, sizeof(src_internal),
                          "InternalInformation (source)") ||
                !query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_INTERNAL_INFO_T,
                          lo.file_id, buf, sizeof(buf),
                          "InternalInformation (link)")) {
                smb2_close(c, lo.file_id);
                smb2_close(c, co.file_id);
                return;
            }
            CHECK(g64(buf, 0) == g64(src_internal, 0),
                  "  ... it shares the source's IndexNumber (%llu)",
                  (unsigned long long) g64(buf, 0));

            st = smb2_read(c, lo.file_id, 0, 6, rd, &rlen);
            CHECK(st == ST_SUCCESS && rlen == 6 && memcmp(rd, "linked", 6) == 0,
                  "  ... it reads back the source's content");
            smb2_close(c, lo.file_id);
        }

        /* Linking onto an existing name without ReplaceIfExists collides. */
        memset(in, 0, sizeof(in));
        nlen  = utf16le("link_dst.bin", in + 20);
        in[0] = 0;
        p32(in, 16, (uint32_t) nlen);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_LINK_INFO_T,
                           co.file_id, in, (uint32_t) (20 + nlen));
        CHECK(st != ST_SUCCESS,
              "  ... a colliding link without ReplaceIfExists is refused "
              "(0x%08x)", st);
    }

    smb2_close(c, co.file_id);
} /* probe_link */

/* ---- security descriptors -----------------------------------------------
 *
 * InfoType SECURITY builds a self-relative SECURITY_DESCRIPTOR from the file's
 * owner, group and ACL.  AdditionalInformation selects which of those the
 * server emits, and the body order is fixed (owner SID, group SID, DACL)
 * because real clients decode it positionally.  Nothing in the corpus asks for
 * a security descriptor, so this whole translation layer is otherwise dark. */
#define SEC_OWNER 0x01u
#define SEC_GROUP 0x02u
#define SEC_DACL  0x04u

static void
probe_security(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint8_t                sd[2048];
    uint32_t               st, len = 0;
    uint32_t               ctrl, off_owner, off_group, off_sacl, off_dacl;

    printf("# --- security descriptors ---\n");

    st = smb2_create(c, "sec.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "setup: CREATE sec.bin -> 0x%08x", st);

    /* The full descriptor. */
    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                         SEC_OWNER | SEC_GROUP | SEC_DACL, sd, sizeof(sd),
                         &len);
    CHECK(st == ST_SUCCESS && len >= 20,
          "QUERY SECURITY(owner|group|dacl) -> 0x%08x (%u bytes)", st, len);

    if (st == ST_SUCCESS && len >= 20) {
        ctrl      = g16(sd, 2);
        off_owner = g32(sd, 4);
        off_group = g32(sd, 8);
        off_sacl  = g32(sd, 12);
        off_dacl  = g32(sd, 16);

        CHECK(sd[0] == 1, "  ... Revision is 1 (%u)", sd[0]);
        /* SE_SELF_RELATIVE (0x8000) must be set: every offset above is
         * relative to the descriptor, which is only meaningful in that form. */
        CHECK((ctrl & 0x8000u) != 0,
              "  ... Control carries SE_SELF_RELATIVE (0x%04x)", ctrl);
        CHECK(off_owner >= 20 && off_owner < len,
              "  ... OffsetOwner is inside the descriptor (%u)", off_owner);
        CHECK(off_group >= 20 && off_group < len,
              "  ... OffsetGroup is inside the descriptor (%u)", off_group);
        CHECK(off_dacl >= 20 && off_dacl < len,
              "  ... OffsetDacl is inside the descriptor (%u)", off_dacl);
        CHECK(off_sacl == 0, "  ... OffsetSacl is 0 (%u)", off_sacl);
        /* Body order is owner, group, DACL -- clients rely on it. */
        CHECK(off_owner < off_group && off_group < off_dacl,
              "  ... the body is ordered owner < group < dacl (%u/%u/%u)",
              off_owner, off_group, off_dacl);
    }

    /* The descriptor decodes, and the mode-derived DACL names the owner by the
     * same SID the owner field carries. */
    if (st == ST_SUCCESS) {
        struct smb2_sd d;

        CHECK(smb2_sd_parse(sd, len, &d) == 0,
              "  ... the descriptor decodes (SIDs and ACEs)");
        CHECK(d.have_owner && d.have_group && d.have_dacl,
              "  ... owner, group and DACL are all present");
        CHECK(d.nace > 0, "  ... the mode-derived DACL is not empty (%d ACE(s))",
              d.nace);
    }

    /* AdditionalInformation actually selects: asking for only the owner must
     * leave the group and DACL offsets zero. */
    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id, SEC_OWNER,
                         sd, sizeof(sd), &len);
    CHECK(st == ST_SUCCESS && len >= 20,
          "QUERY SECURITY(owner only) -> 0x%08x (%u bytes)", st, len);
    if (st == ST_SUCCESS && len >= 20) {
        CHECK(g32(sd, 4) != 0 && g32(sd, 8) == 0 && g32(sd, 16) == 0,
              "  ... only the owner is present (owner=%u group=%u dacl=%u)",
              g32(sd, 4), g32(sd, 8), g32(sd, 16));
    }

    /* A zero AdditionalInformation yields the bare 20-byte header. */
    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id, 0,
                         sd, sizeof(sd), &len);
    CHECK(st == ST_SUCCESS && len == 20,
          "QUERY SECURITY(nothing) is a bare header (0x%08x, %u bytes)", st,
          len);

    /* Too small a buffer is STATUS_BUFFER_TOO_SMALL here -- not the
     * INFO_LENGTH_MISMATCH the fixed-size FILE classes use. */
    st = smb2_query_info_len(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                             SEC_OWNER | SEC_GROUP | SEC_DACL, 8,
                             sd, sizeof(sd), &len);
    CHECK(st == ST_BUFFER_TOO_SMALL,
          "QUERY SECURITY with an 8-byte buffer -> BUFFER_TOO_SMALL (0x%08x)",
          st);

    /* Setting the descriptor straight back must be accepted: it is the same
     * bytes the server just produced, so it exercises the SD -> ACL direction
     * without changing anything. */
    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                         SEC_OWNER | SEC_GROUP | SEC_DACL, sd, sizeof(sd),
                         &len);
    if (st == ST_SUCCESS && len > 0) {
        st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                                SEC_OWNER | SEC_GROUP | SEC_DACL, sd, len);
        CHECK(st == ST_SUCCESS,
              "SET SECURITY with the descriptor just read -> 0x%08x", st);
    }

    smb2_close(c, co.file_id);
} /* probe_security */

/* ---- native SIDs in a security descriptor ------------------------------
 *
 * The descriptor above is one the server authored, so setting it back proves
 * only that the emitter and the parser agree with each other.  What native-SID
 * storage changed is what happens to a descriptor the server did NOT author: a
 * principal named by a real Windows SID that no identity authority can map
 * used to be dropped on the way in, because a principal was a uid/gid and an
 * unmappable SID had no uid.  It is now carried natively -- stored as an
 * opaque CHIMERA_PRINCIPAL_SID and re-emitted verbatim -- so a Windows ACL
 * survives a round trip through a POSIX-backed share.
 *
 * That is invisible to the trace corpus (the model has no security-descriptor
 * surface at all) and invisible to the header-level checks above, so it is
 * pinned here: SET a descriptor naming principals from a domain this server
 * knows nothing about, and require them back byte-for-byte.
 *
 * The session is an anonymous NTLM null session and no identity authority is
 * configured, so the owner and group SIDs are the algorithmic S-1-5-88 form
 * (MS-SMB2's modefromsid convention) and an unmappable owner SID has no uid to
 * resolve to.  Both of those are asserted rather than worked around: the
 * owner-side behaviour is deliberate, and a regression that silently adopted
 * an unresolvable SID as the owner would be a real bug. */

#define ACE_ALLOWED 0
#define ACE_DENIED  1

static void
probe_security_sids(struct smb2_conn *c)
{
    struct smb2_create_out co;
    struct smb2_sd         d;
    struct smb2_sd_ace     want[3];
    uint8_t                sd[2048], built[1024];
    uint32_t               st, len = 0;
    int                    nlen, i, found_denied = 0;

    printf("# --- native SIDs in a security descriptor ---\n");

    st = smb2_create(c, "secsid.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "setup: CREATE secsid.bin -> 0x%08x", st);

    if (st != ST_SUCCESS) {
        return;
    }

    /* With nothing stored, the owner and group come from the algorithmic
     * scheme -- S-1-5-88-1-<uid> and S-1-5-88-2-<gid>.  This is the fallback
     * the stored-SID path must not disturb, so pin it before setting one. */
    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                         SEC_OWNER | SEC_GROUP | SEC_DACL, sd, sizeof(sd),
                         &len);

    if (st == ST_SUCCESS && smb2_sd_parse(sd, len, &d) == 0) {
        CHECK(strncmp(d.owner, "S-1-5-88-1-", 11) == 0,
              "a file with no stored SID reports the algorithmic owner (%s)",
              d.owner);
        CHECK(strncmp(d.group, "S-1-5-88-2-", 11) == 0,
              "  ... and the algorithmic group (%s)", d.group);
    } else {
        CHECK(0, "QUERY SECURITY before SET -> 0x%08x", st);
        smb2_close(c, co.file_id);
        return;
    }

    /* A DACL from a domain this server has never heard of: an unmappable user,
     * a well-known SID, and a DENY ace for a second unmappable user.  The DENY
     * is there because its position is load-bearing -- a canonicalizing
     * emitter that reorders ACEs changes the file's effective permissions. */
    memset(want, 0, sizeof(want));
    want[0].type        = ACE_ALLOWED;
    want[0].access_mask = 0x001f01ffu;                    /* MBT_FILE_ALL_ACCESS */
    snprintf(want[0].sid, sizeof(want[0].sid), "S-1-5-21-1-2-3-1001");
    want[1].type        = ACE_ALLOWED;
    want[1].access_mask = 0x00120089u;                    /* READ            */
    snprintf(want[1].sid, sizeof(want[1].sid), "S-1-1-0"); /* Everyone       */
    want[2].type        = ACE_DENIED;
    want[2].access_mask = 0x00000004u;                    /* APPEND_DATA     */
    snprintf(want[2].sid, sizeof(want[2].sid), "S-1-5-21-1-2-3-9999");

    nlen = smb2_sd_build(built, sizeof(built), "S-1-5-21-1-2-3-500",
                         "S-1-5-21-1-2-3-513", want, 3);
    CHECK(nlen > 0, "built a descriptor with foreign owner/group/DACL (%d bytes)",
          nlen);

    if (nlen <= 0) {
        smb2_close(c, co.file_id);
        return;
    }

    st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                            SEC_OWNER | SEC_GROUP | SEC_DACL, built,
                            (uint32_t) nlen);
    CHECK(st == ST_SUCCESS, "SET SECURITY with foreign SIDs -> 0x%08x", st);

    st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                         SEC_OWNER | SEC_GROUP | SEC_DACL, sd, sizeof(sd),
                         &len);
    CHECK(st == ST_SUCCESS, "QUERY SECURITY after the foreign SET -> 0x%08x",
          st);

    if (st != ST_SUCCESS || smb2_sd_parse(sd, len, &d) != 0) {
        CHECK(0, "  ... the re-read descriptor decodes");
        smb2_close(c, co.file_id);
        return;
    }

    /* The explicit DACL replaced the mode-derived one entirely. */
    CHECK(d.nace == 3, "  ... the explicit DACL has all 3 ACEs (%d)", d.nace);

    /* Every ACE came back verbatim, in the order it was set: an unmappable
     * SID kept as an opaque principal, a well-known SID kept special, and the
     * DENY still ahead of nothing it must not follow. */
    for (i = 0; i < d.nace && i < 3; i++) {
        CHECK(strcmp(d.ace[i].sid, want[i].sid) == 0,
              "  ... ace[%d] SID round-trips (%s)", i, d.ace[i].sid);
        CHECK(d.ace[i].type == want[i].type,
              "  ... ace[%d] type is preserved (%u)", i, d.ace[i].type);
        CHECK(d.ace[i].access_mask == want[i].access_mask,
              "  ... ace[%d] access mask is preserved (0x%08x)", i,
              d.ace[i].access_mask);

        if (d.ace[i].type == ACE_DENIED) {
            found_denied = 1;
        }
    }

    CHECK(found_denied, "  ... the DENY ace survived (not dropped as unmappable)");

    /* An owner SID no identity authority can resolve has no uid to become, so
     * the owner is NOT adopted -- it stays the algorithmic form.  Pinning this
     * guards the other direction: silently taking an unresolvable SID as the
     * owner would detach the file's owner from every POSIX check. */
    CHECK(strncmp(d.owner, "S-1-5-88-1-", 11) == 0,
          "  ... an unresolvable owner SID is not adopted (%s)", d.owner);
    CHECK(strncmp(d.group, "S-1-5-88-2-", 11) == 0,
          "  ... nor an unresolvable group SID (%s)", d.group);

    /* Setting only the DACL must not disturb owner or group. */
    nlen = smb2_sd_build(built, sizeof(built), NULL, NULL, want, 2);

    if (nlen > 0) {
        st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                                SEC_DACL, built, (uint32_t) nlen);
        CHECK(st == ST_SUCCESS, "SET SECURITY(dacl only) -> 0x%08x", st);

        st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                             SEC_OWNER | SEC_GROUP | SEC_DACL, sd, sizeof(sd),
                             &len);

        if (st == ST_SUCCESS && smb2_sd_parse(sd, len, &d) == 0) {
            CHECK(d.nace == 2, "  ... the DACL is replaced (%d ACE(s))", d.nace);
            CHECK(strncmp(d.owner, "S-1-5-88-1-", 11) == 0 &&
                  strncmp(d.group, "S-1-5-88-2-", 11) == 0,
                  "  ... owner and group are untouched (%s / %s)", d.owner,
                  d.group);
        } else {
            CHECK(0, "  ... QUERY after the dacl-only SET -> 0x%08x", st);
        }
    }

    smb2_close(c, co.file_id);
} /* probe_security_sids */

/* ---- directory enumeration ----------------------------------------------
 *
 * QUERY_DIRECTORY is the last wholly-dark command in the server: the model
 * creates directories but never enumerates one, so nothing drives it.  Each of
 * its six information classes is a different fixed header followed by the name,
 * chained by NextEntryOffset -- and every class must report the SAME SET OF
 * NAMES, which is what makes cross-class comparison the strong check here.
 *
 * Enumeration is stateful on the handle, so a second call continues rather
 * than restarting; that is asserted rather than worked around.
 */
struct dir_class {
    const char *name;
    uint8_t     cls;
    int         name_off;   /* byte offset of FileName within an entry */
    int         len_off;    /* byte offset of FileNameLength */
};

/* *INDENT-OFF* */
/* Offsets are where the server's emitter actually lands, which for the two ID
 * classes is past the implicit padding an 8-aligned FileId append inserts:
 * ID_FULL's name starts at 80 (not the 74 the server's own minimum-length
 * table claims) and ID_BOTH's at 104 (not 102).  Both emitters match MS-FSCC;
 * it is the minimums that are understated. */
static const struct dir_class dir_classes[] = {
    { "FileDirectoryInformation",       SMB2_FILE_DIRECTORY_INFO_T,   64, 60 },
    { "FileFullDirectoryInformation",   SMB2_FILE_FULL_DIR_INFO_T,    68, 60 },
    { "FileBothDirectoryInformation",   SMB2_FILE_BOTH_DIR_INFO_T,    94, 60 },
    { "FileNamesInformation",           SMB2_FILE_NAMES_INFO_T,       12,  8 },
    { "FileIdBothDirectoryInformation", SMB2_FILE_ID_BOTH_DIR_INFO_T, 104, 60 },
    { "FileIdFullDirectoryInformation", SMB2_FILE_ID_FULL_DIR_INFO_T,  80, 60 },
};
/* *INDENT-ON* */

/* Query one directory class into `out`, zeroing it first.
 *
 * dir_collect walks the reply by NextEntryOffset, so the buffer has to start
 * from a known state: the server fills only the bytes it actually returned,
 * and a decoder that strays past them would be reading whatever the stack
 * held. */
static uint32_t
qdir(
    struct smb2_conn *c,
    uint8_t           info_class,
    uint8_t           flags,
    const uint8_t     file_id[16],
    const char       *pattern,
    uint32_t          max_out,
    uint8_t          *out,
    uint32_t          cap,
    uint32_t         *out_len)
{
    memset(out, 0, cap);
    return smb2_query_directory(c, info_class, flags, file_id, pattern,
                                max_out, out, cap, out_len);
} /* qdir */

/* Walk one QUERY_DIRECTORY reply, appending each entry's name to `names`.
 * Returns the number of entries decoded, or -1 on a malformed chain. */
static int
dir_collect(
    const struct dir_class *dc,
    const uint8_t          *buf,
    uint32_t                len,
    char                    names[][64],
    int                     max_names,
    int                    *count)
{
    uint32_t off     = 0;
    int      entries = 0;

    while (off + (uint32_t) dc->name_off <= len) {
        uint32_t next = g32(buf, (int) off);
        uint32_t nlen = g32(buf, (int) off + dc->len_off);
        int      i, n = (int) (nlen / 2);

        if (off + dc->name_off + nlen > len) {
            return -1;
        }
        if (*count < max_names && n < 63) {
            for (i = 0; i < n; i++) {
                names[*count][i] = (char) buf[off + dc->name_off + i * 2];
            }
            names[*count][n] = '\0';
            (*count)++;
        }
        entries++;
        if (next == 0) {
            break;
        }
        /* A NextEntryOffset that does not advance would spin forever. */
        if (next < (uint32_t) dc->name_off) {
            return -1;
        }
        off += next;
    }
    return entries;
} /* dir_collect */

static int
names_have(
    char        names[][64],
    int         count,
    const char *want)
{
    int i;

    for (i = 0; i < count; i++) {
        if (strcmp(names[i], want) == 0) {
            return 1;
        }
    }
    return 0;
} /* names_have */

static void
probe_query_directory(struct smb2_conn *c)
{
    struct smb2_create_out dir, f;
    uint8_t                buf[8192];
    char                   names[64][64];
    uint32_t               st, len = 0;
    unsigned int           k;
    int                    count, entries, base_count = 0;

    printf("# --- QUERY_DIRECTORY ---\n");

    /* A directory with a known population.  "." and ".." are reported too, so
     * the assertions are on the three real names being present rather than on
     * an exact entry count. */
    st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    CHECK(st == ST_SUCCESS, "setup: CREATE qdir -> 0x%08x", st);
    if (st != ST_SUCCESS) {
        return;
    }
    smb2_close(c, dir.file_id);

    {
        static const char *kids[] = { "alpha.txt", "beta.txt", "gamma.txt" };
        unsigned int       i;

        for (i = 0; i < 3; i++) {
            char path[64];

            snprintf(path, sizeof(path), "qdir\\%s", kids[i]);
            st = smb2_create(c, path, MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                             MBT_FILE_SHARE_RWD, NULL, &f);
            CHECK(st == ST_SUCCESS, "setup: CREATE %s -> 0x%08x", path, st);
            if (st == ST_SUCCESS) {
                smb2_close(c, f.file_id);
            }
        }
    }

    /* Every class must enumerate the same three names. */
    for (k = 0; k < sizeof(dir_classes) / sizeof(dir_classes[0]); k++) {
        const struct dir_class *dc = &dir_classes[k];

        st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                              MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
        if (st != ST_SUCCESS) {
            CHECK(0, "%s: re-open qdir -> 0x%08x", dc->name, st);
            continue;
        }

        count = 0;
        st    = qdir(c, dc->cls, 0, dir.file_id, "*", 8192,
                     buf, sizeof(buf), &len);
        CHECK(st == ST_SUCCESS && len > 0,
              "%s: QUERY_DIRECTORY(*) -> 0x%08x (%u bytes)", dc->name, st, len);

        /* Only decode a reply the server actually sent: smb2_query_directory
         * fills the buffer on success alone, so collecting after a failure
         * would walk uninitialised stack bytes. */
        entries = (st == ST_SUCCESS)
            ? dir_collect(dc, buf, len, names, 64, &count) : -1;
        CHECK(entries > 0, "  ... the entry chain decodes (%d entries)",
              entries);
        CHECK(names_have(names, count, "alpha.txt") &&
              names_have(names, count, "beta.txt") &&
              names_have(names, count, "gamma.txt"),
              "  ... it lists all three files");

        if (k == 0) {
            base_count = entries;
        } else {
            CHECK(entries == base_count,
                  "  ... it reports the same entry count as "
                  "FileDirectoryInformation (%d vs %d)", entries, base_count);
        }

        smb2_close(c, dir.file_id);
    }

    /* Statefulness: a second call on the same handle continues from where the
     * first stopped, and once the directory is exhausted the server answers
     * STATUS_NO_MORE_FILES rather than repeating itself. */
    st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    if (st == ST_SUCCESS) {
        st = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0,
                  dir.file_id, "*", 8192, buf, sizeof(buf),
                  &len);
        CHECK(st == ST_SUCCESS, "stateful: first call -> 0x%08x", st);

        st = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0,
                  dir.file_id, "*", 8192, buf, sizeof(buf),
                  &len);
        CHECK(st == ST_NO_MORE_FILES,
              "stateful: the exhausted directory answers NO_MORE_FILES "
              "(0x%08x)", st);

        /* SMB2_RESTART_SCANS rewinds the handle's position. */
        count = 0;
        st    = qdir(c, SMB2_FILE_DIRECTORY_INFO_T,
                     SMB2_RESTART_SCANS, dir.file_id, "*",
                     8192, buf, sizeof(buf), &len);
        CHECK(st == ST_SUCCESS && len > 0,
              "RESTART_SCANS rewinds and re-enumerates (0x%08x, %u bytes)",
              st, len);
        if (st == ST_SUCCESS) {
            dir_collect(&dir_classes[0], buf, len, names, 64, &count);
        }
        CHECK(names_have(names, count, "alpha.txt"),
              "  ... the rewound scan lists the files again");

        smb2_close(c, dir.file_id);
    }

    /* SMB2_RETURN_SINGLE_ENTRY caps the reply at one entry. */
    st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    if (st == ST_SUCCESS) {
        count = 0;
        st    = qdir(c, SMB2_FILE_DIRECTORY_INFO_T,
                     SMB2_RETURN_SINGLE_ENTRY, dir.file_id,
                     "*", 8192, buf, sizeof(buf), &len);
        CHECK(st == ST_SUCCESS, "RETURN_SINGLE_ENTRY -> 0x%08x", st);
        entries = (st == ST_SUCCESS)
            ? dir_collect(&dir_classes[0], buf, len, names, 64, &count) : -1;
        CHECK(entries == 1, "  ... exactly one entry is returned (%d)",
              entries);
        smb2_close(c, dir.file_id);
    }

    /* A pattern that matches one file selects it. */
    st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    if (st == ST_SUCCESS) {
        count = 0;
        st    = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0,
                     dir.file_id, "beta.txt", 8192, buf,
                     sizeof(buf), &len);
        CHECK(st == ST_SUCCESS, "pattern 'beta.txt' -> 0x%08x", st);
        if (st == ST_SUCCESS) {
            dir_collect(&dir_classes[0], buf, len, names, 64, &count);
        }
        CHECK(names_have(names, count, "beta.txt") &&
              !names_have(names, count, "alpha.txt"),
              "  ... only the matching name is returned");
        smb2_close(c, dir.file_id);
    }

    /* A pattern that matches nothing on the FIRST query of a handle.
     *
     * DEVIATION, pinned: MS-SMB2 3.3.5.18 distinguishes the two empty cases --
     * a first scan whose pattern matches nothing is STATUS_NO_SUCH_FILE
     * (0xC000000F), while STATUS_NO_MORE_FILES (0x80000006) means "this scan
     * is exhausted".  chimera answers NO_MORE_FILES for both, so a client
     * cannot tell "the name does not exist" from "you already read it all".
     * Recorded as-is rather than changed: it is a one-line status choice, but
     * the extended-tier pike and smbtorture directory cases assert against the
     * current behavior and this tier cannot run them. */
    st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    if (st == ST_SUCCESS) {
        st = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0,
                  dir.file_id, "nothing-here.xyz", 8192, buf,
                  sizeof(buf), &len);
        CHECK(st == ST_NO_MORE_FILES,
              "a first-scan pattern matching nothing answers NO_MORE_FILES "
              "where MS-SMB2 wants NO_SUCH_FILE (0x%08x)", st);
        smb2_close(c, dir.file_id);
    }

    /* An unsupported information class is INVALID_INFO_CLASS. */
    st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    if (st == ST_SUCCESS) {
        st = qdir(c, 0x7F, 0, dir.file_id, "*", 8192, buf,
                  sizeof(buf), &len);
        CHECK(st == ST_INVALID_INFO_CLASS,
              "an unsupported class is INVALID_INFO_CLASS (0x%08x)", st);

        /* An output buffer smaller than one entry header cannot hold a
         * result. */
        st = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0,
                  dir.file_id, "*", 8, buf, sizeof(buf), &len);
        CHECK(st != ST_SUCCESS,
              "an 8-byte output buffer is refused (0x%08x)", st);
        smb2_close(c, dir.file_id);
    }

    /* QUERY_DIRECTORY against a FILE handle is not a directory enumeration. */
    st = smb2_create(c, "qdir_notadir.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &f);
    if (st == ST_SUCCESS) {
        st = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0, f.file_id,
                  "*", 8192, buf, sizeof(buf), &len);
        CHECK(st != ST_SUCCESS,
              "QUERY_DIRECTORY on a file handle is refused (0x%08x)", st);
        smb2_close(c, f.file_id);
    }
} /* probe_query_directory */

/* ---- refusals ----------------------------------------------------------- */

static void
probe_refusals(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint8_t                buf[512];
    uint32_t               st, len;

    printf("# --- buffer-length and class refusals ---\n");

    st = smb2_create(c, "info_refuse.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &co);
    CHECK(st == ST_SUCCESS, "setup: CREATE -> 0x%08x", st);

    /* An OutputBufferLength below the class's fixed size is refused, not
     * truncated: the client would otherwise parse a short struct as a full
     * one.  BasicInformation needs 40. */
    st = smb2_query_info_len(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                             co.file_id, 0, 8, buf, sizeof(buf), &len);
    CHECK(st != ST_SUCCESS,
          "QUERY BasicInformation with an 8-byte buffer is refused (0x%08x)",
          st);

    /* A variable-length class has a fixed MINIMUM (FileAllInformation needs
     * 104, through the FileNameLength field) and a larger true length.  A
     * buffer between the two is STATUS_BUFFER_OVERFLOW.
     *
     * DEVIATION, pinned rather than fixed: chimera truncates output_length to
     * the caller's buffer and then emits the generic 9-byte SMB2 ERROR body
     * anyway, because chimera_smb_is_error_status() counts BUFFER_OVERFLOW as
     * an error and the IOCTL exemption in smb.c does not extend to QUERY_INFO.
     * So the truncation is computed and thrown away, and the client gets no
     * partial data -- the same defect that was fixed for
     * FSCTL_QUERY_ALLOCATED_RANGES.  Left alone here because the extended-tier
     * pike query.py `test_mismatch_0_*` cases cover exactly this short-buffer
     * behavior and this tier cannot run them. */
    st = smb2_query_info_len(c, SMB2_INFO_FILE_T, SMB2_FILE_ALL_INFO_T,
                             co.file_id, 0, 104, buf, sizeof(buf), &len);
    CHECK(st == ST_BUFFER_OVERFLOW && len == 0,
          "QUERY AllInformation with a 104-byte buffer overflows and returns "
          "no partial data (0x%08x, %u bytes)", st, len);

    /* A FILE class the server does not implement must report NOT_IMPLEMENTED,
     * not INVALID_INFO_CLASS: Samba's qfile_buffercheck treats the former as
     * "skip this level" and the latter as a failure. */
    st = smb2_query_info(c, SMB2_INFO_FILE_T, 0x7F, co.file_id, 0,
                         buf, sizeof(buf), &len);
    CHECK(st != ST_SUCCESS,
          "QUERY an unimplemented FILE class is refused (0x%08x)", st);

    /* InfoType QUOTA is not implemented. */
    st = smb2_query_info(c, SMB2_INFO_QUOTA_T, 0, co.file_id, 0,
                         buf, sizeof(buf), &len);
    CHECK(st != ST_SUCCESS, "QUERY InfoType QUOTA is refused (0x%08x)", st);

    smb2_close(c, co.file_id);
} /* probe_refusals */

static void
probe_namespace_boundaries(struct smb2_conn *c)
{
    struct smb2_create_out dir, file, peer, other;
    uint32_t               st, count = 0;

    printf("# --- directory I/O, same-link rename, and delete timing ---\n");
    st = smb2_create_opts(c, "boundary-dir", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    CHECK(st == ST_SUCCESS, "setup writable directory");
    if (st == ST_SUCCESS) {
        st = smb2_write(c, dir.file_id, 0, "x", 1, &count);
        CHECK(st == 0xC0000010u, "directory WRITE is INVALID_DEVICE_REQUEST -> 0x%08x", st);
        st = smb2_rename(c, dir.file_id, "boundary-dir", 0);
        CHECK(st == ST_SUCCESS, "same-link directory rename succeeds -> 0x%08x", st);
        smb2_close(c, dir.file_id);
    }
    st = smb2_create(c, "boundary-self", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &file);
    CHECK(st == ST_SUCCESS, "setup same-link file");
    if (st == ST_SUCCESS) {
        st = smb2_rename(c, file.file_id, "boundary-self", 0);
        CHECK(st == ST_SUCCESS, "same-link file rename without replace succeeds -> 0x%08x", st);
        st = smb2_write(c, file.file_id, 0, "retained", 8, &count);
        CHECK(st == ST_SUCCESS && count == 8, "same-link rename preserves usable handle");
        smb2_close(c, file.file_id);
    }

    for (int create_doc = 0; create_doc < 2; create_doc++) {
        const char *name = create_doc ? "boundary-create-doc" : "boundary-set-doc";
        st = smb2_create_opts(c, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                              MBT_FILE_SHARE_RWD, create_doc ? MBT_FILE_DELETE_ON_CLOSE : 0,
                              NULL, &file);
        CHECK(st == ST_SUCCESS, "setup deletion case %d", create_doc);
        if (st != ST_SUCCESS) {
            continue;
        }
        st = smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &peer);
        CHECK(st == ST_SUCCESS, "peer open before delete marking (%d) -> 0x%08x", create_doc, st);
        if (st != ST_SUCCESS) {
            smb2_close(c, file.file_id);
            continue;
        }
        if (!create_doc) {
            st = smb2_set_disposition(c, file.file_id, 1);
            CHECK(st == ST_SUCCESS, "mark existing link deleted");
        }
        uint8_t pending_info[24];
        if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, file.file_id,
                     pending_info, sizeof(pending_info), "delete intent query")) {
            CHECK(pending_info[20] == !create_doc,
                  "CREATE mode alone is not pending; SET disposition is (%d)", create_doc);
        }
        st = smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS, 0, NULL, &other);
        CHECK(st == (create_doc ? ST_SHARING_VIOLATION : ST_DELETE_PENDING),
              "CREATE-DOC waits for close, SET disposition precedes share conflicts (%d) -> 0x%08x", create_doc, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, other.file_id);
        }
        smb2_close(c, file.file_id);
        if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, peer.file_id,
                     pending_info, sizeof(pending_info), "surviving peer deletion state")) {
            CHECK(pending_info[20] == 1, "surviving peer reports shared delete-pending (%d)", create_doc);
        }
        st = smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &other);
        CHECK(st == ST_DELETE_PENDING, "name remains delete-pending until peer closes (%d) -> 0x%08x", create_doc, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, other.file_id);
        }
        smb2_close(c, peer.file_id);
        st = smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &other);
        CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "last peer close removes pending name (%d) -> 0x%08x", create_doc, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, other.file_id);
        }
    }
} /* probe_namespace_boundaries */

static void
probe_delete_survivor(struct smb2_conn *c)
{
    struct smb2_create_out file, peer, lookup, stream;
    uint32_t               st;

    printf("# --- deferred deletion cancellation and named streams ---\n");
    st = smb2_create(c, "cancel-deferred", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &file);
    CHECK(st == ST_SUCCESS, "setup deferred cancellation file");
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_create(c, "cancel-deferred", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &peer);
    CHECK(st == ST_SUCCESS, "open cancellation survivor");
    if (st != ST_SUCCESS) {
        smb2_close(c, file.file_id);
        return;
    }
    CHECK(smb2_set_disposition(c, file.file_id, 1) == ST_SUCCESS, "arm deferred delete");
    CHECK(smb2_close(c, file.file_id) == ST_SUCCESS, "close delete setter before survivor");
    st = smb2_rename(c, peer.file_id, "cancel-deferred-moved", 0);
    CHECK(st == ST_ACCESS_DENIED, "rename of marked source is denied -> 0x%08x", st);
    CHECK(smb2_set_disposition(c, peer.file_id, 0) == ST_SUCCESS, "survivor cancels deletion");
    smb2_close(c, peer.file_id);
    st = smb2_create(c, "cancel-deferred", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &lookup);
    CHECK(st == ST_SUCCESS, "cancelled pending record preserves name after last close -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, lookup.file_id);
    }

    st = smb2_create(c, "stream-deferred", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &file);
    CHECK(st == ST_SUCCESS, "setup base for stream last-close");
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_create(c, "stream-deferred:fork", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &stream);
    CHECK(st == ST_SUCCESS, "open named-stream final holder");
    if (st != ST_SUCCESS) {
        smb2_close(c, file.file_id);
        return;
    }
    CHECK(smb2_set_disposition(c, stream.file_id, 1) == ST_SUCCESS, "arm own-stream delete");
    CHECK(smb2_set_disposition(c, file.file_id, 1) == ST_SUCCESS, "arm base delete");
    CHECK(smb2_close(c, file.file_id) == ST_SUCCESS, "base close defers while stream holds file");
    st = smb2_create(c, "stream-deferred", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &lookup);
    CHECK(st == ST_DELETE_PENDING, "base remains pending with named stream open -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, lookup.file_id);
    }
    CHECK(smb2_close(c, stream.file_id) == ST_SUCCESS, "final stream close handles both deletion intents");
    st = smb2_create(c, "stream-deferred", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &lookup);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "final named-stream close removes pending base -> 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, lookup.file_id);
    }
} /* probe_delete_survivor */

static void
probe_disposition_validation(struct smb2_conn *c)
{
    struct smb2_create_out file, dir, child, lookup;
    uint8_t                input[40] = { 0 }, info[64];
    uint32_t               st;

    printf("# --- disposition validation before delete-pending publication ---\n");
    st = smb2_create(c, "disposition-readonly.bin", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &file);
    CHECK(st == ST_SUCCESS, "setup readonly disposition file");
    if (st != ST_SUCCESS) {
        return;
    }
    p32(input, 32, 1); /* FILE_ATTRIBUTE_READONLY */
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                       file.file_id, input, sizeof(input));
    CHECK(st == ST_SUCCESS, "set readonly attribute");
    st = smb2_set_disposition(c, file.file_id, 1);
    CHECK(st == 0xC0000121u, "readonly delete rejected with CANNOT_DELETE -> 0x%08x", st);
    if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, file.file_id,
                 info, sizeof(info), "readonly disposition nonmutation")) {
        CHECK(info[20] == 0, "readonly rejection leaves delete-pending clear");
    }
    const uint32_t unsupported[] = { 0x5u, 0x9u };
    for (size_t i = 0; i < sizeof(unsupported) / sizeof(unsupported[0]); i++) {
        p32(input, 0, unsupported[i]);
        st = smb2_set_info(c, SMB2_INFO_FILE_T, 0x40, file.file_id, input, 4);
        CHECK(st == ST_NOT_SUPPORTED, "unsupported disposition semantics rejected -> 0x%08x", st);
    }
    p32(input, 0, 0x3u); /* DELETE | POSIX_SEMANTICS still needs readonly admission. */
    st = smb2_set_info(c, SMB2_INFO_FILE_T, 0x40, file.file_id, input, 4);
    CHECK(st == 0xC0000121u, "POSIX delete preserves readonly admission -> 0x%08x", st);
    p32(input, 0, 0x80000001u);
    st = smb2_set_info(c, SMB2_INFO_FILE_T, 0x40, file.file_id, input, 4);
    CHECK(st == ST_INVALID_PARAMETER, "unknown disposition flags rejected -> 0x%08x", st);
    p32(input, 0, 0x11u); /* DELETE | IGNORE_READONLY_ATTRIBUTE */
    st = smb2_set_info(c, SMB2_INFO_FILE_T, 0x40, file.file_id, input, 4);
    CHECK(st == ST_SUCCESS, "explicit IGNORE_READONLY permits pending deletion -> 0x%08x", st);
    st = smb2_set_disposition(c, file.file_id, 0);
    CHECK(st == ST_SUCCESS, "clear pending deletion while still readonly");
    smb2_close(c, file.file_id);
    st = smb2_create(c, "disposition-readonly.bin", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &lookup);
    CHECK(st == ST_SUCCESS, "rejected/cancelled disposition preserved readonly file");
    if (st == ST_SUCCESS) {
        smb2_close(c, lookup.file_id);
    }
    st = smb2_create(c, "disposition-plain.bin", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &file);
    CHECK(st == ST_SUCCESS, "setup ordinary disposition file");
    if (st == ST_SUCCESS) {
        st = smb2_set_disposition(c, file.file_id, 1);
        CHECK(st == ST_SUCCESS, "ordinary file accepts delete-pending -> 0x%08x", st);
        smb2_close(c, file.file_id);
        st = smb2_create(c, "disposition-plain.bin", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &lookup);
        CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "accepted disposition removes file on close");
        if (st == ST_SUCCESS) {
            smb2_close(c, lookup.file_id);
        }
    }

    st = smb2_create_opts(c, "disposition-dir", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    CHECK(st == ST_SUCCESS, "setup disposition directory");
    if (st != ST_SUCCESS) {
        return;
    }
    st = smb2_create(c, "disposition-dir\\child", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &child);
    CHECK(st == ST_SUCCESS, "setup live directory entry");
    if (st == ST_SUCCESS) {
        smb2_close(c, child.file_id);
    }
    st = smb2_set_disposition(c, dir.file_id, 1);
    CHECK(st == 0xC0000101u, "nonempty directory delete rejected -> 0x%08x", st);
    if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, dir.file_id,
                 info, sizeof(info), "directory disposition nonmutation")) {
        CHECK(info[20] == 0, "nonempty-directory rejection leaves delete-pending clear");
    }
    smb2_close(c, dir.file_id);
    st = smb2_create(c, "disposition-dir\\child", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &lookup);
    CHECK(st == ST_SUCCESS, "rejected directory disposition preserved child");
    if (st == ST_SUCCESS) {
        smb2_close(c, lookup.file_id);
    }
} /* probe_disposition_validation */

/* Queue CLOSE on separate server connections before waiting for either reply.
 * Alternate CREATE and SET_INFO delete intent and use RO/RW handles so neither
 * cache-handle identity nor sequential close order can stand in for file state. */
static void
probe_concurrent_delete_close(struct smb2_conn *c)
{
    struct smb2_conn      *peer_conn = smb2_conn_open(c->env);
    struct smb2_create_out owner, peer, lookup;
    uint32_t               st, owner_status, peer_status;
    char                   name[64];

    smb2_handshake(peer_conn);
    printf("# --- concurrent last-close deletion ---\n");
    for (int round = 0; round < 24; round++) {
        int create_doc = round & 1;
        snprintf(name, sizeof(name), "concurrent-doc-%d", round);
        st = smb2_create_opts(c, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                              MBT_FILE_SHARE_RWD, create_doc ? MBT_FILE_DELETE_ON_CLOSE : 0,
                              NULL, &owner);
        CHECK(st == ST_SUCCESS, "concurrent DOC owner setup %d", round);
        if (st != ST_SUCCESS) {
            continue;
        }
        st = smb2_create(peer_conn, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &peer);
        CHECK(st == ST_SUCCESS, "concurrent DOC peer setup %d", round);
        if (st != ST_SUCCESS) {
            smb2_close(c, owner.file_id);
            continue;
        }
        if (!create_doc) {
            st = smb2_set_disposition(c, owner.file_id, 1);
            CHECK(st == ST_SUCCESS, "concurrent DOC disposition %d", round);
        }
        /* Alternate transport submission order without waiting for completion. */
        for (int n = 0; n < 2; n++) {
            int               send_owner = (n == 0) == ((round & 2) == 0);
            struct smb2_conn *conn       = send_owner ? c : peer_conn;
            const uint8_t    *fid        = send_owner ? owner.file_id : peer.file_id;
            int               b          = smb2c_begin(conn, SMB2_CLOSE, 0);
            uint8_t          *body       = conn->sbuf + b;
            p16(body, 0, 24);
            memcpy(body + 8, fid, 16);
            smb2c_send(conn, 24);
        }
        owner_status = smb2c_wait(c);
        peer_status  = smb2c_wait(peer_conn);
        CHECK(owner_status == ST_SUCCESS && peer_status == ST_SUCCESS,
              "both concurrent closes succeed %d -> 0x%08x/0x%08x",
              round, owner_status, peer_status);
        st = smb2_create(c, name, MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &lookup);
        CHECK(st == ST_OBJECT_NAME_NOT_FOUND,
              "concurrent last close removes name exactly once %d -> 0x%08x", round, st);
        if (st == ST_SUCCESS) {
            smb2_close(c, lookup.file_id);
        }
    }
} /* probe_concurrent_delete_close */

/* An explicit POSIX delete retires the name at the deleting close while old
 * handles retain their object. Recreating that name must not make the final
 * old close delete the replacement. Ordinary DOC is tested separately above. */
static void
probe_posix_delete(struct smb2_conn *c)
{
    const char            *names[] = { "posix-delete-base", "posix-delete-stream:held:$DATA" };
    struct smb2_create_out owner, peer, replacement, lookup;
    uint8_t                flags[4], bytes[8];
    uint32_t               st, count, length;

    printf("# --- explicit POSIX deletion with surviving handles ---\n");
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        st = smb2_create(c, names[i], MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &owner);
        CHECK(st == ST_SUCCESS, "POSIX delete owner setup %zu", i);
        if (st != ST_SUCCESS) {
            continue;
        }
        st = smb2_write(c, owner.file_id, 0, "old", 3, &count);
        CHECK(st == ST_SUCCESS && count == 3, "POSIX delete old data %zu", i);
        st = smb2_create(c, names[i], MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &peer);
        CHECK(st == ST_SUCCESS, "POSIX delete retained peer %zu", i);
        if (st != ST_SUCCESS) {
            smb2_close(c, owner.file_id);
            continue;
        }
        if (i == 0) {
            uint8_t link_input[128] = { 0 };
            int     name_len        = utf16le("posix-delete-alias", link_input + 20);
            p32(link_input, 16, name_len);
            st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_LINK_INFO_T,
                               owner.file_id, link_input, 20 + name_len);
            CHECK(st == ST_SUCCESS, "create surviving hardlink before POSIX delete");
        }
        p32(flags, 0, 0x3u); /* DELETE | POSIX_SEMANTICS */
        st = smb2_set_info(c, SMB2_INFO_FILE_T, 0x40, owner.file_id, flags, sizeof(flags));
        CHECK(st == ST_SUCCESS, "explicit POSIX disposition admitted %zu -> 0x%08x", i, st);
        st = smb2_close(c, owner.file_id);
        CHECK(st == ST_SUCCESS, "POSIX deleting close succeeds %zu -> 0x%08x", i, st);
        st = smb2_create(c, names[i], MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &replacement);
        CHECK(st == ST_SUCCESS, "POSIX deletion permits exclusive recreation before peer closes %zu -> 0x%08x", i, st);
        if (st == ST_SUCCESS) {
            st = smb2_write(c, replacement.file_id, 0, "new", 3, &count);
            CHECK(st == ST_SUCCESS && count == 3, "replacement data written %zu", i);
            smb2_close(c, replacement.file_id);
        }
        st = smb2_read(c, peer.file_id, 0, sizeof(bytes), bytes, &length);
        CHECK(st == ST_SUCCESS && length == 3 && memcmp(bytes, "old", 3) == 0,
              "old peer retains old object after POSIX unlink %zu", i);
        smb2_close(c, peer.file_id);
        st = smb2_create(c, names[i], MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                         MBT_FILE_SHARE_RWD, NULL, &lookup);
        CHECK(st == ST_SUCCESS, "old peer close preserves replacement name %zu", i);
        if (st == ST_SUCCESS) {
            st = smb2_read(c, lookup.file_id, 0, sizeof(bytes), bytes, &length);
            CHECK(st == ST_SUCCESS && length == 3 && memcmp(bytes, "new", 3) == 0,
                  "replacement bytes survive old peer close %zu", i);
            smb2_close(c, lookup.file_id);
        }
        if (i == 0) {
            st = smb2_create(c, "posix-delete-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                             MBT_FILE_SHARE_RWD, NULL, &lookup);
            CHECK(st == ST_SUCCESS, "POSIX deletion preserves separate hardlink");
            if (st == ST_SUCCESS) {
                st = smb2_set_info(c, SMB2_INFO_FILE_T, 0x40,
                                   lookup.file_id, flags, sizeof(flags));
                CHECK(st == ST_SUCCESS, "surviving hardlink accepts independent POSIX deletion");
                smb2_close(c, lookup.file_id);
                st = smb2_create(c, "posix-delete-alias", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                                 MBT_FILE_SHARE_RWD, NULL, &lookup);
                CHECK(st == ST_OBJECT_NAME_NOT_FOUND,
                      "consumed deletion marker does not prevent deleting another hardlink");
                if (st == ST_SUCCESS) {
                    smb2_close(c, lookup.file_id);
                }
            }
        }
    }
} /* probe_posix_delete */

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env        env;
    /* Named streams are off in the server by default; the stream section
     * needs them advertised. */
    struct smb2_env_opts   opts = { .named_streams = 1 };
    struct smb2_conn      *c;
    struct smb2_create_out file, dir;
    uint32_t               st;

    setvbuf(stdout, NULL, _IONBF, 0);

    smb2_env_start_opts(&env, &opts);
    c = smb2_conn_open(&env);
    smb2_handshake(c);

    printf("# dialect=0x%04x\n", c->dialect);

    /* The sweep runs twice: several classes take a different path for a
     * directory (no EOF, the DIRECTORY attribute set, a different normalized
     * name), and a class that only ever sees files would not cover it. */
    st = smb2_create(c, "sweep.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &file);
    if (st == ST_SUCCESS) {
        probe_query_sweep(c, file.file_id, "a file");
        probe_fs_attributes(c, file.file_id, FSA_MEMFS_STREAMS_ON,
                            "memfs, smb_named_streams on");
        smb2_close(c, file.file_id);
    } else {
        CHECK(0, "setup: CREATE sweep.bin -> 0x%08x", st);
    }

    st = smb2_create_opts(c, "sweepdir", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir);
    if (st == ST_SUCCESS) {
        probe_query_sweep(c, dir.file_id, "a directory");
        smb2_close(c, dir.file_id);
    } else {
        CHECK(0, "setup: CREATE sweepdir -> 0x%08x", st);
    }

    probe_agreement(c);
    probe_set_info(c);
    probe_set_info_access(c);
    probe_disposition_validation(c);
    probe_namespace_boundaries(c);
    probe_concurrent_delete_close(c);
    probe_delete_survivor(c);
    probe_posix_delete(c);
    probe_ea(c);
    probe_streams(c);
    probe_link(c);
    probe_security(c);
    probe_security_sids(c);
    probe_query_directory(c);
    probe_refusals(c);

    smb2_env_stop(&env);

    probe_fs_attributes_streams_off();

    if (failures) {
        fprintf(stderr, "%d info-class check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 info-class checks passed\n");
    return 0;
} /* main */
