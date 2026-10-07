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

#define ST_NO_MORE_EAS           0x80000012u
#define ST_NONEXISTENT_EA_ENTRY  0xC0000051u
#define ST_NO_EAS_ON_FILE        0xC0000052u
#define SL_RESTART_SCAN_T        0x00000001u
#define SL_RETURN_SINGLE_ENTRY_T 0x00000002u
#define SL_INDEX_SPECIFIED_T     0x00000004u

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
        } else if (ic->cls == SMB2_FILE_FULL_EA_INFO_T) {
            /* An object with no EAs answers STATUS_NO_EAS_ON_FILE, as NTFS
             * does (MS-FSA 2.1.5.12.12); the EA section below reads a list. */
            CHECK(st == ST_NO_EAS_ON_FILE, "%s: %s -> 0x%08x (want "
                  "NO_EAS_ON_FILE)", what, ic->name, st);
        } else {
            /* A variable-length class may legitimately answer with nothing --
             * a directory's (absent) data stream -- so the sweep only requires
             * that the class is answered.  The stream section below checks
             * non-empty results with real content. */
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
#define FSA_SUPPORTS_OBJECT_IDS        0x00010000
#define FSA_NAMED_STREAMS              0x00040000
#define FSA_SUPPORTS_BLOCK_REFCOUNTING 0x08000000

/* memfs stores rich ACLs, punches holes, reflinks, keeps xattrs (where object
 * IDs live) and named streams, so with the knob on it advertises everything
 * chimera can derive. */
#define FSA_MEMFS_STREAMS_ON           (FSA_CASE_SENSITIVE_SEARCH |      \
                                        FSA_CASE_PRESERVED_NAMES |       \
                                        FSA_UNICODE_ON_DISK |            \
                                        FSA_PERSISTENT_ACLS |            \
                                        FSA_SUPPORTS_SPARSE_FILES |      \
                                        FSA_SUPPORTS_REPARSE_POINTS |    \
                                        FSA_SUPPORTS_OBJECT_IDS |        \
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

/* A FileFullEaInformation query with the scan flags, EaIndex, and EA list the
 * generic helper leaves at zero.  Unlike it, this one returns the data that
 * came back with a BUFFER_OVERFLOW, since a partial EA list is the point. */
static uint32_t
query_ea(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint32_t          flags,
    uint32_t          index,
    const uint8_t    *list,
    uint32_t          list_len,
    uint32_t          out_buf_len,
    uint8_t          *out,
    uint32_t          out_cap,
    uint32_t         *out_len)
{
    int      b    = smb2c_begin(c, SMB2_QUERY_INFO, 0);
    uint8_t *body = c->sbuf + b;
    uint32_t st;

    p16(body, 0, 41);
    body[2] = SMB2_INFO_FILE_T;
    body[3] = SMB2_FILE_FULL_EA_INFO_T;
    p32(body, 4, out_buf_len);
    p16(body, 8, list_len ? (uint16_t) (SMB2_HDR_SIZE + 40) : 0);
    p16(body, 10, 0);
    p32(body, 12, list_len);
    p32(body, 16, index);              /* AdditionalInformation = EaIndex */
    p32(body, 20, flags);
    memcpy(body + 24, file_id, 16);
    if (list_len) {
        memcpy(body + 40, list, list_len);
    }

    st       = smb2c_xfer(c, 40 + (int) list_len);
    *out_len = 0;
    if (st == ST_SUCCESS || st == ST_BUFFER_OVERFLOW) {
        const uint8_t *rb   = c->rbuf + 4 + SMB2_HDR_SIZE;
        uint16_t       doff = g16(rb, 2);
        uint32_t       dlen = g32(rb, 4);

        if (dlen > out_cap) {
            dlen = out_cap;
        }
        memcpy(out, c->rbuf + 4 + doff, dlen);
        *out_len = dlen;
    }
    return st;
} /* query_ea */

/* Count the entries of a FILE_FULL_EA_INFORMATION chain and copy out the name
 * and value length of the one at position want (0-based). */
static int
ea_walk(
    const uint8_t *out,
    uint32_t       len,
    int            want,
    char          *name,
    uint32_t      *vlen)
{
    uint32_t off = 0;
    int      n   = 0;

    name[0] = '\0';
    while (off + 8 <= len) {
        uint32_t next = g32(out, (int) off);
        uint8_t  nlen = out[off + 5];

        if (off + 8 + nlen + 1 > len) {
            break;
        }
        if (n == want) {
            memcpy(name, out + off + 8, nlen);
            name[nlen] = '\0';
            *vlen      = g16(out, (int) off + 6);
        }
        n++;
        if (next == 0) {
            break;
        }
        off += next;
    }
    return n;
} /* ea_walk */

/* The scan state MS-FSA keeps per open (Open.NextEaEntry), the client's EaIndex
 * and EA list, and the overflow rules -- the parts of NtQueryEaFile IFSTest's
 * EaInformation group drives.  The file holds USER.ONE and USER.TWO on entry;
 * a third EA is set in lower case to see it come back upper-cased. */
static void
probe_ea_scan(
    struct smb2_conn *c,
    const uint8_t     file_id[16])
{
    uint8_t  in[128], out[1024];
    char     name[64], first[64], second[64];
    uint32_t st, len, vlen = 0;
    int      n;

    n  = ea_put(in, 0, "lower.three", "3", 1);
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                       file_id, in, (uint32_t) n);
    CHECK(st == ST_SUCCESS, "SET a lower-case EA name -> 0x%08x", st);

    st = query_ea(c, file_id, SL_RESTART_SCAN_T, 0, NULL, 0, sizeof(out),
                  out, sizeof(out), &len);
    n = ea_walk(out, len, 2, name, &vlen);
    CHECK(st == ST_SUCCESS && n == 3, "a restarted scan returns all three "
          "EAs (0x%08x, %d entries)", st, n);
    CHECK(strcmp(name, "LOWER.THREE") == 0, "  ... in the order set, and "
          "upper-cased as NTFS returns them (3rd is \"%s\")", name);

    /* With every entry returned, the scan has nothing left. */
    st = query_ea(c, file_id, 0, 0, NULL, 0, sizeof(out), out, sizeof(out),
                  &len);
    CHECK(st == ST_NO_MORE_EAS, "a scan continued past the end -> "
          "NO_MORE_EAS (0x%08x)", st);

    /* One entry at a time, each query resuming where the last stopped. */
    st = query_ea(c, file_id, SL_RESTART_SCAN_T | SL_RETURN_SINGLE_ENTRY_T, 0,
                  NULL, 0, sizeof(out), out, sizeof(out), &len);
    n = ea_walk(out, len, 0, first, &vlen);
    CHECK(st == ST_SUCCESS && n == 1, "SL_RETURN_SINGLE_ENTRY returns one "
          "entry (0x%08x, %d)", st, n);
    st = query_ea(c, file_id, SL_RETURN_SINGLE_ENTRY_T, 0, NULL, 0,
                  sizeof(out), out, sizeof(out), &len);
    n = ea_walk(out, len, 0, second, &vlen);
    CHECK(st == ST_SUCCESS && n == 1 && strcmp(first, second) != 0,
          "  ... and the next query resumes at the next one (\"%s\" then "
          "\"%s\")", first, second);

    /* EaIndex is 1-based, the end is one past the last, and beyond that the
     * entry does not exist. */
    st = query_ea(c, file_id, SL_INDEX_SPECIFIED_T | SL_RETURN_SINGLE_ENTRY_T,
                  2, NULL, 0, sizeof(out), out, sizeof(out), &len);
    n = ea_walk(out, len, 0, name, &vlen);
    CHECK(st == ST_SUCCESS && n == 1 && strcmp(name, second) == 0,
          "EaIndex 2 returns the second entry (0x%08x, \"%s\")", st, name);
    st = query_ea(c, file_id, SL_INDEX_SPECIFIED_T, 4, NULL, 0, sizeof(out),
                  out, sizeof(out), &len);
    CHECK(st == ST_NO_MORE_EAS, "EaIndex one past the last -> NO_MORE_EAS "
          "(0x%08x)", st);
    st = query_ea(c, file_id, SL_INDEX_SPECIFIED_T, 5, NULL, 0, sizeof(out),
                  out, sizeof(out), &len);
    CHECK(st == ST_NONEXISTENT_EA_ENTRY, "EaIndex beyond that -> "
          "NONEXISTENT_EA_ENTRY (0x%08x)", st);
    st = query_ea(c, file_id, SL_INDEX_SPECIFIED_T, 0, NULL, 0, sizeof(out),
                  out, sizeof(out), &len);
    CHECK(st == ST_NONEXISTENT_EA_ENTRY, "EaIndex 0 -> NONEXISTENT_EA_ENTRY "
          "(0x%08x)", st);

    /* A buffer that holds the first entry but not the rest gets that entry
     * and BUFFER_OVERFLOW; one too small for any entry, BUFFER_TOO_SMALL. */
    st = query_ea(c, file_id, SL_RESTART_SCAN_T, 0, NULL, 0, 28, out,
                  sizeof(out), &len);
    n = ea_walk(out, len, 0, name, &vlen);
    CHECK(st == ST_BUFFER_OVERFLOW && n == 1, "a buffer with room for one "
          "entry -> BUFFER_OVERFLOW with that entry (0x%08x, %d entries, %u "
          "bytes)", st, n, len);
    st = query_ea(c, file_id, SL_RESTART_SCAN_T, 0, NULL, 0, 12, out,
                  sizeof(out), &len);
    CHECK(st == ST_BUFFER_TOO_SMALL, "a buffer with room for no entry -> "
          "BUFFER_TOO_SMALL (0x%08x)", st);

    /* An EA list names the entries to return, in its own order, matched
     * without regard to case; one the file lacks comes back empty. */
    memset(in, 0, sizeof(in));
    p32(in, 0, 16);                    /* FILE_GET_EA_INFORMATION: next */
    in[4] = 8;
    memcpy(in + 5, "user.two", 9);
    p32(in, 16, 0);
    in[20] = 7;
    memcpy(in + 21, "MISSING", 8);
    n  = 29;
    st = query_ea(c, file_id, 0, 0, in, (uint32_t) n, sizeof(out), out,
                  sizeof(out), &len);
    CHECK(st == ST_SUCCESS && ea_walk(out, len, 0, name, &vlen) == 2 &&
          strcmp(name, "USER.TWO") == 0 && vlen == 6,
          "an EA list returns the named EA first (0x%08x, \"%s\", %u bytes)",
          st, name, vlen);
    ea_walk(out, len, 1, name, &vlen);
    CHECK(strcmp(name, "MISSING") == 0 && vlen == 0,
          "  ... and an absent one with no value (\"%s\", %u bytes)", name,
          vlen);

    n  = ea_put(in, 0, "lower.three", "", 1);
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                       file_id, in, (uint32_t) n);
    CHECK(st == ST_SUCCESS, "cleanup: delete the lower-case EA -> 0x%08x", st);
} /* probe_ea_scan */

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

    /* A fresh file has no EAs, which NTFS reports as STATUS_NO_EAS_ON_FILE
     * rather than as an empty list. */
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
                         co.file_id, 0, out, sizeof(out), &len);
    CHECK(st == ST_NO_EAS_ON_FILE,
          "FullEaInformation on a fresh file -> NO_EAS_ON_FILE (0x%08x)", st);

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

    probe_ea_scan(c, co.file_id);

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

/* ---- NTFS file semantics IFSTest checks ---------------------------------
 *
 * Behaviours a Windows client sees from NTFS that a POSIX backend does not give
 * for free: AllocationSize in whole clusters following the MS-FSA allocation
 * rules, DeletePending as a property of the file, ARCHIVE set by a write, and a
 * rename that never replaces a directory. */
static uint64_t
std_alloc(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t         *eof)
{
    uint8_t out[64];

    if (!query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, file_id,
                  out, sizeof(out), "StandardInformation")) {
        *eof = 0;
        return 0;
    }
    *eof = g64(out, 8);
    return g64(out, 0);
} /* std_alloc */

static uint32_t
set_alloc(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    uint64_t          alloc)
{
    uint8_t buf[8];

    p64(buf, 0, alloc);
    return smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_ALLOCATION_INFO_T,
                         file_id, buf, sizeof(buf));
} /* set_alloc */

static void
probe_ntfs_semantics(struct smb2_conn *c)
{
    static uint8_t         payload[60000];
    struct smb2_create_out a, b, d1, d2;
    uint8_t                out[64], basic[40];
    uint64_t               alloc, eof, mtime;
    uint32_t               st, count = 0;

    printf("# --- NTFS file semantics ---\n");

    /* AllocationSize (MS-FSA 2.1.5.15.1 / 2.1.5.15.5): the end of file in
     * whole 4 KiB clusters whatever the backend allocates, an AllocationSize
     * set below EOF truncates to it, one above EOF is kept as a reservation,
     * and a truncation of the end of file gives clusters back only when the
     * handle closes. */
    st = smb2_create(c, "alloc.bin", MBT_FILE_OVERWRITE_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &a);
    CHECK(st == ST_SUCCESS, "setup: CREATE alloc.bin -> 0x%08x", st);
    st = smb2_write(c, a.file_id, 0, payload, sizeof(payload), &count);
    CHECK(st == ST_SUCCESS, "setup: WRITE 60000 bytes -> 0x%08x", st);
    alloc = std_alloc(c, a.file_id, &eof);
    CHECK(alloc == 0xF000, "60000 bytes are allocated in clusters (0x%llx, "
          "want 0xF000)", (unsigned long long) alloc);
    st    = set_alloc(c, a.file_id, 0x2000);
    alloc = std_alloc(c, a.file_id, &eof);
    CHECK(st == ST_SUCCESS && alloc == 0x2000 && eof == 0x2000,
          "AllocationSize below EOF truncates to it (0x%08x, alloc 0x%llx, "
          "eof 0x%llx)", st, (unsigned long long) alloc,
          (unsigned long long) eof);
    st    = set_alloc(c, a.file_id, 0x11001);
    alloc = std_alloc(c, a.file_id, &eof);
    CHECK(st == ST_SUCCESS && alloc == 0x12000 && eof == 0x2000,
          "AllocationSize above EOF reserves whole clusters (0x%08x, alloc "
          "0x%llx, eof 0x%llx)", st, (unsigned long long) alloc,
          (unsigned long long) eof);
    st    = smb2_set_eof(c, a.file_id, 0x11800);
    alloc = std_alloc(c, a.file_id, &eof);
    CHECK(st == ST_SUCCESS && alloc == 0x12000,
          "an EOF inside the reservation keeps it (alloc 0x%llx)",
          (unsigned long long) alloc);
    st    = smb2_set_eof(c, a.file_id, 0x800);
    alloc = std_alloc(c, a.file_id, &eof);
    CHECK(st == ST_SUCCESS && alloc == 0x12000 && eof == 0x800,
          "a truncation keeps the clusters while the handle is open, as NTFS "
          "frees them at cleanup (alloc 0x%llx, eof 0x%llx)",
          (unsigned long long) alloc, (unsigned long long) eof);
    smb2_close(c, a.file_id);
    st = smb2_create(c, "alloc.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &a);
    CHECK(st == ST_SUCCESS, "setup: reopen alloc.bin -> 0x%08x", st);
    alloc = std_alloc(c, a.file_id, &eof);
    CHECK(alloc == 0x1000, "  ... and the close gives them back (alloc 0x%llx)",
          (unsigned long long) alloc);

    /* DeletePending is the file's (MS-FSA Open.Link.IsDeleted): a disposition
     * set through one handle shows through another. */
    st = smb2_create(c, "alloc.bin", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &b);
    CHECK(st == ST_SUCCESS, "setup: second open of alloc.bin -> 0x%08x", st);
    st = smb2_set_disposition(c, a.file_id, 1);
    CHECK(st == ST_SUCCESS, "SET disposition on the first handle -> 0x%08x",
          st);
    if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T, b.file_id,
                 out, sizeof(out), "StandardInformation (second handle)")) {
        CHECK(out[20] == 1, "  ... the second handle reports DeletePending "
              "(%u)", out[20]);
    }
    smb2_close(c, a.file_id);
    smb2_close(c, b.file_id);

    /* A write marks a data file ARCHIVE (MS-FSA 2.1.4.17), even after the
     * client cleared it. */
    st = smb2_create(c, "archive.bin", MBT_FILE_OVERWRITE_IF,
                     MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &a);
    CHECK(st == ST_SUCCESS, "setup: CREATE archive.bin -> 0x%08x", st);
    memset(basic, 0, sizeof(basic));
    p32(basic, 32, 0x2);               /* HIDDEN, ARCHIVE clear */
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, a.file_id,
                       basic, sizeof(basic));
    CHECK(st == ST_SUCCESS, "SET attributes HIDDEN only -> 0x%08x", st);
    mtime = query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, a.file_id,
                     out, sizeof(out), "BasicInformation") ? g64(out, 16) : 0;
    st = smb2_write(c, a.file_id, 0, payload, 16, &count);
    CHECK(st == ST_SUCCESS, "setup: WRITE 16 bytes -> 0x%08x", st);
    if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, a.file_id,
                 out, sizeof(out), "BasicInformation")) {
        CHECK(g32(out, 32) == 0x22, "a write sets ARCHIVE (attributes 0x%x, "
              "want 0x22)", g32(out, 32));
        /* Setting only the attributes does not take control of the write
         * time: the write still advances it. */
        CHECK(g64(out, 16) > mtime, "  ... and advances the write time after "
              "an attributes-only set");
    }

    /* An access time set explicitly through a handle survives reads through
     * it (MS-FSA Open.UserSetAccessTime). */
    memset(basic, 0, sizeof(basic));
    p64(basic, 8, 100000000);          /* LastAccessTime: 10s after 1601 */
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, a.file_id,
                       basic, sizeof(basic));
    CHECK(st == ST_SUCCESS, "SET an old LastAccessTime -> 0x%08x", st);
    st = smb2_read(c, a.file_id, 0, 16, out, &count);
    CHECK(st == ST_SUCCESS, "setup: READ 16 bytes -> 0x%08x", st);
    if (query_ok(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, a.file_id,
                 out, sizeof(out), "BasicInformation")) {
        CHECK(g64(out, 8) == 100000000, "  ... a read keeps it (0x%llx)",
              (unsigned long long) g64(out, 8));
    }

    /* A read-only file can be superseded, though not overwritten. */
    memset(basic, 0, sizeof(basic));
    p32(basic, 32, MBT_FILE_ATTRIBUTE_READONLY);
    st = smb2_set_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, a.file_id,
                       basic, sizeof(basic));
    CHECK(st == ST_SUCCESS, "SET READONLY -> 0x%08x", st);
    smb2_close(c, a.file_id);
    st = smb2_create(c, "archive.bin", MBT_FILE_OVERWRITE, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &a);
    CHECK(st == ST_ACCESS_DENIED, "OVERWRITE of a read-only file -> "
          "ACCESS_DENIED (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, a.file_id);
    }
    st = smb2_create(c, "archive.bin", MBT_FILE_SUPERSEDE, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &a);
    CHECK(st == ST_SUCCESS, "SUPERSEDE of a read-only file -> SUCCESS "
          "(0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(c, a.file_id);
    }

    /* A rename never replaces a directory, even with ReplaceIfExists
     * (MS-FSA 2.1.5.15.12). */
    st = smb2_create_opts(c, "rendir1", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &d1);
    CHECK(st == ST_SUCCESS, "setup: CREATE rendir1 -> 0x%08x", st);
    st = smb2_create_opts(c, "rendir2", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                          MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &d2);
    CHECK(st == ST_SUCCESS, "setup: CREATE rendir2 -> 0x%08x", st);
    smb2_close(c, d2.file_id);
    st = smb2_rename(c, d1.file_id, "rendir2", 0);
    CHECK(st == ST_OBJECT_NAME_COLLISION, "a directory renamed onto another "
          "without ReplaceIfExists -> OBJECT_NAME_COLLISION (0x%08x)", st);
    st = smb2_rename(c, d1.file_id, "rendir2", 1);
    CHECK(st == ST_ACCESS_DENIED, "  ... and with it -> ACCESS_DENIED "
          "(0x%08x)", st);
    smb2_close(c, d1.file_id);
} /* probe_ntfs_semantics */

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

    /* A BUILTIN alias has no gid, but Windows keeps it as a file's group when
     * given it (IFSTest SetGroupSecurityTest sets BUILTIN\Administrators), so
     * it is kept as the group's native SID; the gid is untouched. */
    nlen = smb2_sd_build(built, sizeof(built), NULL, "S-1-5-32-544", NULL, 0);

    if (nlen > 0) {
        st = smb2_set_info_addl(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                                SEC_GROUP, built, (uint32_t) nlen);
        CHECK(st == ST_SUCCESS, "SET SECURITY(group BUILTIN\\Administrators) "
              "-> 0x%08x", st);

        st = smb2_query_info(c, SMB2_INFO_SECURITY_T, 0, co.file_id,
                             SEC_OWNER | SEC_GROUP, sd, sizeof(sd), &len);

        if (st == ST_SUCCESS && smb2_sd_parse(sd, len, &d) == 0) {
            CHECK(strcmp(d.group, "S-1-5-32-544") == 0,
                  "  ... the BUILTIN group is kept (%s)", d.group);
        } else {
            CHECK(0, "  ... QUERY after the group SET -> 0x%08x", st);
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

    /* MS-SMB2 3.3.5.18: the open must hold FILE_LIST_DIRECTORY, or the
     * enumeration is refused with STATUS_ACCESS_DENIED (#1391) -- the check
     * CHANGE_NOTIFY already makes.  The allowed side too: FILE_LIST_DIRECTORY
     * alone, and GENERIC_READ, which maps to it. */
    {
        static const struct {
            const char *name;
            uint32_t    access;
            uint32_t    expect;
        }
        /* *INDENT-OFF* */
        opens[] = {
            { "FILE_READ_ATTRIBUTES", MBT_FILE_READ_ATTRIBUTES, ST_ACCESS_DENIED },
            { "FILE_LIST_DIRECTORY",  MBT_FILE_LIST_DIRECTORY,  ST_SUCCESS       },
            { "GENERIC_READ",         MBT_GENERIC_READ,         ST_SUCCESS       },
        };
        /* *INDENT-ON* */
        unsigned int i;

        for (i = 0; i < sizeof(opens) / sizeof(opens[0]); i++) {
            st = smb2_create_opts(c, "qdir", MBT_FILE_OPEN, opens[i].access,
                                  MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE,
                                  NULL, &dir);
            CHECK(st == ST_SUCCESS, "open qdir with %s -> 0x%08x",
                  opens[i].name, st);
            if (st != ST_SUCCESS) {
                continue;
            }

            count = 0;
            st    = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0, dir.file_id, "*",
                         8192, buf, sizeof(buf), &len);
            CHECK(st == opens[i].expect,
                  "QUERY_DIRECTORY on a %s open -> 0x%08x (want 0x%08x)",
                  opens[i].name, st, opens[i].expect);
            if (st == ST_SUCCESS) {
                dir_collect(&dir_classes[0], buf, len, names, 64, &count);
                CHECK(names_have(names, count, "alpha.txt"),
                      "  ... the %s open lists the directory", opens[i].name);
            }
            smb2_close(c, dir.file_id);
        }
    }

    /* QUERY_DIRECTORY against a FILE handle is not a directory enumeration:
     * STATUS_INVALID_PARAMETER (MS-SMB2 3.3.5.18), whatever the open's
     * access -- the type error outranks the FILE_LIST_DIRECTORY check, so an
     * open without it gets INVALID_PARAMETER too, not ACCESS_DENIED. */
    {
        static const struct {
            const char *name;
            uint32_t    access;
        }
        /* *INDENT-OFF* */
        file_opens[] = {
            { "FILE_ALL_ACCESS",      MBT_FILE_ALL_ACCESS      },
            { "FILE_READ_ATTRIBUTES", MBT_FILE_READ_ATTRIBUTES },
        };
        /* *INDENT-ON* */
        unsigned int i;

        for (i = 0; i < sizeof(file_opens) / sizeof(file_opens[0]); i++) {
            st = smb2_create(c, "qdir_notadir.bin", MBT_FILE_OPEN_IF,
                             file_opens[i].access, MBT_FILE_SHARE_RWD, NULL,
                             &f);
            CHECK(st == ST_SUCCESS, "open qdir_notadir.bin with %s -> 0x%08x",
                  file_opens[i].name, st);
            if (st != ST_SUCCESS) {
                continue;
            }
            st = qdir(c, SMB2_FILE_DIRECTORY_INFO_T, 0, f.file_id,
                      "*", 8192, buf, sizeof(buf), &len);
            CHECK(st == ST_INVALID_PARAMETER,
                  "QUERY_DIRECTORY on a %s file handle -> 0x%08x "
                  "(want INVALID_PARAMETER)", file_opens[i].name, st);
            smb2_close(c, f.file_id);
        }
    }
} /* probe_query_directory */

/* ---- refusals ----------------------------------------------------------- */

/* FileAttributes of a file just created with FileAttributes = NORMAL. */
static uint32_t
created_file_attributes(
    struct smb2_conn *c,
    const char       *name,
    uint32_t          file_attributes,
    uint32_t         *st)
{
    struct smb2_create_out co;
    uint8_t                basic[64];
    uint32_t               len = 0, attrs = 0;

    *st = smb2_create_attrs(c, name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, file_attributes, &co);
    if (*st != ST_SUCCESS) {
        return 0;
    }
    if (smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T, co.file_id, 0,
                        basic, sizeof(basic), &len) == ST_SUCCESS && len >= 36) {
        /* FILE_BASIC_INFORMATION: 4 timestamps (32) then FileAttributes(4). */
        attrs = g32(basic, 32);
    }
    smb2_close(c, co.file_id);
    return attrs;
} /* created_file_attributes */

/* A CREATE's DOS attributes come from its own FileAttributes and nothing
 * else.  The request slot is pooled, and the create path ORs ARCHIVE into the
 * slot's set_attr; when the field was not reset, a plain create reused the
 * previous CREATE's READONLY bit, so a new file came out read-only and every
 * later write open of it was refused (found with the Windows client).
 * Alternate the two kinds of create so each normal one follows a READONLY one
 * on whatever slot the server hands out. */
static void
probe_create_attributes_not_inherited(struct smb2_conn *c)
{
    char     name[32];
    uint32_t st, attrs;
    int      leaked = 0, ro_seen = 0;

    for (int i = 0; i < 8; i++) {
        snprintf(name, sizeof(name), "attr-ro-%d.bin", i);
        attrs = created_file_attributes(c, name, MBT_FILE_ATTRIBUTE_READONLY, &st);
        if (st == ST_SUCCESS && (attrs & MBT_FILE_ATTRIBUTE_READONLY)) {
            ro_seen++;
        }

        snprintf(name, sizeof(name), "attr-plain-%d.bin", i);
        attrs = created_file_attributes(c, name, MBT_FILE_ATTRIBUTE_NORMAL, &st);
        CHECK(st == ST_SUCCESS, "CREATE %s (FileAttributes NORMAL) -> 0x%08x", name, st);
        if (attrs & MBT_FILE_ATTRIBUTE_READONLY) {
            leaked++;
        }
    }

    CHECK(ro_seen == 8, "a CREATE with FileAttributes READONLY makes a read-only file "
          "(%d of 8)", ro_seen);
    CHECK(leaked == 0, "a CREATE with FileAttributes NORMAL never inherits READONLY "
          "from an earlier CREATE (%d of 8 did)", leaked);
} /* probe_create_attributes_not_inherited */

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
    probe_ntfs_semantics(c);
    probe_ea(c);
    probe_streams(c);
    probe_link(c);
    probe_security(c);
    probe_security_sids(c);
    probe_query_directory(c);
    probe_refusals(c);
    probe_create_attributes_not_inherited(c);

    smb2_env_stop(&env);

    probe_fs_attributes_streams_off();

    if (failures) {
        fprintf(stderr, "%d info-class check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 info-class checks passed\n");
    return 0;
} /* main */
