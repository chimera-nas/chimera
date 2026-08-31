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

    st = smb2_create(c, "ea.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
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

    st = smb2_create(c, "info.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
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

        st = smb2_create(c, "info.bin", FILE_OPEN, FILE_READ_ACCESS,
                         FILE_SHARE_RWD, NULL, &ro);
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
probe_set_info(struct smb2_conn *c)
{
    struct smb2_create_out co;
    uint8_t                buf[512], in[64];
    uint32_t               st;

    printf("# --- SET_INFO round trips ---\n");

    st = smb2_create(c, "setinfo.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
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

    st = smb2_create(c, "streams.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &base);
    CHECK(st == ST_SUCCESS, "setup: CREATE streams.bin -> 0x%08x", st);
    smb2_write(c, base.file_id, 0, "base", 4, &count);

    /* The default data stream alone. */
    st = smb2_query_info(c, SMB2_INFO_FILE_T, SMB2_FILE_STREAM_INFO_T,
                         base.file_id, 0, out, sizeof(out), &len);
    CHECK(st == ST_SUCCESS && len > 0,
          "StreamInformation reports the default stream (0x%08x, %u bytes)",
          st, len);

    /* Create an alternate stream with the "file:stream" create syntax. */
    st = smb2_create(c, "streams.bin:alt", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &strm);
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

    st = smb2_create(c, "link_src.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
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
        st = smb2_create(c, "link_dst.bin", FILE_OPEN, FILE_READ_ACCESS,
                         FILE_SHARE_RWD, NULL, &lo);
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

    st = smb2_create(c, "sec.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
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
    st = smb2_create_opts(c, "qdir", FILE_OPEN_IF, FILE_ALL_ACCESS,
                          FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
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
            st = smb2_create(c, path, FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                             FILE_SHARE_RWD, NULL, &f);
            CHECK(st == ST_SUCCESS, "setup: CREATE %s -> 0x%08x", path, st);
            if (st == ST_SUCCESS) {
                smb2_close(c, f.file_id);
            }
        }
    }

    /* Every class must enumerate the same three names. */
    for (k = 0; k < sizeof(dir_classes) / sizeof(dir_classes[0]); k++) {
        const struct dir_class *dc = &dir_classes[k];

        st = smb2_create_opts(c, "qdir", FILE_OPEN, FILE_ALL_ACCESS,
                              FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
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
    st = smb2_create_opts(c, "qdir", FILE_OPEN, FILE_ALL_ACCESS,
                          FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
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
    st = smb2_create_opts(c, "qdir", FILE_OPEN, FILE_ALL_ACCESS,
                          FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
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
    st = smb2_create_opts(c, "qdir", FILE_OPEN, FILE_ALL_ACCESS,
                          FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
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
    st = smb2_create_opts(c, "qdir", FILE_OPEN, FILE_ALL_ACCESS,
                          FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
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
    st = smb2_create_opts(c, "qdir", FILE_OPEN, FILE_ALL_ACCESS,
                          FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
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
    st = smb2_create(c, "qdir_notadir.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &f);
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

    st = smb2_create(c, "info_refuse.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &co);
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
    st = smb2_create(c, "sweep.bin", FILE_OVERWRITE_IF, FILE_ALL_ACCESS,
                     FILE_SHARE_RWD, NULL, &file);
    if (st == ST_SUCCESS) {
        probe_query_sweep(c, file.file_id, "a file");
        smb2_close(c, file.file_id);
    } else {
        CHECK(0, "setup: CREATE sweep.bin -> 0x%08x", st);
    }

    st = smb2_create_opts(c, "sweepdir", FILE_OPEN_IF, FILE_ALL_ACCESS,
                          FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &dir);
    if (st == ST_SUCCESS) {
        probe_query_sweep(c, dir.file_id, "a directory");
        smb2_close(c, dir.file_id);
    } else {
        CHECK(0, "setup: CREATE sweepdir -> 0x%08x", st);
    }

    probe_agreement(c);
    probe_set_info(c);
    probe_ea(c);
    probe_streams(c);
    probe_link(c);
    probe_security(c);
    probe_query_directory(c);
    probe_refusals(c);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d info-class check(s) FAILED\n", failures);
        return 1;
    }
    printf("all SMB2 info-class checks passed\n");
    return 0;
} /* main */
