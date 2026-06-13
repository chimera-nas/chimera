// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <ctype.h>
#include <sys/stat.h>
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "common/format.h"
#include "s3_internal.h"
#include "s3_procs.h"
#include "s3_etag.h"

/* Chunk size for response iovecs. The listing is rendered into a chain of
 * fixed chunks so a large (but max-keys bounded) page never overruns a single
 * buffer the way the old fixed 1 MiB scratch did. */
#define CHIMERA_S3_OUT_CHUNK (256 * 1024)

/*
 * S3 object listing (ListObjects V1 + ListObjectsV2 + ListObjectVersions).
 *
 * The VFS `find` walks the bucket subtree depth-first, invoking our filter to
 * decide descent and our callback for every entry. We collect the matching
 * objects and rolled-up CommonPrefixes into an in-memory array, sort it
 * lexicographically (S3 mandates sorted keys), apply the requested page window
 * (marker / continuation-token / start-after + max-keys), and render the V1 or
 * V2 response shape. There is no server-side cursor: each page re-walks and
 * re-sorts, then slices past the caller's token. This is O(n) per page but
 * correct and stateless.
 *
 * chimera has no object versioning: every object is its own single, latest,
 * "null" version. ListObjectVersions therefore reuses the same collection/sort/
 * page machinery and only differs in the rendered XML (ListVersionsResult with
 * <Version> elements and KeyMarker pagination).
 */

/* ---------------------------------------------------------------------------
* Chunked output builder
* ------------------------------------------------------------------------- */

struct chimera_s3_out {
    struct evpl      *evpl;
    struct evpl_iovec iov[CHIMERA_S3_IOV_MAX];
    int               niov;
    char             *base;  /* start of current chunk */
    char             *cur;   /* write cursor within current chunk */
    int               cap;   /* usable bytes in current chunk */
};

static void
chimera_s3_out_init(
    struct chimera_s3_out *o,
    struct evpl           *evpl)
{
    o->evpl = evpl;
    o->niov = 0;
    o->base = NULL;
    o->cur  = NULL;
    o->cap  = 0;
} /* chimera_s3_out_init */

static void
chimera_s3_out_flush(struct chimera_s3_out *o)
{
    if (o->base) {
        evpl_iovec_set_length(&o->iov[o->niov], o->cur - o->base);
        o->niov++;
        o->base = NULL;
    }
} /* chimera_s3_out_flush */

/* Ensure the current chunk has room for `need` bytes, opening a new one if
 * not. `need` for a single append never exceeds the temp buffer in
 * chimera_s3_out_append, which is comfortably below CHIMERA_S3_OUT_CHUNK. */
static void
chimera_s3_out_reserve(
    struct chimera_s3_out *o,
    int                    need)
{
    int sz;

    if (o->base && (o->cur - o->base) + need <= o->cap) {
        return;
    }

    chimera_s3_out_flush(o);

    chimera_s3_abort_if(o->niov >= CHIMERA_S3_IOV_MAX,
                        "s3 list response exceeds %d iovecs", CHIMERA_S3_IOV_MAX);

    sz = need > CHIMERA_S3_OUT_CHUNK ? need : CHIMERA_S3_OUT_CHUNK;

    evpl_iovec_alloc(o->evpl, sz, 0, 1, 0, &o->iov[o->niov]);

    o->base = evpl_iovec_data(&o->iov[o->niov]);
    o->cur  = o->base;
    o->cap  = evpl_iovec_length(&o->iov[o->niov]);
} /* chimera_s3_out_reserve */

static void
chimera_s3_out_append(
    struct chimera_s3_out *o,
    const char            *fmt,
    ...)
{
    /* Worst case for one append is a single XML element wrapping one escaped
     * key (each byte may expand to "&quot;"/"&apos;" = 6 chars) plus fixed
     * markup, so size for the escaped key expansion plus slack. */
    char    tmp[CHIMERA_S3_KEY_MAX * 6 + 1024];
    va_list ap;
    int     n;

    va_start(ap, fmt);
    n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);

    if (n <= 0) {
        return;
    }
    if (n >= (int) sizeof(tmp)) {
        n = sizeof(tmp) - 1;
    }

    chimera_s3_out_reserve(o, n);

    memcpy(o->cur, tmp, n);
    o->cur += n;
} /* chimera_s3_out_append */

/* ---------------------------------------------------------------------------
* String encoding for response values
* ------------------------------------------------------------------------- */

static inline int
chimera_s3_url_unreserved(int c)
{
    return isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~';
} /* chimera_s3_url_unreserved */

/* Encode a value for emission inside an XML element. When the client asked for
 * encoding-type=url we percent-encode (which is also XML-safe); otherwise we
 * XML-escape the metacharacters. Returns the NUL-terminated length. */
static int
chimera_s3_encode(
    struct chimera_s3_request *request,
    char                      *dst,
    int                        cap,
    const char                *s,
    int                        slen)
{
    static const char hex[] = "0123456789ABCDEF";
    int               o     = 0;
    int               i;

    if (request->list.encoding_url) {
        for (i = 0; i < slen && o < cap - 4; i++) {
            unsigned char c = (unsigned char) s[i];
            if (chimera_s3_url_unreserved(c)) {
                dst[o++] = (char) c;
            } else {
                dst[o++] = '%';
                dst[o++] = hex[c >> 4];
                dst[o++] = hex[c & 0xf];
            }
        }
    } else {
        for (i = 0; i < slen && o < cap - 7; i++) {
            char c = s[i];
            switch (c) {
                case '&':
                    memcpy(dst + o, "&amp;", 5); o += 5; break;
                case '<':
                    memcpy(dst + o, "&lt;", 4); o += 4; break;
                case '>':
                    memcpy(dst + o, "&gt;", 4); o += 4; break;
                case '"':
                    memcpy(dst + o, "&quot;", 6); o += 6; break;
                case '\'':
                    memcpy(dst + o, "&apos;", 6); o += 6; break;
                default:
                    dst[o++] = c; break;
            } /* switch */
        }
    }

    dst[o] = '\0';
    return o;
} /* chimera_s3_encode */

/* ---------------------------------------------------------------------------
* Opaque continuation tokens
*
* ListObjectsV2 continuation tokens are opaque to the client, which echoes
* them back verbatim. We encode the last key of a page as base64url (no
* padding) so the token survives both XML emission and query-string
* round-tripping regardless of encoding-type, then decode it to recover the
* resume key.
* ------------------------------------------------------------------------- */

static const char chimera_s3_b64url[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static int
chimera_s3_b64url_encode(
    char                *dst,
    int                  cap,
    const unsigned char *src,
    int                  slen)
{
    int o = 0, i = 0;

    while (i < slen && o + 4 < cap) {
        uint32_t v = src[i] << 16;
        int      n = 1;

        if (i + 1 < slen) {
            v |= src[i + 1] << 8;
            n++;
        }
        if (i + 2 < slen) {
            v |= src[i + 2];
            n++;
        }

        dst[o++] = chimera_s3_b64url[(v >> 18) & 0x3f];
        dst[o++] = chimera_s3_b64url[(v >> 12) & 0x3f];
        if (n >= 2) {
            dst[o++] = chimera_s3_b64url[(v >> 6) & 0x3f];
        }
        if (n >= 3) {
            dst[o++] = chimera_s3_b64url[v & 0x3f];
        }
        i += 3;
    }

    dst[o] = '\0';
    return o;
} /* chimera_s3_b64url_encode */

static int
chimera_s3_b64url_val(int c)
{
    if (c >= 'A' && c <= 'Z') {
        return c - 'A';
    }
    if (c >= 'a' && c <= 'z') {
        return c - 'a' + 26;
    }
    if (c >= '0' && c <= '9') {
        return c - '0' + 52;
    }
    if (c == '-') {
        return 62;
    }
    if (c == '_') {
        return 63;
    }
    return -1;
} /* chimera_s3_b64url_val */

static int
chimera_s3_b64url_decode(
    unsigned char *dst,
    int            cap,
    const char    *src,
    int            slen)
{
    uint32_t v    = 0;
    int      bits = 0;
    int      o    = 0;
    int      i;

    for (i = 0; i < slen; i++) {
        int d = chimera_s3_b64url_val((unsigned char) src[i]);
        if (d < 0) {
            continue;
        }
        v     = (v << 6) | d;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            if (o < cap) {
                dst[o++] = (unsigned char) ((v >> bits) & 0xff);
            }
        }
    }

    return o;
} /* chimera_s3_b64url_decode */

/* ---------------------------------------------------------------------------
* Query-parameter setup
* ------------------------------------------------------------------------- */

void
chimera_s3_list_setup(
    struct chimera_s3_request *request,
    int                        list_type,
    int                        max_keys,
    int                        encoding_url,
    const char                *prefix,
    int                        prefix_len,
    const char                *delimiter,
    int                        delimiter_len,
    const char                *marker,
    int                        marker_len,
    const char                *ctoken,
    int                        ctoken_len,
    const char                *startafter,
    int                        startafter_len)
{
    const char *slash;
    const char *sk  = NULL;
    int         skl = 0;

    request->is_list           = 1;
    request->list.entries      = NULL;
    request->list.n_entries    = 0;
    request->list.cap_entries  = 0;
    request->list.list_type    = (list_type == 2) ? 2 : 1;
    request->list.encoding_url = encoding_url;
    /* versions and fetch_owner are set by the dispatcher after setup;
     * default off. */
    request->list.versions    = 0;
    request->list.fetch_owner = 0;

    if (max_keys < 0) {
        max_keys = 1000;
    }
    if (max_keys > 1000) {
        /* S3 returns at most 1000 keys per page regardless of the request. */
        max_keys = 1000;
    }
    request->list.max_keys = max_keys;

    if (prefix_len > CHIMERA_S3_KEY_MAX - 1) {
        prefix_len = CHIMERA_S3_KEY_MAX - 1;
    }
    memcpy(request->list.prefix, prefix, prefix_len);
    request->list.prefix[prefix_len] = '\0';
    request->list.prefix_len         = prefix_len;

    if (delimiter_len > CHIMERA_S3_DELIM_MAX - 1) {
        delimiter_len = CHIMERA_S3_DELIM_MAX - 1;
    }
    memcpy(request->list.delimiter, delimiter, delimiter_len);
    request->list.delimiter[delimiter_len] = '\0';
    request->list.delimiter_len            = delimiter_len;

    /* For the common '/' delimiter we can prune the VFS walk to the directory
     * containing the prefix and never descend below it. enumdir is the prefix
     * truncated at its last '/' (the directory whose children we enumerate). */
    request->list.enumdir[0]  = '\0';
    request->list.enumdir_len = 0;
    if (delimiter_len == 1 && delimiter[0] == '/') {
        slash = rindex(request->list.prefix, '/');
        if (slash) {
            int el = slash - request->list.prefix;
            memcpy(request->list.enumdir, request->list.prefix, el);
            request->list.enumdir[el] = '\0';
            request->list.enumdir_len = el;
        }
    }

    if (marker_len > CHIMERA_S3_KEY_MAX - 1) {
        marker_len = CHIMERA_S3_KEY_MAX - 1;
    }
    memcpy(request->list.marker, marker, marker_len);
    request->list.marker[marker_len] = '\0';
    request->list.marker_len         = marker_len;

    if (ctoken_len > CHIMERA_S3_KEY_MAX - 1) {
        ctoken_len = CHIMERA_S3_KEY_MAX - 1;
    }
    memcpy(request->list.ctoken, ctoken, ctoken_len);
    request->list.ctoken[ctoken_len] = '\0';
    request->list.ctoken_len         = ctoken_len;

    if (startafter_len > CHIMERA_S3_KEY_MAX - 1) {
        startafter_len = CHIMERA_S3_KEY_MAX - 1;
    }
    memcpy(request->list.startafter, startafter, startafter_len);
    request->list.startafter[startafter_len] = '\0';
    request->list.startafter_len             = startafter_len;

    /* Effective pagination start (exclusive). V2: continuation-token (an opaque
     * base64url blob) wins over start-after; V1 uses marker. start-after and
     * marker are real keys supplied by the caller. */
    request->list.has_start = 0;
    request->list.start_len = 0;
    request->list.start[0]  = '\0';

    if (request->list.list_type == 2 && ctoken_len > 0) {
        int dl = chimera_s3_b64url_decode((unsigned char *) request->list.start,
                                          CHIMERA_S3_KEY_MAX - 1,
                                          request->list.ctoken, ctoken_len);
        request->list.start[dl] = '\0';
        request->list.start_len = dl;
        request->list.has_start = 1;
    } else {
        if (request->list.list_type == 2) {
            if (startafter_len > 0) {
                sk  = request->list.startafter;
                skl = startafter_len;
            }
        } else if (marker_len > 0) {
            sk  = request->list.marker;
            skl = marker_len;
        }

        if (sk) {
            memcpy(request->list.start, sk, skl);
            request->list.start[skl] = '\0';
            request->list.start_len  = skl;
            request->list.has_start  = 1;
        }
    }
} /* chimera_s3_list_setup */

/* ---------------------------------------------------------------------------
* Entry collection
* ------------------------------------------------------------------------- */

static struct chimera_s3_list_entry *
chimera_s3_list_new_entry(struct chimera_s3_request *request)
{
    if (request->list.n_entries == request->list.cap_entries) {
        int newcap = request->list.cap_entries ? request->list.cap_entries * 2 : 64;
        request->list.entries = realloc(request->list.entries,
                                        newcap * sizeof(*request->list.entries));
        request->list.cap_entries = newcap;
    }
    return &request->list.entries[request->list.n_entries++];
} /* chimera_s3_list_new_entry */

static void
chimera_s3_list_add_object(
    struct chimera_s3_request      *request,
    const char                     *key,
    int                             keylen,
    const struct chimera_vfs_attrs *attr)
{
    struct chimera_s3_list_entry *e = chimera_s3_list_new_entry(request);

    e->is_prefix = 0;
    e->key       = strndup(key, keylen);
    e->size      = attr->va_size;
    e->mtime     = attr->va_mtime;
    chimera_s3_compute_etag(e->etag, attr);
} /* chimera_s3_list_add_object */

static void
chimera_s3_list_add_prefix(
    struct chimera_s3_request *request,
    const char                *key,
    int                        keylen)
{
    struct chimera_s3_list_entry *e = chimera_s3_list_new_entry(request);

    e->is_prefix = 1;
    e->key       = strndup(key, keylen);
    e->size      = 0;
    e->etag[0]   = 0;
    e->etag[1]   = 0;
    memset(&e->mtime, 0, sizeof(e->mtime));
} /* chimera_s3_list_add_prefix */

/* Strip the single leading '/' that vfs_find prepends to every path so all
 * comparisons run against the bucket-relative S3 key. */
static inline const char *
chimera_s3_list_key(
    const char *path,
    int         pathlen,
    int        *keylen)
{
    if (pathlen > 0 && path[0] == '/') {
        *keylen = pathlen - 1;
        return path + 1;
    }
    *keylen = pathlen;
    return path;
} /* chimera_s3_list_key */

/* ---------------------------------------------------------------------------
* VFS find filter (descent control) and callback (collection)
* ------------------------------------------------------------------------- */

/* Returns non-zero to PRUNE (do not descend into this directory). */
static int
chimera_s3_list_filter(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_s3_request *request = private_data;
    int                        klen;
    const char                *k = chimera_s3_list_key(path, pathlen, &klen);

    if (request->list.delimiter_len == 1 && request->list.delimiter[0] == '/') {
        /* Folder-style: only descend ancestors-or-equal of enumdir so we read
        * exactly one level and roll subdirectories up into CommonPrefixes. */
        const char *e  = request->list.enumdir;
        int         el = request->list.enumdir_len;

        if (el >= klen && memcmp(e, k, klen) == 0 &&
            (el == klen || e[klen] == '/')) {
            return 0;
        }
        return 1;
    } else {
        /* Flat or arbitrary-delimiter: descend any directory that is on the
         * path to the prefix or wholly inside the prefix subtree. */
        const char *p  = request->list.prefix;
        int         pl = request->list.prefix_len;

        if (klen >= pl) {
            return memcmp(k, p, pl) == 0 ? 0 : 1;
        }
        return (memcmp(k, p, klen) == 0 && p[klen] == '/') ? 0 : 1;
    }
} /* chimera_s3_list_filter */

static int
chimera_s3_list_find_callback(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_s3_request *request = private_data;
    int                        klen;
    const char                *k = chimera_s3_list_key(path, pathlen, &klen);
    int                        isdir;
    int                        pl = request->list.prefix_len;
    const char                *p  = request->list.prefix;
    int                        dl = request->list.delimiter_len;

    chimera_s3_abort_if((attr->va_set_mask & (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT)) !=
                        (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT),
                        "find returned without expected attributes");

    isdir = (attr->va_mode & S_IFMT) == S_IFDIR;

    if (dl == 0) {
        /* Flat listing: every file under the prefix is an object. */
        if (isdir || klen < pl || memcmp(k, p, pl) != 0) {
            return 0;
        }
        chimera_s3_list_add_object(request, k, klen, attr);
        return 0;
    }

    if (isdir) {
        /* Only '/' subdirectories collapse to CommonPrefixes here; other
         * delimiters are resolved over file keys below, so directories are
         * pure traversal. */
        if (!(dl == 1 && request->list.delimiter[0] == '/')) {
            return 0;
        }
        /* The subdirectory itself is the CommonPrefix "<key>/". A directory
         * shorter than the prefix (e.g. the enumdir we descended through) is
         * traversal only, not a result. */
        if (klen < pl || memcmp(k, p, pl) != 0) {
            return 0;
        }
        {
            char cp[CHIMERA_S3_KEY_MAX + 2];
            int  n = klen < CHIMERA_S3_KEY_MAX ? klen : CHIMERA_S3_KEY_MAX;
            memcpy(cp, k, n);
            cp[n]     = '/';
            cp[n + 1] = '\0';
            chimera_s3_list_add_prefix(request, cp, n + 1);
        }
        return 0;
    } else {
        /* File key: roll up at the first delimiter past the prefix, else it is
         * an object directly under the prefix. */
        const char *rest;
        int         restlen, i, idx = -1;

        if (klen < pl || memcmp(k, p, pl) != 0) {
            return 0;
        }

        rest    = k + pl;
        restlen = klen - pl;

        for (i = 0; i + dl <= restlen; i++) {
            if (memcmp(rest + i, request->list.delimiter, dl) == 0) {
                idx = i;
                break;
            }
        }

        if (idx >= 0) {
            chimera_s3_list_add_prefix(request, k, pl + idx + dl);
        } else {
            chimera_s3_list_add_object(request, k, klen, attr);
        }
        return 0;
    }
} /* chimera_s3_list_find_callback */

/* ---------------------------------------------------------------------------
* Sort, page, render
* ------------------------------------------------------------------------- */

static int
chimera_s3_list_cmp(
    const void *a,
    const void *b)
{
    const struct chimera_s3_list_entry *ea = a;
    const struct chimera_s3_list_entry *eb = b;

    return strcmp(ea->key, eb->key);
} /* chimera_s3_list_cmp */

static void
chimera_s3_list_find_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    struct chimera_s3_list_entry    *ents    = request->list.entries;
    struct chimera_s3_out            out;
    int                              n = request->list.n_entries;
    int                              w, r, i;
    int                              start_idx, emit, key_count;
    int                              truncated;
    int                              versions = request->list.versions;
    char                             enc[CHIMERA_S3_KEY_MAX * 6 + 8];
    const char                      *next = NULL;
    uint64_t                         total;

    /* A permission failure walking the bucket directory (the requester lacks
     * read/execute on it) must surface as AccessDenied rather than an empty
     * listing, so per-credential enforcement is visible to the client. */
    if (error_code == CHIMERA_VFS_EACCES || error_code == CHIMERA_VFS_EPERM) {
        for (i = 0; i < n; i++) {
            free(ents[i].key);
        }
        free(ents);
        request->list.entries     = NULL;
        request->list.n_entries   = 0;
        request->list.cap_entries = 0;

        request->status    = CHIMERA_S3_STATUS_ACCESS_DENIED;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* S3 returns keys in lexicographic order; the VFS walk does not. */
    if (n > 0) {
        qsort(ents, n, sizeof(*ents), chimera_s3_list_cmp);
    }

    /* Collapse duplicate CommonPrefixes (an arbitrary-delimiter walk rolls
     * many file keys into the same prefix). Duplicates are adjacent post-sort. */
    w = 0;
    for (r = 0; r < n; r++) {
        if (w > 0 && ents[w - 1].is_prefix && ents[r].is_prefix &&
            strcmp(ents[w - 1].key, ents[r].key) == 0) {
            free(ents[r].key);
            continue;
        }
        ents[w++] = ents[r];
    }
    n = w;

    /* Page window: skip everything at or before the caller's start key. */
    start_idx = 0;
    if (request->list.has_start) {
        while (start_idx < n &&
               strcmp(ents[start_idx].key, request->list.start) <= 0) {
            start_idx++;
        }
    }

    emit      = n - start_idx;
    truncated = 0;
    if (request->list.max_keys == 0) {
        /* Degenerate request: S3 returns an empty page that is NOT truncated. */
        emit = 0;
    } else if (emit > request->list.max_keys) {
        emit      = request->list.max_keys;
        truncated = 1;
    }
    key_count = emit;

    if (truncated) {
        if (emit > 0) {
            next = ents[start_idx + emit - 1].key;
        } else {
            next = request->list.start;
        }
    }

    /* ---- header ---- */
    chimera_s3_out_init(&out, evpl);

    chimera_s3_out_append(&out, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

    if (versions) {
        chimera_s3_out_append(&out,
                              "<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

        chimera_s3_encode(request, enc, sizeof(enc), request->bucket_name, request->bucket_namelen);
        chimera_s3_out_append(&out, " <Name>%s</Name>\n", enc);

        chimera_s3_encode(request, enc, sizeof(enc), request->list.prefix, request->list.prefix_len);
        chimera_s3_out_append(&out, " <Prefix>%s</Prefix>\n", enc);

        chimera_s3_encode(request, enc, sizeof(enc), request->list.marker, request->list.marker_len);
        chimera_s3_out_append(&out, " <KeyMarker>%s</KeyMarker>\n", enc);
        chimera_s3_out_append(&out, " <VersionIdMarker></VersionIdMarker>\n");
        if (next) {
            chimera_s3_encode(request, enc, sizeof(enc), next, strlen(next));
            chimera_s3_out_append(&out, " <NextKeyMarker>%s</NextKeyMarker>\n", enc);
            chimera_s3_out_append(&out, " <NextVersionIdMarker>null</NextVersionIdMarker>\n");
        }
        chimera_s3_out_append(&out, " <MaxKeys>%d</MaxKeys>\n", request->list.max_keys);
        if (request->list.delimiter_len) {
            chimera_s3_encode(request, enc, sizeof(enc),
                              request->list.delimiter, request->list.delimiter_len);
            chimera_s3_out_append(&out, " <Delimiter>%s</Delimiter>\n", enc);
        }
        chimera_s3_out_append(&out, " <IsTruncated>%s</IsTruncated>\n",
                              truncated ? "true" : "false");
        if (request->list.encoding_url) {
            chimera_s3_out_append(&out, " <EncodingType>url</EncodingType>\n");
        }
    } else {
        chimera_s3_out_append(&out,
                              "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

        chimera_s3_encode(request, enc, sizeof(enc), request->bucket_name, request->bucket_namelen);
        chimera_s3_out_append(&out, " <Name>%s</Name>\n", enc);

        chimera_s3_encode(request, enc, sizeof(enc), request->list.prefix, request->list.prefix_len);
        chimera_s3_out_append(&out, " <Prefix>%s</Prefix>\n", enc);

        if (request->list.list_type == 2) {
            chimera_s3_out_append(&out, " <KeyCount>%d</KeyCount>\n", key_count);
            chimera_s3_out_append(&out, " <MaxKeys>%d</MaxKeys>\n", request->list.max_keys);
            if (request->list.delimiter_len) {
                chimera_s3_encode(request, enc, sizeof(enc),
                                  request->list.delimiter, request->list.delimiter_len);
                chimera_s3_out_append(&out, " <Delimiter>%s</Delimiter>\n", enc);
            }
            chimera_s3_out_append(&out, " <IsTruncated>%s</IsTruncated>\n",
                                  truncated ? "true" : "false");
            /* V2 always echoes the (possibly empty) ContinuationToken. */
            chimera_s3_encode(request, enc, sizeof(enc),
                              request->list.ctoken, request->list.ctoken_len);
            chimera_s3_out_append(&out, " <ContinuationToken>%s</ContinuationToken>\n", enc);
            if (next) {
                /* base64url is unreserved, so it needs no XML/URL escaping. */
                chimera_s3_b64url_encode(enc, sizeof(enc),
                                         (const unsigned char *) next, strlen(next));
                chimera_s3_out_append(&out, " <NextContinuationToken>%s</NextContinuationToken>\n", enc);
            }
            if (request->list.startafter_len) {
                chimera_s3_encode(request, enc, sizeof(enc),
                                  request->list.startafter, request->list.startafter_len);
                chimera_s3_out_append(&out, " <StartAfter>%s</StartAfter>\n", enc);
            }
        } else {
            chimera_s3_encode(request, enc, sizeof(enc),
                              request->list.marker, request->list.marker_len);
            chimera_s3_out_append(&out, " <Marker>%s</Marker>\n", enc);
            if (next) {
                chimera_s3_encode(request, enc, sizeof(enc), next, strlen(next));
                chimera_s3_out_append(&out, " <NextMarker>%s</NextMarker>\n", enc);
            }
            chimera_s3_out_append(&out, " <MaxKeys>%d</MaxKeys>\n", request->list.max_keys);
            if (request->list.delimiter_len) {
                chimera_s3_encode(request, enc, sizeof(enc),
                                  request->list.delimiter, request->list.delimiter_len);
                chimera_s3_out_append(&out, " <Delimiter>%s</Delimiter>\n", enc);
            }
            chimera_s3_out_append(&out, " <IsTruncated>%s</IsTruncated>\n",
                                  truncated ? "true" : "false");
        }

        if (request->list.encoding_url) {
            chimera_s3_out_append(&out, " <EncodingType>url</EncodingType>\n");
        }
    }

    /* ---- objects, then common prefixes (each already sorted) ---- */
    for (i = start_idx; i < start_idx + emit; i++) {
        struct chimera_s3_list_entry *e = &ents[i];
        char                          date[64], etag[64];
        char                         *hp = etag;

        if (e->is_prefix) {
            continue;
        }

        chimera_s3_format_date(date, sizeof(date), &e->mtime);

        *hp++ = '"';
        hp   += format_hex(hp, sizeof(etag) - 2, e->etag, sizeof(e->etag));
        *hp++ = '"';
        *hp   = '\0';

        chimera_s3_encode(request, enc, sizeof(enc), e->key, strlen(e->key));

        if (versions) {
            chimera_s3_out_append(&out, " <Version>\n");
            chimera_s3_out_append(&out, "  <Key>%s</Key>\n", enc);
            chimera_s3_out_append(&out, "  <VersionId>null</VersionId>\n");
            chimera_s3_out_append(&out, "  <IsLatest>true</IsLatest>\n");
            chimera_s3_out_append(&out, "  <LastModified>%s</LastModified>\n", date);
            chimera_s3_out_append(&out, "  <ETag>%s</ETag>\n", etag);
            chimera_s3_out_append(&out, "  <Size>%lu</Size>\n", e->size);
            chimera_s3_out_append(&out, "  <StorageClass>STANDARD</StorageClass>\n");
            chimera_s3_out_append(&out, " </Version>\n");
        } else {
            chimera_s3_out_append(&out, " <Contents>\n");
            chimera_s3_out_append(&out, "  <Key>%s</Key>\n", enc);
            chimera_s3_out_append(&out, "  <LastModified>%s</LastModified>\n", date);
            chimera_s3_out_append(&out, "  <ETag>%s</ETag>\n", etag);
            chimera_s3_out_append(&out, "  <Size>%lu</Size>\n", e->size);
            chimera_s3_out_append(&out, "  <StorageClass>STANDARD</StorageClass>\n");
            if (request->list.fetch_owner) {
                /* V2 fetch-owner=true: same canonical owner as ListBuckets/ACL. */
                chimera_s3_out_append(&out, "  <Owner>\n");
                chimera_s3_out_append(&out, "   <ID>chimera</ID>\n");
                chimera_s3_out_append(&out, "   <DisplayName>chimera</DisplayName>\n");
                chimera_s3_out_append(&out, "  </Owner>\n");
            }
            chimera_s3_out_append(&out, " </Contents>\n");
        }
    }

    for (i = start_idx; i < start_idx + emit; i++) {
        struct chimera_s3_list_entry *e = &ents[i];

        if (!e->is_prefix) {
            continue;
        }

        chimera_s3_encode(request, enc, sizeof(enc), e->key, strlen(e->key));

        chimera_s3_out_append(&out, " <CommonPrefixes>\n");
        chimera_s3_out_append(&out, "  <Prefix>%s</Prefix>\n", enc);
        chimera_s3_out_append(&out, " </CommonPrefixes>\n");
    }

    chimera_s3_out_append(&out, versions ?
                          "</ListVersionsResult>\n" : "</ListBucketResult>\n");

    chimera_s3_out_flush(&out);

    /* ---- ship it ---- */
    total = 0;
    for (i = 0; i < out.niov; i++) {
        total += evpl_iovec_length(&out.iov[i]);
    }

    if (out.niov > 0) {
        evpl_http_request_add_datav(request->http_request, out.iov, out.niov);
    }

    request->file_length      = total;
    request->file_real_length = total;
    request->file_offset      = 0;

    /* ---- release collected entries ---- */
    for (i = 0; i < n; i++) {
        free(ents[i].key);
    }
    free(ents);
    request->list.entries     = NULL;
    request->list.n_entries   = 0;
    request->list.cap_entries = 0;

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_list_find_complete */

void
chimera_s3_list(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    chimera_vfs_find(thread->vfs, &request->cred,
                     request->bucket_fh,
                     request->bucket_fhlen,
                     CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                     chimera_s3_list_filter,
                     chimera_s3_list_find_callback,
                     chimera_s3_list_find_complete,
                     request);
} /* chimera_s3_list */
