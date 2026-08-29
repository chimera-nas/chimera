// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Ground truth for the control plane's effect on the data protocols.
 *
 * One process, no ports: a chimera server with NFS and the REST API, both on
 * the inproc transport, driven by an NFS3/MOUNT rpc2 client and an HTTP client
 * sharing one event loop (nfs3_mbt_common.h + ctl_http.h).  That is what makes
 * these assertions possible at all -- each one is "issue an administrative
 * change over HTTP, then observe what the very next NFS request sees", which
 * needs both clients aimed at the same live server.
 *
 * The questions it answers, and why each one matters:
 *
 *   P1  Does an export become reachable the moment it is created, and
 *       unreachable the moment it is deleted?  This is the entire contract of
 *       the exports API; if MOUNT does not track it, nothing else matters.
 *
 *   P2  Does deleting an export invalidate the filehandles already minted
 *       under it?  A client that mounted before the deletion holds handles
 *       that name it.  Linux knfsd resolves the export on every request
 *       (nfsd_set_fh_dentry -> rqst_exp_find) and answers ESTALE once it is
 *       gone; chimera resolves the export id only to apply policy and lets an
 *       unknown id pass (nfs_common.h, chimera_nfs_fh_decode).  The
 *       consequence is not academic: the ro, squash and sec policies all hang
 *       off that lookup, so a handle naming a deleted export is not merely
 *       still usable, it is usable with NO policy applied.  P2 measures
 *       exactly that, in four parts: plain access, read-only, root-squash,
 *       and re-creation under a new id.
 *
 *   P3  Is referential integrity actually enforced -- can a mount be removed
 *       out from under an export, or a filesystem out from under a mount?
 *
 *   P4  Do the counters behind /metrics move when protocol requests are
 *       served, and is the exposition well-formed?
 *
 * Every check names what it asserts and reports what it got, so a divergence
 * points at the property rather than at a line number.
 */

#include "nfs3_mbt_common.h"
#include "ctl_http.h"

#include <jansson.h>

#define PROBE_REST_PORT    8080
#define PROBE_METRICS_PORT 9000

/* Per-procedure NFS latency histogram; its _count sample is the request
 * counter for that procedure.  Chimera has no aggregate request counter, so
 * this is the metric that moves when protocol traffic is served. */
#define NFS_GETATTR_COUNT \
        "chimera_nfs_op_latency_nanoseconds_count{name=\"NFSPROC3_GETATTR\"}"

static int passed, failed;

static void
ck(
    int         ok,
    const char *name,
    const char *detail)
{
    if (ok) {
        fprintf(stderr, "  PASS: %s\n", name);
        passed++;
    } else {
        fprintf(stderr, "  FAIL: %s (%s)\n", name, detail ? detail : "");
        failed++;
    }
} /* ck */

static void
ck_http(
    struct ctl_res *res,
    int             want,
    const char     *name)
{
    char detail[512];

    snprintf(detail, sizeof(detail), "expected %d, got %d; body: %.*s",
             want, res->status,
             res->body_len > 200 ? 200 : res->body_len, res->body);
    ck(res->status == want, name, detail);
} /* ck_http */

static void
ck_nfs(
    uint32_t    got,
    uint32_t    want,
    const char *name)
{
    char detail[128];

    snprintf(detail, sizeof(detail), "expected nfsstat3 %u, got %u",
             want, got);
    ck(got == want, name, detail);
} /* ck_nfs */

/*
 * A property chimera does not have yet.
 *
 * Reported as a PASS of the CURRENT behavior plus a loud DEVIATION line, so
 * this probe stays green until chimera is fixed and then goes red -- which is
 * the signal to delete the entry, exactly as the NFS3 deviation probe works.
 * `want` is what the model specifies; `have` is what chimera does today.
 */
static void
ck_deviation(
    uint32_t    got,
    uint32_t    want,
    uint32_t    have,
    const char *name,
    const char *why)
{
    char detail[256];

    if (got == have) {
        fprintf(stderr, "  DEVIATION: %s: chimera answers %u, "
                "the model requires %u -- %s\n", name, have, want, why);
        passed++;
        return;
    }

    snprintf(detail, sizeof(detail),
             "expected the known deviation (%u) or the fix (%u), got %u",
             have, want, got);
    ck(got == want, name, detail);
} /* ck_deviation */

/* ---- REST helpers ------------------------------------------------------- */

static void
rest_ok(
    struct ctl_conn            *c,
    enum evpl_http_request_type method,
    const char                 *url,
    const char                 *body,
    int                         want,
    const char                 *what)
{
    struct ctl_res res;

    ctl_http(c, method, url, body, &res);

    if (res.status != want) {
        fprintf(stderr, "setup failed: %s -> %d (wanted %d): %.*s\n",
                what, res.status, want,
                res.body_len > 300 ? 300 : res.body_len, res.body);
        exit(1);
    }
} /* rest_ok */

/* Value of a Prometheus counter in an exposition page, or -1 if absent.
 * Matches the first sample line whose name (with or without labels) is
 * `metric`; that is enough for the aggregate counters checked here. */
static double
metric_value(
    const char *page,
    const char *metric)
{
    const char *p   = page;
    size_t      len = strlen(metric);

    while ((p = strstr(p, metric)) != NULL) {
        const char *line_start = p;
        const char *v;

        /* Must be at the start of a line and not be a HELP/TYPE comment. */
        while (line_start > page && line_start[-1] != '\n') {
            line_start--;
        }
        if (line_start != p || *p == '#') {
            p += len;
            continue;
        }

        /* The name must end here, at a label brace, or at whitespace. */
        v = p + len;
        if (*v != '{' && *v != ' ' && *v != '\t') {
            p += len;
            continue;
        }

        if (*v == '{') {
            v = strchr(v, '}');
            if (!v) {
                return -1;
            }
            v++;
        }

        return strtod(v, NULL);
    }

    return -1;
} /* metric_value */

int
main(
    int   argc,
    char *argv[])
{
    struct mbt_env          env;
    struct mbt_env_opts     opts = { 0 };
    struct ctl_conn        *api, *scrape;
    struct evpl_http_agent *agent;
    struct ctl_res          res;
    struct mbt_result      *r;
    struct mbt_fh           root, ro_root, sq_root;
    uint8_t                 payload[16];

    opts.rest_port    = PROBE_REST_PORT;
    opts.metrics_port = PROBE_METRICS_PORT;

    /* Open the server and the NFS client, but do NOT let the harness create
     * its own filesystem/mount/export: provisioning over REST is the point. */
    mbt_env_open_opts(&env, &opts);

    /* One agent, two connections: the REST API and the scrape endpoint are
     * different servers on the same loop. */
    agent  = evpl_http_init(env.evpl);
    api    = ctl_conn_open(env.evpl, agent, PROBE_REST_PORT);
    scrape = ctl_conn_open(env.evpl, agent, PROBE_METRICS_PORT);

    memset(payload, 0xa5, sizeof(payload));

    /* ================= P1: an export's reachability ==================== */

    /* Before anything exists, MOUNT of the path must fail. */
    r = mbt_mnt(&env, "/e0");
    ck_nfs(r->status, MNT3ERR_NOENT, "P1/mnt-unknown-export-is-noent");

    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_POST, "/api/v1/filesystems",
            "{\"module\":\"memfs\",\"name\":\"fs0\"}", 201, "create fs0");
    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_POST, "/api/v1/mounts",
            "{\"name\":\"m0\",\"module\":\"memfs\",\"path\":\"fs0\"}", 201,
            "mount m0");

    /* A mount alone is not an export. */
    r = mbt_mnt(&env, "/m0");
    ck_nfs(r->status, MNT3ERR_NOENT, "P1/mnt-mount-without-export-is-noent");

    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_POST, "/api/v1/exports",
            "{\"name\":\"/e0\",\"path\":\"/m0\",\"access\":\"rw\"}", 201,
            "export e0");

    r = mbt_mnt(&env, "/e0");
    ck_nfs(r->status, MNT3_OK, "P1/mnt-after-create-succeeds");
    ck(r->obj_fh.has, "P1/mnt-returns-a-filehandle", "no handle in reply");
    root = r->obj_fh;

    /* The handle works. */
    r = mbt_getattr(&env, &root);
    ck_nfs(r->status, NFS3_OK, "P1/getattr-on-export-root");

    /* ============ P2: deleting an export vs. live filehandles ========== */

    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_DELETE, "/api/v1/exports//e0", NULL,
            204, "delete e0");

    /* Reachability by name is gone immediately -- that part is not in doubt. */
    r = mbt_mnt(&env, "/e0");
    ck_nfs(r->status, MNT3ERR_NOENT, "P2/mnt-after-delete-is-noent");

    /*
     * ...but the handle minted before the deletion is still presented.  RFC
     * 1813 has no word for "unexported", and Linux answers ESTALE: the export
     * is resolved per request, and a handle whose export is gone no longer
     * names anything the server is willing to serve.  Anything else means an
     * administrator cannot revoke access from a client that already mounted.
     */
    r = mbt_getattr(&env, &root);
    ck_deviation(r->status, NFS3ERR_STALE, NFS3_OK,
                 "P2/getattr-on-deleted-export",
                 "removing an export unlinks the export record and unmounts "
                 "nothing, so the inner VFS handle still resolves");

    /* Recreate it so the rest of the probe has a working export. */
    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_POST, "/api/v1/exports",
            "{\"name\":\"/e0\",\"path\":\"/m0\",\"access\":\"rw\"}", 201,
            "recreate e0");

    /* ---- P2b: a read-only export, then deleted ------------------------ */

    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_POST, "/api/v1/exports",
            "{\"name\":\"/ro\",\"path\":\"/m0\",\"access\":\"ro\"}", 201,
            "export ro");

    r = mbt_mnt(&env, "/ro");
    ck_nfs(r->status, MNT3_OK, "P2b/mnt-ro-export");
    ro_root = r->obj_fh;

    r = mbt_create(&env, &ro_root, "f", 1, UNCHECKED, 0644, NULL);
    ck_nfs(r->status, NFS3ERR_ROFS, "P2b/create-on-ro-export-is-rofs");

    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_DELETE, "/api/v1/exports//ro", NULL,
            204, "delete ro");

    /*
     * The read-only policy lives on the export record, and the handle carries
     * only its id.  With the record gone the gate has nothing to consult.
     * The model requires STALE; chimera fails open, so a client that held a
     * handle under a read-only export can now write through it.
     */
    r = mbt_create(&env, &ro_root, "f", 1, UNCHECKED, 0644, NULL);
    ck_deviation(r->status, NFS3ERR_STALE, NFS3_OK,
                 "P2b/create-on-deleted-ro-export",
                 "the ro gate resolves the export id and an unknown id is "
                 "treated as unrestricted");

    /* ---- P2c: a root-squashing export, then deleted -------------------- */

    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_POST, "/api/v1/exports",
            "{\"name\":\"/sq\",\"path\":\"/m0\",\"access\":\"rw\","
            "\"squash\":\"all\",\"anonuid\":65534,\"anongid\":65534}", 201,
            "export sq");

    r = mbt_mnt(&env, "/sq");
    ck_nfs(r->status, MNT3_OK, "P2c/mnt-squash-export");
    sq_root = r->obj_fh;

    /* The client's credential is uid 0 (nfs3_mbt_common.h), so squash=all
     * must land the new file on the anonymous identity. */
    r = mbt_create(&env, &sq_root, "sq1", 3, UNCHECKED, 0644, NULL);
    ck_nfs(r->status, NFS3_OK, "P2c/create-under-squash");
    ck(r->obj_attrs.has && r->obj_attrs.a.uid == 65534,
       "P2c/squash-applies-anonuid",
       "the created file is not owned by anonuid");

    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_DELETE, "/api/v1/exports//sq", NULL,
            204, "delete sq");

    r = mbt_create(&env, &sq_root, "sq2", 3, UNCHECKED, 0644, NULL);
    if (r->status == NFS3ERR_STALE) {
        ck(1, "P2c/create-on-deleted-squash-export", NULL);
    } else {
        fprintf(stderr, "  DEVIATION: P2c/create-on-deleted-squash-export: "
                "chimera answers %u (uid %u), the model requires "
                "NFS3ERR_STALE (%u) -- the squash policy is read off the "
                "export record, so deleting it un-squashes every handle "
                "minted under it\n",
                r->status,
                r->obj_attrs.has ? r->obj_attrs.a.uid : (uint32_t) -1,
                NFS3ERR_STALE);
        passed++;
        /* Pin the shape of the deviation, so it cannot silently change into
        * something else: the write succeeds AS ROOT rather than as anon. */
        ck(r->status == NFS3_OK && r->obj_attrs.has &&
           r->obj_attrs.a.uid == 0,
           "P2c/deleted-squash-export-unsquashes",
           "expected the un-squashed write to land as uid 0");
    }

    /* ---- P2d: re-created under a different id -------------------------- */

    /* /e0 still exists from above; give it an explicit new identity by
     * deleting and recreating it with a pinned export_id.  The handle taken
     * before still carries the OLD id. */
    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_DELETE, "/api/v1/exports//e0", NULL,
            204, "delete e0 again");
    rest_ok(api, EVPL_HTTP_REQUEST_TYPE_POST, "/api/v1/exports",
            "{\"name\":\"/e0\",\"path\":\"/m0\",\"access\":\"ro\","
            "\"export_id\":4242}", 201, "recreate e0 read-only with id 4242");

    /* A fresh MOUNT gets the new id and therefore the new policy. */
    r = mbt_mnt(&env, "/e0");
    ck_nfs(r->status, MNT3_OK, "P2d/mnt-recreated-export");
    {
        struct mbt_fh fresh = r->obj_fh;

        r = mbt_create(&env, &fresh, "g", 1, UNCHECKED, 0644, NULL);
        ck_nfs(r->status, NFS3ERR_ROFS, "P2d/fresh-handle-gets-new-policy");
    }

    /* The stale handle carries the old id, which now names nothing. */
    r = mbt_create(&env, &root, "h", 1, UNCHECKED, 0644, NULL);
    ck_deviation(r->status, NFS3ERR_STALE, NFS3_OK,
                 "P2d/old-handle-escapes-new-policy",
                 "the handle's embedded export id still names the deleted "
                 "export, so the new read-only policy does not reach it");

    /* ================= P3: referential integrity ======================= */

    ctl_http(api, EVPL_HTTP_REQUEST_TYPE_DELETE, "/api/v1/mounts/m0", NULL,
             &res);
    ck_http(&res, 409, "P3/mount-delete-refused-while-exported");

    ctl_http(api, EVPL_HTTP_REQUEST_TYPE_DELETE,
             "/api/v1/filesystems/memfs/fs0", NULL, &res);
    ck_http(&res, 409, "P3/fs-delete-refused-while-mounted");

    /* And the mount survived both refusals, so NFS still works. */
    r = mbt_mnt(&env, "/e0");
    ck_nfs(r->status, MNT3_OK, "P3/export-still-reachable-after-refusals");

    /* ===================== P4: the scrape endpoint ===================== */

    ctl_get(scrape, "/metrics", &res);
    ck_http(&res, 200, "P4/metrics-status");
    ck(strstr(res.body, "# TYPE") != NULL, "P4/metrics-is-an-exposition",
       "no TYPE lines in the body");
    ck(strstr(res.body, "evpl_") != NULL, "P4/metrics-carries-evpl-registry",
       "no evpl_ metrics");

    {
        double before, after;
        char  *page;
        int    i;

        page   = strdup(res.body);
        before = metric_value(page, NFS_GETATTR_COUNT);
        free(page);

        for (i = 0; i < 8; i++) {
            mbt_getattr(&env, &root);
        }

        ctl_get(scrape, "/metrics", &res);
        after = metric_value(res.body, NFS_GETATTR_COUNT);

        if (before < 0 && after < 0) {
            fprintf(stderr, "  INFO: P4: %s is not exported; skipping the "
                    "counter-movement check\n", NFS_GETATTR_COUNT);
        } else {
            char detail[128];

            snprintf(detail, sizeof(detail), "before=%.0f after=%.0f",
                     before, after);
            ck(after >= before + 8, "P4/nfs-getattr-count-advances", detail);
        }
    }

    ctl_conn_close(scrape);
    ctl_conn_close(api);
    evpl_http_destroy(agent);

    mbt_env_stop(&env);

    fprintf(stderr, "\n%d passed, %d failed\n", passed, failed);

    return failed ? 1 : 0;
} /* main */
