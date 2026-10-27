// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Ground-truth probe for the auxiliary NFS protocols (portmap/rpcbind,
 * MOUNT, NLM, NSM).
 *
 * The model in ext/specs/quint/nfsaux is written against RFC 1833 / RFC
 * 1813 Appendix I+II and gated down to what chimera implements.  Two kinds
 * of fact go into that gating, and neither is derivable from the RFCs:
 *
 *   - which procedures chimera registers at all (an unregistered one is
 *     answered PROC_UNAVAIL by the rpc2 layer, accept_stat 3), and
 *   - the handful of constants the replies carry (the portmap service
 *     table, the universal-address host, the MOUNT auth flavors, the NSM
 *     state number a fresh non-persistent server starts at).
 *
 * This probe pins both against the live in-process server, with no trace
 * corpus involved, so a change on either side turns this test red at the
 * source instead of showing up as a mass divergence in the replay.
 *
 * Run with --dump to print everything it observes rather than asserting;
 * that is the mode used when (re)deriving the model's constants.
 */

#include "common/compiler.h"
#include "common/getopt.h"
#include "common/atomic.h"

#include "nfs_aux_mbt_common.h"
#include "common/mbt_watchdog.h"
#include "server/nfs/nfs_nlm_test_hooks.h"

#define PROBE_EXPORT     "/share"
#define PROBE_EXPORT2    "/share2"
#define PROBE_UADDR_HOST "127.0.0.1"
#define PROBE_CALLER_A   "quint-a"
#define PROBE_CALLER_B   "quint-b"

/* RFC 5531 accept_stat carried back to the caller as a positive status. */
#define RPC_PROC_UNAVAIL 3

static int failures;
static int dump;

static void
fail(
    const char *fmt,
    ...) CHIMERA_PRINTF(1, 2);

static void
fail(
    const char *fmt,
    ...)
{
    va_list ap;

    va_start(ap, fmt);
    fprintf(stderr, "FAIL: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    failures++;
} /* fail */

static void
check_eq(
    const char *what,
    long        got,
    long        want)
{
    if (dump) {
        printf("  %-42s %ld\n", what, got);
        return;
    }
    if (got != want) {
        fail("%s: got %ld, want %ld", what, got, want);
    }
} /* check_eq */

static void
check_u64(
    const char        *what,
    unsigned long long got,
    unsigned long long want)
{
    if (dump) {
        printf("  %-42s %llu\n", what, got);
        return;
    }
    if (got != want) {
        fail("%s: got %llu, want %llu", what, got, want);
    }
} /* check_u64 */

static void
check_str(
    const char *what,
    const char *got,
    const char *want)
{
    if (dump) {
        printf("  %-42s '%s'\n", what, got);
        return;
    }
    if (strcmp(got, want) != 0) {
        fail("%s: got '%s', want '%s'", what, got, want);
    }
} /* check_str */

/* ---------------------------------------------------------------------- */

static const struct {
    uint32_t prog, vers, prot, port;
} expect_services[] = {
    { 100000, 2, 6, 111   },
    { 100000, 3, 6, 111   },
    { 100000, 4, 6, 111   },
    { 100003, 3, 6, 2049  },
    { 100003, 4, 6, 2049  },
    { 100005, 3, 6, 20048 },
    { 100021, 4, 6, 32803 },
    { 100024, 1, 6, 32765 },
};

#define NEXPECT_SERVICES (int) (sizeof(expect_services) / \
                                sizeof(expect_services[0]))

static void
probe_portmap(struct mbt_env *env)
{
    struct mbt_aux_result *r;
    char                   label[96];
    int                    i;

    printf("portmap/rpcbind:\n");

    /* NULL carries no reply body; the accept_stat is the whole check. */
    mbt_pm_null(env);
    check_eq("PMAPPROC_NULL rpc status", env->res.rpc_err, 0);

    for (i = 0; i < NEXPECT_SERVICES; i++) {
        r = mbt_pm_getport(env, expect_services[i].prog,
                           expect_services[i].vers, expect_services[i].prot);
        snprintf(label, sizeof(label), "GETPORT %u.%u.%u",
                 expect_services[i].prog, expect_services[i].vers,
                 expect_services[i].prot);
        check_eq(label, r->port, expect_services[i].port);
    }

    /* Exact-match semantics: a served program at an unserved version, and
     * a served (prog, vers) over an unserved transport, are both 0. */
    r = mbt_pm_getport(env, 100003, 2, 6);
    check_eq("GETPORT 100003.2.6 (unserved vers)", r->port, 0);
    r = mbt_pm_getport(env, 100003, 3, 17);
    check_eq("GETPORT 100003.3.17 (udp)", r->port, 0);
    r = mbt_pm_getport(env, 300000, 1, 6);
    check_eq("GETPORT 300000.1.6 (unknown prog)", r->port, 0);

    r = mbt_pm_dump(env);
    check_eq("PMAPPROC_DUMP entries", r->nmaps, NEXPECT_SERVICES);
    for (i = 0; i < r->nmaps && i < NEXPECT_SERVICES; i++) {
        snprintf(label, sizeof(label), "DUMP[%d] prog.vers.prot.port", i);
        if (dump) {
            printf("  %-42s %u.%u.%u.%u\n", label, r->maps[i].prog,
                   r->maps[i].vers, r->maps[i].prot, r->maps[i].port);
            continue;
        }
        if (r->maps[i].prog != expect_services[i].prog ||
            r->maps[i].vers != expect_services[i].vers ||
            r->maps[i].prot != expect_services[i].prot ||
            r->maps[i].port != expect_services[i].port) {
            fail("DUMP[%d]: got %u.%u.%u.%u, want %u.%u.%u.%u", i,
                 r->maps[i].prog, r->maps[i].vers, r->maps[i].prot,
                 r->maps[i].port, expect_services[i].prog,
                 expect_services[i].vers, expect_services[i].prot,
                 expect_services[i].port);
        }
    }

    /* rpcbind GETADDR: the port is encoded as the last two components of
     * the universal address; the host half comes from server.portmap_hostname
     * because the harness sets it (the inproc local address is not an IP). */
    r = mbt_rb_getaddr(env, 3, 100003, 3, "tcp");
    check_str("rpcbproc_getaddr 100003.3 tcp", r->uaddr,
              PROBE_UADDR_HOST ".8.1");
    r = mbt_rb_getaddr(env, 4, 100005, 3, "tcp");
    check_str("RPCBPROC_GETADDR 100005.3 tcp", r->uaddr,
              PROBE_UADDR_HOST ".78.80");
    r = mbt_rb_getaddr(env, 4, 100021, 4, "tcp");
    check_str("RPCBPROC_GETADDR 100021.4 tcp", r->uaddr,
              PROBE_UADDR_HOST ".128.35");
    r = mbt_rb_getaddr(env, 3, 100003, 3, "udp");
    check_str("rpcbproc_getaddr 100003.3 udp", r->uaddr, "");
    r = mbt_rb_getaddr(env, 4, 100003, 3, "tcp6");
    check_str("RPCBPROC_GETADDR 100003.3 tcp6", r->uaddr, "");

    r = mbt_rb_dump(env, 3);
    check_eq("rpcbproc_dump entries", r->nrpcb, NEXPECT_SERVICES);
    if (r->nrpcb > 0) {
        check_str("rpcbproc_dump[0] netid", r->rpcb[0].netid, "tcp");
        check_str("rpcbproc_dump[0] addr", r->rpcb[0].addr,
                  PROBE_UADDR_HOST ".0.111");
        check_str("rpcbproc_dump[0] owner", r->rpcb[0].owner, "");
    }
    r = mbt_rb_dump(env, 4);
    check_eq("RPCBPROC_DUMP entries", r->nrpcb, NEXPECT_SERVICES);

    /* Everything chimera does not register.  These are what the model's
     * IMPLEMENTED gate encodes. */
    mbt_pm_set(env, 100003, 3, 6, 2049, 0);
    check_eq("PMAPPROC_SET accept_stat", env->res.rpc_err, RPC_PROC_UNAVAIL);
    mbt_pm_set(env, 100003, 3, 6, 2049, 1);
    check_eq("PMAPPROC_UNSET accept_stat", env->res.rpc_err, RPC_PROC_UNAVAIL);
    mbt_pm_callit(env, 100003, 3, 0);
    check_eq("PMAPPROC_CALLIT accept_stat", env->res.rpc_err,
             RPC_PROC_UNAVAIL);
    mbt_rb_gettime(env, 3);
    check_eq("rpcbproc_gettime accept_stat", env->res.rpc_err,
             RPC_PROC_UNAVAIL);
    mbt_rb_gettime(env, 4);
    check_eq("RPCBPROC_GETTIME accept_stat", env->res.rpc_err,
             RPC_PROC_UNAVAIL);
    mbt_rb_getversaddr(env, 100003, 3, "tcp");
    check_eq("RPCBPROC_GETVERSADDR accept_stat", env->res.rpc_err,
             RPC_PROC_UNAVAIL);
} /* probe_portmap */

/* ---------------------------------------------------------------------- */

static struct mbt_fh root_fh;
static struct mbt_fh file_fh[2];

static void
probe_mount(struct mbt_env *env)
{
    struct mbt_aux_result *r;
    int                    i;

    printf("mount:\n");

    mbt_mount_null(env);
    check_eq("MOUNTPROC3_NULL rpc status", env->res.rpc_err, 0);

    r = mbt_mount_mnt(env, PROBE_EXPORT);
    check_eq("MNT /share status", env->res.status, MNT3_OK);
    check_eq("MNT /share auth flavor count", r->nauth, 2);
    if (r->nauth == 2) {
        check_eq("MNT /share auth[0]", r->auth[0], 0);   /* AUTH_NONE */
        check_eq("MNT /share auth[1]", r->auth[1], 1);   /* AUTH_SYS  */
    }
    root_fh = env->res.obj_fh;

    mbt_mount_mnt(env, PROBE_EXPORT2);
    check_eq("MNT /share2 status", env->res.status, MNT3_OK);

    /* No export matches -> NOENT (chimera maps the lookup failure itself). */
    mbt_mount_mnt(env, "/nope");
    check_eq("MNT /nope status", env->res.status, MNT3ERR_NOENT);

    /* Export matches, path under it does not exist. */
    mbt_mount_mnt(env, "/share/nope");
    check_eq("MNT /share/nope status", env->res.status, MNT3ERR_NOENT);

    /* Export matches only as a string prefix, not at a component boundary. */
    mbt_mount_mnt(env, "/shareXX");
    check_eq("MNT /shareXX status", env->res.status, MNT3ERR_NOENT);

    /* Leading slash is optional (missing_leading_slash in the resolver). */
    mbt_mount_mnt(env, "share");
    check_eq("MNT share (no leading /) status", env->res.status, MNT3_OK);

    /* A path whose parent component is a regular file: the walk got as far
     * as f0 and could not descend. */
    mbt_mount_mnt(env, "/share/f0/x");
    check_eq("MNT /share/f0/x status", env->res.status, MNT3ERR_NOTDIR);

    /* MNT of a regular file succeeds: the object is not type-checked. */
    mbt_mount_mnt(env, "/share/f0");
    check_eq("MNT /share/f0 status", env->res.status, MNT3_OK);

    /* The mount table records the client-requested path -- including for a
     * MNT that then failed, because the row goes in as soon as an export
     * matches -- most recent first, and never twice for one path.  Of the
     * eight MNTs above, /nope and /shareXX matched no export and left
     * nothing; the other six each left one row. */
    r = mbt_mount_dump(env);
    check_eq("mount table size", r->nmounts, 6);
    for (i = 0; i < r->nmounts; i++) {
        check_str("mount table host", r->mounts[i].host, "inproc");
    }
    if (r->nmounts == 6) {
        check_str("mount table[0]", r->mounts[0].dir, "/share/f0");
        check_str("mount table[1]", r->mounts[1].dir, "/share/f0/x");
        check_str("mount table[2]", r->mounts[2].dir, "share");
        check_str("mount table[3]", r->mounts[3].dir, "/share/nope");
        check_str("mount table[4]", r->mounts[4].dir, "/share2");
        check_str("mount table[5]", r->mounts[5].dir, "/share");
    }

    /* Exports are reported most recently created first, with no group
     * restriction. */
    r = mbt_mount_export(env);
    check_eq("export count", r->nexports, 2);
    if (r->nexports == 2) {
        check_str("export[0]", r->exports[0].dir, PROBE_EXPORT2);
        check_str("export[1]", r->exports[1].dir, PROBE_EXPORT);
        check_eq("export[0] group count", r->exports[0].ngroups, 0);
    }

    mbt_mount_umnt(env, PROBE_EXPORT2);
    r = mbt_mount_dump(env);
    check_eq("mount table size after UMNT /share2", r->nmounts, 5);

    mbt_mount_umntall(env);
    r = mbt_mount_dump(env);
    check_eq("rmtab entries after UMNTALL", r->nmounts, 0);

} /* probe_mount */

/* ---------------------------------------------------------------------- */

/* The export root handle, before anything else needs it. */
static void
probe_root_handle(struct mbt_env *env)
{
    struct mbt_result *res = mbt_mnt(env, PROBE_EXPORT);

    if (res->rpc_err != 0 || res->status != MNT3_OK || !res->obj_fh.has) {
        fprintf(stderr, "probe: MNT %s failed: rpc_err=%d status=%u\n",
                PROBE_EXPORT, res->rpc_err, res->status);
        exit(1);
    }
    root_fh = res->obj_fh;
    mbt_mount_umntall(env);
} /* probe_root_handle */

static void
probe_setup_files(struct mbt_env *env)
{
    struct mbt_result *res;
    int                i;
    char               name[8];

    for (i = 0; i < 2; i++) {
        snprintf(name, sizeof(name), "f%d", i);
        res = mbt_create(env, &root_fh, name, (uint32_t) strlen(name),
                         UNCHECKED, 420, NULL);
        if (res->status != 0 && res->status != 17) {
            fprintf(stderr, "probe: CREATE %s failed: %u\n", name,
                    res->status);
            exit(1);
        }
        res = mbt_lookup(env, &root_fh, name, (uint32_t) strlen(name));
        if (res->status != 0 || !res->obj_fh.has) {
            fprintf(stderr, "probe: LOOKUP %s failed: %u\n", name,
                    res->status);
            exit(1);
        }
        file_fh[i] = res->obj_fh;
    }
} /* probe_setup_files */

/* ---------------------------------------------------------------------- */

static const uint8_t oh_a[] = { 0xa0, 0xa1 };
static const uint8_t oh_b[] = { 0xb0, 0xb1 };
static const uint8_t ck[]   = { 0xc0, 0xff, 0xee };

static void
probe_nlm(struct mbt_env *env)
{
    struct mbt_aux_result *r;
    struct mbt_fh          bogus;

    printf("nlm:\n");

    mbt_nlm_null(env);
    check_eq("NLMPROC4_NULL rpc status", env->res.rpc_err, 0);

    /* An unparseable file handle is STALE_FH, not a crash. */
    memset(&bogus, 0, sizeof(bogus));
    bogus.has = 1;
    bogus.len = 8;
    memset(bogus.data, 0x5a, 8);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_A, &bogus, oh_a, sizeof(oh_a), 1, 1,
                     0, 16, ck, sizeof(ck));
    check_eq("TEST on a bogus fh", r->nlm_stat, NLM4_STALE_FH);
    check_eq("TEST cookie echoed", r->cookie_len, sizeof(ck));

    /* No claims on the file yet. */
    r = mbt_nlm_test(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 16, ck, sizeof(ck));
    check_eq("TEST on an unlocked file", r->nlm_stat, NLM4_GRANTED);

    /* Exclusive lock, then the same owner's identical re-LOCK (idempotent),
     * then a different owner's conflicting request. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 0, 16, ck, sizeof(ck));
    check_eq("LOCK excl [0,16) as A", r->nlm_stat, NLM4_GRANTED);

    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 0, 16, ck, sizeof(ck));
    check_eq("LOCK excl [0,16) again as A", r->nlm_stat, NLM4_GRANTED);

    r = mbt_nlm_lock(env, 2, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 1, 0, 0, 0, 8, 16, ck, sizeof(ck));
    check_eq("LOCK excl [8,24) as B (overlaps)", r->nlm_stat, NLM4_DENIED);

    r = mbt_nlm_lock(env, 2, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 1, 0, 0, 0, 32, 16, ck, sizeof(ck));
    check_eq("LOCK excl [32,48) as B (disjoint)", r->nlm_stat, NLM4_GRANTED);

    r = mbt_nlm_lock(env, 2, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 0, 0, 0, 0, 64, 16, ck, sizeof(ck));
    check_eq("LOCK shared [64,80) as B", r->nlm_stat, NLM4_GRANTED);

    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 0, 0, 0, 0, 64, 16, ck, sizeof(ck));
    check_eq("LOCK shared [64,80) as A (shared/shared)", r->nlm_stat,
             NLM4_GRANTED);

    /* A already holds [64,80) shared, but asking for it exclusive is an
     * upgrade, not a retry: it is decided like any other request, and B's
     * shared lock on the range refuses it.  A's shared lock stays. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 64, 16, ck, sizeof(ck));
    check_eq("LOCK excl [64,80) as A over its own shared (B shares it)",
             r->nlm_stat, NLM4_DENIED);

    /* TEST reports the conflicting holder's range and mode. */
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 1, 0, 16, ck, sizeof(ck));
    check_eq("TEST excl [0,16) as B", r->nlm_stat, NLM4_DENIED);
    check_eq("TEST holder exclusive", r->holder_exclusive, 1);
    check_eq("TEST holder svid", r->holder_svid, 0);
    check_eq("TEST holder oh len", r->holder_oh_len, 0);
    check_u64("TEST holder offset", r->holder_offset, 0);
    check_u64("TEST holder length", r->holder_length, 16);

    /* A to-EOF holder is reported with l_len 0: the one spelling Linux lockd
     * decodes as to-EOF from a nonzero offset (it computes end = off + len - 1,
     * so 0xffffffffffffffff wraps).  Take the lock with the OTHER wire spelling
     * to show the report normalizes rather than echoing the requester. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 4096, MBT_NLM_LEN_EOF, ck, sizeof(ck));
    check_eq("LOCK excl [4096,EOF) as A", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 1, 6144, 16, ck, sizeof(ck));
    check_eq("TEST excl [6144,6160) as B", r->nlm_stat, NLM4_DENIED);
    check_eq("TEST to-EOF holder exclusive", r->holder_exclusive, 1);
    check_u64("TEST to-EOF holder offset", r->holder_offset, 4096);
    check_u64("TEST to-EOF holder length is wire 0", r->holder_length, 0);
    r = mbt_nlm_unlock(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                       1, 4096, 0, ck, sizeof(ck));
    check_eq("UNLOCK [4096,EOF) as A", r->nlm_stat, NLM4_GRANTED);

    /* UNLOCK is always GRANTED, present or not. */
    r = mbt_nlm_unlock(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                       1, 0, 16, ck, sizeof(ck));
    check_eq("UNLOCK [0,16) as A", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                       1, 0, 16, ck, sizeof(ck));
    check_eq("UNLOCK [0,16) as A again", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, "no-such-host", &file_fh[0], oh_a,
                       sizeof(oh_a), 1, 0, 16, ck, sizeof(ck));
    check_eq("UNLOCK by an unknown caller", r->nlm_stat, NLM4_GRANTED);

    /* B can now take the range A released. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 1, 0, 0, 0, 0, 16, ck, sizeof(ck));
    check_eq("LOCK excl [0,16) as B after A unlocked", r->nlm_stat,
             NLM4_GRANTED);

    /* UNLOCK of part of a held range releases exactly those bytes and keeps
     * the rest (RFC 1813 A.6.4 is POSIX fcntl semantics; Linux lockd forwards
     * the process's unlock range as-is, which is what cthon04 lock test 10
     * exercises).  A middle unlock splits the lock into two remainders, an
     * edge unlock trims it, and a to-EOF remainder keeps its to-EOF spelling.
     * f1 is left lock-free for the sections below. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[1], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 0, 3, ck, sizeof(ck));
    check_eq("LOCK excl [0,3) as A on f1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, PROBE_CALLER_A, &file_fh[1], oh_a, sizeof(oh_a),
                       1, 1, 1, ck, sizeof(ck));
    check_eq("UNLOCK [1,2) as A (middle of [0,3))", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 1, 1, ck, sizeof(ck));
    check_eq("TEST excl [1,2) as B finds the hole", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 0, 1, ck, sizeof(ck));
    check_eq("TEST excl [0,1) as B hits the left remainder", r->nlm_stat,
             NLM4_DENIED);
    check_u64("split left remainder offset", r->holder_offset, 0);
    check_u64("split left remainder length", r->holder_length, 1);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 2, 1, ck, sizeof(ck));
    check_eq("TEST excl [2,3) as B hits the right remainder", r->nlm_stat,
             NLM4_DENIED);
    check_u64("split right remainder offset", r->holder_offset, 2);
    check_u64("split right remainder length", r->holder_length, 1);

    /* Edge trim: unlocking the first byte leaves only [2,3). */
    r = mbt_nlm_unlock(env, 0, PROBE_CALLER_A, &file_fh[1], oh_a, sizeof(oh_a),
                       1, 0, 1, ck, sizeof(ck));
    check_eq("UNLOCK [0,1) as A (left edge)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 0, 2, ck, sizeof(ck));
    check_eq("TEST excl [0,2) as B after the trim", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 2, 1, ck, sizeof(ck));
    check_eq("TEST excl [2,3) as B still held", r->nlm_stat, NLM4_DENIED);

    /* A to-EOF lock trimmed at its start keeps a to-EOF remainder (wire 0). */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[1], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 100, 0, ck, sizeof(ck));
    check_eq("LOCK excl [100,EOF) as A on f1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, PROBE_CALLER_A, &file_fh[1], oh_a, sizeof(oh_a),
                       1, 100, 10, ck, sizeof(ck));
    check_eq("UNLOCK [100,110) as A (start of to-EOF)", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 105, 1, ck, sizeof(ck));
    check_eq("TEST excl [105,106) as B in the trimmed part", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 110, 1, ck, sizeof(ck));
    check_eq("TEST excl [110,111) as B hits the to-EOF remainder", r->nlm_stat,
             NLM4_DENIED);
    check_u64("to-EOF remainder offset", r->holder_offset, 110);
    check_u64("to-EOF remainder length is wire 0", r->holder_length, 0);

    /* The whole-file UNLOCK sweeps every remainder. */
    r = mbt_nlm_unlock(env, 0, PROBE_CALLER_A, &file_fh[1], oh_a, sizeof(oh_a),
                       1, 0, 0, ck, sizeof(ck));
    check_eq("UNLOCK [0,EOF) as A on f1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 0, 0, ck, sizeof(ck));
    check_eq("TEST excl [0,EOF) as B on f1 after the sweep", r->nlm_stat,
             NLM4_GRANTED);

    /* CANCEL always answers GRANTED. */
    r = mbt_nlm_cancel(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a,
                       sizeof(oh_a), 1, 1, 0, 0, 16, ck, sizeof(ck));
    check_eq("CANCEL with nothing pending", r->nlm_stat, NLM4_GRANTED);

    /* The server's own GRANTED receiver. */
    r = mbt_nlm_granted(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a,
                        sizeof(oh_a), 1, 1, 0, 16, ck, sizeof(ck));
    check_eq("GRANTED (server-side receipt)", r->nlm_stat, NLM4_GRANTED);

    /* The DOS share half is accepted without enforcement. */
    r = mbt_nlm_share(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                      3, 3, 0, ck, sizeof(ck));
    check_eq("SHARE stat", r->nlm_stat, NLM4_GRANTED);
    check_eq("SHARE sequence", r->sequence, 0);
    r = mbt_nlm_share(env, 1, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                      3, 3, 0, ck, sizeof(ck));
    check_eq("UNSHARE stat", r->nlm_stat, NLM4_GRANTED);

    /* Reserved slots are registered and ack. */
    mbt_nlm_reserved(env, 16);
    check_eq("RESERVED_16 accept_stat", env->res.rpc_err, 0);
    mbt_nlm_reserved(env, 19);
    check_eq("RESERVED_19 accept_stat", env->res.rpc_err, 0);

    /* The *_RES direction acks. */
    mbt_nlm_send_res(env, 11, NLM4_GRANTED, ck, sizeof(ck));
    check_eq("TEST_RES accept_stat", env->res.rpc_err, 0);
    mbt_nlm_send_res(env, 12, NLM4_GRANTED, ck, sizeof(ck));
    check_eq("LOCK_RES accept_stat", env->res.rpc_err, 0);
    mbt_nlm_send_res(env, 15, NLM4_GRANTED, ck, sizeof(ck));
    check_eq("GRANTED_RES accept_stat", env->res.rpc_err, 0);

    /* The asynchronous half: a *_MSG is acked with a void reply and the
     * result comes back as a *_RES call on this connection. */
    mbt_aux_async_reset(env);
    mbt_nlm_test(env, 1, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b), 2,
                 1, 0, 16, ck, sizeof(ck));
    check_eq("TEST_MSG accept_stat", env->res.rpc_err, 0);
    mbt_aux_drain_for(env, 1, 2000000);
    check_eq("TEST_MSG async replies", mbt_aux(env)->nasync, 1);
    if (mbt_aux(env)->nasync == 1) {
        check_eq("TEST_MSG async proc", mbt_aux(env)->async[0].proc, 11);
        check_eq("TEST_MSG async stat", mbt_aux(env)->async[0].stat,
                 NLM4_GRANTED);
    }

    mbt_aux_async_reset(env);
    mbt_nlm_lock(env, 7, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b), 2,
                 1, 0, 0, 0, 0, 16, ck, sizeof(ck));
    check_eq("LOCK_MSG accept_stat", env->res.rpc_err, 0);
    mbt_aux_drain_for(env, 1, 2000000);
    check_eq("LOCK_MSG async replies", mbt_aux(env)->nasync, 1);
    if (mbt_aux(env)->nasync == 1) {
        check_eq("LOCK_MSG async proc", mbt_aux(env)->async[0].proc, 12);
        check_eq("LOCK_MSG async stat", mbt_aux(env)->async[0].stat,
                 NLM4_GRANTED);
    }

    mbt_aux_async_reset(env);
    mbt_nlm_unlock(env, 1, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b), 2,
                   0, 16, ck, sizeof(ck));
    mbt_aux_drain_for(env, 1, 2000000);
    check_eq("UNLOCK_MSG async replies", mbt_aux(env)->nasync, 1);
    if (mbt_aux(env)->nasync == 1) {
        check_eq("UNLOCK_MSG async proc", mbt_aux(env)->async[0].proc, 14);
    }

    mbt_aux_async_reset(env);
    mbt_nlm_cancel(env, 1, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b), 2,
                   1, 1, 0, 16, ck, sizeof(ck));
    mbt_aux_drain_for(env, 1, 2000000);
    check_eq("CANCEL_MSG async replies", mbt_aux(env)->nasync, 1);
    if (mbt_aux(env)->nasync == 1) {
        check_eq("CANCEL_MSG async proc", mbt_aux(env)->async[0].proc, 13);
    }

    mbt_aux_async_reset(env);
    mbt_nlm_granted(env, 1, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                    2, 1, 0, 16, ck, sizeof(ck));
    check_eq("GRANTED_MSG accept_stat", env->res.rpc_err, 0);
    mbt_aux_drain_us(env, 200000);
    check_eq("GRANTED_MSG async replies", mbt_aux(env)->nasync, 0);

    /* A third owner taking an exclusive lock over B's shared range: the
     * requester holds nothing of its own, so the idempotency short-circuit
     * cannot apply and the claim core decides. */
    r = mbt_nlm_lock(env, 2, "quint-c", &file_fh[0], oh_a, sizeof(oh_a), 3, 1,
                     0, 0, 0, 64, 16, ck, sizeof(ck));
    check_eq("LOCK excl [64,80) as C over B's shared", r->nlm_stat,
             NLM4_DENIED);

    /* Same owner handle, different svid: a distinct lock owner, so it
     * conflicts rather than coalescing. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     99, 1, 0, 0, 0, 32, 16, ck, sizeof(ck));
    check_eq("LOCK excl [32,48) as B svid 99 over B svid 2", r->nlm_stat,
             NLM4_DENIED);

    /* A blocking LOCK on a range another owner holds: queued, answered with
     * the RFC 1813 interim NLM4_BLOCKED. */
    mbt_aux_async_reset(env);
    r = mbt_nlm_lock(env, 2, "quint-d", &file_fh[0], oh_a, sizeof(oh_a), 4, 1,
                     1 /* block */, 0, 0, 32, 16, ck, sizeof(ck));
    check_eq("blocking LOCK over a held range", r->nlm_stat, NLM4_BLOCKED);
    /* Releasing the blocker should hand the queued lock its grant
     * out-of-band, via an NLM_GRANTED callback. */
    mbt_nlm_unlock(env, 0, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b), 2,
                   32, 16, ck, sizeof(ck));
    /* The deferred grant IS made -- see the TEST below -- but it is
     * delivered as an out-of-band NLMPROC4_GRANTED addressed to the
     * client's own lock manager, which this harness is not, so nothing
     * arrives here.  That is what the model's
     * NLM_GRANT_CALLBACK_OBSERVABLE = false records. */
    mbt_aux_drain_for(env, 1, 500000);
    check_eq("async messages after the blocker left", mbt_aux(env)->nasync, 0);
    r = mbt_nlm_test(env, 0, "quint-e", &file_fh[0], oh_a, sizeof(oh_a), 5, 1,
                     32, 16, ck, sizeof(ck));
    check_eq("TEST [32,48) after the blocker left (d was granted)",
             r->nlm_stat, NLM4_DENIED);
    mbt_nlm_unlock(env, 0, "quint-d", &file_fh[0], oh_a, sizeof(oh_a), 4, 32,
                   16, ck, sizeof(ck));

    /* NM_LOCK: like LOCK but the holder is not monitored. */
    r = mbt_nlm_lock(env, 22, PROBE_CALLER_A, &file_fh[1], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 128, 16, ck, sizeof(ck));
    check_eq("NM_LOCK excl [128,144) as A", r->nlm_stat, NLM4_GRANTED);

    /*
     * A blocking request queued behind a lock that FREE_ALL removes: does
     * releasing the blocker that way promote the waiter, the way releasing
     * it with UNLOCK does?
     */
    mbt_nlm_free_all(env, "quint-w1", 1);
    mbt_nlm_free_all(env, "quint-w2", 1);
    r = mbt_nlm_lock(env, 2, "quint-w1", &file_fh[0], oh_a, sizeof(oh_a), 1,
                     1, 0, 0, 0, 1024, 16, ck, sizeof(ck));
    check_eq("LOCK excl [1024,1040) as w1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-w2", &file_fh[0], oh_b, sizeof(oh_b), 2,
                     1, 1 /* block */, 0, 0, 1024, 16, ck, sizeof(ck));
    check_eq("blocking LOCK [1024,1040) as w2", r->nlm_stat, NLM4_BLOCKED);
    mbt_nlm_free_all(env, "quint-w1", 1);
    mbt_aux_drain_us(env, 200000);
    r = mbt_nlm_test(env, 0, "quint-w3", &file_fh[0], oh_a, sizeof(oh_a), 3,
                     1, 1024, 16, ck, sizeof(ck));
    check_eq("TEST [1024,1040) after FREE_ALL of the blocker (promoted)",
             r->nlm_stat, NLM4_DENIED);
    mbt_nlm_free_all(env, "quint-w2", 1);

    /* The trace shape that first exposed a divergence: the waiter spans
     * TWO of the blocker's locks, and the blocker also has a queued request
     * of its own on the same file for FREE_ALL to cancel. */
    r = mbt_nlm_lock(env, 2, "quint-w1", &file_fh[1], oh_a, sizeof(oh_a), 1,
                     1, 0, 0, 0, 8, 8, ck, sizeof(ck));
    check_eq("LOCK excl f1 [8,16) as w1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-w1", &file_fh[1], oh_a, sizeof(oh_a), 1,
                     1, 0, 0, 0, 16, 8, ck, sizeof(ck));
    check_eq("LOCK excl f1 [16,24) as w1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-w2", &file_fh[1], oh_b, sizeof(oh_b), 2,
                     1, 1, 0, 0, 8, 16, ck, sizeof(ck));
    check_eq("blocking LOCK f1 [8,24) as w2 (spans both)", r->nlm_stat,
             NLM4_BLOCKED);
    r = mbt_nlm_lock(env, 2, "quint-w1", &file_fh[1], oh_b, sizeof(oh_b), 9,
                     1, 1, 0, 0, 8, 8, ck, sizeof(ck));
    check_eq("blocking LOCK f1 [8,16) as w1/other-owner", r->nlm_stat,
             NLM4_BLOCKED);
    mbt_nlm_free_all(env, "quint-w1", 1);
    mbt_aux_drain_us(env, 200000);
    check_eq("TEST f1 [16,24) after FREE_ALL of both blockers (promoted)",
             mbt_nlm_test(env, 0, "quint-w3", &file_fh[1], oh_a, sizeof(oh_a),
                          3, 1, 16, 8, ck, sizeof(ck))->nlm_stat,
             NLM4_DENIED);
    mbt_nlm_free_all(env, "quint-w2", 1);

    /* An UNLOCK that straddles TWO of the blocker's locks, one of them a
     * remainder of an earlier carve, must keep the remainders in the order
     * the parents had: FREE_ALL releases that list one lock at a time and
     * pumps the waiters after each, so the order decides who is promoted.
     * w1 holds [512,516) then [517,520); the first carve shrinks the former
     * to [512,515), the second leaves [512,513) and [519,520).  w2 queued
     * for the whole of [512,520) must be promoted ahead of w3 queued for
     * [519,520): releasing [512,513) first lets nobody through, releasing
     * [519,520) then hands w2 everything.  The other order gives w3 the
     * tail byte and starves w2. */
    r = mbt_nlm_lock(env, 2, "quint-w1", &file_fh[1], oh_a, sizeof(oh_a), 1,
                     1, 0, 0, 0, 512, 4, ck, sizeof(ck));
    check_eq("LOCK excl f1 [512,516) as w1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-w1", &file_fh[1], oh_a, sizeof(oh_a), 1,
                     1, 0, 0, 0, 517, 3, ck, sizeof(ck));
    check_eq("LOCK excl f1 [517,520) as w1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, "quint-w1", &file_fh[1], oh_a, sizeof(oh_a), 1,
                       515, 1, ck, sizeof(ck));
    check_eq("UNLOCK f1 [515,516) as w1 (first carve)", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, "quint-w1", &file_fh[1], oh_a, sizeof(oh_a), 1,
                       513, 6, ck, sizeof(ck));
    check_eq("UNLOCK f1 [513,519) as w1 (straddles both)", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-w2", &file_fh[1], oh_b, sizeof(oh_b), 2,
                     1, 1 /* block */, 0, 0, 512, 8, ck, sizeof(ck));
    check_eq("blocking LOCK f1 [512,520) as w2", r->nlm_stat, NLM4_BLOCKED);
    r = mbt_nlm_lock(env, 2, "quint-w3", &file_fh[1], oh_a, sizeof(oh_a), 3,
                     1, 1 /* block */, 0, 0, 519, 1, ck, sizeof(ck));
    check_eq("blocking LOCK f1 [519,520) as w3", r->nlm_stat, NLM4_BLOCKED);
    mbt_nlm_free_all(env, "quint-w1", 1);
    mbt_aux_drain_us(env, 200000);
    r = mbt_nlm_test(env, 0, "quint-w4", &file_fh[1], oh_a, sizeof(oh_a), 4,
                     1, 512, 1, ck, sizeof(ck));
    check_eq("TEST f1 [512,513) after FREE_ALL of two remainders (w2 promoted)",
             r->nlm_stat, NLM4_DENIED);
    check_u64("w2 holds the whole range: offset", r->holder_offset, 512);
    check_u64("w2 holds the whole range: length", r->holder_length, 8);
    mbt_nlm_free_all(env, "quint-w2", 1);
    mbt_nlm_free_all(env, "quint-w3", 1);

    /* The same shape, but the blocker leaves via UNLOCK rather than
     * FREE_ALL -- the case the replay corpus already covers. */
    r = mbt_nlm_lock(env, 2, "quint-w1", &file_fh[0], oh_a, sizeof(oh_a), 1,
                     1, 0, 0, 0, 2048, 16, ck, sizeof(ck));
    check_eq("LOCK excl [2048,2064) as w1", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-w2", &file_fh[0], oh_b, sizeof(oh_b), 2,
                     1, 1, 0, 0, 2048, 16, ck, sizeof(ck));
    check_eq("blocking LOCK [2048,2064) as w2", r->nlm_stat, NLM4_BLOCKED);
    mbt_nlm_unlock(env, 0, "quint-w1", &file_fh[0], oh_a, sizeof(oh_a), 1,
                   2048, 16, ck, sizeof(ck));
    mbt_aux_drain_us(env, 200000);
    r = mbt_nlm_test(env, 0, "quint-w3", &file_fh[0], oh_a, sizeof(oh_a), 3,
                     1, 2048, 16, ck, sizeof(ck));
    check_eq("TEST [2048,2064) after UNLOCK of the blocker (promoted)",
             r->nlm_stat, NLM4_DENIED);
    mbt_nlm_free_all(env, "quint-w2", 1);

    /* FREE_ALL drops every lock the named client holds. */
    mbt_nlm_free_all(env, PROBE_CALLER_B, 1);
    check_eq("FREE_ALL accept_stat", env->res.rpc_err, 0);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 16, ck, sizeof(ck));
    check_eq("TEST [0,16) after FREE_ALL of B", r->nlm_stat, NLM4_GRANTED);
} /* probe_nlm */

/* ---------------------------------------------------------------------- */

/*
 * A LOCK over bytes its own owner already holds replaces the owner's
 * coverage of them: POSIX fcntl semantics, which Linux lockd assumes for
 * every re-lock.  An exclusive re-locked as shared is a downgrade other
 * owners can share, a shared re-locked as exclusive an upgrade decided like
 * any other request, and the parts of an old lock outside the new range keep
 * their mode.  Every case runs on f1 at RL_BASE and above, clear of the
 * ranges probe_nlm used, and FREE_ALL clears its callers before the next.
 */
#define RL_BASE 1048576ULL

static void
relock_reset(struct mbt_env *env)
{
    mbt_nlm_free_all(env, "quint-r1", 1);
    mbt_nlm_free_all(env, "quint-r2", 1);
    mbt_nlm_free_all(env, "quint-r3", 1);
} /* relock_reset */

static void
probe_nlm_relock(struct mbt_env *env)
{
    const struct mbt_fh   *f = &file_fh[1];
    struct mbt_aux_result *r;
    uint64_t               b;

    printf("nlm re-lock:\n");
    relock_reset(env);

    /* The reported case: r1 downgrades its whole exclusive lock, so r2 can
     * share the range and r3's exclusive test names a shared holder. */
    b = RL_BASE;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("whole: r1 LOCK excl [0,16)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("whole: r1 LOCK shared [0,16) (downgrade)", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("whole: r2 LOCK shared [0,16) shares it", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 1, b, 16,
                     ck, sizeof(ck));
    check_eq("whole: r3 TEST excl [0,16)", r->nlm_stat, NLM4_DENIED);
    check_eq("whole: holder is shared", r->holder_exclusive, 0);
    relock_reset(env);

    /* Downgrading the middle of an exclusive lock keeps both edges
     * exclusive. */
    b = RL_BASE + 0x1000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 30, ck, sizeof(ck));
    check_eq("middle: r1 LOCK excl [0,30)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b + 10, 10, ck, sizeof(ck));
    check_eq("middle: r1 LOCK shared [10,20)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0,
                     b + 10, 10, ck, sizeof(ck));
    check_eq("middle: r2 TEST shared [10,20)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0,
                     b, 10, ck, sizeof(ck));
    check_eq("middle: r2 TEST shared [0,10)", r->nlm_stat, NLM4_DENIED);
    check_eq("middle: head holder exclusive", r->holder_exclusive, 1);
    check_u64("middle: head holder offset", r->holder_offset, b);
    check_u64("middle: head holder length", r->holder_length, 10);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0,
                     b + 20, 10, ck, sizeof(ck));
    check_eq("middle: r2 TEST shared [20,30)", r->nlm_stat, NLM4_DENIED);
    check_u64("middle: tail holder offset", r->holder_offset, b + 20);
    check_u64("middle: tail holder length", r->holder_length, 10);
    relock_reset(env);

    /* An upgrade and then a downgrade: neither leaves the old mode behind. */
    b = RL_BASE + 0x2000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("updown: r1 LOCK shared [0,16)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("updown: r1 LOCK excl [0,16) (upgrade)", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0, b, 16,
                     ck, sizeof(ck));
    check_eq("updown: r2 TEST shared after the upgrade", r->nlm_stat,
             NLM4_DENIED);
    check_eq("updown: holder is exclusive", r->holder_exclusive, 1);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("updown: r1 LOCK shared [0,16) (downgrade)", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0, b, 16,
                     ck, sizeof(ck));
    check_eq("updown: r2 TEST shared after the downgrade", r->nlm_stat,
             NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 1, b, 16,
                     ck, sizeof(ck));
    check_eq("updown: r2 TEST excl after the downgrade", r->nlm_stat,
             NLM4_DENIED);
    check_eq("updown: holder is shared again", r->holder_exclusive, 0);
    relock_reset(env);

    /* A refused upgrade changes nothing: r1's shared lock still stands once
     * r2 lets go. */
    b = RL_BASE + 0x3000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("refused: r1 LOCK shared [0,16)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("refused: r2 LOCK shared [0,16)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("refused: r1 LOCK excl [0,16) over r2's shared", r->nlm_stat,
             NLM4_DENIED);
    r = mbt_nlm_unlock(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, b, 16,
                       ck, sizeof(ck));
    check_eq("refused: r2 UNLOCK [0,16)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 1, b, 16,
                     ck, sizeof(ck));
    check_eq("refused: r3 TEST excl [0,16)", r->nlm_stat, NLM4_DENIED);
    check_eq("refused: r1's lock is still shared", r->holder_exclusive, 0);
    check_u64("refused: holder offset", r->holder_offset, b);
    check_u64("refused: holder length", r->holder_length, 16);
    relock_reset(env);

    /* A blocked upgrade, once granted, takes over the shared lock it was
     * upgrading and keeps the part outside the upgraded range shared. */
    b = RL_BASE + 0x4000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b, 32, ck, sizeof(ck));
    check_eq("blocked: r1 LOCK shared [0,32)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0, 0, 0, 0,
                     b, 16, ck, sizeof(ck));
    check_eq("blocked: r2 LOCK shared [0,16)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1,
                     1 /* block */, 0, 0, b, 16, ck, sizeof(ck));
    check_eq("blocked: r1 blocking LOCK excl [0,16)", r->nlm_stat,
             NLM4_BLOCKED);
    mbt_nlm_unlock(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, b, 16, ck,
                   sizeof(ck));
    mbt_aux_drain_us(env, 200000);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 0, b, 16,
                     ck, sizeof(ck));
    check_eq("blocked: r3 TEST shared [0,16) after the grant", r->nlm_stat,
             NLM4_DENIED);
    check_eq("blocked: the upgrade was granted", r->holder_exclusive, 1);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 1,
                     b + 16, 16, ck, sizeof(ck));
    check_eq("blocked: r3 TEST excl [16,32)", r->nlm_stat, NLM4_DENIED);
    check_eq("blocked: tail holder is shared", r->holder_exclusive, 0);
    check_u64("blocked: tail holder offset", r->holder_offset, b + 16);
    check_u64("blocked: tail holder length", r->holder_length, 16);
    relock_reset(env);

    /* An exclusive re-lock spanning a shared and an exclusive lock trims the
     * shared one to what lies outside it and cuts the exclusive one's head
     * off; NLM keeps the three locks apart rather than merging them. */
    b = RL_BASE + 0x5000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b, 10, ck, sizeof(ck));
    check_eq("span: r1 LOCK shared [0,10)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b + 20, 10, ck, sizeof(ck));
    check_eq("span: r1 LOCK excl [20,30)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b + 5, 20, ck, sizeof(ck));
    check_eq("span: r1 LOCK excl [5,25)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0, b, 5,
                     ck, sizeof(ck));
    check_eq("span: r2 TEST shared [0,5)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 1, b, 5,
                     ck, sizeof(ck));
    check_eq("span: r2 TEST excl [0,5)", r->nlm_stat, NLM4_DENIED);
    check_eq("span: head holder is shared", r->holder_exclusive, 0);
    check_u64("span: head holder offset", r->holder_offset, b);
    check_u64("span: head holder length", r->holder_length, 5);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0,
                     b + 26, 1, ck, sizeof(ck));
    check_eq("span: r2 TEST shared [26,27)", r->nlm_stat, NLM4_DENIED);
    check_u64("span: tail holder offset", r->holder_offset, b + 25);
    check_u64("span: tail holder length", r->holder_length, 5);
    relock_reset(env);

    /* FREE_ALL after a partial downgrade releases the remainders too. */
    b = RL_BASE + 0x6000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 30, ck, sizeof(ck));
    check_eq("free_all: r1 LOCK excl [0,30)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b + 10, 10, ck, sizeof(ck));
    check_eq("free_all: r1 LOCK shared [10,20)", r->nlm_stat, NLM4_GRANTED);
    mbt_nlm_free_all(env, "quint-r1", 1);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 1, b, 30,
                     ck, sizeof(ck));
    check_eq("free_all: r2 TEST excl [0,30) after FREE_ALL", r->nlm_stat,
             NLM4_GRANTED);
    relock_reset(env);

    /* FREE_ALL releases a client's locks in the order it took them, and
     * pumps the blocking queue after each, so that order decides which of
     * two queued waiters wins.  A remainder of a carve must therefore keep
     * its parent's place even when the carve only touched one of the
     * parent's sibling remainders: r1 ends up with [5,10) and [20,30) of one
     * original lock, and releasing [5,10) first (as the original's place
     * says) means r2's older [5,30) wait is granted before r3's [20,30). */
    b = RL_BASE + 0x7000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 30, ck, sizeof(ck));
    check_eq("order: r1 LOCK excl [0,30)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b + 10, 10, ck, sizeof(ck));
    check_eq("order: r1 LOCK shared [10,20)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, "quint-r1", f, oh_a, sizeof(oh_a), 1, b + 10,
                       10, ck, sizeof(ck));
    check_eq("order: r1 UNLOCK [10,20)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_unlock(env, 0, "quint-r1", f, oh_a, sizeof(oh_a), 1, b, 5,
                       ck, sizeof(ck));
    check_eq("order: r1 UNLOCK [0,5)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r2", f, oh_b, sizeof(oh_b), 2, 1,
                     1 /* block */, 0, 0, b + 5, 25, ck, sizeof(ck));
    check_eq("order: r2 blocking LOCK excl [5,30)", r->nlm_stat,
             NLM4_BLOCKED);
    r = mbt_nlm_lock(env, 2, "quint-r3", f, oh_a, sizeof(oh_a), 3, 1,
                     1 /* block */, 0, 0, b + 20, 10, ck, sizeof(ck));
    check_eq("order: r3 blocking LOCK excl [20,30)", r->nlm_stat,
             NLM4_BLOCKED);
    mbt_nlm_free_all(env, "quint-r1", 1);
    mbt_aux_drain_us(env, 200000);
    r = mbt_nlm_test(env, 0, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, b + 5,
                     5, ck, sizeof(ck));
    check_eq("order: r1 TEST excl [5,10) after FREE_ALL (r2 won)",
             r->nlm_stat, NLM4_DENIED);
    check_eq("order: winner is exclusive", r->holder_exclusive, 1);
    check_u64("order: winner offset", r->holder_offset, b + 5);
    check_u64("order: winner length", r->holder_length, 25);
    relock_reset(env);

    /* Carves nest: the pump at the end of r1's UNLOCK carve grants r2's
     * blocked downgrade, whose own carve pumps and grants r1's blocked
     * re-lock, all before r1's UNLOCK remainder [0,20) is back on r1's lock
     * list.  That re-lock still has to take [10,20) over from the remainder:
     * r1 ends up with exclusive [0,10) and shared [10,35), r2 with shared
     * [20,40) and exclusive [40,50). */
    b = RL_BASE + 0x8000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 30, ck, sizeof(ck));
    check_eq("nested: r1 LOCK excl [0,30)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r2", f, oh_b, sizeof(oh_b), 2, 1, 0, 0, 0,
                     b + 30, 20, ck, sizeof(ck));
    check_eq("nested: r2 LOCK excl [30,50)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0,
                     1 /* block */, 0, 0, b + 20, 20, ck, sizeof(ck));
    check_eq("nested: r2 blocking LOCK shared [20,40)", r->nlm_stat,
             NLM4_BLOCKED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0,
                     1 /* block */, 0, 0, b + 10, 25, ck, sizeof(ck));
    check_eq("nested: r1 blocking LOCK shared [10,35)", r->nlm_stat,
             NLM4_BLOCKED);
    r = mbt_nlm_unlock(env, 0, "quint-r1", f, oh_a, sizeof(oh_a), 1, b + 20,
                       10, ck, sizeof(ck));
    check_eq("nested: r1 UNLOCK [20,30)", r->nlm_stat, NLM4_GRANTED);
    mbt_aux_drain_us(env, 200000);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 0,
                     b + 10, 10, ck, sizeof(ck));
    check_eq("nested: r3 TEST shared [10,20) (r1 downgraded it)",
             r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 1, b, 10,
                     ck, sizeof(ck));
    check_eq("nested: r3 TEST excl [0,10)", r->nlm_stat, NLM4_DENIED);
    check_eq("nested: head holder is exclusive", r->holder_exclusive, 1);
    check_u64("nested: head holder offset", r->holder_offset, b);
    check_u64("nested: head holder length", r->holder_length, 10);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 1,
                     b + 35, 5, ck, sizeof(ck));
    check_eq("nested: r3 TEST excl [35,40)", r->nlm_stat, NLM4_DENIED);
    check_eq("nested: r2's downgrade holder is shared", r->holder_exclusive,
             0);
    check_u64("nested: r2's downgrade offset", r->holder_offset, b + 20);
    check_u64("nested: r2's downgrade length", r->holder_length, 20);
    r = mbt_nlm_test(env, 0, "quint-r3", f, oh_a, sizeof(oh_a), 3, 0,
                     b + 40, 10, ck, sizeof(ck));
    check_eq("nested: r3 TEST shared [40,50)", r->nlm_stat, NLM4_DENIED);
    check_eq("nested: r2's tail is exclusive", r->holder_exclusive, 1);
    check_u64("nested: r2's tail offset", r->holder_offset, b + 40);
    check_u64("nested: r2's tail length", r->holder_length, 10);
    relock_reset(env);

    /* Downgrading inside a to-EOF lock leaves a to-EOF tail (wire 0). */
    b = RL_BASE + 0x100000;
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     b, 0, ck, sizeof(ck));
    check_eq("eof: r1 LOCK excl [0,EOF)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-r1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     b + 10, 10, ck, sizeof(ck));
    check_eq("eof: r1 LOCK shared [10,20)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0,
                     b + 10, 10, ck, sizeof(ck));
    check_eq("eof: r2 TEST shared [10,20)", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_test(env, 0, "quint-r2", f, oh_b, sizeof(oh_b), 2, 0,
                     b + 1000, 1, ck, sizeof(ck));
    check_eq("eof: r2 TEST shared [1000,1001)", r->nlm_stat, NLM4_DENIED);
    check_u64("eof: tail holder offset", r->holder_offset, b + 20);
    check_u64("eof: tail holder length is wire 0", r->holder_length, 0);
    relock_reset(env);
} /* probe_nlm_relock */

/*
 * CANCEL after a deferred grant whose NLM_GRANTED never reached the client.
 * A blocking LOCK answered NLM4_BLOCKED is granted out of band once the
 * blocker lets go.  Behind a Kubernetes Service that callback lands
 * on chimera's own NLM service, and this harness reproduces exactly that: the
 * deferred GRANTED goes to the server's own receiver, never to the probe.
 * When the client's waiter gives up it sends CANCEL, and that CANCEL has to
 * release the grant the client never saw or the lock is orphaned.  A grant
 * the client confirmed by re-polling LOCK belongs to the client, and CANCEL
 * must not take it back.
 */
#define CG_BASE 65536ULL
#define CG_LEN  16
/* A second range clear of the first, for the cases that need two. */
#define CG_R2   (CG_BASE + 2 * CG_LEN)

static const uint8_t oh_c4[] = { 0xc4, 0xc4 };

static void
cg_check(
    const char *tag,
    const char *what,
    long        got,
    long        want)
{
    char label[128];

    snprintf(label, sizeof(label), "%s: %s", tag, what);
    check_eq(label, got, want);
} /* cg_check */

static void
cg_reset(struct mbt_env *env)
{
    mbt_nlm_free_all(env, "quint-c1", 1);
    mbt_nlm_free_all(env, "quint-c2", 1);
    mbt_nlm_free_all(env, "quint-c3", 1);
    mbt_nlm_free_all(env, "quint-c4", 1);
} /* cg_reset */

/* c3's exclusive TEST over [offset, offset + length): GRANTED when free. */
static uint32_t
cg_test_range(
    struct mbt_env *env,
    uint64_t        offset,
    uint64_t        length)
{
    return mbt_nlm_test(env, 0, "quint-c3", &file_fh[0], oh_a, sizeof(oh_a),
                        3, 1, offset, length, ck, sizeof(ck))->nlm_stat;
} /* cg_test_range */

/* c3's exclusive TEST over the range: GRANTED when it is free. */
static uint32_t
cg_test(struct mbt_env *env)
{
    return cg_test_range(env, CG_BASE, CG_LEN);
} /* cg_test */

/* Server-initiated messages logged since the last mbt_aux_async_reset that
 * carry `proc`, and `stat` unless it is negative. */
static int
cg_count_async(
    struct mbt_env *env,
    int             proc,
    long            stat)
{
    const struct mbt_aux *a = mbt_aux(env);
    int                   i, n = 0;

    for (i = 0; i < a->nasync; i++) {
        if (a->async[i].proc == proc &&
            (stat < 0 || a->async[i].stat == (uint32_t) stat)) {
            n++;
        }
    }
    return n;
} /* cg_count_async */

/* c2's CANCEL of its exclusive blocking request, as the client sends it. */
static uint32_t
cg_cancel_c2(
    struct mbt_env *env,
    int             msg,
    int             exclusive,
    uint64_t        length)
{
    return mbt_nlm_cancel(env, msg, "quint-c2", &file_fh[0], oh_b,
                          sizeof(oh_b), 2, exclusive, 1, CG_BASE, length, ck,
                          sizeof(ck))->nlm_stat;
} /* cg_cancel_c2 */

/* c1 holds the range exclusively and c2 queues an exclusive blocking LOCK
 * behind it, sent as `proc`: 2 (LOCK) or 7 (LOCK_MSG, whose NLM4_BLOCKED
 * comes back as a LOCK_RES). */
static void
cg_queue_c2(
    struct mbt_env *env,
    const char     *tag,
    int             proc)
{
    const struct mbt_fh   *f = &file_fh[0];
    struct mbt_aux_result *r;

    cg_reset(env);
    r = mbt_nlm_lock(env, 2, "quint-c1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     CG_BASE, CG_LEN, ck, sizeof(ck));
    cg_check(tag, "c1 LOCK excl", r->nlm_stat, NLM4_GRANTED);
    if (proc == 7) {
        mbt_aux_async_reset(env);
        mbt_nlm_lock(env, 7, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1, 0, 0,
                     CG_BASE, CG_LEN, ck, sizeof(ck));
        mbt_aux_drain_for(env, 1, 2000000);
        cg_check(tag, "c2 blocking LOCK_MSG excl: LOCK_RES BLOCKED",
                 cg_count_async(env, 12, NLM4_BLOCKED), 1);
    } else {
        r = mbt_nlm_lock(env, 2, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1, 0,
                         0, CG_BASE, CG_LEN, ck, sizeof(ck));
        cg_check(tag, "c2 blocking LOCK excl", r->nlm_stat, NLM4_BLOCKED);
    }
} /* cg_queue_c2 */

/* c1 holds the range exclusively, c2 queues an exclusive blocking LOCK
 * behind it (and c4 too when asked), then c1 unlocks: c2 is granted out of
 * band and the callback is lost. */
static void
cg_grant_to_c2(
    struct mbt_env *env,
    const char     *tag,
    int             with_c4)
{
    const struct mbt_fh   *f = &file_fh[0];
    struct mbt_aux_result *r;

    cg_queue_c2(env, tag, 2);
    if (with_c4) {
        r = mbt_nlm_lock(env, 2, "quint-c4", f, oh_c4, sizeof(oh_c4), 4, 1, 1,
                         0, 0, CG_BASE, CG_LEN, ck, sizeof(ck));
        cg_check(tag, "c4 blocking LOCK excl", r->nlm_stat, NLM4_BLOCKED);
    }
    mbt_nlm_unlock(env, 0, "quint-c1", f, oh_a, sizeof(oh_a), 1, CG_BASE,
                   CG_LEN, ck, sizeof(ck));
    mbt_aux_drain_us(env, 200000);
    cg_check(tag, "c3 TEST excl after c1 left (c2 granted)", cg_test(env),
             NLM4_DENIED);
} /* cg_grant_to_c2 */

static void
probe_nlm_cancel_after_grant(struct mbt_env *env)
{
    const struct mbt_fh   *f = &file_fh[0];
    struct mbt_aux_result *r;

    printf("nlm cancel after a deferred grant:\n");

    /* The client never saw the grant; its waiter gives up. */
    cg_grant_to_c2(env, "orphan", 0);
    cg_check("orphan", "c2 CANCEL", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    cg_check("orphan", "c3 TEST excl after the CANCEL (released)",
             cg_test(env), NLM4_GRANTED);

    /* The client re-polled and was told GRANTED: the lock is the client's. */
    cg_grant_to_c2(env, "confirmed", 0);
    r = mbt_nlm_lock(env, 2, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1, 0, 0,
                     CG_BASE, CG_LEN, ck, sizeof(ck));
    cg_check("confirmed", "c2 re-poll LOCK", r->nlm_stat, NLM4_GRANTED);
    cg_check("confirmed", "c2 CANCEL", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    cg_check("confirmed", "c3 TEST excl after the CANCEL (still held)",
             cg_test(env), NLM4_DENIED);

    /* Releasing the cancelled grant promotes the next waiter. */
    cg_grant_to_c2(env, "promote", 1);
    cg_check("promote", "c2 CANCEL", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    mbt_aux_drain_us(env, 200000);
    cg_check("promote", "c3 TEST excl after the CANCEL (c4 promoted)",
             cg_test(env), NLM4_DENIED);
    mbt_nlm_unlock(env, 0, "quint-c4", f, oh_c4, sizeof(oh_c4), 4, CG_BASE,
                   CG_LEN, ck, sizeof(ck));
    cg_check("promote", "c3 TEST excl after c4 unlocked", cg_test(env),
             NLM4_GRANTED);

    /* CANCEL_MSG releases the same way and still answers once. */
    cg_grant_to_c2(env, "cancel_msg", 0);
    mbt_aux_async_reset(env);
    cg_cancel_c2(env, 1, 1, CG_LEN);
    mbt_aux_drain_for(env, 1, 2000000);
    cg_check("cancel_msg", "CANCEL_MSG async replies", mbt_aux(env)->nasync, 1);
    cg_check("cancel_msg", "c3 TEST excl after the CANCEL_MSG (released)",
             cg_test(env), NLM4_GRANTED);

    /* A CANCEL naming another mode or range is not for this grant. */
    cg_grant_to_c2(env, "mismatch", 0);
    cg_check("mismatch", "c2 CANCEL shared", cg_cancel_c2(env, 0, 0, CG_LEN),
             NLM4_GRANTED);
    cg_check("mismatch", "c2 CANCEL longer range",
             cg_cancel_c2(env, 0, 1, 2 * CG_LEN), NLM4_GRANTED);
    cg_check("mismatch", "c3 TEST excl after the CANCELs (still held)",
             cg_test(env), NLM4_DENIED);

    /* A LOCK_MSG's grant is lost the same way; its NLM4_BLOCKED came back as
     * a LOCK_RES.  CANCEL releases it, and nothing follows on the async
     * side: the request was answered, and CANCEL replies synchronously. */
    cg_queue_c2(env, "lock_msg", 7);
    mbt_nlm_unlock(env, 0, "quint-c1", f, oh_a, sizeof(oh_a), 1, CG_BASE,
                   CG_LEN, ck, sizeof(ck));
    mbt_aux_drain_us(env, 200000);
    cg_check("lock_msg", "c3 TEST excl after c1 left (c2 granted)",
             cg_test(env), NLM4_DENIED);
    mbt_aux_async_reset(env);
    cg_check("lock_msg", "c2 CANCEL", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    cg_check("lock_msg", "c3 TEST excl after the CANCEL (released)",
             cg_test(env), NLM4_GRANTED);
    mbt_aux_drain_us(env, 200000);
    cg_check("lock_msg", "async messages after the CANCEL",
             mbt_aux(env)->nasync, 0);

    /* A blocked client re-sends its LOCK while it waits (Linux lockd every
     * 30 s).  The re-send, as LOCK or LOCK_MSG, joins the request already
     * queued and is answered NLM4_BLOCKED again; one CANCEL then withdraws
     * it, and c1 leaving grants nothing: no second copy was queued. */
    cg_queue_c2(env, "repoll", 2);
    r = mbt_nlm_lock(env, 2, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1, 0, 0,
                     CG_BASE, CG_LEN, ck, sizeof(ck));
    cg_check("repoll", "c2 re-sent blocking LOCK excl", r->nlm_stat,
             NLM4_BLOCKED);
    mbt_aux_async_reset(env);
    mbt_nlm_lock(env, 7, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1, 0, 0,
                 CG_BASE, CG_LEN, ck, sizeof(ck));
    mbt_aux_drain_for(env, 1, 2000000);
    cg_check("repoll", "c2 re-sent blocking LOCK_MSG excl: LOCK_RES BLOCKED",
             cg_count_async(env, 12, NLM4_BLOCKED), 1);
    cg_check("repoll", "c2 CANCEL", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    mbt_aux_async_reset(env);
    mbt_nlm_unlock(env, 0, "quint-c1", f, oh_a, sizeof(oh_a), 1, CG_BASE,
                   CG_LEN, ck, sizeof(ck));
    mbt_aux_drain_us(env, 200000);
    cg_check("repoll", "c3 TEST excl after c1 left (nothing was queued)",
             cg_test(env), NLM4_GRANTED);
    cg_check("repoll", "async messages after c1 left",
             mbt_aux(env)->nasync, 0);

    /* c2 unlocks the middle of its unconfirmed grant.  What is left are
     * locks of their own, narrower than the request, so a CANCEL naming the
     * request releases neither. */
    cg_grant_to_c2(env, "carved", 0);
    r = mbt_nlm_unlock(env, 0, "quint-c2", f, oh_b, sizeof(oh_b), 2,
                       CG_BASE + 4, 4, ck, sizeof(ck));
    cg_check("carved", "c2 UNLOCK [4,8)", r->nlm_stat, NLM4_GRANTED);
    cg_check("carved", "c3 TEST excl [4,8) (carved out)",
             cg_test_range(env, CG_BASE + 4, 4), NLM4_GRANTED);
    cg_check("carved", "c2 CANCEL", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    cg_check("carved", "c3 TEST excl [0,4) after the CANCEL (still held)",
             cg_test_range(env, CG_BASE, 4), NLM4_DENIED);
    cg_check("carved", "c3 TEST excl [8,16) after the CANCEL (still held)",
             cg_test_range(env, CG_BASE + 8, CG_LEN - 8), NLM4_DENIED);

    /* A pending request on another range is withdrawn, and the unconfirmed
     * grant stays: c2 holds an unconfirmed grant on R1 and has a request
     * pending on R2 behind c1, and CANCELling the R2 request leaves R1 held
     * until a CANCEL of its own.  One tuple both pending and held
     * unconfirmed, where do_cancel's "a pending request wins" would decide,
     * is unreachable from a conforming client: a re-LOCK of a held tuple is
     * answered as a duplicate (confirming it), never queued. */
    cg_grant_to_c2(env, "other_range", 0);
    r = mbt_nlm_lock(env, 2, "quint-c1", f, oh_a, sizeof(oh_a), 1, 1, 0, 0, 0,
                     CG_R2, CG_LEN, ck, sizeof(ck));
    cg_check("other_range", "c1 LOCK excl R2", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1, 0, 0,
                     CG_R2, CG_LEN, ck, sizeof(ck));
    cg_check("other_range", "c2 blocking LOCK excl R2", r->nlm_stat,
             NLM4_BLOCKED);
    r = mbt_nlm_cancel(env, 0, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1,
                       CG_R2, CG_LEN, ck, sizeof(ck));
    cg_check("other_range", "c2 CANCEL R2", r->nlm_stat, NLM4_GRANTED);
    cg_check("other_range", "c3 TEST excl R1 (c2's grant untouched)",
             cg_test(env), NLM4_DENIED);
    cg_check("other_range", "c3 TEST excl R2 (c1 still holds)",
             cg_test_range(env, CG_R2, CG_LEN), NLM4_DENIED);
    mbt_nlm_unlock(env, 0, "quint-c1", f, oh_a, sizeof(oh_a), 1, CG_R2,
                   CG_LEN, ck, sizeof(ck));
    mbt_aux_drain_us(env, 200000);
    cg_check("other_range", "c3 TEST excl R2 after c1 left (withdrawn)",
             cg_test_range(env, CG_R2, CG_LEN), NLM4_GRANTED);
    cg_check("other_range", "c2 CANCEL R1", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    cg_check("other_range", "c3 TEST excl R1 after its CANCEL (released)",
             cg_test(env), NLM4_GRANTED);

    /* A blocked upgrade: c2's deferred exclusive grant replaced the shared
     * lock c2 already held.  The client still believes it holds that shared
     * lock after cancelling the upgrade, so CANCEL must not free the range. */
    cg_reset(env);
    r = mbt_nlm_lock(env, 2, "quint-c2", f, oh_b, sizeof(oh_b), 2, 0, 0, 0, 0,
                     CG_BASE, CG_LEN, ck, sizeof(ck));
    cg_check("upgrade", "c2 LOCK shared", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-c1", f, oh_a, sizeof(oh_a), 1, 0, 0, 0, 0,
                     CG_BASE, CG_LEN, ck, sizeof(ck));
    cg_check("upgrade", "c1 LOCK shared", r->nlm_stat, NLM4_GRANTED);
    r = mbt_nlm_lock(env, 2, "quint-c2", f, oh_b, sizeof(oh_b), 2, 1, 1, 0, 0,
                     CG_BASE, CG_LEN, ck, sizeof(ck));
    cg_check("upgrade", "c2 blocking LOCK excl (upgrade)", r->nlm_stat,
             NLM4_BLOCKED);
    mbt_nlm_unlock(env, 0, "quint-c1", f, oh_a, sizeof(oh_a), 1, CG_BASE,
                   CG_LEN, ck, sizeof(ck));
    mbt_aux_drain_us(env, 200000);
    cg_check("upgrade", "c3 TEST excl after c1 left (c2 upgraded)",
             cg_test(env), NLM4_DENIED);
    cg_check("upgrade", "c2 CANCEL", cg_cancel_c2(env, 0, 1, CG_LEN),
             NLM4_GRANTED);
    cg_check("upgrade", "c3 TEST excl after the CANCEL (c2 still holds)",
             cg_test(env), NLM4_DENIED);

    cg_reset(env);
} /* probe_nlm_cancel_after_grant */

/*
 * CANCEL racing the pump that grants the request it names.  c1's UNLOCK
 * frees the range, and from inside that release the claim core grants c2's
 * queued LOCK by calling the NLM acquire completion on the thread serving
 * the UNLOCK.  A CANCEL served by another thread just before that
 * completion takes the NLM state mutex finds c2's request still pending but
 * already off the claim core's queue, so it cannot withdraw it: the
 * completion that follows has to take the grant back rather than deliver
 * it, or the client that gave up holds a lock it will never release.
 *
 * The test hooks park that completion before it takes the mutex, and the
 * CANCEL goes out on another connection, one a live thread serves.  c1's
 * UNLOCK goes out as UNLOCK_MSG and is not waited on: its void reply is
 * flushed by the very thread that is parked.  "No GRANTED went out" is
 * observed at the granter through the submit hook -- the probe has no NLM
 * listener, and a GRANTED delivered in process lands on chimera's own
 * service, so the probe would not see one either way.
 */
#define RG_NCONN    3
#define RG_TICK_US  5000
#define RG_PARK_US  2000000
#define RG_PING_US  500000
#define RG_REPLY_US 2000000

/* Shared with the server thread the hook parks, hence the atomics. */
struct rg_hooks {
    int armed;        /* park the next completion of c2 ... */
    int want_blocked; /* ... that was (1) or was not (0) answered BLOCKED */
    int parked;       /* a server thread sits in rg_park */
    int release;      /* let it go */
    int submits;      /* grants handed to the granter */
};

static struct rg_hooks rg;

/* Reply flags of the requests sent without waiting.  Static: a reply can
 * land after the wait for it gave up. */
static int             rg_unlock_acked;
static int             rg_ping_acked[RG_NCONN];

static void
rg_park(
    int32_t svid,
    bool    was_blocked,
    void   *arg)
{
    struct rg_hooks *h = &rg;

    (void) arg;
    if (svid != 2 || (int) was_blocked != h->want_blocked ||
        !chimera_atomic_exchange_n(&h->armed, 0, CHIMERA_MEMORY_ACQ_REL)) {
        return;
    }
    chimera_atomic_store_n(&h->parked, 1, CHIMERA_MEMORY_RELEASE);
    while (!chimera_atomic_load_n(&h->release, CHIMERA_MEMORY_ACQUIRE)) {
        usleep(1000);
    }
} /* rg_park */

static void
rg_submit(
    const struct nlm_grant_request *req,
    void                           *arg)
{
    struct rg_hooks *h = &rg;

    (void) req;
    (void) arg;
    chimera_atomic_fetch_add(&h->submits, 1, CHIMERA_MEMORY_RELAXED);
} /* rg_submit */

static void
rg_install(int want_blocked)
{
    memset(&rg, 0, sizeof(rg));
    rg.armed                                    = 1;
    rg.want_blocked                             = want_blocked;
    chimera_nfs_nlm_test_hooks.grant_submitted  = rg_submit;
    chimera_nfs_nlm_test_hooks.acquire_cb_enter = rg_park;
} /* rg_install */

/* Every exit path goes through here, so no server thread stays parked: it
 * disarms and releases, and unsets the hooks only once the traffic the
 * release lets through has drained.  The hooks read rg directly and rg is
 * left as it is (release stays set): a thread that won the armed exchange
 * but has not yet read release must not park, and rg_install resets it only
 * to arm the next variant. */
static void
rg_uninstall(struct mbt_env *env)
{
    chimera_atomic_store_n(&rg.armed, 0, CHIMERA_MEMORY_RELEASE);
    chimera_atomic_store_n(&rg.release, 1, CHIMERA_MEMORY_RELEASE);
    mbt_aux_drain_us(env, 200000);
    chimera_nfs_nlm_test_hooks.acquire_cb_enter = NULL;
    chimera_nfs_nlm_test_hooks.grant_submitted  = NULL;
} /* rg_uninstall */

static void
rg_void_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    (void) evpl;
    (void) verf;
    *(int *) private_data = status == 0 ? 1 : -1;
} /* rg_void_cb */

/* Pump the client loop until *flag is set or timeout_us has passed. */
static int
rg_wait_flag(
    struct mbt_env *env,
    const int      *flag,
    uint64_t        timeout_us)
{
    uint64_t waited;

    for (waited = 0; !*flag && waited < timeout_us; waited += RG_TICK_US) {
        mbt_aux_drain_us(env, RG_TICK_US);
    }
    return *flag;
} /* rg_wait_flag */

static int
rg_wait_parked(struct mbt_env *env)
{
    uint64_t waited;

    for (waited = 0; waited < RG_PARK_US; waited += RG_TICK_US) {
        if (chimera_atomic_load_n(&rg.parked, CHIMERA_MEMORY_ACQUIRE)) {
            return 1;
        }
        mbt_aux_drain_us(env, RG_TICK_US);
    }
    return chimera_atomic_load_n(&rg.parked, CHIMERA_MEMORY_ACQUIRE);
} /* rg_wait_parked */

/* NLM NULL on conns[i]: 1 once answered within timeout_us. */
static int
rg_ping(
    struct mbt_env         *env,
    struct evpl_rpc2_conn **conns,
    int                     i,
    uint64_t                timeout_us)
{
    rg_ping_acked[i] = 0;
    env->nlm_v4.send_call_NLMPROC4_NULL(&env->nlm_v4.rpc2, env->evpl,
                                        conns[i], &env->aux_cred, 0, 0, NULL,
                                        0, 0, rg_void_cb, &rg_ping_acked[i]);
    return rg_wait_flag(env, &rg_ping_acked[i], timeout_us) == 1;
} /* rg_ping */

/* c1's UNLOCK_MSG, sent without waiting for its void reply. */
static void
rg_unlock_c1_nowait(struct mbt_env *env)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_unlockargs     args;

    rg_unlock_acked = 0;
    mbt_nlm_fill_cookie(&args.cookie, &scratch, ck, sizeof(ck));
    mbt_nlm_fill_lock(&args.alock, &scratch, "quint-c1", &file_fh[0], oh_a,
                      sizeof(oh_a), 1, CG_BASE, CG_LEN);
    env->nlm_v4.send_call_NLMPROC4_UNLOCK_MSG(&env->nlm_v4.rpc2, env->evpl,
                                              env->nlm_conn, &env->aux_cred,
                                              &args, 0, 0, NULL, 0, 0,
                                              rg_void_cb, &rg_unlock_acked);
} /* rg_unlock_c1_nowait */

/* c2's LOCK reply, for the LOCK sent without waiting.  Static for the same
 * reason as the flags above. */
static struct {
    int      done;
    uint32_t stat;
} rg_lock;

static void
rg_lock_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *reply,
    int                          status,
    void                        *private_data)
{
    (void) evpl;
    (void) verf;
    (void) private_data;
    if (status == 0) {
        rg_lock.stat = reply->stat;
    }
    rg_lock.done = status == 0 ? 1 : -1;
} /* rg_lock_cb */

/* c2's exclusive blocking LOCK (proc 2) or LOCK_MSG (proc 7), sent without
 * waiting for its reply; a LOCK_MSG's void reply sets rg_lock.done, its
 * answer comes back as a LOCK_RES. */
static void
rg_lock_c2_nowait(
    struct mbt_env *env,
    int             proc)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_lockargs       args;

    rg_lock.done = 0;
    rg_lock.stat = UINT32_MAX;
    mbt_nlm_fill_cookie(&args.cookie, &scratch, ck, sizeof(ck));
    args.block     = true;
    args.exclusive = true;
    args.reclaim   = false;
    args.state     = 0;
    mbt_nlm_fill_lock(&args.alock, &scratch, "quint-c2", &file_fh[0], oh_b,
                      sizeof(oh_b), 2, CG_BASE, CG_LEN);
    if (proc == 7) {
        env->nlm_v4.send_call_NLMPROC4_LOCK_MSG(&env->nlm_v4.rpc2, env->evpl,
                                                env->nlm_conn, &env->aux_cred,
                                                &args, 0, 0, NULL, 0, 0,
                                                rg_void_cb, &rg_lock.done);
        return;
    }
    env->nlm_v4.send_call_NLMPROC4_LOCK(&env->nlm_v4.rpc2, env->evpl,
                                        env->nlm_conn, &env->aux_cred, &args,
                                        0, 0, NULL, 0, 0, rg_lock_cb, NULL);
} /* rg_lock_c2_nowait */

/* c2's CANCEL -- or with `reap`, a FREE_ALL of c2 -- while the thread
 * serving env->nlm_conn is parked, sent on the first of conns[] another
 * thread answers (a connection whose NULL is stuck behind the park only gets
 * it answered after the release).  0 when no connection answers. */
static int
rg_withdraw_c2_live(
    struct mbt_env         *env,
    struct evpl_rpc2_conn **conns,
    const char             *tag,
    int                     reap)
{
    struct evpl_rpc2_conn *live = NULL;
    struct evpl_rpc2_conn *saved;
    int                    i;

    for (i = 0; i < RG_NCONN && !live; i++) {
        if (rg_ping(env, conns, i, RG_PING_US)) {
            live = conns[i];
        }
    }
    if (!live) {
        fail("%s: no NLM connection answered while the grant was parked",
             tag);
        return 0;
    }

    saved         = env->nlm_conn;
    env->nlm_conn = live;
    if (reap) {
        mbt_nlm_free_all(env, "quint-c2", 1);
        cg_check(tag, "c2 FREE_ALL while the grant is parked",
                 env->res.rpc_err, 0);
    } else {
        cg_check(tag, "c2 CANCEL while the grant is parked",
                 cg_cancel_c2(env, 0, 1, CG_LEN), NLM4_GRANTED);
    }
    env->nlm_conn = saved;
    return 1;
} /* rg_withdraw_c2_live */

/* One race, with c2's LOCK sent as `proc` (2 = LOCK, 7 = LOCK_MSG). */
static void
rg_case(
    struct mbt_env         *env,
    struct evpl_rpc2_conn **conns,
    int                     proc)
{
    const char *tag = proc == 7 ? "race_lock_msg" : "race_lock";

    cg_queue_c2(env, tag, proc);
    rg_install(1);

    mbt_aux_async_reset(env);
    rg_unlock_c1_nowait(env);
    if (!rg_wait_parked(env)) {
        fail("%s: c1's UNLOCK never reached c2's acquire completion", tag);
        goto out;
    }
    if (!rg_withdraw_c2_live(env, conns, tag, 0)) {
        goto out;
    }

    chimera_atomic_store_n(&rg.release, 1, CHIMERA_MEMORY_RELEASE);
    /* Wait for what the released thread sends rather than a fixed window:
     * c1's UNLOCK_RES and, for a LOCK_MSG, c2's LOCK_RES. */
    mbt_aux_drain_for(env, proc == 7 ? 2 : 1, RG_REPLY_US);

    cg_check(tag, "c1 UNLOCK_MSG answered", rg_wait_flag(env, &rg_unlock_acked,
                                                         RG_REPLY_US), 1);
    cg_check(tag, "c3 TEST excl after the raced CANCEL (released)",
             cg_test(env), NLM4_GRANTED);
    cg_check(tag, "grants submitted to the granter",
             chimera_atomic_load_n(&rg.submits, CHIMERA_MEMORY_ACQUIRE), 0);
    if (proc == 7) {
        cg_check(tag, "LOCK_RES DENIED", cg_count_async(env, 12, NLM4_DENIED),
                 1);
        cg_check(tag, "LOCK_RES GRANTED",
                 cg_count_async(env, 12, NLM4_GRANTED), 0);
    } else {
        cg_check(tag, "LOCK_RES of any kind", cg_count_async(env, 12, -1), 0);
    }

 out:
    rg_uninstall(env);
    cg_reset(env);
} /* rg_case */

/* A CANCEL that arrives before the LOCK is answered at all.  Linux lockd
 * sends one when the process is killed during the LOCK call: nlmclnt_lock
 * presets the result to NLM4_BLOCKED, so its cancel path runs although no
 * BLOCKED ever came back.  Here c2's LOCK finds the range free and is
 * granted at once; its completion is parked before it publishes the grant,
 * the CANCEL lands, and the still-open LOCK must then be answered
 * NLM4_DENIED with the range left free.  Sent as `proc` (2 = LOCK, 7 =
 * LOCK_MSG): the LOCK_MSG's void reply says nothing, so its NLM4_DENIED must
 * arrive as exactly one LOCK_RES.
 *
 * With `reap`, a FREE_ALL of c2 lands there instead.  The reaper cannot
 * withdraw a ticket the claim core has already decided, so it marks the
 * request reaped and leaves it to the completion, which releases the grant.
 * The still-open LOCK is answered NLM4_DENIED as above, but a LOCK_MSG gets
 * no LOCK_RES: a reaped client is owed none. */
static void
rg_early_case(
    struct mbt_env         *env,
    struct evpl_rpc2_conn **conns,
    int                     proc,
    int                     reap)
{
    const char *tag;

    if (reap) {
        tag = proc == 7 ? "early_reap_lock_msg" : "early_reap";
    } else {
        tag = proc == 7 ? "early_cancel_lock_msg" : "early_cancel";
    }

    cg_reset(env);
    rg_install(0);

    mbt_aux_async_reset(env);
    rg_lock_c2_nowait(env, proc);
    if (!rg_wait_parked(env)) {
        fail("%s: c2's LOCK never reached its acquire completion", tag);
        goto out;
    }
    if (!rg_withdraw_c2_live(env, conns, tag, reap)) {
        goto out;
    }

    chimera_atomic_store_n(&rg.release, 1, CHIMERA_MEMORY_RELEASE);
    /* A LOCK_MSG taken back by CANCEL is answered by a LOCK_RES: wait for
     * it rather than a fixed window.  Every other variant expects none,
     * which only a window can show. */
    if (proc == 7 && !reap) {
        mbt_aux_drain_for(env, 1, RG_REPLY_US);
    } else {
        mbt_aux_drain_us(env, 200000);
    }

    cg_check(tag, "c2 LOCK answered", rg_wait_flag(env, &rg_lock.done,
                                                   RG_REPLY_US), 1);
    if (proc != 7) {
        cg_check(tag, "c2 LOCK reply", rg_lock.stat, NLM4_DENIED);
    }
    cg_check(tag, "c3 TEST excl after the release (free)",
             cg_test(env), NLM4_GRANTED);
    cg_check(tag, "grants submitted to the granter",
             chimera_atomic_load_n(&rg.submits, CHIMERA_MEMORY_ACQUIRE), 0);
    if (proc == 7 && !reap) {
        cg_check(tag, "LOCK_RES DENIED", cg_count_async(env, 12, NLM4_DENIED),
                 1);
        cg_check(tag, "LOCK_RES GRANTED",
                 cg_count_async(env, 12, NLM4_GRANTED), 0);
    } else {
        cg_check(tag, "LOCK_RES of any kind", cg_count_async(env, 12, -1), 0);
    }

 out:
    rg_uninstall(env);
    cg_reset(env);
} /* rg_early_case */

static void
probe_nlm_cancel_races_grant(struct mbt_env *env)
{
    struct evpl_rpc2_conn *conns[RG_NCONN];
    int                    i;

    printf("nlm cancel racing the grant:\n");

    for (i = 0; i < RG_NCONN; i++) {
        conns[i] = mbt_aux_nlm_connect2(env);
        if (!rg_ping(env, conns, i, RG_REPLY_US)) {
            fail("race: extra NLM connection %d never answered", i);
        }
    }

    rg_case(env, conns, 2);
    rg_case(env, conns, 7);
    rg_early_case(env, conns, 2, 0);
    rg_early_case(env, conns, 7, 0);
    rg_early_case(env, conns, 2, 1);
    rg_early_case(env, conns, 7, 1);

    for (i = 0; i < RG_NCONN; i++) {
        evpl_rpc2_client_disconnect(env->rpc2_thread, conns[i]);
    }
} /* probe_nlm_cancel_races_grant */

/* ---------------------------------------------------------------------- */

static void
probe_nsm(struct mbt_env *env)
{
    struct mbt_aux_result *r;

    printf("nsm:\n");

    mbt_sm_null(env);
    check_eq("SM_NULL rpc status", env->res.rpc_err, 0);

    r = mbt_sm_stat(env, "somehost");
    check_eq("SM_STAT res_stat", r->sm_res, 0);        /* STAT_SUCC */
    /* A server whose lock state cannot survive a restart starts at 1. */
    check_eq("SM_STAT state", r->sm_state, 1);

    r = mbt_sm_mon(env, "somehost", "me", 100021, 4, 24);
    check_eq("SM_MON res_stat", r->sm_res, 0);
    check_eq("SM_MON state matches SM_STAT", r->sm_state, r->sm_state);

    r = mbt_sm_unmon(env, "somehost", "me");
    check_eq("SM_UNMON state", r->sm_state, 1);
    r = mbt_sm_unmon_all(env, "me");
    check_eq("SM_UNMON_ALL state", r->sm_state, 1);

    /* SM_NOTIFY for a host that holds locks releases them. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 256, 16, ck, sizeof(ck));
    check_eq("LOCK excl [256,272) as A (for SM_NOTIFY)", r->nlm_stat,
             NLM4_GRANTED);
    mbt_sm_notify(env, PROBE_CALLER_A, 7);
    check_eq("SM_NOTIFY accept_stat", env->res.rpc_err, 0);
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 1, 256, 16, ck, sizeof(ck));
    check_eq("TEST [256,272) after SM_NOTIFY of A", r->nlm_stat,
             NLM4_GRANTED);

    /* SM_NOTIFY naming a host with no NLM client falls back to matching
     * monitors by the notify's source address.  Under inproc every peer
     * address is the same string, so the fallback matches every monitored
     * host -- pin whether that releases unrelated locks. */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_B, &file_fh[1], oh_b, sizeof(oh_b),
                     2, 1, 0, 0, 0, 512, 16, ck, sizeof(ck));
    check_eq("LOCK excl [512,528) as B (for the fallback)", r->nlm_stat,
             NLM4_GRANTED);
    mbt_sm_notify(env, "a-host-that-never-locked", 9);
    r = mbt_nlm_test(env, 0, "quint-e", &file_fh[1], oh_a, sizeof(oh_a), 5, 1,
                     512, 16, ck, sizeof(ck));
    check_eq("TEST [512,528) after an unknown-host SM_NOTIFY (released)",
             r->nlm_stat, NLM4_GRANTED);

    /* SM_SIMU_CRASH bumps the state number, keeping it odd.  With monitored
     * hosts present it also spawns the reboot-notify worker, which under
     * inproc resolves and calls back into this very server. */
    mbt_sm_simu_crash(env);
    check_eq("SM_SIMU_CRASH accept_stat", env->res.rpc_err, 0);
    r = mbt_sm_stat(env, "somehost");
    check_eq("SM_STAT state after SIMU_CRASH", r->sm_state, 3);
    mbt_sm_simu_crash(env);
    r = mbt_sm_stat(env, "somehost");
    check_eq("SM_STAT state after a second SIMU_CRASH", r->sm_state, 5);
} /* probe_nsm */

/* ---------------------------------------------------------------------- */

int
main(
    int    argc,
    char **argv)
{
    /* *INDENT-OFF* -- uncrustify oscillates on this table's alignment. */
    static struct option long_options[] = {
        { "dump", no_argument,       0, 'd' },
        { "sec",  required_argument, 0, 'S' },
        { 0,      0,                 0, 0   },
    };
    /* *INDENT-ON* */
    struct mbt_env      env;
    struct mbt_env_opts opts;
    int                 c;

    while ((c = getopt_long(argc, argv, "dS:", long_options, NULL)) != -1) {
        switch (c) {
            case 'd':
                dump = 1;
                break;
            case 'S':
                /* Read by mbt_sec_scan_argv below; accepted here so getopt
                 * does not reject it. */
                break;
            default:
                fprintf(stderr, "usage: %s [--dump] "
                        "[--sec sys|krb5|krb5i|krb5p]\n", argv[0]);
                return 2;
        } /* switch */
    }

    memset(&opts, 0, sizeof(opts));
    opts.sec              = mbt_sec_scan_argv(argc, argv);
    opts.portmap_hostname = PROBE_UADDR_HOST;
    opts.disable_caches   = 1;

    /* A hung reply spins forever inside mbt_call_wait with everything in one
     * process; SIGALRM's default disposition turns that into a test failure. */
    mbt_watchdog_arm(180);

    mbt_aux_env_open(&env, &opts);
    /* The aux harness asserts against a fixed "/share" export -- its MNT
     * cases include deliberate near-misses like "/shareXX" -- so it mounts
     * under that name rather than the per-trace one the batch replayers
     * use.  The filesystem itself is still per-trace. */
    mbt_env_fs_setup_as(&env, "fs0", "share");
    if (chimera_server_create_export(env.server, PROBE_EXPORT2, "/share", 0,
                                     NULL) != 0) {
        fprintf(stderr, "probe: failed to create the %s export\n",
                PROBE_EXPORT2);
        /* Unwind what mbt_aux_env_open built; returning straight out leaks the
         * environment's READ scratch buffer. */
        mbt_env_fs_teardown_as(&env, "fs0", "share");
        mbt_env_stop(&env);
        return 1;
    }

    probe_portmap(&env);
    /* MOUNT resolution is probed against a namespace that exists, so the
     * objects have to be created first -- which needs the export root
     * handle, hence the bare MNT here. */
    probe_root_handle(&env);
    probe_setup_files(&env);
    probe_mount(&env);
    probe_nlm(&env);
    probe_nlm_relock(&env);
    probe_nlm_cancel_after_grant(&env);
    probe_nlm_cancel_races_grant(&env);
    probe_nsm(&env);

    chimera_server_remove_export(env.server, PROBE_EXPORT2);
    mbt_env_fs_teardown_as(&env, "fs0", "share");
    mbt_env_stop(&env);
    mbt_watchdog_disarm();

    if (failures) {
        fprintf(stderr, "\n%d aux probe expectation(s) failed\n", failures);
        return 1;
    }
    printf("\nnfs aux probe: all expectations hold\n");
    return 0;
} /* main */
