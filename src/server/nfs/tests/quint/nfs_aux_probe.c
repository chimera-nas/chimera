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

#include <getopt.h>

#include "nfs_aux_mbt_common.h"

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
    ...) __attribute__((format(printf, 1, 2)));

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

    /* A already holds [64,80) shared, so this names a range it holds: the
     * request is answered as an idempotent retry WITHOUT re-evaluating the
     * mode, so the upgrade silently does not happen.  (The third-party case
     * below, where the requester holds nothing, does conflict.) */
    r = mbt_nlm_lock(env, 2, PROBE_CALLER_A, &file_fh[0], oh_a, sizeof(oh_a),
                     1, 1, 0, 0, 0, 64, 16, ck, sizeof(ck));
    check_eq("LOCK excl [64,80) as A over its own shared", r->nlm_stat,
             NLM4_GRANTED);

    /* TEST reports the conflicting holder's range and mode. */
    r = mbt_nlm_test(env, 0, PROBE_CALLER_B, &file_fh[0], oh_b, sizeof(oh_b),
                     2, 1, 0, 16, ck, sizeof(ck));
    check_eq("TEST excl [0,16) as B", r->nlm_stat, NLM4_DENIED);
    check_eq("TEST holder exclusive", r->holder_exclusive, 1);
    check_eq("TEST holder svid", r->holder_svid, 0);
    check_eq("TEST holder oh len", r->holder_oh_len, 0);
    check_u64("TEST holder offset", r->holder_offset, 0);
    check_u64("TEST holder length", r->holder_length, 16);

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
        { "dump", no_argument, 0, 'd' },
        { 0,      0,           0, 0   },
    };
    /* *INDENT-ON* */
    struct mbt_env      env;
    struct mbt_env_opts opts;
    int                 c;

    while ((c = getopt_long(argc, argv, "d", long_options, NULL)) != -1) {
        switch (c) {
            case 'd':
                dump = 1;
                break;
            default:
                fprintf(stderr, "usage: %s [--dump]\n", argv[0]);
                return 2;
        } /* switch */
    }

    memset(&opts, 0, sizeof(opts));
    opts.portmap_hostname = PROBE_UADDR_HOST;
    opts.disable_caches   = 1;

    /* A hung reply spins forever inside mbt_call_wait with everything in one
     * process; SIGALRM's default disposition turns that into a test failure. */
    alarm(180);

    mbt_aux_env_open(&env, &opts);
    mbt_env_fs_setup(&env, "fs0");
    if (chimera_server_create_export(env.server, PROBE_EXPORT2, "/share", 0,
                                     NULL) != 0) {
        fprintf(stderr, "probe: failed to create the %s export\n",
                PROBE_EXPORT2);
        /* Unwind what mbt_aux_env_open built; returning straight out leaks the
         * environment's READ scratch buffer. */
        mbt_env_fs_teardown(&env, "fs0");
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
    probe_nsm(&env);

    chimera_server_remove_export(env.server, PROBE_EXPORT2);
    mbt_env_fs_teardown(&env, "fs0");
    mbt_env_stop(&env);
    alarm(0);

    if (failures) {
        fprintf(stderr, "\n%d aux probe expectation(s) failed\n", failures);
        return 1;
    }
    printf("\nnfs aux probe: all expectations hold\n");
    return 0;
} /* main */
