// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Per-export read-only ("access": "ro") enforcement for NFSv4: the decision
 * logic in nfs4_rofs.h and the NFS4_OP_FLAG_MUTATES set that drives it.
 *
 * Two things are checked here that no end-to-end test can reach.  First, the
 * flag set is pinned exactly, in both directions: a new mutating op added to
 * nfs4_op_matrix.h without the flag is a silent enforcement hole rather than a
 * failure anywhere else, and a conditionally-mutating op flagged by mistake
 * would reject reads.  Second, the gate branches that the chimera posix client
 * cannot exercise -- the OPEN and OPENATTR argument-dependent cases, LAYOUTGET,
 * and the cross-export saved-filehandle side of RENAME/LINK -- are driven
 * directly.  See src/posix/tests/test_export_ro.c for the end-to-end half.
 *
 * Checks are explicit rather than assert()-based: release builds define NDEBUG,
 * which would compile an assert-only test down to nothing.
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "nfs_internal.h"
#include "nfs4_xdr.h"
#include "nfs4_op_matrix.h"
#include "nfs4_rofs.h"

#define ARRAY_COUNT(a) (sizeof(a) / sizeof((a)[0]))

static int       failures = 0;

/* Every op that mutates unconditionally, whatever its arguments. */
static const int expect_mutates[] = {
    OP_CREATE,
    OP_LINK,
    OP_REMOVE,
    OP_RENAME,
    OP_SETATTR,
    OP_WRITE,
    OP_LAYOUTCOMMIT,
    OP_ALLOCATE,
    OP_COPY,
    OP_DEALLOCATE,
    OP_WRITE_SAME,
    OP_CLONE,
    OP_SETXATTR,
    OP_REMOVEXATTR,
};

/*
 * Ops that mutate only for some arguments, or that need an error other than
 * NFS4ERR_ROFS.  These are gated explicitly in nfs4_rofs_gate and must NOT
 * carry the flag -- flagging them would reject reads:
 *   OPEN       mutates only for share_access WRITE or OPEN4_CREATE
 *   OPENATTR   mutates only when createdir is set
 *   LAYOUTGET  answers NFS4ERR_LAYOUTUNAVAILABLE so clients fall back to the
 *              metadata server instead of failing the read outright
 */
static const int expect_conditional[] = {
    OP_OPEN,
    OP_OPENATTR,
    OP_LAYOUTGET,
};

static int
in_list(
    const int *list,
    size_t     count,
    int        op)
{
    size_t i;

    for (i = 0; i < count; i++) {
        if (list[i] == op) {
            return 1;
        }
    }
    return 0;
} /* in_list */

static void
test_mutates_set_is_exact(void)
{
    int op;

    for (op = NFS4_OP_MIN; op <= NFS4_OP_MAX; op++) {
        int flagged = (nfs4_op_support[op].flags & NFS4_OP_FLAG_MUTATES) != 0;
        int expect  = in_list(expect_mutates, ARRAY_COUNT(expect_mutates), op);

        if (flagged == expect) {
            continue;
        }

        failures++;
        if (expect) {
            fprintf(stderr,
                    "FAIL: op %d expected NFS4_OP_FLAG_MUTATES but does not "
                    "have it -- it would escape the read-only export gate\n",
                    op);
        } else {
            fprintf(stderr,
                    "FAIL: op %d has NFS4_OP_FLAG_MUTATES unexpectedly -- if "
                    "it only mutates conditionally, gate it in nfs4_rofs_gate "
                    "instead so reads are not rejected\n",
                    op);
        }
    }
} /* test_mutates_set_is_exact */

static void
test_conditionally_gated_ops_are_not_flagged(void)
{
    size_t i;

    for (i = 0; i < ARRAY_COUNT(expect_conditional); i++) {
        int op = expect_conditional[i];

        if (nfs4_op_support[op].flags & NFS4_OP_FLAG_MUTATES) {
            failures++;
            fprintf(stderr,
                    "FAIL: conditionally-mutating op %d must not carry "
                    "NFS4_OP_FLAG_MUTATES\n", op);
        }

        /* Sanity: still a supported op, not a hole in the matrix. */
        if (nfs4_op_support[op].minors == 0) {
            failures++;
            fprintf(stderr,
                    "FAIL: op %d is not supported in any minor version; the "
                    "expected-conditional list is stale\n", op);
        }
    }
} /* test_conditionally_gated_ops_are_not_flagged */

/* COMMIT writes no new data and is deliberately never gated -- gating it would
 * strand unstable WRITE data written through a read-write sibling export of the
 * same mount, or before a runtime flip to read-only. */
static void
test_commit_is_not_flagged(void)
{
    if (nfs4_op_support[OP_COMMIT].flags & NFS4_OP_FLAG_MUTATES) {
        failures++;
        fprintf(stderr, "FAIL: OP_COMMIT must not be gated read-only\n");
    }
} /* test_commit_is_not_flagged */

/* The flag must not collide with the dispatch flags sharing the same field. */
static void
test_flag_bits_are_distinct(void)
{
    if ((NFS4_OP_FLAG_MUTATES & NFS4_OP_FLAG_MUST_BE_FIRST) ||
        (NFS4_OP_FLAG_MUTATES & NFS4_OP_FLAG_NO_REQ_SESSION)) {
        failures++;
        fprintf(stderr,
                "FAIL: NFS4_OP_FLAG_MUTATES overlaps another dispatch flag\n");
    }
} /* test_flag_bits_are_distinct */

static void
expect_status(
    const struct nfs4_rofs_input *in,
    nfsstat4                      want,
    const char                   *desc)
{
    nfsstat4 got = nfs4_rofs_check(in);

    if (got != want) {
        failures++;
        fprintf(stderr, "FAIL: %s: got %d, expected %d\n", desc, got, want);
    }
} /* expect_status */

/* A read-write export must never be gated, whatever the op. */
static void
test_rw_export_is_never_gated(void)
{
    int op;

    for (op = NFS4_OP_MIN; op <= NFS4_OP_MAX; op++) {
        struct nfs4_rofs_input in = {
            .op               = op,
            .export_ro        = false,
            .open_writes      = true,
            .openattr_creates = true,
        };

        expect_status(&in, NFS4_OK, "read-write export must not be gated");
    }
} /* test_rw_export_is_never_gated */

static void
test_unconditional_mutators_are_gated(void)
{
    size_t i;

    for (i = 0; i < ARRAY_COUNT(expect_mutates); i++) {
        struct nfs4_rofs_input in = {
            .op        = expect_mutates[i],
            .export_ro = true,
        };

        expect_status(&in, NFS4ERR_ROFS, "mutating op on read-only export");
    }
} /* test_unconditional_mutators_are_gated */

/* OPEN is refused only when it would write: share_access WRITE or a create.
 * A read-only OPEN must still be granted, or reads break entirely. */
static void
test_open_gated_only_when_it_writes(void)
{
    struct nfs4_rofs_input reading = {
        .op          = OP_OPEN,
        .export_ro   = true,
        .open_writes = false,
    };
    struct nfs4_rofs_input writing = {
        .op          = OP_OPEN,
        .export_ro   = true,
        .open_writes = true,
    };

    expect_status(&reading, NFS4_OK, "read-only OPEN on read-only export");
    expect_status(&writing, NFS4ERR_ROFS, "write OPEN on read-only export");
} /* test_open_gated_only_when_it_writes */

/* OPENATTR mutates only when it is asked to create the attribute directory. */
static void
test_openattr_gated_only_when_creating(void)
{
    struct nfs4_rofs_input reading = {
        .op               = OP_OPENATTR,
        .export_ro        = true,
        .openattr_creates = false,
    };
    struct nfs4_rofs_input creating = {
        .op               = OP_OPENATTR,
        .export_ro        = true,
        .openattr_creates = true,
    };

    expect_status(&reading, NFS4_OK, "OPENATTR createdir=false");
    expect_status(&creating, NFS4ERR_ROFS, "OPENATTR createdir=true");
} /* test_openattr_gated_only_when_creating */

/* LAYOUTGET must answer LAYOUTUNAVAILABLE, not ROFS: clients fall back to I/O
 * through the metadata server on the former but can fail the read on the latter. */
static void
test_layoutget_reports_layoutunavailable(void)
{
    struct nfs4_rofs_input in = {
        .op        = OP_LAYOUTGET,
        .export_ro = true,
    };

    expect_status(&in, NFS4ERR_LAYOUTUNAVAILABLE, "LAYOUTGET on read-only export");
} /* test_layoutget_reports_layoutunavailable */

/*
 * The cross-export half of RENAME/LINK: the current filehandle sits on a
 * writable export while the saved side is read-only.  Two exports can share one
 * backing VFS mount, so without this the mutation would succeed at the VFS
 * layer.  Unreachable through a POSIX client (cross-mount rename returns EXDEV),
 * which is exactly why it is checked here.
 */
static void
test_saved_side_gated_for_rename_and_link(void)
{
    const int ops[] = { OP_RENAME, OP_LINK };
    size_t    i;

    for (i = 0; i < ARRAY_COUNT(ops); i++) {
        struct nfs4_rofs_input cross = {
            .op              = ops[i],
            .export_ro       = false,
            .saved_export_ro = true,
            .have_saved_fh   = true,
        };
        /* No SAVEFH ran: the saved side must not be consulted at all, so the
         * handler can report NFS4ERR_NOFILEHANDLE instead of ROFS. */
        struct nfs4_rofs_input no_savefh = {
            .op              = ops[i],
            .export_ro       = false,
            .saved_export_ro = true,
            .have_saved_fh   = false,
        };

        expect_status(&cross, NFS4ERR_ROFS,
                      "RENAME/LINK with read-only saved-side export");
        expect_status(&no_savefh, NFS4_OK,
                      "RENAME/LINK with no SAVEFH must not consult saved side");
    }

    /* An op that does not touch the saved filehandle ignores it entirely. */
    struct nfs4_rofs_input unrelated = {
        .op              = OP_READ,
        .export_ro       = false,
        .saved_export_ro = true,
        .have_saved_fh   = true,
    };

    expect_status(&unrelated, NFS4_OK,
                  "non-RENAME/LINK op ignores the saved-side export");
} /* test_saved_side_gated_for_rename_and_link */

/* Read-path ops must keep working on a read-only export. */
static void
test_read_ops_are_not_gated(void)
{
    const int ops[] = {
        OP_ACCESS,   OP_GETATTR,     OP_GETFH,        OP_LOOKUP,        OP_LOOKUPP,
        OP_READ,     OP_READDIR,     OP_READLINK,     OP_SECINFO,       OP_COMMIT,
        OP_GETXATTR, OP_LISTXATTRS,  OP_SEEK,         OP_CLOSE,         OP_LOCK,
    };
    size_t    i;

    for (i = 0; i < ARRAY_COUNT(ops); i++) {
        struct nfs4_rofs_input in = {
            .op        = ops[i],
            .export_ro = true,
        };

        expect_status(&in, NFS4_OK, "read-path op on read-only export");
    }
} /* test_read_ops_are_not_gated */

int
main(void)
{
    test_mutates_set_is_exact();
    test_conditionally_gated_ops_are_not_flagged();
    test_commit_is_not_flagged();
    test_flag_bits_are_distinct();
    test_rw_export_is_never_gated();
    test_unconditional_mutators_are_gated();
    test_open_gated_only_when_it_writes();
    test_openattr_gated_only_when_creating();
    test_layoutget_reports_layoutunavailable();
    test_saved_side_gated_for_rename_and_link();
    test_read_ops_are_not_gated();

    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }

    return 0;
} /* main */
