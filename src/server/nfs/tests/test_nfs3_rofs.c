// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Per-export read-only ("access": "ro") enforcement for NFSv3: the gate
 * helpers in nfs3_procs.h, driven against a fixture export table.
 *
 * The end-to-end half (src/posix/tests/test_export_ro.c) covers every mutating
 * procedure same-export, but the POSIX client cannot reach the cross-export
 * side of chimera_nfs3_check_rofs2 -- cross-mount RENAME/LINK fails
 * client-side with EXDEV -- so both directions (read-only source into a
 * writable directory and the reverse) are driven here, the NFSv3 counterpart
 * of test_rofs_gate.c's saved-filehandle checks.  The lookup contract of
 * chimera_nfs_export_id_is_ro is pinned as well: fail-open for export id 0 and
 * unknown ids (documented in nfs_common.h -- a deleted export must not brick
 * handles that stay valid through a surviving sibling export), and fail-closed
 * for an over-set access mask such as RO|RW, which must read as read-only
 * rather than silently granting write access.
 *
 * Checks are explicit rather than assert()-based: release builds define NDEBUG,
 * which would compile an assert-only test down to nothing.
 */

#include <stdint.h>
#include <stdio.h>

#include "nfs3_procs.h"

#define RO_EXPORT_ID      3
#define RW_EXPORT_ID      4
#define OVERSET_EXPORT_ID 5
#define UNKNOWN_EXPORT_ID 6

static int                              failures = 0;

/* exports_by_id spans the full uint16_t id space, so the shared fixture is
 * large; keep it static rather than on the stack. */
static struct chimera_server_nfs_shared shared;
static struct chimera_server_nfs_thread thread;
static struct nfs_request               request;

static struct chimera_nfs_export        export_ro = {
    .id     = RO_EXPORT_ID,
    .access = CHIMERA_NFS_EXPORT_ACCESS_RO,
};

static struct chimera_nfs_export        export_rw = {
    .id     = RW_EXPORT_ID,
    .access = CHIMERA_NFS_EXPORT_ACCESS_RW,
};

/* The access field is copied verbatim from the public API with no validation,
 * so an over-set mask is representable; it must fail closed. */
static struct chimera_nfs_export        export_overset = {
    .id     = OVERSET_EXPORT_ID,
    .access = CHIMERA_NFS_EXPORT_ACCESS_RO | CHIMERA_NFS_EXPORT_ACCESS_RW,
};

static void
expect_single(
    uint16_t    export_id,
    nfsstat3    want,
    const char *desc)
{
    nfsstat3 got = chimera_nfs3_check_rofs(&request, export_id);

    if (got != want) {
        failures++;
        fprintf(stderr, "FAIL: %s: got %d, expected %d\n", desc, got, want);
    }
} /* expect_single */

static void
expect_dual(
    uint16_t    export_id,
    uint16_t    dir2_export_id,
    nfsstat3    want,
    const char *desc)
{
    nfsstat3 got;

    request.export_id = export_id;
    got               = chimera_nfs3_check_rofs2(&request, dir2_export_id);

    if (got != want) {
        failures++;
        fprintf(stderr, "FAIL: %s: got %d, expected %d\n", desc, got, want);
    }
} /* expect_dual */

/* Single-handle gate: the shape every mutating one-directory proc calls. */
static void
test_single_handle_gate(void)
{
    expect_single(RO_EXPORT_ID, NFS3ERR_ROFS,
                  "read-only export must be gated");
    expect_single(RW_EXPORT_ID, NFS3_OK,
                  "read-write export must not be gated");
    expect_single(OVERSET_EXPORT_ID, NFS3ERR_ROFS,
                  "over-set access mask (RO|RW) must fail closed as read-only");
    expect_single(0, NFS3_OK,
                  "export id 0 (pseudo-root / none) is fail-open by contract");
    expect_single(UNKNOWN_EXPORT_ID, NFS3_OK,
                  "unknown export id is fail-open by contract");
} /* test_single_handle_gate */

/* Two-handle gate (RENAME/LINK): both sides must be writable, in both
 * cross-export directions.  A regression that consults req->export_id twice
 * or drops the second handle's export fails here. */
static void
test_dual_handle_gate(void)
{
    expect_dual(RW_EXPORT_ID, RO_EXPORT_ID, NFS3ERR_ROFS,
                "writable current export into read-only second export");
    expect_dual(RO_EXPORT_ID, RW_EXPORT_ID, NFS3ERR_ROFS,
                "read-only current export into writable second export");
    expect_dual(RO_EXPORT_ID, RO_EXPORT_ID, NFS3ERR_ROFS,
                "both exports read-only");
    expect_dual(RW_EXPORT_ID, RW_EXPORT_ID, NFS3_OK,
                "both exports writable must not be gated");
    expect_dual(RW_EXPORT_ID, UNKNOWN_EXPORT_ID, NFS3_OK,
                "unknown second export id is fail-open by contract");
} /* test_dual_handle_gate */

int
main(void)
{
    shared.exports_by_id[RO_EXPORT_ID]      = &export_ro;
    shared.exports_by_id[RW_EXPORT_ID]      = &export_rw;
    shared.exports_by_id[OVERSET_EXPORT_ID] = &export_overset;
    thread.shared                           = &shared;
    request.thread                          = &thread;

    test_single_handle_gate();
    test_dual_handle_gate();

    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }

    return 0;
} /* main */
