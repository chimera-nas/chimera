# NFSv4 third compound implementation pass

See the [fourth pass](nfs4-compound-fourth-pass.md) for newer OPEN/CLOSE behavior.

Implementation on the uncommitted `compounds-refinement` tree, 2026-09-22.
This follows the [second-pass review and implementation](nfs4-compound-adherence-followup.md).

## Added operations

READ_PLUS now encodes a type check, extent classification, and conditional DATA
read in the same VFS compound. HOLE, EOF, and zero-count answers skip the data
read. Stateid authorization runs against the execution cursor. Multiple results
have independent storage, and DATA is marshalled only after accepted finish.
The classifier opens read-only, preserving access on read-only objects.

COPY and CLONE now encode both endpoint checks, source-size lookup, and transfer
in one compound. The source is the saved filehandle and the destination is the
current filehandle. Both stateids are checked against those objects and their
access modes at execution time. COPY rejects same-file requests, range overflow,
and source ranges beyond EOF before transfer. Zero counts resolve from source
size. Anonymous COPY obtains owned source/destination handles, with explicit
read/write access, inside the compound. CLONE retains its native capability and
same-module requirement.

Source inspection temporarily selects the saved object, then restores the
destination without changing the wire saved slot or private current-stateid.
The failure path also preserves the protocol's destination filehandle. Stateid
handle references and implicit-open results remain owned until reset/cleanup;
retry rebinds endpoints from the new attempt. Multiple transfers with SAVEFH,
PUTFH, and RESTOREFH can remain in the same run.

ACL inputs to SETATTR, CREATE, and OPEN now have immutable per-operation storage.
VFS makes a writable copy per attempt, so backend changes to input attributes
cannot contaminate retry. ACL GETATTR stages its result during execution and
checks the actual ACL snapshot's conservative encoded-size bound against the
reply arena and reservations for other results. RESOURCE therefore stops later
mutations. Accepted completion publishes the staged response; retry discards it.
This removes the ACL-specific conversion guards while preserving the existing
OPEN and delegation constraints.

## Supporting fixes and review findings

The generic COPY fallback now accepts separate source/destination claim actors
and preserves them through its internal read/write calls. The existing public
API remains a wrapper with no actors. Different modules use streaming fallback
instead of sending an alien source handle to a native backend copy operation.
Inline callbacks advance through a trampoline, and zero/short writes return EIO.
Callback-scoped pre/post ACLs are copied until final completion.

Wire tests exposed that OPEN_CURRENT owns only the cursor, so its result cannot
retain an anonymous COPY endpoint after cursor movement. The builder now uses
the resource-producing OPEN operation. Independent review caught the missing
write-access flag on anonymous destinations and the failure-path cursor issue;
both are fixed.

The VFS range adders permit deferred endpoint binding and reject missing handles
at execution. A read-only accessor exposes the saved execution filehandle to
synchronous frontend callouts.

## Remaining boundaries

This is still supported-run conversion. COPY/CLONE require a saved filehandle
established within the run; cross-export credentials and inherited saved-slot
seeding are not generalized. They do not yet extend the reserved OPEN suffix.
Special-stateid CLONE, delegation/layout stateids, and identified NFS-proxy COPY
retain legacy dispatch. The proxy consumes write buffers and is deliberately
excluded from generic VFS streaming fallback pending ownership reconciliation.

READ_PLUS remains subject to reply bounds and existing delegation/data-server
guards. Large ACLs may conservatively return RESOURCE because identity-name
encoding reserves worst-case space. Malformed attribute input can still return
to the unsubmitted legacy handler for exact protocol error reporting.

Multiple/coalesced/exclusive/v4.0 OPEN, CLOSE, OPEN_DOWNGRADE, lock state changes,
namespace/export coordination, and asynchronous delegation/pNFS work still need
broader protocol-state reservations. Backend transactions and actual rollback
remain deferred. Synthetic finish rejection demonstrates frontend lifetime and
publication behavior, not backend atomicity.

## Validation

Debug+ASan build passed. The wire harness now exercises 18 v4.1 and 42 v4.2
requests, requiring exactly one tagged VFS submission for each. All 60 passed.
Cases include independent READ_PLUS DATA/HOLE/EOF results, anonymous COPY,
copy-to-EOF, same-file/out-of-range rejection, mismatched endpoint stateids,
multiple transfers with saved/current cursor changes, clone copy-on-write,
distinct ACL input/output snapshots, and reply-budget failure before WRITE.

The broad run selected 319 CTest entries. All NFSv4.0/4.1/4.2 protocol suites,
SDK, FUSE, S3, NFS units, and compound/ACL/claim tests passed. Two Linux/io_uring
NFS3 model probes skipped for missing `name_to_handle_at` scratch support.
The new COPY stress fixture's stack-address assertion required a correction
for ASan's fake stack; its focused final result is recorded below.

The corrected fixture measures the actual stack frame and passed its final
focused ASan rerun (1/1). No production change followed the broad run. Across
the broad run and that rerun, 317 selected tests passed and two were skipped.
Focused log: `/tmp/chimera-compounds-copy-final.log`. Formatting, diff, Python,
and shell syntax checks passed.

New VFS tests exercise separate COPY endpoint identities across asynchronous
completion, zero/short-write rejection, cross-module fallback, 256 synchronous
chunks (64 MiB with bounded buffers), and callback-scoped ACL lifetime. The
compound test mutates an attempt's writable input ACL, injects finish EAGAIN,
and checks that the next attempt sees the pristine ACL with one publication.

Build log: `/tmp/chimera-compounds-third-build.log`.
Broad regression log: `/tmp/chimera-compounds-third-final-tests.log`.
KVM/backend transaction rollback coverage remains unavailable as previously
documented. All changes remain uncommitted.
