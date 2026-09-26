# NFSv4 sixth compound pass: coalesced OPEN

The user requested coalesced OPEN support. This pass extends ordinary,
nondelegated, nonexclusive NFSv4.1/v4.2 OPEN to existing owners and repeated
opens of the same file. Changes remain uncommitted.

Follow-up: the [seventh pass](nfs4-compound-seventh-pass.md) adds ordinary
v4.1/v4.2 OPEN_DOWNGRADE and corrects CURRENT substitution after RESTOREFH.
The limits and validation below describe the sixth-pass snapshot.

## Implementation

Before execution, the adapter reserves each OPEN owner once, freezes its existing
states and preallocates candidate state slots. Competing state mutations cannot
enter those reserved states. Resources and serialization are retained across
attempts; execution callbacks update only the attempt-private journal.

Each OPEN checks requested permissions and share conflicts, stages the union of
access/deny modes and the correlated OPEN_DOWNGRADE history, admits a separate
VFS share claim, obtains a suitable data handle, and performs any conditional
truncate. A final checkpoint advances the private state only after all these
steps succeed. Later OPENs use the journal's latest state. Each reply retains its
own stateid sequence number; coalescing preserves stateid identity. A failed
later OPEN leaves the successful prefix intact. Explicit older stateid versions
remain older versions, and sequence-zero references resolve the current state.
The seventh pass corrects saved-CURRENT handling: I/O substitutes sequence zero;
CLOSE and OPEN_DOWNGRADE preserve the saved version.

CLOSE updates this journal too. Closing and reopening the same owner/file creates
a fresh identity. Closed public and provisional claims are excluded only within
this compound's execution view. At accepted finish, the adapter publishes each
state's final accepted version, closes old states before installing replacements,
and transfers the admitted claim without a release/reacquire admission gap.
Unused candidates and intermediate handles/claims are released during disposal.
Rejected finish retries rebuild the journal from the frozen public states.

The VFS provides an atomic claim move/replace helper and optional inherited
OPEN grant sources. A union handle can retain previously granted READ permission
while adding newly authorized WRITE permission, or vice versa. Both source
filehandles must match the target; only permissions bound to an existing OPEN
are inherited. The frontend does not mutate shared handle grants in a callout.

## Remaining limits

- NFSv4.0 replay, exclusive/delegated OPEN, OPEN_DOWNGRADE, LOCK/LOCKU and
  child-lock CLOSE still retain legacy boundaries.
- Owner reservation conservatively falls back for active borrowers, child locks,
  named-stream states, competing reservations, and owners exceeding the bounded
  state journal. This is admission before execution, not a forced end at every
  coalescing OPEN.
- Cross-export credentials, namespace junctions, capacity/reply limits,
  asynchronous delegation/pNFS and the previously documented anonymous range
  admission boundaries remain.
- Publication has no recoverable/retryable failure, but fresh-state hash insertion
  still uses uthash allocation with process-fatal OOM behavior, as the previous
  fresh OPEN path did. It is not allocation-free; eliminating this limit needs
  reserved hash storage and growth, including CLOSE/reopen table recreation.
- An NFS proxy backend may recheck permissions on a remote union OPEN; backend
  compound/transaction support is still deferred. Synthetic finish rejection
  exercises read-only filesystem work and does not demonstrate backend rollback.

## Validation

The final Debug+ASan build passed without compiler warnings. All four wire
variants passed: 64 v4.1 and 93 v4.2 cases, with and without finish rejection.
Sixteen new cases cover existing/fresh same-owner repeated OPENs, access upgrades
in both directions, stable identity and sequence progression, same-owner multiple
files, correlated downgrade history, saved/explicit old versions, self/share
conflicts, successful-prefix preservation and same-owner CLOSE/reopen. Measured
coalesced sequences require one VFS submission. The retry harness requires the
specific two-coalesced-OPEN case to reject exactly once and then accept, using a
unique fixture identity independent of asynchronous logging or address reuse.

Focused claim tests passed 175 assertions, including 18 union-claim cases.
Compound tests cover inherited permissions after an ACL change, both grant
sources, wrong-file rejection for either source, ignoring unbound permissions,
and accepted retry. State/lifetime tests passed 2/2, including reservation abort,
competing mutations, stable identity, replacement publication and CLOSE/reopen.
Independent agent reviews checked adapter resource ownership, successful-prefix
publication, retry teardown, cursor/handle selection and state-mutation exclusion.

The final broad selection ran 326 CTest entries in 26.73 seconds: 324 passed,
two skipped, no failures. It includes all three pynfs suites, 224 SDK tests,
eight FUSE tests, 57 boto3 S3 tests, Ceph/model tests, NFS/VFS units and five SMB
compatibility entries. The same two Linux/io_uring NFS3 model probes skipped
because the scratch filesystem lacks name_to_handle_at support. Python/shell
syntax and git diff whitespace checks passed. No production edits followed the
broad run. KVM tests and real backend transaction rollback were not run.

Logs:
- `/tmp/chimera-compounds-sixth-final-build.log`
- `/tmp/chimera-compounds-sixth-final-boundaries.log`
- `/tmp/chimera-compounds-sixth-final-tests.log`
- `/tmp/chimera-compounds-owner-journal-tests.log`
- `/tmp/chimera-coalesced-claim-tests.log`
- `/tmp/chimera-open-grant-tests.log`
