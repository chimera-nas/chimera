<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# SMB compound refinement, October 5

## Completed in this pass

All production SMB compound submissions now use the shared finish-aware retry
adapter. This includes fallback I/O, metadata, security, directory enumeration,
copy offload, sparse operations, stream deletion and CREATE. Ordinary operation
errors retain successful-prefix semantics; only rejected finish EAGAIN triggers
replay. Streaming enumeration resets private output before replay.

Fallback WRITE previously reported its operation status even when finish
rejected the attempt. It now requires accepted finish. EA enumeration and
rename discovery likewise cannot publish successful-prefix results from a
rejected attempt. CREATE rejects failed finish before transferring handles or
entering share-conflict recovery; its early replay path clears attempt-derived
gate answers while retaining handles accepted by an earlier split run.

CREATE-time delete-on-close now uses the shared builder. Authorization and
readonly checks precede admission; cache recall precedes the DOC publication
fence. The private open and its delete intent publish only after accepted
finish. The fence is reacquired per attempt. Safe related QUERY_INFO, READ,
WRITE and FLUSH suffixes can share the CREATE compound. A CREATE with DOC
followed by CLOSE still has an acceptance boundary.
Namespace-only DOC CREATE waits for lease-break notification rather than ACK;
data and cache requests retain coherence waits. The new wire case reproduced
a 20-second wait with an unacknowledging RH holder before this correction.

Fallback rename combines destination-directory lease coordination and mutation
in one compound, removing the separate OPEN/CLAIM completion chain. The
temporary deny probe is released before coordination completes. Contained-file
recalls are explicit coordination operations whose completed answers survive
retry. Both fallback and native rename preserve the POSIX-mode exemption from
SMB directory-lease denial. The missing native exemption caused the previously
failing POSIX-over-SMB batch and strict suites.

Diskfs and cairn now implement and advertise REMOVE_MATCH_FH using complete FH
comparison inside their existing operation transactions. This enables the
ordinary SET_REPARSE identity-migration path on those backends. It does not add
backend compound transactions. Passthrough/proxy backends without an atomic
conditional unlink still cannot safely use that path.

## Boundaries still present

* Fallback CREATE still uses live generic CLAIM operations. An attempt that has
  crossed that executor barrier cannot be replayed; rejected finish fails and
  cleans up admission. Durable reconnect/recovery, AppInstance replacement,
  some cache admission and other split lifecycle cases need provisional state
  and accepted publication before their boundaries can be removed.
* SET_REPARSE remains a standalone identity-replacement compound. Combining it
  with following commands needs an ordered identity overlay. DOC, RANGE,
  cache, streams, durable/resilient state and peer opens still lack the identity
  migration journals required to support replacement safely. These cases are
  rejected before unlink, not silently handled through a handle-only fallback.
* Directory rename can still split for unbounded contained-open scans,
  cross-share path translation, and occupied or unsupported replacement
  targets. Coalescing its destination lease probe removes one such split,
  not all of them.
* Some cache/durable CLOSE work still ends a batch; standalone GETATTR/cleanup
  stages now have the retry policy but are not one transaction with all of the
  surrounding protocol lifecycle work.

There are no ordinary northside per-operation VFS calls in production SMB.
Recall/claim coordination, disposal and protocol registry lifecycle remain
separate control facilities. Zero raw compound submissions does not mean that
all lifecycle changes are replayable or that every SMB wire compound is one
VFS compound.

## Regression coverage

The new standalone wire probe forces fallback dispatch and rejects finish for
READ, QUERY_INFO, EA enumeration, directory enumeration, RENAME, WRITE, FLUSH
and CREATE. It checks bounded retry exhaustion and successful-prefix rejection.
A fake transactional WRITE reports operation success without publishing bytes,
then rejects finish, proving that operation success alone cannot produce SMB
success. Mutating memfs operations otherwise fail before dispatch during retry
injection; the tests do not assume memfs rollback.

The probe also verifies one submission with three groups for DOC CREATE,
related WRITE and READ, and retries an existing-file DOC CREATE before checking
that only the accepted CLOSE removes the name. Identity-scoped removal tests
exercise stale and current FHs on memfs, diskfs and cairn. The POSIX wire probe
also covers replacement of symlink and regular destinations by a symlink.

Final build and test results are recorded in MEMORY.md. Detailed logs and the
source snapshot before this pass are under
`/tmp/chimera-smb-remaining-20261005/`.
