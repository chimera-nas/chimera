<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# SMB metadata compound consolidation, October 6

## Changes

All five object-ID FSCTLs now use one replayable builder, both in the shared
SMB batch and in standalone dispatch. Current-ID discovery, the share-root
index lookup, opening an indexed file to check uniqueness, storing/removing
the ID and updating its index execute within the same command group.
Temporary VFS opens belong to the compound and are released on retry/free.
The existing best-effort index and birth-ID repair policy applies to operation
errors only; rejected finish is never converted into success.

This fixes a rebase regression: shared CREATE_OR_GET_OBJECT_ID still fabricated
an ID even when SET_OBJECT_ID had persisted a different value. The shared path
now also preserves main's short-output-buffer status and access checks.

Stored generic reparse SET and DELETE now share a builder across native and
standalone dispatch. Attributes, directory emptiness, existing tag/GUID checks,
xattr mutation and DOS attributes stay in one compound. Generic tags are
handled before the special-file discriminator, fixing dependence on stale
rp_nfs_type in recycled request storage. GET_REPARSE's standalone callback
chains and duplicate symlink encoder were removed in favor of the shared GET
builder; GETATTR and READLINK/GETXATTR no longer require separate submissions.
Node responses now use the same buffer sizing as the former standalone path.
Identity-replacing symlink/device SET_REPARSE retains its separate admission
and publication machinery.

Fallback READ appends an optional sticky-atime restore inside its READ compound
and shares the restore builder with native READ. The terminal callback takes
buffers from the READ's recorded index rather than the last operation, and
publishes position only after accepted finish. Best-effort restore failure
cannot hide compound finish rejection.

Fallback CLOSE combines optional allocation trimming and post-query attributes
in one compound. It continues retirement/DOC cleanup even after an exhausted
metadata finish, but returns that failure instead of silently reporting success.
Its existing early open retirement and subsequent DOC/stream cleanup remain
lifecycle boundaries; this does not make fallback CLOSE wholly transactional.

Named-stream rename now opens its base and renames within one compound. It no
longer transfers a temporary base handle through an intermediate completion;
notifications and the open's stream-name update still wait for accepted finish.

## Validation

Quick wire probes cover a seven-command object-ID sequence (SET, GET, extended
update, CREATE_OR_GET, DELETE, reassignment and GET), duplicate detection through
a dynamically opened indexed file, and read-only finish rejection/retry.
Stored reparse tests cover SET/GET/query and DELETE/query batches, tag/GUID
validation, standalone dispatch, and rejected mutation retry/exhaustion.
Mutating tests stop before filesystem effects when rejecting an attempt; they
do not assume memfs has rollback.

Standalone I/O probes check one submission for READ plus atime restoration,
restored timestamps, successful data replies when restoration fails, retry
exhaustion, and CLOSE trim/post-query ordering and rejected finish reporting.

The stream probe also verifies a single submission containing base resolution
and rename, with two rejected pre-mutation attempts before accepted rename.

Debug and Release builds pass. The full `make -k check CTEST_PARALLEL=8`
sweeps each pass 286/289 quick tests; only the three established remote pNFS
variants fail (the existing status/fileid/zero-FSSTAT mismatch families).
After the last stream-rename change, final builds and all **81/81 SMB-labeled
quick tests** pass again in each configuration. Formatting, VFS SDK/northside
API guards, REUSE, copyright and whitespace checks pass. An initial formatter
idempotence issue in two multi-field initializers was fixed and rechecked.

Extended smbtorture compound/compound_async/compound_find pass 3/3 on memfs;
the Linux variants reproduce the same seven DOC/cleanup failure signatures.
The broader IOCTL suite fails on memfs and Linux; the named-stream IOCTL suite
passes. An isolated library built from all 54 SMB objects at pre-pass commit
`1e1bde7ab35735a6dc802808c6dc79f8a2069804` reproduces exactly the same 10 IOCTL
failure signatures / 12 occurrences. The new persisted object-ID assertion
fails against that library and passes against the final implementation.

These additional IOCTL failures predate this pass, but have not been compared
against main to determine whether they are regressions within the overall PR:

* Both backends reject duplicate-extents operations with source/destination
  byte-range locks where smbtorture expects success.
* Linux has eight further failures: sparse destination copying, copy beyond
  source EOF, zero-length copy, duplicate-extents beyond source/destination
  EOF or with zero length, sparse range reporting, and invalid hole punching.

Clang Debug/Release compilation retains exactly the baseline **40 warning
signatures / 112 occurrences**, with no additions or increases. The analysis
targets remain red on these existing findings.
Native Windows, physical RDMA and the complete extended tier were not run.

Production code is 640 lines smaller in this pass. Logs are under
`/tmp/chimera-smb-metadata-*`; the isolated pre-pass library is under
`/tmp/chimera-smb-metadata-baseline/`.

## Remaining SMB boundaries

* Linux passthrough DOC/cleanup is still a confirmed regression. Matched-FH
  removal requires an atomic backend identity check; Linux currently advertises
  no such capability and implements plain unlinkat. Replacing it with an
  unchecked unlink or check-then-unlink would lose the replacement-file safety
  already enforced elsewhere. A backend/namespace design is still required.
* Durable reconnect/recovery, AppInstance replacement, remaining cache admission,
  and cache/durable/DOC batch boundaries need provisional lifecycle state.
* Fallback CREATE's generic live CLAIM remains a finish-retry barrier.
* Identity-replacing SET_REPARSE needs an ordered identity overlay for following
  commands and safe migration of additional open state.
* Directory rename retains contained-open, cross-share and replacement-target
  boundaries. Named-stream rename is now a single standalone compound, but
  needs a provisional stream-name overlay before it can join subsequent commands.
* Fallback close/teardown still separates open retirement, stream deletion and
  final DOC removal. This pass only consolidates its metadata work.
* General command-scoped cancellation inside arbitrary multi-command batches
  remains incomplete.
* Actual backend compound transactions, rollback and cross-request atomicity
  are deferred. In particular object-ID uniqueness/index consistency retains
  the existing concurrency and best-effort guarantees until backend transaction
  support is supplied.
