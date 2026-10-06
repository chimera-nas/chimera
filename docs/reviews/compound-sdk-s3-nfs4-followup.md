# SDK/S3 retry and NFSv4 adoption follow-up

2026-09-23, uncommitted `compounds-refinement` worktree. Two agents implemented
source-only changes; the primary agent owns the combined build, runtime tests,
integration fixes, and final completion assessment. This supersedes the universal
SDK/S3 retry and proxy-COPY gaps in the earlier conversion assessments.

## Changes

- All 56 production SDK/POSIX/S3 compound submission sites (31 SDK, 3 direct
  POSIX, 22 S3) use a common bounded finish-retry adapter. Only finish EAGAIN
  replays the attempt, up to eight retries. Ordinary operation EAGAIN retains
  successful-prefix semantics and reaches the terminal completion without replay.
  The terminal frontend callback runs once. Already accepted streaming chunks
  are never replayed by this adapter. Allocation failure falls back to ordinary
  completion with the actual finish result.
- Multipart temporary-name bookkeeping stays private until finish accepts the
  attempt. Resumed CopyObject and CompleteMultipart work is appended during
  execution, after reset, preserving publication flags and cursors for empty
  tails. S3 exhausted finish rejection maps to InternalError rather than an
  ordinary lookup handler's NoSuchKey fallback.
- The VFS retry API returns whether it initiated a retry, avoiding silent stalls
  when retry is unavailable. Empty submitted compounds have a snapshot marker;
  construction errors survive retry. NFSv3/NFSv4 callers honor the return value.
- NFSv4 COPY through an NFS proxy remains inside the compound. The obsolete
  frontend and VFS fallback exclusions were removed after checking that both
  NFS WRITE backends clone borrowed payload references before marshalling.
- Ordinary LOOKUP and SECINFO names remain compound-native when `/` is exported.
  Possible export-junction names still fall back. Execution-time policy checks
  repeat on retries; a newly introduced junction yields NFS4ERR_DELAY so a fresh
  request can use the namespace path.

## Integration findings

The first combined ASan build passed. The first focused run exposed a real
UploadPartCopy bug on cairn: its named-file fallback computed an attempt-private
name length but passed the unchanged request length to OPEN. The operand now
uses the attempt-private length. Accepted ordinary UploadPart failures also
retain the name needed for scratch cleanup. Rejected attempts do not publish it.

The new proxy tests initially collided on upstream mount/portmap services; the
export-update fixture also blocked an event thread needed to service REST. Proxy
upstreams now use a separate connected network namespace; the finish gate uses
an event-loop timer, allowing REST to progress. The new junction helper uses a
locked root-export snapshot. These fixture failures were separate from the
production conversion.

The broad matrix exposed two obsolete model profiles: NFSv3/NFSv4 POSIX
traces pinned COPY support off because the old VFS fallback refused proxy
handles. Both profiles now enable COPY, their Python capability expectations
match, and the obsolete ND8 unsupported-COPY deviation was removed from the C
replayer. The primary agent regenerated the 106 affected traces from their
original seeds, including both model self-tests, to validate data and state
against the newly supported behavior rather than suppress mismatches.

## Validation

The combined Debug/AddressSanitizer build passes. Final results across **554
distinct CTest selections: 538 passed, 16 skipped, zero unresolved failures**.
The 20 focused regressions all passed. The 534-case broad matrix initially
passed 516, skipped 16, and failed only the two stale-profile POSIX batches;
both passed after profile correction and trace regeneration (53 traces each).
The final rebuild changed only the model replayer; production binaries were
unchanged after the passing focused/broad runs.

Twelve skips require a scratch filesystem supporting name_to_handle_at; four
fchownat variants skip non-root credentials. Existing model deviation/capability
classifications remain, except the retired ND8 COPY exception. A passing batch
does not imply exhaustive protocol conformance.

Coverage includes transient/exhausted SDK reads and directory enumeration,
accepted-only application buffers/callbacks, vectored-write payload retention,
ordinary operation EAGAIN without retry, S3 transfers/metadata/multipart with
finish injection, six NFSv4 adoption cases, empty VFS retry/construction errors,
NFS3 compounds, existing NFSv4 delegation/pNFS tests, and the broader client,
POSIX, NFS, S3, FUSE and VFS selections.

Evidence:

- `/tmp/chimera-next-build2.log` (final production build)
- `/tmp/chimera-next-build3.log` (model replayer rebuild)
- `/tmp/chimera-next-focused2.log` and `chimera-next-focused-LastTest.log`
- `/tmp/chimera-next-matrix1.log` and `chimera-next-matrix-LastTest.log`
- `/tmp/chimera-next-posix-gen.log` (two self-tests and 106 generated traces)
- `/tmp/chimera-next-model-rerun1.log` and `chimera-next-model-LastTest.log`
- `/tmp/chimera-next-dispatch-audit.json` (source inventory)

`git diff --check` passes in the parent tree and specs submodule. Nothing was
committed or merged.

## Completion assessment and remaining work

Both agents completed their selected scope after integration corrections.
SDK/S3 ordinary filesystem routing and the universal retry policy are complete;
the NFSv4 assignment was a bounded next batch, not a claim that all remaining
boundaries were removed. Root independently audited submission sites, shared
retry behavior, borrowed proxy payload ownership, and the remaining adapter
guards, then validated the combined tree.

Source inventory confirms no raw compound submissions or ordinary direct
filesystem dispatches remain in SDK/POSIX/S3. Administration, mount/mkfs/rmfs,
handle lifetime, and POSIX lock/claim state remain separate. SDK read_into stages
private data and copies accepted results; READDIR returns bounded pages of up to
512 entries and invokes application callbacks only after acceptance.

S3 large transfers intentionally use multiple accepted compounds. Earlier chunks
and streamed HTTP bytes cannot be undone by retrying a later chunk. Private
scratch assembly preserves final publication and multipart inputs; it does not
make an entire multi-compound request atomic. Multipart tables and bucket maps
remain frontend state published after accepted completion.

NFSv4 still has the following conversion work, in suggested priority order:

1. Resolve noncrossing namespace cases during execution. LOOKUPP still stops
   after a prior cursor move and when `/` is exported. Names matching exports
   still fall back even below the export root. Actual junctions and cross-export
   PUTFH/RESTOREFH require operation-specific export and credential transitions.
2. Integrate v4.0 delegation-enabled OPEN and CLAIM_DELEGATE variants with grant
   publication and the owned open-owner replay response, before releasing frozen
   journals.
3. Add typed pNFS layout operations and staged publication for LAYOUTGET/COMMIT;
   coordinate pNFS-enabled REMOVE with accepted data-server cleanup. DELEGRETURN
   and LAYOUTRETURN still run separately; recall-return progress must remain
   independent of the operation waiting for it.
4. Convert synthetic attribute-directory/OPENATTR and named-stream operations.
5. Express invalid names, attribute masks, cookies, range geometry and selected
   read-only/grace gates as pure failing checkpoints instead of splitting runs.
   Some post-CLOSE anonymous size/range operations and non-READ/WRITE stateid
   classes still have conservative boundaries. Capacity/reservation/reply-budget
   limits remain legitimate bounded splits.

Removing ordinary root-export lookup and proxy-COPY exclusions does not make
every wire compound one VFS compound. Export rechecks avoid stale build/retry
policy; they do not provide a configuration snapshot spanning asynchronous
execution. Stronger configuration isolation needs generations or reservations;
holding an export mutex across asynchronous I/O is not the solution.

Backend transaction begin/end hooks, per-operation transaction association,
rollback, and deferred VFS cache/notification publication remain future work.
Fault injectors reject read-only work or veto before the first mutation; their
passing results cannot establish rollback after an executed mutation.
