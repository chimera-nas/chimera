# Compound conversion status after FUSE/POSIX locking

2026-09-23. Source audit of the uncommitted `compounds-refinement` worktree.
Updated after the REST debug endpoint conversion later on the same day.
This updates the remaining-work assessment after the SDK/S3 retry and
FUSE/POSIX locking passes. It does not claim backend transaction support.

## Overall status

| Frontend | Current routing | Remaining adoption work |
| --- | --- | --- |
| SMB | Wire compounds still sequence ordinary handlers; no VFS compound references in `src/server/smb` | Broad conversion, including preserving command-specific error continuation and related handle state |
| NFSv4 | Broad compound adapter, including coalesced ordinary OPEN and supported LOCK/LOCKU paths | Namespace/export transitions, selected state/delegation paths, pNFS, named attributes, conservative validation/resource fallbacks |
| NFSv3 filesystem procedures | All ordinary filesystem procedures compound-routed | No ordinary filesystem dispatch gap found; NLM and persistent replay storage are separate |
| NLM | Direct open/claim/pending-grant machinery | LOCK/TEST/UNLOCK/CANCEL and owner/recovery lifecycle need compound-aware design |
| S3 | Ordinary filesystem work, including multipart, compound-routed | Deliberate chunk/publication boundaries; no remaining direct ordinary filesystem call found |
| SDK/POSIX | Ordinary filesystem operations and POSIX locking compound-routed | Lock composition/projection limitations; administrative and lifetime APIs remain separate |
| FUSE | Ordinary request paths, FLUSH, and locking compound-routed | Mount bootstrap lookup; lock composition limitations; coherence control remains separate |
| REST | Debug filesystem endpoint compound-routed; management API mostly outside filesystem-operation scope | No remaining ordinary filesystem dispatch gap found |

The audit inspected source calls and dispatch guards, not just compound names.
Legacy NFSv4 handlers still contain direct calls because the adapter falls back
to them; their number is not a count of unconverted protocol operations. S3,
SDK, POSIX and FUSE helper/lifetime calls were reviewed separately from ordinary
filesystem dispatch. The original audit made no production changes. The subsequent REST conversion
and its validation are recorded below.

## SMB: largest conversion project

`src/server/smb/smb.c:1148` advances a `chimera_smb_compound` by dispatching each
request handler. That object groups wire requests and replies, not VFS execution.
There are no `chimera_vfs_compound` references in the SMB source directory.

Filesystem CREATE/open, READ/WRITE, FLUSH, CLOSE/deletion, directory and metadata
queries, SET_INFO/security, streams/reparse operations, range/copy operations,
and locking all need review/conversion. Simply wrapping each handler in a
separate VFS compound would leave the principal wire-to-VFS mapping goal unmet.

The adapter needs attempt-private related FileIds/open state, retained payloads,
and accepted publication of durable/share/lock state, notifications and external
output. SMB command error-continuation semantics need an explicit policy rather
than blindly using VFS stop-on-first-error. Session setup, negotiation, pipes,
and cancellation are not all inode transaction operations.

## NFSv4: remaining fallback categories

The supported-op switch is `src/server/nfs/nfs4_compound_vfs.c:771`; admission
and fallback logic starts at `chimera_nfs4_compound_try_vfs`. Ordinary OPEN no
longer inherently ends every compound. Supported state journals also cover
LOCK/LOCKU, CLOSE and OPEN_DOWNGRADE. Remaining gaps are specific cases:

1. **Namespace and credentials.** Pseudo-root and synthetic attribute-directory
   handles cannot seed ordinary VFS execution. Later PUTFH stays within the same
   export; inherited saved handles must match that export. LOOKUP names that
   might denote export junctions still fall back even below an export root.
   LOOKUPP falls back after cursor movement, at mount roots, and when a root
   export is configured. Ordinary root-export LOOKUP/SECINFO was already fixed.
   Noncrossing cases can be resolved by execution callouts; actual crossings
   need explicit export/security/credential transitions. Export rechecks are
   not a configuration snapshot spanning asynchronous execution.
2. **OPEN and state cases.** All v4.0 OPENs with delegations enabled still fall
   back (`:5805`). CLAIM_DELEGATE variants remain outside the adapter; admitted
   claim forms are NULL, FH and PREVIOUS. Nonjournalable v4.0 replay/reservation
   paths retain legacy boundaries. Some state operations require a live session
   object. Selected delegation/layout stateid classes and anonymous size/range
   operations after CLOSE remain conservatively excluded. These are extensions
   of existing journals/callouts, not grounds to redo ordinary OPEN conversion.
3. **pNFS.** LAYOUTGET still directly opens and obtains layout data
   (`nfs4_pnfs.c:859`, `:1005`). LAYOUTCOMMIT directly opens, reads attributes
   and conditionally sets them (`:1220`, `:1257`, `:1337`). REMOVE falls back
   whenever pNFS is enabled; MDS mutation and DS cleanup need accepted ordering.
   Typed layout operations and staged layout-state publication remain needed.
4. **Named attributes/streams.** OPENATTR and synthetic attribute-directory
   operations remain legacy paths (`nfs4_proc_openattr.c:82`, `:124`). This is
   distinct from the ordinary xattr opcodes already supported by the adapter.
5. **State-return/control boundaries.** DELEGRETURN, LAYOUTRETURN, FREE_STATEID
   and RELEASE_LOCKOWNER are not in the adapter's supported-op switch. These
   need a publication/lifetime design, not a mechanical filesystem wrapper.
   Returns must remain able to unblock a different operation waiting on recall.
6. **Avoidable validation splits.** Invalid names, masks, cookies, range
   geometry, and selected grace/read-only/minor-version gates still return to
   individual handlers for the error. Pure failing execution checkpoints could
   keep these cases inside the VFS compound while preserving error precedence.
   Reply headroom, operation budgets and journal reservation limits also split
   or decline runs; bounded resource limits are legitimate, although current
   estimates are conservative.

## NLM and ancillary NFS persistence

NFSv3 filesystem conversion does not cover NLM. `nfs_nlm.c:1136` and `:1354`
still open directly; test/acquire/cancel/release and pending grant/replay state
use the older claim machinery. The last pass fixed interval splitting and
downgrades, but did not convert this machinery. It should adopt the shared
typed-lock/journal approach while preserving remote grant/cancel behavior.

NFSv3/NFSv4 duplicate-request persistence, NFSv4 recovery and NSM monitor/recovery
storage still use direct put/get/delete/search-key APIs (`nfs3_drc.c`,
`nfs4_drc.c`, `nfs4_recovery.c`, `nfs_nsm.c`). These are ancillary persistence,
not ordinary inode calls. They need an explicit decision about transaction
participation and accepted ordering; the existing filesystem compound routing
alone does not atomically couple replay/recovery records with filesystem changes.

## Converted paths with intentional or incomplete composition

- S3 GET/PUT, copies and multipart assembly use bounded accepted compounds.
  Large requests can cross several compounds and final scratch publication may
  be separate. Accepted earlier chunks and streamed HTTP bytes cannot be undone
  by retrying a later chunk. Multipart inputs are preserved by the converted
  assembly; this is not the old destructive MOVE problem. Frontend multipart
  tables and bucket maps are published after accepted completion, not backend
  transaction participants.
- SDK directory callbacks run on accepted bounded pages; read_into uses private
  staging. These are converted paths, not pending direct-I/O conversions.
- FUSE/POSIX typed locking currently permits exactly one lock operation, plus
  optional cursor seeds (`vfs_compound.c:4712`). It cannot compose with ordinary
  filesystem mutations yet. Immediate backend-projected mutations reject an
  installed optimistic finish adapter before effects; mandatory owner release
  remains nonretryable cleanup. Supporting mixed transactional lock/filesystem
  compounds needs backend support and a policy for unbounded lock waits.
- Successful SEEK_END mutation switches that owner/file to backend-only lock
  arbitration until close, leaving a cross-protocol local visibility limitation.
  The NFS3 backend's GETATTR plus NLM normalization is not atomic against resize.
  These are remaining correctness/architecture limits, not absent routing.
- FUSE root lookup during mount bootstrap remains direct
  (`fuse_mount.c:76`). Coherence grants, invalidation acknowledgements/watches,
  handle references/releases and lock-domain retirement remain control/lifetime
  operations. Their existence does not make normal FUSE requests unconverted.
- REST's optional test-only `/api/v1/debug/fsop` now uses one compound per
  unlink/rename/link/chmod request, with lookup/open/setattr inside chmod. Its
  single terminal callback publishes HTTP status after the shared bounded finish
  retry adapter completes; copied inputs and intermediate handles are owned by
  the compound. No direct filesystem dispatch remains in the endpoint.
- SDK mkfs/rmfs/mount/unmount, configuration and teardown are outside the ordinary
  request conversion. Ordinary internal VFS/backend helper calls likewise are
  not frontend omissions, but will need transaction association propagation.

## Deferred transaction infrastructure

Backend begin/end/abort, association of every nested operation with its compound,
rollback and real optimistic finish conflicts remain the explicitly deferred
backend project. Existing operation completion also updates VFS caches and emits
notifications before aggregate finish (for example `vfs_proc_remove_at.c:61`).
Those publications must be deferred or otherwise made transaction-aware; backend
rollback by itself will not retract them.

Current finish fault-injection tests validate frontend retry/publication contracts
within their tested scenarios. They do not prove rollback after executed backend
mutations. Latest prior integrated ASan validation: 585 distinct CTests, 581 pass,
4 intentional non-root fchownat skips, zero failures; the nine cleanup fixtures
also passed after fixture corrections. See the locking implementation report for
logs and limitations. This audit did not rerun those tests.

## Suggested sequence

1. Design the SMB wire-to-VFS adapter, including per-command error policy and
   staged open/share/durable state. This is the largest unfinished frontend.
2. Finish NLM conversion using the shared lock machinery.
3. Continue NFSv4 with noncrossing namespace/validation cases, then delegation,
   layouts and streams as distinct substantial changes.
4. Convert the small FUSE bootstrap path if universal routing is the desired
   cleanup bar; explicitly classify ancillary persistence/lifecycle. REST is done.
5. Before claiming optimistic transactions, implement backend lifecycle and
   deferred VFS publication, then revisit mixed lock composition and prove
   conflict rollback after real mutations.


## REST conversion validation

The focused ASan regression uses the real HTTP endpoint and checks filesystem
results over NFS. It verifies all four operations through transient finish
rejection, chmod symlink following, link preserving symlink identity, unlink
preserving the symlink target, exhausted rejection, ordinary operation EAGAIN
without replay, missing paths and validation errors without compound dispatch.
The fixture vetoes mutations before execution on rejected attempts and does not
claim backend rollback. All eight REST/control-plane CTests passed, including
authentication and the model batch. The five selected pynfs DELEG16–20 cases
also passed (unlink, rename source/target, link and chmod recall).

Build/test logs: `/tmp/chimera-rest-compound-build{3,4}.log`,
`/tmp/chimera-rest-compound-suite.log`, `/tmp/chimera-rest-compound-deleg.log`;
pynfs results: `/tmp/chimera-rest-compound-deleg.xml`. ASan used
`detect_leaks=0`, consistent with the prior validation. No commit or merge.
