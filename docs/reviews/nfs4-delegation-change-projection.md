# Delegation CHANGE projection and payload ownership

The twelfth refinement pass fixes a reproduced delegated GETATTR sequence in
which a repeated client change report equaled the last synthesized server value.
That equality previously cleared the effective dirty state, exposing older
backend CHANGE and SIZE values. The shared legacy/compound combine helper now
retains a sticky dirty flag through delegation return. Each successful dirty
query advances CHANGE and uses the holder's SIZE. Failed or incomplete callback
results return DELAY while that foreign write holder remains. Compound execution
also returns DELAY if the queried holder disappears: its backend snapshot was
taken before the callback and cannot safely represent a concurrent final flush.
The legacy continuation refetches that snapshot instead.

The combine journal is private until accepted completion. Retry resets its
attempt state while retaining the already captured callback result, so a finish
retry neither repeats CB_GETATTR nor publishes an extra version. Marshalling
failure does not publish the pending combine journal.

A second measured regression occurred after DELEGRETURN: a client had observed
synthesized CHANGE 10, then a normal backend GETATTR exposed CHANGE 4. The
`nfs4_change` helper retains a per-file projection for files granted write
delegations. It records the raw backend baseline and the last accepted visible
value. Subsequent backend increments advance the visible value by the same
delta, so a retained floor does not conceal normal writes. GETATTR, VERIFY and
READDIR use the same projection. Projection always sets the explicit VFS CHANGE
attribute and does not rewrite the backend ctime to encode a version.

Projection observations belong to the request attempt. Rejected attempts discard
them; accepted completion publishes them. Backend counter reset or wrap first
returns DELAY and publishes a new baseline on accepted completion; the next
request can use it. Arithmetic overflow returns RESOURCE instead of silently
clamping CHANGE and hiding later edits. The table retains at most 65,536 file
identities, with no eviction that could regress a previously exposed value.
Once full, the server declines optional write delegations on new files. Existing
tracked files continue to work.

This projection lasts for the server process. Preserving synthesized attributes
through server restart requires backend metadata or a persistent recovery
record; this frontend change does not add either. It also does not persist the
synthetic time_modify/time_metadata values produced during dirty callbacks.
The retained projection specifically guarantees CHANGE continuity during the
running server's lifetime.

Focused tests cover identical reports, same-compound queries, callback failure,
holder return during a pending callback, discarded attempts, backend increments,
counter reset, overflow and capacity. Wire tests additionally check post-return
GETATTR continuity, WRITE progression, and matching VERIFY/READDIR values. The
normal and finish-retry delegation suites each contain 24 measured cases and
passed with the delegation unit test on the integrated build.

The same pass fixes a separate proxy WRITE ownership defect. NFS3/NFS4 generated
XDR encoders consume payload descriptors, while the frontend must retain its
input for retry. Proxy writes now clone the descriptors before marshalling and
release only unconsumed clones afterward. A focused test invokes the actual
NFS3 and NFS4 marshallers with local/global, full, partial, empty and repeated
payloads; it passed under ASAN. The parent integration also owns the companion
compound READ descriptor-lifetime correction.

The pNFS model corpus subsequently exposed a proxy SETATTR result-contract gap:
the backend performed the mutation but supplied no requested post attributes,
so LAYOUTCOMMIT reported a new size of zero. Proxy SETATTR now brackets the
mutation with requested GETATTR operations in the same remote compound.
Session SETATTR uses the current version of a retained OPEN identity; an
already-authorized size change through a read-only upstream OPEN uses the
credential-authorized anonymous stateid. LAYOUTCOMMIT validates the actual
layout identity with an acquired reference, reports whether size really
changed, and honors mtime-only requests. Its focused wire fixture checks
identity rejection, extension size, no shrink and timestamp-only updates.
That validation exposed two older state-table gaps: layout IDs were encoded
with a client identifier in the server-epoch field, and generic layout acquire
did not increment the reference later dropped by release. Layout creation,
version updates and callback recall now consistently encode the server epoch;
unit and wire fixtures exercise table lookup, reference accounting and stable
identity across an upgrade. The integrated normal/retry pNFS feature suites,
delegation/layout unit test and proxy payload unit passed together (4/4).

LAYOUTCOMMIT still decides whether to extend using GETATTR followed by SETATTR.
A concurrent extender can race that decision. The layout recall barrier blocks
new layout grants; it does not serialize all data writes or make size updates
an atomic maximum. Eliminating this race requires a backend atomic extension
operation or equivalent serialization shared by every writer, beyond the
frontend corrections here.

The remaining pNFS OPEN/truncate counterexample exposed an upstream CLOSE
version race. An already transmitted OPEN can coalesce an identity after the
last local handle captures the CLOSE stateid. Exact-version CLOSE then fails
OLD_STATEID, while local bookkeeping retires the entry; later OPEN replies
upgrade an untracked state and trigger repeated reopen attempts. Session CLOSE
now transmits sequence zero for the fixed identity, selecting its current
version. Combined with the proxy's safe-prefix DELAY retry, this eliminates the
observed failure. The original 80-operation trace passed, followed by all 18
applicable pNFS traces on each of memfs, diskfs and cairn (4,320 checked compounds,
zero divergences and zero logged CLOSE failures). Each backend also skipped six
traces requiring unsupported proxy extended-attribute operations.

The final identity audit also found that LAYOUTRETURN and LAYOUTGET used only
the caller's filehandle and layout sequence number: fabricated `other` bytes
could affect a real grant. Both now compare the complete identity. FILE return
pins the client, takes a layout reference and validates/destroys under the
client mutex. Grant/widen publication uses the same mutex, rechecks identity
after asynchronous backend work, and remains inside the existing layout-grant
barrier. Initial OPEN/LOCK inputs validate client, filehandle, principal and
version; their existing permissive share-mode policy remains unchanged.
Missing FILE layouts preserve idempotent return success. Forged GET/RETURN wire
tests use TEST_STATEID to prove that the real grant survives unchanged, followed
by a successful valid return. Normal/retry feature runs and all three pNFS
corpora passed again after these changes.

Primary protocol reference: [RFC 8881, section 10.4.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-10.4.3).
