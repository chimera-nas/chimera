# Persisted SMB CLOSE in native compounds

Wave16 validated. Combined Debug+ASan build and all 51 selected CTest entries
pass; see [the current checkpoint](smb-compound-current-status.md) for scope and logs.

Clean persistent CLOSE now shares a VFS compound with preceding commands. It
remains terminal so a suffix sees the accepted FileId, durable-registry and cache
membership transitions. Pending notifications, legacy lock entries and complex
delete-on-close still retain their lifecycle boundaries.

The close group performs optional POSTQUERY, private claim retirement, exact
retained-handle binding, typed DELETE_KEY_AT and typed CLOSE. A cancellation scope
starts when claim retirement succeeds and ends after CLOSE. Deletion cannot run
before retirement admission, which can fail; once retirement succeeds,
cancellation cannot skip record deletion or retained-resource cleanup. The
backend record is addressed through the same handle/FH route used by persistent
CREATE, with the persistent FileId key copied into the compound during build.

The operation callback only records its result privately. FileId unhashing,
durable live/parked owner transfer, membership removal and diagnostics happen
after accepted finish, using the existing bucket-to-registry lock ordering.
No allocations were added to accepted publication. The ordinary stream identity
validation and paired base/stream claim retirement apply to persisted streams.
EINTR now produces STATUS_CANCELLED for native CLOSE.

## Deliberate failure-policy limit

This preserves the legacy best-effort deletion policy. Missing keys succeed;
other delete failures are recorded, reported in the accepted diagnostic and
normalized so logical CLOSE completes. This is now an awaited typed operation
within the compound instead of a fire-and-forget operation outside its lifetime.
It does **not** guarantee removal of a record after a backend failure. A repair
queue or durable tombstone protocol remains necessary to prevent recovery from a
stale record after such a failure. Failed-CREATE cleanup has the related gap.

Backend rollback still does not exist. None of the new fixtures rejects finish
after deleting a real record. Existing retry fixtures cover nonpersistent
resource-only CLOSE.

## Regression coverage added

The cache-close wire probe adds eleven persisted CLOSE fixtures on an actual
continuously available share, in a separate server environment from every
nonpersistent finish-rejection fixture:

- Uncached regular files, regular leases with and without NON_DIRECTORY, BATCH
  oplocks, and existing-directory leases.
- Deterministic disconnect ownership transfer to the parked registry during held
  finish, checking unique owner and tree-pin drain after acceptance.
- Cancellation from the private completion of claim retirement and separately
  after DELETE_KEY_AT, checking that deletion and exact CLOSE still drain and the
  accepted logical close returns STATUS_CANCELLED.
- An already removed key and an injected delete EIO. The error fixture verifies
  that the real key remains, then removes it as fixture cleanup.
- A real transport disconnect and warm persistent reconnect of an existing
  directory, followed by native CLOSE.

The inspector checks the live FileId, unretired public ACCESS owners, persistent
registry identity and cache membership while finish is held. A separate bounded
SEARCH_KEYS_AT reads the actual key store to verify record absence (or the
explicit failure residual). Wire checks include retained-prefix replies, terminal
group counts, stale reconnect rejection, closed-FileId rejection and deny-all
reopen.

Initial integration exposed a separate existing CREATE correctness gap rather
than missing CLOSE record deletion: fresh directory creation and stream CREATE
can advertise a persistent durable grant without writing its recovery record.
The grant decision in smb_proc_create.c:chimera_smb_create_grant_durable uses the
wire request and CA share; chimera_smb_create_mkdir_callback bypasses the
OPEN_AT_HS persistence route, and chimera_smb_create_issue_open explicitly
excludes has_stream from persist_prepare. Registration sets both PERSISTED and
entry->persistent only if persist_pid was set. A later persistent reconnect is
then rejected by chimera_smb_durable_claim because entry->persistent is false.
The fixture therefore precreates a directory and requests persistent FILE_OPEN,
which does write a record. Actual-PERSISTED and actual-key assertions remain
strict. Synthetic recovery records were not used to claim stream coverage.
The CLOSE implementation supports an already persisted stream identity, but
production stream persistence remains an unverified and currently broken CREATE
prerequisite.

## Remaining notification boundary

Notification cleanup cannot safely be moved wholesale into a retryable callback:
the existing cleanup sends responses and drains thread-affine ready queues, and
watch installation lacks a shared admission/retirement gate. The construction-time
absence check also does not protect a native CLOSE from a watch installed later
while its finish is held. That existing race requires explicit watch lifetime and
accepted cleanup ownership, including requests parked on another worker.

The existing DOC fence can support a narrower native admission prerequisite:
CHANGE_NOTIFY can enter through doc_mutation_begin and publish a newly created
watch while holding the registry lock, after synchronized closed-FileId
revalidation. A conflicting CLOSE fence must defer that request on its owning
worker. CLOSE must also recheck watch absence after acquiring its fence, because
installation can win between construction-time eligibility and fence admission;
it must defer the untouched command to legacy if a watch appeared. Watch creation,
update and queue parking need an explicit bucket/registry/watch lock-order audit.
This would close the native late-install window without claiming that existing
cross-worker cleanup or legacy CLOSE installation races are solved.
