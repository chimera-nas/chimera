<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# SMB compound handle lifetime, October 5

## Confirmed regression and correction

Fallback FileId resolution retained the SMB open object but did not preserve its
VFS handle. Native CLOSE could unpublish the FileId, clear `open->handle`, and
release its reference while another request still borrowed that handle. Fallback
CLOSE had the same lifetime problem. A compound retry could reuse a released
handle; a multi-stage operation could build its next compound with a NULL handle.

The new signed SMB3 multichannel probe deterministically reproduces the latter:
hold the finish of an EA LISTXATTRS compound, close the FileId on a bound channel,
then resume the EA request. Before the fix, the GETXATTR compound failed instead
of returning the attributes. This is a confirmed wire regression, not only a
source-level lifetime concern.

CLOSE now separates logical retirement from physical handle release. It still
unpublishes the FileId, retires protocol claims, processes delete-on-close, and
completes without waiting for every pending request. The SMB open retains the
original VFS handle until its outstanding references and claim owners drain.
That final release occurs when the open is recycled. Fallback DOC retains the
original descriptor if borrowers remain, consuming an independent reference
for the existing cache-release path. An explicit deferred-release flag prevents
final retirement from repeating logical DOC.

Preserving the original descriptor also preserves the canonical actor identity;
there is no per-request replacement handle or address substitution. The existing
exclusive identity-rebind gate prevents SET_REPARSE from replacing the descriptor
while another request holds an open reference. Native sequence-owned references,
including typed CLOSE inputs, remain independent.

QUERY_DIRECTORY's extra cached-handle reference and request field are removed.
The general open-reference lifetime now provides that protection, including for
multi-stage COPYCHUNK and sparse scans. This change does not make the remaining
split lifecycle operations one backend transaction, or guarantee success if an
independent protocol or filesystem condition invalidates an operation.

## Coverage and remaining failures

The new quick-tier probe exercises native and fallback CLOSE against delayed EA
queries, READ, named-stream READ, and two-handle COPYCHUNK. Read-only finish can
reject once with EAGAIN, after CLOSE has completed, before retrying. COPYCHUNK
holds/retries its source-size query, closes both source and destination FileIds,
and then verifies two real chunk writes and their resulting bytes. A successful
held READ also completes across delete-on-close. New requests using a closed
FileId are rejected. Teardown verifies that backend references drain.

The existing DOC probe also had an asynchronous injection race: its global arm
could intercept a delayed disconnected request from another session. Its hook
now matches the intended SessionId and MessageId before checking group count or
injecting finish rejection. The original coalescing assertions remain intact.
Both probes passed 30 consecutive runs each in both Debug and Release after
this correction.

All 57 SMB quick tests passed in the initial Debug run. Extended smbtorture
compound, compound_async, and compound_find passed on memfs. The same three
suites fail on Linux passthrough. An isolated library rebuilt from the pre-pass
SMB sources reproduces all seven failure signatures identically: deletion and
cleanup return INTERNAL_ERROR, with subsequent OBJECT_NAME_COLLISION failures.
Detailed logging confirms ENOTSUP (95) from the fallback DOC removal compound.
That path requires a matched-FH removal which Linux does not advertise. These
remain separate confirmed SMB failures; this pass does not resolve them.

The final `make -k check` passes 279/282 quick tests in each of Debug and Release,
including all 57 SMB tests in each. Only the three established remote pNFS suites
fail, with unchanged mismatch signatures. Both Clang stages complete with the
baseline 42 warning signatures / 116 occurrences, and no new diagnostics. Syntax,
API/include guards, REUSE and copyright checks pass. The overall check remains
red on the baseline pNFS failures. Production code is 28 lines smaller in this
pass.

Detailed logs, the pre-pass source snapshot, and the isolated baseline comparison
are under
`/tmp/chimera-smb-lifetime-20261005/`.

## Remaining SMB conversion scope

* Durable reconnect/recovery, AppInstance replacement, remaining cache admission,
  and cache/durable/DOC batch boundaries still need provisional lifecycle state.
* Stateful SET_REPARSE and a following-command identity overlay remain separate
  from the safe identity replacement already implemented.
* Directory rename still has contained-open scans, cross-share translation, and
  replacement-target boundaries.
* Command-scoped cancellation remains broader than this handle-lifetime fix.
* Fallback CREATE's live generic CLAIM remains a retry barrier. Backend compound
  transactions and rollback remain follow-up work.
* Restore Linux DOC/cleanup without losing the matched-identity protection,
  and add a quick Linux regression so memfs-only SMB coverage cannot hide the
  backend capability gap.
