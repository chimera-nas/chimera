<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# Compound integration with main, October 6

## Published snapshot and rebase

The accumulated conversion was published to draft PR #1692 at
`cfd9dde74af5129d629aefb36b7c4118b6ef4c82` before rebasing its 139 commits onto
main `c69e3a10278f5aa2c97cee608cb08bacafb4ec38`. The local backup branch
`backup/compound-pre-main-20261006` retains that published snapshot.

The integration preserves main's rootfs mount namespace, runtime backend module
loading, Windows export declarations, FUSE io_uring handling, NFS recovery
updates, and new SMB semantics. Root resolution uses the VFS instance or a
PUTROOT operation. Tests depend on runtime backend modules instead of linking
MODULE libraries. The combined backend SDK layout has version 6.

Main introduced additional SMB per-operation VFS calls for object IDs, stored
reparse points, retrieval pointers, stream rename, and timestamp/allocation
maintenance. These now use compounds; the northside API guard remains enabled.
RENAME_STREAM is a VFS compound operation, including the empty name that selects
the unnamed data fork. SMB stream destinations must bypass the ordinary namespace
rename encoder so `:moved` moves stream data rather than creating a literal path.

## Semantics reconciled

Both the shared SMB encoder and fallback execution retain applicable behavior
from main: backup-intent opens, the read-only supersede exception, generic reparse
handling, rename destination vetoes and same-file exceptions, rounded allocation,
allocation retention until CLOSE, sticky access/write times, archive attributes,
and stream-qualified notifications. Speculative callbacks update request scratch
or batch state; EA enumeration position and sticky-time state publish after
accepted finish.

EA queries remain in the shared encoder. They retain main's index/restart/single
entry behavior, input name-list order, absent values, uppercase output names,
partial overflow replies, and unpadded final entry. Stored reparse tag queries and
GET_REPARSE dynamically append GETXATTR inside their current command group.

The integration also updates tests whose assumptions changed on main: root-relative
POSIX probe paths, wire EOF lock length zero, object-ID capability advertisement,
EA encoding/resume behavior, stream notification action/name, and supersede and
same-file rename results. Retry and one-submission assertions remain enabled.

## Remaining conversion and validation work

This rebase does not close the outstanding lifecycle and transaction design work
listed in the October 5 SMB and NLM reviews. In particular:

- Linux matched-filehandle delete-on-close cleanup remains capability-limited.
- SMB durable reconnect/recovery, AppInstance replacement, cache admission,
  SET_REPARSE identity migration, broader directory rename composition, and
  arbitrary command cancellation still have batching/publication boundaries.
- Main's new object-ID maintenance, generic SET/DELETE_REPARSE, and named-stream
  rename now execute through compounds but retain multiple accepted phases.
  Coalescing those phases is additional SMB composition work. Fallback sticky
  READ restoration and CLOSE allocation trimming also retain separate phases.
- Public open-cache and recovery/control interfaces still need narrowing.
- Backend transaction begin/finish/abort, rollback, transaction-aware caches,
  and commit-to-journal publication interlocks remain explicit follow-up scope.
- The pre-rebase quick-suite failures were the three remote pNFS variants.
  Native Windows and physical RDMA execution have not been validated locally.

## Combined validation

Debug and Release builds pass. Each full quick sweep initially passed 284/288:
the three established remote pNFS failures and a remaining stream-supersede
fixture assertion. That assertion was reconciled with main's behavior and passed
its final targeted rerun in both configurations, leaving an effective **285/288**
in each. All **79 SMB-labeled quick tests** pass, including protocol and POSIX
loopback coverage. The final stream probe also passes in both builds.

Extended smbtorture `compound`, `compound_async`, and `compound_find` pass **3/3**
on memfs and fail **3/3** on Linux. The Linux runs reproduce the same seven test
failure signatures as the October 5 baseline, with no additions.

The analysis builds retain **40 previously recorded warning signatures / 112
occurrences**, compared with 42 / 116 before the rebase. One new diagnostic in a
test's failure-printing path was fixed by initializing its output buffer; the
focused Debug analyzer rerun reports no bugs, and the subsequent Release
analysis includes the fix. No new or increased diagnostic signatures remain.
These unresolved analyzer findings still make the analysis targets fail.

Formatting, SDK include and northside API guards, REUSE, copyright, and whitespace
checks pass. The required
`make -k check CTEST_PARALLEL=8` sweep remains red on the remote pNFS failures and
existing analyzer findings; final targeted reruns address the fixture and
formatting failures from the initial sweep. Native Windows, physical RDMA, and
the complete extended tier were not exercised.

Logs are under `/tmp/chimera-rebase-*`, notably `chimera-rebase-check.log`,
`chimera-rebase-lifecycle-{debug-,}final.log`,
`chimera-rebase-extended-compounds.log`, and `chimera-rebase-stream-scan.log`.
The source integration commit is `3bc85cd8`.

Companion merge commits are specs `501b2ebdf8ed602851a9c61e05a0bd5fd7a2a4a5`,
libevpl `38c85bf28a300d602ec3843608ff5cd36c77973d`, and xdrzcc
`fdbfadcbcfaa78f602ed0fded25e0ab33eade745`. These retain the compound branch
changes while integrating their main branches.
