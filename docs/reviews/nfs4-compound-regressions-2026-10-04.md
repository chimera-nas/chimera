<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Unlicense
-->

# NFSv4 compound conversion regression review — 2026-10-04

Follow-up: both findings below are fixed in the worktree. Session OPENs reserve
their mandatory decline before execution; optional delegations receive separate
space admission after accepted finish, including any terminal error and room
for the next operation's error. Closed states still receive WANT-aware declines,
with NOT_WANTED/CANCELLED reasons. Existing v4.0 owner replay keeps space for an
already-granted delegation. The original review and reproductions below record
the behavior before these fixes.

The regression fixture now asserts both fixes with delegations disabled and
enabled, a working callback path, v4.1/v4.2, accepted/retried finishes, small
reply/cache limits, cached replay, failed suffixes, multiple OPENs and a real
WRITE successor. The new CLOSE assertion fails against the old binary. The
broader selected suite passes 107/107 in both Debug and Release. Follow-up
build/check evidence is under `/tmp/chimera-nfs4-open-fixes-20261004/`.

Required `make -k check CTEST_PARALLEL=8` completed with exit 2. Debug quick
passed 273/278 with the five recorded baseline failures; Release passed
272/278 with those five plus an intermittent SMB lease-notification mismatch
(`leases_memfs_plain`, trace `smb2Leases_stepLease_300_0x21_4`, state 45).
That additional suite passed three consecutive reruns without changes. Both
Clang stages completed with the same 42 warning signatures / 116 occurrences
as the baseline, with no additions or reductions. Formatting, SDK include
boundaries, REUSE, copyright and diff checks passed. The complete check is
not green; `compare-final.txt`, `check.log`, and `smb-lease-rerun.log` retain
the distinction between baseline failures and the additional transient.

Reviewed the current `compound-boilerplate` worktree, including its uncommitted
conversion changes, against PR base
`c971e5a5d1e423296640c163fdf5ca0adfe92ae7`. HEAD is `a9e0efe078`; reviewing
HEAD alone would miss the recent NFSv4 work. This is a regression review, not an
inventory of all unsupported NFS/RPC features. No production code was changed.

The review identified two reproducible P2 defects to fix in this PR. Neither requires new
backend transaction support or a transport redesign.

1. **P2: Reply admission rejects OPENs whose replies fit the negotiated limit.**

   `src/server/nfs/nfs4_reply.h:118–125` reserves the largest possible delegation
   reply, including a 256-byte WHO, for every OPEN except WANT_NO_DELEG. It does
   so even when delegation grants are disabled. The new execution gate at
   `src/server/nfs/nfs4_compound_vfs.c:7054–7062` uses that bound to reject the
   operation before it executes. The completion counter also retains this
   bound for subsequent operations (`nfs4_vfs_reply_complete`).

   Reproducer: ordinary OPEN_NOCREATE with no delegation preference, default
   memfs configuration with delegations disabled, and a session negotiated to
   allow 256-byte replies. Measure the same operation on a large session for
   comparison. Counts below include SEQUENCE, COMPOUND framing, the same tag,
   and the 24-byte AUTH_SYS RPC reply header.

   | Session reply limit | OPEN preference | Result | Actual reply bytes |
   | --- | --- | --- | --- |
   | 1 MiB | No preference | NFS4_OK | 160 |
   | 256 | No preference | NFS4ERR_REP_TOO_BIG | 112, error response |
   | 256 | WANT_NO_DELEG | NFS4_OK | 168 |

   The rejected success reply would fit with 96 bytes to spare. The base
   dispatcher tested the actual marshalled response, so it did not have this
   specific false rejection. Its late admission problems are not a reason to
   restore that implementation: keep admission before mutation.

   Fix: make the per-OPEN reservation account for the delegation forms that
   can actually be emitted. With grants disabled, reserve the correct decline
   form. With grants enabled, a delegation is optional: if necessary, select
   a decline before mutation and retain that choice through accepted
   publication and retries. Do not reserve an impossible optional grant and
   fail the OPEN instead. Audit the other conservative mutation bounds while
   fixing this, but this review only reproduced the OPEN case.

   Add tests for fitting OPEN responses with default share_access, explicit
   WANT flags, grants disabled/enabled, cached reply limits, and later
   operations sharing the same response budget. The common Probe.open_op
   fixture always adds WANT_NO_DELEG, which explains why existing admission
   tests missed the ordinary no-preference case.

2. **P2: A coalesced CLOSE suppresses the earlier OPEN's delegation decline.**

   `src/server/nfs/nfs4_compound_vfs.c:4174–4176` calls the OPEN delegation
   grant/decline helper only if the OPEN state remains live at the end of the
   entire VFS span. A later CLOSE makes this false. The earlier successful
   OPEN is then left with the bare OPEN_DELEGATE_NONE initialized during
   result filling, bypassing the WANT-aware decline helper in
   `src/server/nfs/nfs4_proc_open.c:30–58`.

   Reproducer, again with delegations disabled:

   | Operations after SEQUENCE | OPEN result |
   | --- | --- |
   | PUTFH(directory), OPEN(WANT_NO_DELEG) | OPEN_DELEGATE_NONE_EXT / WND4_RESOURCE |
   | PUTFH(directory), OPEN(WANT_NO_DELEG), CLOSE(current stateid) | OPEN_DELEGATE_NONE |

   Both compounds complete successfully. The old ordinary OPEN callbacks
   called the grant/decline helper before advancing to CLOSE, so this is a
   conversion regression. The server's support for WANT flags requires the
   extended decline form when it declines a flagged OPEN; see
   [RFC 8881 section 18.16.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.16.3).
   This finding does not demand granting a delegation on an already closed
   state. Named-stream OPEN already had a bare-decline limitation in the base
   and is not included as a newly introduced defect.

   Fix: separate each OPEN's mandatory response shaping from optional
   delegation publication. Always preserve the appropriate NONE/NONE_EXT
   response for a successful OPEN, even if a later operation closes its
   state. Actual grants still happen only after accepted finish and only
   for eligible live state. Keep replay snapshots consistent with the final
   per-operation response.

   Add OPEN/CLOSE tests with WANT_NO_DELEG and WANT_CANCEL, plus a failed
   suffix and finish retry. Check the OPEN result union and reason, not just
   the compound status and VFS submission count.

Verification used the existing current Debug and Release binaries; the Debug
binary postdates the reviewed NFSv4 sources. All 103 selected Debug CTests
passed with this selection:

```sh
CHIMERA_TEST_ROOT=/worktrees/compounds/build/mbt-scratch \
ctest --test-dir build/Debug -C extended --output-on-failure -j8 \
  -R '^chimera/server/nfs/(compound_|open_owner|replay_slot|layout_barrier)'
```

Both defects reproduced on NFSv4.1 and NFSv4.2 in both Debug and Release: eight
diagnostic CTest cases. These are observational probes that complete while
demonstrating the defective behavior, not passing assertions of corrected
behavior. No full build, quick-tier sweep, physical RDMA run, or baseline
binary comparison was performed in this review.

Evidence is retained under
`/tmp/chimera-nfs4-regression-review-20261004/`: `focused-debug.log`,
`probe-matrix.log`, `probe.py`, `reply_size_probe.py`, and the temporary
`CTestTestfile.cmake`. The logs contain the preserved daemon-session paths.

The source review also traced successful-prefix error placement, private
OPEN/LOCK state and claim views, retry reset, owner and client reservations,
delegation/layout retirement, namespace/credential transitions, variable
reply staging, and accepted state publication. No additional confirmed
regression emerged from that inspection. This is not proof against all
concurrency failures, and future backend rollback remains outside the PR.

Keep general RPC/RDMA descriptor validation, remote consumption tracking,
and Position Zero/Long Call support out of this PR's regression checklist
unless a concrete conversion dependency is demonstrated. Likewise, retained
protocol-administration boundaries and finite VFS capacities are not by
themselves regressions. Existing FSID/partial layout-return limitations are
not new findings from this conversion.
