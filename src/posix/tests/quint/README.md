<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: LGPL-2.1-only
-->

# POSIX model replay and remaining allowances

The model and generator live in `ext/specs/quint/posix`; this directory owns
Chimera's profiles and replay harness. A failure can be a Chimera bug, a model
bug, a permitted implementation choice, or an adapter limitation. Reproduce
the operation and its pre-state before changing the expected answer.

## Review of the current corpus

The recent branch work corrected path traversal order, search permissions,
symlink resolution, creation ownership, rename attribute invalidation, and
no-op metadata updates. The ext4 comparison also corrected model assumptions
about competing errors, unchanged groups in chown, and moving a directory
between parents. These are useful distinctions: an error precedence should not
be pinned when the interface permits either answer, but a required permission
check should remain asserted.

All six POSIX configs have empty `deviations` lists. PD21 was retired:
a hole punch wholly beyond EOF may preserve set-id bits, as ext4 on Linux 6.8
does, or clear them, as newer kernels do. The model carries exactly these two
modes until chmod or a privilege-clearing mutation resolves the choice; it does
not ignore mode differences for other operations. The replayer still records
harness allowances. Both the ordinary and strict corpora use this same replayer, so
strict twins do not disable those allowances. `devliveness.py` checks which
configured branches the generated model exercised; it does not inspect whether
the implementation still needs a harness allowance.

The following allowances were retired in this review:

* **ND3:** broadly accepted path permission errors, and sometimes retried the
  operation as root. Its rationale predated search-permission support in the
  model. Removing it exposed `posixNfs4_stepPerms_128_0x1_2`, step 121: an
  `O_CREAT|O_WRONLY` open of an existing `0222` file requested READ|WRITE on the
  NFSv4 wire. Linux and io_uring also requested O_RDWR for write-only opens,
  exposed by the same permission trace and three lock traces on ext4. The fixes
  request only the caller's access and distinguish write-only cached handles.
  NFS server upgrades retain complementary read/write handles and select the
  correct one for I/O, including through lock stateids. Permission failures
  now fail the replay; the root retries are gone. `test_openat` pins both
  reader/writer opening orders and the unprivileged `0222` case.
* **PD19:** accepted any successful clone for which the model expected EINVAL.
  The documented beyond-EOF bug had already been fixed. Its remaining hit was
  `posixMemfs_step_128_0x1_7`, step 100, on diskfs: a same-file, overlapping
  clone succeeded. Diskfs now rejects overlapping ranges before modifying
  extents. The copy-range regression test also checks partial overlap in both
  directions, unchanged data on failure, and successful adjacent ranges.

* **ND6:** accepted EACCES on descriptor operations for both NFS versions.
  NFSv3's stateless READ/WRITE and size-setting SETATTR checks now live in the
  model as a transport policy, so failures preserve offsets and contents.
  Host-backed SETATTR checks DAC explicitly rather than relying on a privileged
  cached descriptor's `ftruncate`.
  NFSv4 keeps open-time rights: CLAIM_FH now uses the authorized open path
  and binds its grant, including for non-owner openers after chmod.

The SMB client also preserves a symlink's inherited setgid-directory group
when stamping ownership after SET_REPARSE_POINT. NFSv3 rolls back a failed
silly rename and keeps the marked handle alive until the RPC completes;
cleanup follows the file across distinct cached opens until the last closes,
then invalidates file and parent attributes. The final audit requires actual
hidden names matching the inode to explain extra links; if cleanup races the
scan, it rechecks the same inode against the exact model link count.

Directory reads check object type before DAC, preserving EISDIR after chmod.
The FUSE syscall adapter now matches glibc's read-lock probe for `F_TEST`,
permits `F_TEST`/`F_ULOCK` on read-only descriptors, and clamps same-file copy
ranges to source EOF before checking overlap. The corpus exercises these
adapter rules independently from the POSIX client.

The intermittent backwards timestamp was reproduced in the VFS clock's
wall/stopwatch correction: descheduling between its two samples introduced a
false positive offset that disappeared at the next refresh. Wall time now comes
from stopwatch's dedicated API. The TSC path pairs samples, rejects long
sampling gaps, and slews corrections without backwards steps; the non-TSC path
reads `CLOCK_REALTIME` directly. Wall corrections leave monotonic timing alone.
Deterministic stopwatch tests cover both correction directions, convergence,
preemption, concurrency, and the fallback, with a VFS integration test as well.

## Next allowances to adjudicate

These are investigation targets, not a claim that each is a filesystem bug:

| Allowance | Remaining question |
| --- | --- |
| PD17b / PD17d | Replace broad EEXIST/EACCES swaps with acceptance sets derived from the actual competing conditions in the model. |
| ND8 | Copy-range overlap validation versus unsupported offload: capture both error conditions and determine the legal acceptance set. |
| ND7 / PT2 | Represent emulated and extent-aware sparse seeks explicitly; preserve the resulting descriptor offset in the oracle. |
| ND1 / ND2 / ND5 | NFS silly-rename visibility, link counts, and directory residue need an explicit adapter contract. |
| ND9 / ND10 | Narrow per-RPC permission and stale-handle allowances to demonstrated NFS cases and protocol versions. |
| SD-DAC / SD-SETID | SMB replay skips multi-user DAC operations under one authenticated identity; this is a coverage limit. |
| SD-DFD-REUSE | A replaced pathname must not silently stand in for a held directory's identity. Compare the adapter with a real kernel SMB client. |
| SD-TIME / PD-DIRSIZE / PD24 | Separate timestamp precision, unspecified directory sizes, and finite-model descriptor bounds from conformance defects. |

The ext4 reference run does not validate reflink semantics on a filesystem
without reflink support. For those operations use a supporting implementation
and its interface contract; overlapping same-file ranges are documented as
invalid for XFS and Btrfs in
[the Linux FICLONERANGE manual](https://man7.org/linux/man-pages/man2/ioctl_ficlonerange.2.html).

## Reproduction

Build the replayer and run the ordinary cells:

```sh
ninja -C build/Debug posix_mbt_replay
ulimit -n 10240
EVPL_IO_URING_ENTRIES=1024 scripts/test_limits_wrapper.sh \
  ctest --test-dir build/Debug -R 'chimera/posix/mbt/batch_' --output-on-failure -j 3
```

This matches CI's io_uring ring size and raises the Linux memory-lock limit.
Without those settings, parallel replays can fail in io_uring setup with
`ENOMEM`. The file-descriptor limit also matters on macOS.

A single trace can be replayed directly:

```sh
build/Debug/src/posix/tests/quint/posix_mbt_replay \
  --backend nfs4_memfs \
  --trace build/Debug/specs-corpus/posix/nfs4/posixNfs4_stepPerms_128_0x1_2.itf.json
```

For Linux passthrough cells, run with root privileges and point
`CHIMERA_MBT_SCRATCH` at an ext4/XFS scratch directory supporting
`name_to_handle_at`. A skipped passthrough cell is not conformance evidence.
The focused POSIX syscall tests use `CHIMERA_TEST_ROOT` instead; point that
variable at an ext4/XFS directory as well when running those regressions.
Inspect successful replay output for `harness allowances:` as well as failures.

## Verification of this review (2026-09-21)

Initial validation on `quint-ci-enhance` used the POSIX model at `b775722`, unchanged
in merged specs commit `537633f`, and the merged stopwatch wall-time
enhancement at `f98ac24` (through libevpl `6a31ffd` and merged
prometheus-c `09e27dc`). The merged dependency source trees match the tested
trees; specs also includes generator portability and reference-harness fixes.

* Linux Release: all 56 selected CTest checks passed. These cover all twelve
  POSIX backend cells, normal and strict FUSE, five POSIX strict twins, six
  deviation-liveness gates, three NFSv4 protocol cells, 27 focused openat,
  symlink, and copy-range regressions, and the VFS clock regression. Host-backed
  cells ran with root privileges on ext4 scratch storage; none were skipped.
* macOS Release: all 39 selected checks passed: six normal POSIX cells, five
  strict twins, six deviation-liveness gates, three NFSv4 protocol cells,
  eighteen focused regressions, and the VFS clock regression.
* The previously failing 512-step FUSE timestamp trace passed twenty repeated
  replays after the clock replacement. No timestamp allowance was added.
* Stopwatch's three CTest suites passed on macOS ARM64 and Linux ARM64. The
  deterministic TSC tests exercise both the default slew limit and 500 ppm,
  including concurrent refreshes and scheduling delays. ThreadSanitizer and
  UndefinedBehaviorSanitizer passed. An x86 build under Rosetta selected TSC
  and passed the live clock test across a refresh interval. Upstream stopwatch
  CI also passed Debug and Release on Linux x64/ARM64, macOS ARM64, and Windows
  x64/ARM64, plus CodeQL.
* Model generation passed all three self-test suites and generated 746 traces.
  A freshly generated 90-trace reference corpus passed against Linux 6.8 ext4
  with its measured filesystem profile.
* After repinning merged specs, all three model self-test suites passed again,
  and two freshly generated traces matched the validated corpus exactly in
  variables and states. The preceding Chimera revision also passed every
  upstream CI check, including Quint coverage and Linux/macOS analysis.
* The full tree passed `make syntax-check` with uncrustify 0.78.1, matching CI,
  and the standalone SDK include check passed.

The dependency follow-up pins merged libevpl `b0a7f4c`, including its Windows,
SPDK, and libfabric changes. Diskfs now opens and closes devices on a dedicated
event-loop thread that remains alive until all worker queues and unmount I/O
have finished. NFS diagnostic names cover the new transport enum values.

With this pin, all 57 selected Linux checks and all 40 macOS checks passed,
with no skips: the matrices above plus the diskfs mount/crash/recovery smoke
test. The smoke test also passed in a macOS AddressSanitizer build. Targeted
Clang analysis of the changed diskfs and NFS sources, formatting, the SDK
include boundary, copyright checks, and REUSE lint passed. Linux runs used
CI's 1024-entry libevpl rings, raised resource limits, and ext4 directories for
both `CHIMERA_MBT_SCRATCH` and the focused tests' `CHIMERA_TEST_ROOT`.

Libevpl's merged PR passed all 34 CI checks. Its four devcontainer variants
(AMD64/ARM64, Debug/Release) each passed all 301 tests without retries, and all
16 model replay coverage suites passed. The merged source tree is identical
to that validated PR head.
