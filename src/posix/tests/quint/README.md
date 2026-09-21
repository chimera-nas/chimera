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

The NFSv3 config enables PD21 for set-id preservation when an emulated hole
punch sends no writes; the other five POSIX configs have empty `deviations`
lists. This does **not** mean the replay has no other exceptions. `posix_mbt_replay.c` still records harness
allowances. Both the ordinary and strict corpora use this same replayer, so
strict twins do not disable those allowances. `devliveness.py` checks which
configured branches the generated model exercised; it does not inspect whether
the implementation still needs a harness allowance.

Two such allowances were retired in this review:

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

Neither fix weakens the model or adds an expected-failure entry.

## Next allowances to adjudicate

These are investigation targets, not a claim that each is a filesystem bug:

| Allowance | Remaining question |
| --- | --- |
| PD17b / PD17d | Replace broad EEXIST/EACCES swaps with acceptance sets derived from the actual competing conditions in the model. |
| ND8 | Copy-range overlap validation versus unsupported offload: capture both error conditions and determine the legal acceptance set. |
| ND7 / PT2 | Represent emulated and extent-aware sparse seeks explicitly; preserve the resulting descriptor offset in the oracle. |
| ND1 / ND2 / ND5 | NFS silly-rename visibility, link counts, and directory residue need an explicit adapter contract. |
| ND6 / ND9 / ND10 | Narrow per-RPC permission and stale-handle allowances to demonstrated NFS cases and protocol versions. |
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
ctest --test-dir build/Debug -R 'chimera/posix/mbt/batch_' --output-on-failure -j 3
```

A single trace can be replayed directly:

```sh
build/Debug/src/posix/tests/quint/posix_mbt_replay \
  --backend nfs4_memfs \
  --trace build/Debug/specs-corpus/posix/nfs4/posixNfs4_stepPerms_128_0x1_2.itf.json
```

For Linux passthrough cells, run with root privileges and point
`CHIMERA_MBT_SCRATCH` at an ext4/XFS scratch directory supporting
`name_to_handle_at`. A skipped passthrough cell is not conformance evidence.
Inspect successful replay output for `harness allowances:` as well as failures.

## Verification of this review (2026-09-21)

* macOS Debug: all six POSIX MBT cells, three NFSv4 protocol MBT cells, and
  twelve open/copy regression tests passed (21 CTest tests).
* Linux Release: all twelve POSIX MBT cells passed, including linux/io_uring
  and their NFS loopbacks with ext4 scratch storage. Fourteen focused
  open/copy tests also passed, including diskfs's overlapping-clone checks.
* The final unprivileged write-only regression passed on all six native
  openat backends and all seven selected Linux openat backends.
* One initial Linux SMB run reported a ctime moving backwards at step 118 of
  `posixSmb_stepPerms_128_0x1_2`. Its isolated replay and the full rerun passed.
  No new timestamp allowance was added; this remains an intermittent finding
  to investigate under load.
* Formatting uses uncrustify 0.78.1, matching CI. Homebrew 0.83 produces
  incompatible formatting; its earlier failures were a tool-version mismatch.
  The standalone SDK include check reports existing boundary violations. These
  results are not a clean full repository gate.

The initial validation above ran on `pnfs-data-split` with the older model
at `2717cb0`. The fixes are also ported to `quint-ci-enhance`; validation of
that branch uses its current model at `20516c4`. Its Linux Release build and
14 focused open/copy regressions plus three NFSv4 protocol MBT cells pass.
The complete tree passes `make syntax-check` with uncrustify 0.78.1.
