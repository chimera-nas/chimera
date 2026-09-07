<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: LGPL-2.1-only
-->

# diskfs — a white-box stress model for the on-disk internals

The backend-matrix suites run diskfs *functionally*: they confirm it behaves
like a filesystem. They never make it work hard enough to reach the machinery
that only runs under stress — the B+tree's interior-node splits and multi-level
rebalances, the space allocator's fragmentation and reclaim paths, and the
intent log's crash-recovery replay. Those are the darkest corners of diskfs, and
none of them is observable from any protocol.

So this model does not describe an interface. It is a generator of **bulk
workloads** aimed squarely at that machinery, and its oracle is diskfs's own
internal state, read directly through a white-box test SDK
(`../../diskfs_test.h`) rather than inferred from what a protocol returns.

## Why it lives here and not in `ext/specs`

The other models — POSIX, NFSv3, NFSv4, SMB2 — describe published protocols,
worth stating independently of any implementation and shared between projects
through a submodule. This one describes chimera's own on-disk format and the
invariants of its allocator and B+tree; there is no second implementation to
share it with, and it is meaningless without the white-box SDK it is checked
against. Like the control-plane (`ctl`) model, its corpus is therefore generated
at build time from the `.qnt` files here, and the replay ctest is registered
only where quint is installed (the devcontainer image has it).

## Bulk operations

A trace is short, but each step is a *bulk* operation carrying a count that the
replay harness expands into many VFS calls — so a directory of ten thousand
entries is a handful of model steps, not ten thousand:

| op | what the harness does | what it reaches |
|---|---|---|
| `OFanout(dir, n)`  | create `n` fresh files in `dir` | leaf splits; at ~80 entries the tree reaches height 2, at ~8k height 3 (interior splits) |
| `OShrink(dir, n)`  | delete `n` of `dir`'s files | leaf merge / borrow, and root-collapse as the tree shrinks |
| `OChurn(dir, n, r)`| `r`×(create then delete `n`) | space-map fragmentation without changing the live set |
| `OFragFile(b)`     | write `b` non-adjacent 4 KiB blocks to a new file | extent-map B+tree growth; free-space fragmentation |
| `OTruncate(b)`     | truncate the last frag file to `b` blocks | extent removal / reclaim |
| `OReclaim`         | await background reclaim, then assert the allocator quiesced | deferred-free drain, apply-frontier catch-up |
| `ORemount`         | clean unmount + mount | free-map persist + reload; namespace survival |
| `OCrash`           | crash (skip the clean finalize) + mount | intent-log **replay recovery**; acknowledged data must survive |

The model owns only an abstract file count per directory. It uses it to *steer*
— keep growing one directory until its tree is deep, then collapse it — and to
keep the workload bounded below the device's capacity (the metadata-exhaustion
corner is a separate, smaller-device flavor). The real names, the liveness
bookkeeping and the byte-level content shadow live in the harness.

## The oracle is the internal state

After **every** step the harness runs `diskfs_test_check`, which walks diskfs's
in-memory allocator under the allocation-group locks and asserts the invariants
that a corruption or an accounting slip would break:

* each AG's free-extent tree is sorted, non-overlapping and fully coalesced;
* its free-extent lengths sum to the AG's free-byte counter;
* the per-AG counters sum to the per-device counters;
* total free never exceeds usable capacity;
* every live reservation claim lies within its AG.

Frag files carry a deterministic content pattern the harness verifies on read
and **re-verifies after every remount and every crash** — so a write that a
crash's intent-log replay failed to restore is caught as a content mismatch at
the step it surfaced.

## Corpus flavours

* **`stepGrow`** — grow one directory toward a deep tree, periodically
  collapsing it. Leaf split / merge / borrow / root-collapse.
* **`stepFragment`** — churn + scattered-block files + truncate + reclaim. The
  space-map fragmentation and reclaim paths.
* **`stepDurable`** — real work interleaved with remounts and crashes. The
  intent-log replay path, and the durability of acknowledged data across it.
* **`step`** — all of the above mixed.

The `diskfsDeep` profile raises the per-directory cap so one directory is driven
past the *second* split into a height-3 tree, reaching the interior-node
rebalance. It is expensive (tens of thousands of creates), so the corpus draws a
single short trace from it.

## Files

| file | what is in it |
|---|---|
| `diskfs.qnt` | the bulk-operation state machine + step flavours |
| `diskfs_run.qnt` | the profiles (reference depth vs. the deep-tree profile) |
| `diskfsTest.qnt` | deterministic self-checks, run as a build gate |
| `diskfs_mbt_replay.c` | the corpus replayer + content shadow + invariant check |
| `../diskfs_test_harness.h` | the in-process VFS driver (pread-backed device) |
| `../diskfs_smoke_test.c` | ground-truth smoke test; needs no corpus, always runs |
| `../../diskfs_test.{h,c}` | the white-box SDK (compiled into the diskfs module) |

## What runs where

The replay drives diskfs on a real device file through the portable `pread`
block backend, so it runs off Linux too — including natively on macOS, where the
`io_uring` / `libaio` backends do not exist. Nothing binds a port and nothing
forks a server; every trace is one process opening a scratch device under
`/tmp`.
