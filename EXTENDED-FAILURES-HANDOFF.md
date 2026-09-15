# Extended-run failures — handoff notes

Written 2026-09-07, from a session that spent most of its time on extended-run
triage. Everything below is **unresolved or unverified**. Confirmed fixes are
listed at the end only as context for what changed underneath these.

Reference run throughout: **34126509828** (2026-09-07 13:16 UTC, head
`9d242f57`), the first run carrying every fix merged this session. Earlier runs
cited by id where the comparison matters.

Read the "Traps" section before doing CI archaeology — several of my wrong turns
were tooling errors, not analysis errors, and they cost real time.

---

## 1. `smb2.maxfid` — diskfs intent-log stall (UNRESOLVED)

**Symptom.** `smbtorture_smb2_maxfid_diskfs_io_uring` (and `_diskfs_aio`) hit
their 600 s cap. Fails every run, all four smbtorture shards.

**What the stall detector says** (this is the good data — `IL stall:` lines come
from the diagnostic added in #1639):

```
IL stall: blocked on retire slot 26057 -- record seq=26057 end_txn_id=26058
log_offset=17436672 len=16384 chunks=1/1 outstanding entries=1; 1 of 1 ring slot(s) not done
```

Invariants across every dump I have looked at, in three different runs:

- `redo_inflight=1`, `submitted-not-durable=1` — **always exactly one
  outstanding journal write.** Never two.
- always `chunks=1/1` — a single-chunk record, 12–20 KiB.
- `gsq_tail` differs between dumps in the same run (10478 → 82617 → 110585), so
  the pipeline **recovers and re-stalls**. These are repeated transient stalls,
  ~3–5 s each, not one terminal wedge. maxfid dies on the 600 s cap, not on a
  single hang.
- `cairn` never stalls (it does not use the intent log). `diskfs_aio` stalls
  **identically** to `diskfs_io_uring`.

**The depth-one invariant is the most useful fact.** A slow device would stall
at any queue depth. Stalling only when exactly one request is outstanding points
at completion *delivery* when the ring is otherwise idle, not at the device.

**What I tried, and what it ruled out.**

Three libevpl fixes, all merged, **none of which fixed this**:

- libevpl#162 — the eventfd registration result was discarded; the completion
  handler returned early without draining; `IORING_SQ_NEED_WAKEUP` was read from
  `io_uring::flags` (setup flags, bit 0 = `IORING_SETUP_IOPOLL`, never set) so
  that branch had been **dead** for most of the file's life.
- libevpl#165 — fixing that word *activated* a racy path: the wakeup ran
  **before** `io_uring_submit()`, so the poller woke to an unchanged
  kernel-visible tail, found nothing, slept again, and cleared the flag
  `io_uring_submit()` was about to consult. Deleted it; `io_uring_submit()`
  already does the check in the only correct order.
- The chimera pin for both is on main.

After all three: **the stall is unchanged** (4 dumps per cell, same signature).
I claimed #165 was "the shape of the diskfs stall" — that was wrong.

**Also ruled out by the deferral analysis:** SQEs cannot be stranded by an
un-run deferral. Pending deferrals force `msecs = 0` in the evpl loop and
deferrals always drain at the end of every iteration before the next wait.

**Where I would look next.** `diskfs_aio` and `diskfs_io_uring` are different
*block* backends, so the block layer is exonerated. What they share is (a) the
diskfs intent-log commit pipeline and (b) the libevpl io_uring **event core**.
I have now spent three fixes on (b) without moving this, so (a) — diskfs's own
commit/retire accounting — deserves the next look. Specifically: what can leave
exactly one record submitted-but-not-retired for seconds, then release it.

Note `DISKFS_COMMIT_LOWAT` is compared with `==` (`if (--il->redo_inflight ==
DISKFS_COMMIT_LOWAT) evpl_ring_doorbell(...)`). I convinced myself this is safe
because decrements pass through every value, but I did not prove it under
concurrent increments. Worth a second pair of eyes.

---

## 2. `diskfs_evict` — deterministic hang (UNRESOLVED, best next candidate)

**Symptom.** `chimera/posix/diskfs_evict_diskfs_io_uring` and
`diskfs_evict_diskfs_aio` time out at 900 s. Appears in the `posix` shard and in
the ubuntu24/rocky10 distro Debug jobs; **which distro varies between runs**, so
it is not distro-specific.

**It is deterministic.** Identical output in three separate runs:

```
sweep ok: 20000 files
sweep ok: 13333 files
unlinking the rest...
<hang>
```

Same `13333` every time. `EVICT_NFILES` is 20000 and the loop unlinks the
`i % 3 != 0` two-thirds — so 13333 is the count about to be unlinked, and the
hang is in the **post-remount unlink loop** in
`src/posix/tests/test_diskfs_evict.c` (~line 244).

**The stall detector rules out the intent log.** Zero `IL pipeline STALLED`
lines in any of these jobs, in any run. This is **not** the same bug as maxfid.
That was the single most useful thing the #1639 diagnostic produced.

**Not attempted.** The loop prints nothing per file, so we do not know whether
it dies on iteration 1 or 13000. One `fprintf` every N iterations (stderr is
unbuffered, so it survives the kill) would localise it immediately. This is
cheap and I would do it first.

Nothing merged this session touches it, so it is unchanged and unexplained.

---

## 3. fio SIGILL (UNRESOLVED — hardware ruled out)

**Symptom.** `chimera/fio/fio_memfs_basic`, `fio_diskfs_io_uring_basic`,
`fio_diskfs_aio_basic` all die in ~0.13 s:

```
netns_test_wrapper.sh: line 64: NNNN Illegal instruction (core dumped) ip netns exec ...
```

Intermittent across cells and runs; has hit **both** amd64 and arm64, both Debug
and Release, so it is not arch- or config-specific.

**What the diagnostic (#1648, merged) established.** On a failing runner:

```
cpu:     AMD EPYC 9V74 80-Core Processor
isa:     avx2 present  bmi2 present  fma present  sse4_2 present
isa:     avx512f ABSENT
plugin:  /build/src/fio/libchimera_fio.so   (141488 bytes, present)
```

chimera builds amd64 with `-march=x86-64-v3`, which needs avx2/bmi2/fma — **all
present**. So the ISA-mismatch theory is dead, and the plugin exists.

**What remains.** `src/fio/CMakeLists.txt` has `include_directories(/fio)` — the
plugin compiles against the **fio source tree baked into the devcontainer
image**, and is then dlopened by `/usr/local/bin/fio` from that same image. Any
divergence between them means calls through `fio_ioops` at wrong offsets →
jump into garbage → SIGILL, with no bad instruction anywhere. That would be
image-dependent rather than runner-dependent, which fits the intermittency
better than the CPU theory did.

**But the obvious version of that does not hold**: the plugin sets
`.version = FIO_IOOPS_VERSION` and fio version-checks external ioengines at
load, rejecting a stale plugin **cleanly** rather than crashing. So a plain
version mismatch is not it. A same-version struct-layout change would slip
through, but that is unusual.

**Next step.** A second diagnostic commit (merged, in #1654) now reports the
dying binary's `--version`. The next occurrence will say whether the image's fio
has drifted from the pin (`fio-3.40`, cloned in `.devcontainer/Dockerfile`).
That data did not exist yet when this session ended.

---

## 4. KVM shard — never completes (UNVERIFIED after the fix)

**History.** Both kvm jobs were **cancelled at exactly 6:00:00** in four
consecutive runs (34014118017, 34059804458, 34043969522, 34087748270). The shard
has therefore **never produced a complete result**, so nothing inside it can be
evaluated.

**Root cause (diagnosed, fix merged as #1657).** qemu ran in the foreground and
in a pipeline, so `$!` never named it, and nothing killed it. ctest kills the
wrapper on timeout; the orphaned qemu keeps the stdout pipe open; ctest blocks
forever waiting for EOF, and **no further test reports at all**. That is why
34014118017 went silent for five hours after test #236 with no `***Timeout` line
and nothing from any other test.

**What #1657 does.** Two mechanisms, because one signal is not enough — verified
against a wrapper-shaped harness with a deliberately non-forwarding `timeout`
stand-in, checking the guest by recorded pid:

- `cleanup` walks this shell's descendant tree and terminates it. bash **does**
  run an EXIT trap on SIGTERM (tested), which is what ctest sends first.
- `timeout(1)` bounds qemu itself (`KVM_QEMU_DEADLINE`, default 2400 s) — it is
  a separate process and keeps enforcing after the wrapper is SIGKILLed, when no
  trap can run.

**Unverified.** At session end the kvm shards in 34126509828 were at ~1h10m,
still running — already past the point where a wedged run showed anything, but
not yet finished. **First thing to check next session: did they complete?**

---

## 5. `nfstest_posix` × diskfs timeouts inside KVM (UNVERIFIED)

Observed in 34014118017 before it wedged: 4 timeouts, all in the `nfstest_posix`
family — `diskfs_io_uring` at nfs3/4.0/4.1, and `diskfs_aio` at 4.0. `memfs` and
`cairn` passed; `cthon` on the *same* diskfs backends passed.

**Correction to an earlier claim of mine:** I said "no KVM tests have passed
since the runner migration". That was **wrong** — that run was **231 Passed, 4
Timeout** before it wedged. The suites largely work; only the shard never
finishes.

**Two competing hypotheses, neither confirmed:**

1. **Contention.** `nfstest_posix` has no explicit `TIMEOUT` so it inherits the
   CI-wide `--timeout 300`; its siblings `nfstest_lock`/`nfstest_dio` needed
   `TIMEOUT 900` for exactly this reason. Standalone at `-j 1` I measured memfs
   50 s and diskfs_io_uring **135 s** — both well inside 300 — and the shard runs
   `ctest -j $(nproc)` with `PROCESSORS 2`, so several guests compete.
2. **The io_uring race.** Those tests run diskfs on the io_uring event core, so
   the premature-wakeup bug (libevpl#165) is a live candidate for the slowness.

**The measurement cannot distinguish them**, because I ran it with the *new*
libevpl pin already applied. I changed two variables at once. A run with
`-j $(nproc)` and `--timeout 300` on a fixed pin would separate them.

**`TIMEOUT 900` for the whole `nfstest` family sits unmerged in #1650.** It is
**hardening, not a verified fix** — the commit message currently overstates
this and should be reworded before merge.

---

## 6. WPTS `ReplayCreateDurableHandleV2*` (UNRESOLVED — instrumented in PR #1671)

**Symptom.** In `wpts-smb2model/memfs_multichannel_persistent_encryption_s2`:

```
Failed ReplayCreateDurableHandleV2PersistentTestCaseS780 [20 s]
System.TimeoutException: The operation has timed out.
  at ... CreateRequest(ModelDialectRevision, ReplayModelShareType,
      ReplayModelClientSupportPersistent, ReplayModelSwitchChannelType, ...)
```

A replay CREATE over a **switched channel** with persistent handles got **no
reply at all** — a hang, not a wrong status. `s0` and `s1` passed 50/50 in the
same run.

**Intermittent.** Across runs the failing shard has been `s1`, then `s9`/`s10`,
now `s2` — never the same twice. The previous run's Debug wpts failed *only* on
`ChangeStreamName` with no smb2model failures at all.

**Two code-level gaps found (neither confirmed as the cause):**

1. **The park is unbounded.** chimera parks a CREATE that triggers an
   ack-required lease break (`STATUS_PENDING`, `conn->parked_requests`) and every
   unpark path comes from `smb_proc_oplock_break.c` — an inbound ack or a
   broadcast when leases settle. There is **no timer anywhere** in the park
   machinery (`grep -c evpl_add_timer smb_async_interim.c
   smb_proc_oplock_break.c` → `0 0`). If an ack never arrives the create waits
   forever. MS-SMB2 has the server bound that wait and proceed.
2. **Parking is per-connection.** Requests live on `conn->parked_requests` and
   resumes are keyed on connection/thread. Under **multichannel** a break sent
   on one channel can be acked on another — and the failing test is literally
   exercising `ReplayModelSwitchChannelType`. This would explain why only the
   multichannel suites flake.

**Blocked on evidence.** The server log for that window contains **nothing**
about parks, breaks, durable handles or replay (those are debug-level). We
cannot even confirm the create parked.

**Recommended next step:** log at info level when a create parks and when it
resumes, including the connection identity. One occurrence then distinguishes
"the ack never came" (→ needs the timeout) from "the ack came on the other
channel and we missed it" (→ needs cross-channel resume). Those have different
fixes and guessing between them is how you ship the wrong one.

Do **not** add a break-ack timeout blind: it needs a decision about what the
server does on expiry (break to none and complete the create, presumably) and a
value consistent with MS-SMB2. Getting it wrong converts an intermittent hang
into a systematic wrong answer.

---

## 7. `smbtorture_smb2_ioctl_linux` — `dup_extents_dest_lock` (NEW, UNATTRIBUTED)

**Symptom.** New in 34126509828; passed in the two prior runs.

```
ioctl.c:6993: status was NT_STATUS_NOT_SUPPORTED, expected NT_STATUS_OK:
    FSCTL_DUP_EXTENTS_TO_FILE
```

**What the test requires** (samba `source4/torture/smb2/ioctl.c`, fetched via
`gh api repos/samba-team/samba/contents/...`): it takes an exclusive byte-range
lock on the destination via a second handle, confirms a plain write gets
`FILE_LOCK_CONFLICT`, then asserts dup-extents returns **OK** — *"In contrast to
copy-chunk, dup extents doesn't cause a lock conflict here."*

**chimera already gets the lock semantics right.** `smb_proc_copychunk.c`
enforces byte-range locks; `smb_proc_copyoffload.c` (dup extents) deliberately
does not. Despite the test's name, the lock is not what fails.

**Most likely mechanism.** `chimera_linux_clone_range` issues
`ioctl(FICLONERANGE)`, which returns `EOPNOTSUPP` on filesystems without reflink
(ext4, overlayfs) → `ENOTSUP` → `NOT_SUPPORTED`. Meanwhile
`SMB2_FS_ATTR_SUPPORTS_BLOCK_REFCOUNTING` is advertised **unconditionally**
(`smb_proc_query_info.c:1084`, a fixed OR-list with no backend reference). The
test checks that flag and *skips* when absent — so chimera promises reflink and
the backend then cannot deliver.

**Why I did not raise a PR.**

- **No merged commit explains the newness.** Only four landed since the last
  clean run; none touches FSCTL, cloning or capability reporting. A capability
  that has always been unconditional does not start failing on its own — the
  likelier variable is the **runner's filesystem**, making this environmental.
- **I could not enumerate the other subtests.** The JUnit hit its 65536-byte cap;
  I only saw `bad_handle` success, two compressed cases skipped, `dest_lock`
  failed. If the other *positive* dup-extents cases also failed, that supports
  the filesystem theory; if they passed, it refutes it.
- **The fix is a design decision.** Gating the flag on real backend capability
  means plumbing a "can this backend clone?" query through the VFS. Dropping the
  flag outright would make the suite *skip* on reflink-capable filesystems too,
  losing real coverage.

**Next step:** check an untruncated per-cell log (the smbtorture **matrix**
artifact, not the shard JUnit) for whether the other positive dup-extents cases
failed in the same run.

---

## 8. `batch_pnfs_cairn` heap-use-after-free (OPEN, no fix)

Seen in a **merge-queue** run, not extended. ASAN:

```
READ of size 4, thread T221
  evpl_wakeup_signal (wakeup.h:97)
  evpl_ring_doorbell
  chimera_vfs_complete_delegate (vfs_internal.h:579)
  cairn_thread_commit (cairn.c:1739)
freed by T0:
  chimera_vfs_thread_destroy (vfs.c:1321)
  chimera_server_unmount (server.c:2433)
```

`chimera_server_unmount` creates a temp vfs_thread, waits **only for the umount
callback**, then destroys it — while a delegated cairn commit issued on that
thread can still complete and ring the freed thread's doorbell.

Did not reproduce locally in 12 runs (macOS vs arm64 container timing). Note the
close fence (#1641) is in that build and touches this path; the fence makes
umount wait *longer*, which should shrink the window rather than open it, but I
could not prove the counterfactual.

---

## Confirmed fixed this session (context only)

- **S3 `AccessDenied`** on all 8 rocky jobs — #1646. Test keys were unbound and
  acted as nobody after the per-key POSIX identity change.
- **pynfs SEC7 + FFLOOS** on all 4 jobs — #1647.
- **WPTS `BVT_SMB2Basic_ChangeNotify_ChangeStreamName`** — #1656. A named-stream
  CREATE completed via `chimera_smb_create_finish_with_eas` and never entered
  `chimera_smb_create_open_finish`, where the directory notification is emitted,
  so creating a stream notified nobody. **Note:** I had this recorded for
  weeks as "the client renames a stream" — the test does no rename at all. Read
  the test source, not the note.
- **JUnit artifacts from sharded runs** — #1636, which is what made most of this
  triage possible.

Failure count went 23 → 9 over the session.

---

## Open PRs at session end

| PR | state |
|---|---|
| chimera#1650 | KVM diagnostic job + `TIMEOUT 900`. **Needs rebase** (stale libevpl pin, main moved) and the `TIMEOUT 900` commit message reworded from "fix" to "hardening" |
| specs#26 | `CI complete` aggregate check |

**Settings work, not code** (found while auditing why PRs could merge before
checks finished):

- **libevpl** `protect-main` ruleset is `enforcement: disabled` and has only
  `deletion` + `non_fast_forward` — no pull-request rule, no required checks. Its
  `ci-complete` job already exists and is well-formed; it just is not required.
- **specs** ruleset is active but requires only `build` + `reuse-lint`, so
  `samba`/`ganesha`/`knfsd` can still be running at merge. specs#26 adds the
  aggregate; the ruleset must then require `CI complete`.

---

## Traps that cost me time — read this first

- **`pgrep -f` self-matches.** Three separate times I "confirmed" a failure that
  was my own harness: `pgrep -af qemu-system-` matched the shell whose command
  line contained that string (in the comment explaining the check);
  `pgrep -f "sleep 300"` matched the `timeout` process wrapping it;
  `pgrep -x sleep` matched my own test's `sleep 3`. **Check a recorded pid.**
- **Comments inside a backslash continuation comment out the command.** I put an
  explanation above `-e EVPL_IO_URING_ENTRIES=...` inside a `docker run ... \`
  list; the `\` continues onto the `#` line and kills the rest. All 15 test jobs
  failed in 4 s with `docker: 'docker run' requires at least 1 argument`. Verify
  structurally, not by eye.
- **`build-test.yml` does not run on `pull_request`** — it is `merge_group` +
  `push` to main only. A job added there cannot be iterated on from a PR.
  `pr-checks.yml` is the PR workflow, and its jobs build the dev image
  themselves (so they work on a fork's read-only token).
- **KVM/extended ctest needs `-C extended`.** Without it `-R` matches nothing and
  reports `No tests were found!!!` — which, with a `|| true`, is a green job that
  tested nothing.
- **libevpl's `src/rpc2/tests/` is not test-only for chimera.**
  `src/server/CMakeLists.txt` compiles `krb5_local.c` straight into the MBT
  targets. A pin bump broke 8 krb5p tests because `krb5_local_initiator_provider`
  changed from taking the realm to taking an identity — and both accessors return
  `void *`, so it compiled silently. (Main has since fixed this properly with
  `mbt_gss_identity(env, uid)`.)
- **JUnit `system-out` is capped** (64 KB for failed tests, and much smaller in
  practice — I saw 8873 bytes for maxfid). Absence of a log line there proves
  nothing. Server-side detail lives in the **smbtorture matrix** artifact, which
  keeps untruncated per-cell logs.
- **Fetch upstream test sources directly** with
  `gh api repos/<owner>/<repo>/contents/<path> --jq .content | base64 -d`. The
  web fetcher's summaries dropped exactly the details that mattered for both the
  WPTS and samba tests.

---
---

# Update — run 34315998434 (2026-09-09 05:42 UTC, head `85db16fa`)

Written 2026-09-09. This supersedes the status lines above; the analysis above
is kept because it is what led to the fixes.

The shape of the run changed completely. It finished in **1h40m** instead of
6h04m, and the four KVM shards produced complete results for the first time.
Nine jobs are still red, but each now carries **one to three** failing tests
instead of whole suites.

## Resolved since the 2026-09-07 notes

| Was | Now |
|---|---|
| **#1 `smb2.maxfid`** — 600 s cap, every run, all four smbtorture shards | **PASSES on all six backends.** Gated cells: diskfs_io_uring 204 s, diskfs_aio 180 s, cairn 239 s, io_uring 51 s. One ~3 s `IL stall` line remains, down from repeated stalls. |
| **#3 fio SIGILL** | **Gone.** All three `chimera/fio/*` tests pass on both distro Debug jobs. Fixed by `ded360e8` (build fio without `-march=native`). |
| **#4 KVM shard never completes** | **Fixed.** All eight KVM jobs finished. The four non-nfstest shards are **100% green: 1546 tests, 0 failures.** |
| **#7 `dup_extents_dest_lock`** | **Passes** on linux (and every other backend). It was environmental, as suspected. The only dup-extents failure left is `dup_extents_sparse_dest` on memfs, already in the disabled baseline. |
| **#6 WPTS** | Green in this run — amd64 Debug, amd64 Release and both arm64 wpts jobs all passed. |
| **#8 `batch_pnfs_cairn` UAF** | Root-caused and fixed in `9bf819c3`: the doorbell is now rung inside the critical section. |

**The root cause for #1 and most of #2 was SQPOLL**, not the intent log and not
completion delivery. `9bf819c3` advances libevpl to #166, which makes io_uring
SQPOLL opt-in and off by default. Every evpl thread's ring carried a kernel
sqpoll thread spinning for a second after each submission, so a depth-one
journal write paid a scheduler hop on every I/O. Measured on the evict create
phase, same binary: libaio 133 s, io_uring with SQPOLL 311 s, io_uring without
68 s. **The `IL stall` dumps were the symptom of slowness, never a hang** — the
depth-one invariant that looked so diagnostic was just the journal's natural
queue depth being maximally penalised.

## Still open

### A. `boto3_v4_io_uring_streaming` — ROOT CAUSED + FIXED (PR #1667)

`Test rocky10 amd64 Debug`, the only failure in 5512 tests.

```
=== TEST FAILED: streaming - streamdir/multi: content corrupted;
    300000 bytes stored, first diff at offset 168928 ===
```

The SigV4 aws-chunked payload is 300000 bytes in 64 KiB chunks, chosen to cross
the server's internal 128 KiB read boundary. **The length is right and the
content is wrong**, so this is not chunk framing leaking into the object.

**`io_uring` is the only backend that fails.** memfs, linux and cairn all pass
the same case in the same job. A decoder bug would fail everywhere, so suspect
the io_uring passthrough's read or write assembly, not the S3 chunk decoder.
Note that `9bf819c3` changed io_uring submission timing by removing SQPOLL, and
`593a8cf9` reworked the split readv/statx completion accounting — that pair only
touches `r_eof`, not data, but it establishes that a READ is two CQEs that land
in either order.

**History: this is the first occurrence in the last 11 Extended runs.** A sweep
of 34281303417, 34254877035, 34239900116, 34223500202, 34191238682,
34164803105, 34126509828 and four older runs found the strings `content
corrupted` and `first diff at offset` **zero** times across ~80 failing jobs.
The same cell on the same platform (rocky10 amd64 Debug) passed at 1.25 s on
09-07. The assertion itself is old: it landed 2026-05-21 in `f3c88f58`
(`s3: de-chunk SigV4 streaming (aws-chunked) uploads`), so this is a real
behaviour change, not a newly-added check.

**It is a race, not a bisectable regression.** Every commit between the last
clean run's head (`ded360e8`) and this run's head is diskfs-only:

```
85db16fa ad2bf282 8ba7befb 6f3caa7a abf7769c
```

`git diff --name-only ded360e8..85db16fa` outside `src/vfs/diskfs/` is **empty**.
Nothing in the window touches S3 or the io_uring passthrough, so a one-in-eleven
intermittent race is the only reading that fits. `9bf819c3` (SQPOLL off) landed
09-08 12:25 and has now been in four runs with one failure, which makes it a
plausible exposer of a latent race but proves nothing on its own.

Offset 168928 is **not** chunk-aligned: 168928 = 2x65536 + 37856. That may
matter when chasing it.

Reproducing needs Linux; io_uring cannot run on this mac. Given it is a race,
budget for a loop: run the io_uring streaming cell a few hundred times under
parallel load rather than expecting a single run to show it.

---

## ROOT CAUSE (confirmed analytically and reproduced)

**The S3 GET path assembles the response body in read-completion order, not in
file-offset order.**

`chimera_s3_get_send` (`src/server/s3/s3_get.c:101-140`) loops on `goto again`,
issuing one `chimera_vfs_read` per 128 KiB (`config->io_size`, set at
`s3_get.c` via `s3.c:1385`) and advancing `file_cur_offset` / `file_left` at
*submission* time. All of the reads are therefore in flight at once.

`chimera_s3_get_send_callback` (`s3_get.c:53-88`) then does:

```c
if (niov) {
    chimera_s3_response_add_datav(evpl, request, iov, niov);
}
```

`chimera_s3_response_add_datav` (`s3_internal.h:478-491`) forwards straight to
`evpl_http_request_add_datav`, which **appends to the body stream**. There is no
offset, no sequence number and no reordering buffer. Whatever order the CQEs
land in is the order the object's bytes go out on the wire.

For the 300000-byte `streamdir/multi` object that is three reads:

| # | offset | length |
|---|---|---|
| 1 | 0 | 131072 |
| 2 | 131072 | 131072 |
| 3 | 262144 | 37856 |

### Why the offset 168928 identifies the bug uniquely

The test payload is `bytes((i * 7) & 0xff for i in range(300000))`, which is
periodic with period 256. Both 131072 and 262144 are multiples of 256, so most
permutations are **invisible**. Enumerating all six:

| completion order (by offset) | first differing byte |
|---|---|
| 0, 131072, 262144 | none |
| 131072, 0, 262144 | none |
| 0, 262144, 131072 | **168928** |
| 131072, 262144, 0 | **168928** |
| 262144, 0, 131072 | **37856** |
| 262144, 131072, 0 | **37856** |

Only two offsets are observable at all, and CI reported exactly one of them.

### Reproduced

In a privileged container on kernel 6.8 aarch64, ext4 on a docker volume at
`/build/test` (the passthrough backends need `name_to_handle_at`, so virtiofs
and overlayfs both fail the mount with EOPNOTSUPP). PUT a 300000-byte
position-dependent object, `echo 1 > /proc/sys/vm/drop_caches` to force the
reads to miss page cache and go async, then GET and compare:

```
corrupt in 33 of 40 iterations
first_diff values observed: only 168928 and 37856
```

Exactly the two predicted offsets, nothing else. A probe on the callback
recorded the completions directly:

```
GETORDER: completed offset=262144 count=37856  eof=1
GETORDER: OUT OF ORDER -- expected offset=0 got=262144
GETORDER: completed offset=131072 count=131072 eof=0
GETORDER: completed offset=0      count=131072 eof=0
```

Note `drop_caches` value **1**, not 3: dropping dentries and inodes as well
breaks the passthrough's open-by-handle lookups and the GET fails with NoSuchKey
before it can be compared.

### Why only io_uring, and why so rarely

- **io_uring** does not set `CHIMERA_VFS_CAP_BLOCKING`; its CQEs complete in
  whatever order the kernel finishes them. When the data is in page cache the
  reads complete inline, in order, and the bug is invisible. Under memory
  pressure the reads go async through io-wq and reorder. That is why it only
  shows up in a loaded CI run and never on an idle box: 60 idle iterations
  produced zero reorders.
- **memfs, cairn and diskfs** complete in submission order, so they cannot hit it.
- **linux** sets `CAP_BLOCKING` and routes through the sync delegation pool. It
  is not obviously immune; it simply has not been caught.
- **Every other S3 test payload is a single repeated byte** (`b'x' * 4096`,
  `b'y' * 35000000`, `b'z' * (5 MiB + 123)`, `bytes([part_number]) * part_size`).
  Reordering is mathematically undetectable in all of them. `streamdir/multi` is
  the only position-dependent payload in the whole S3 suite, which is why this
  one cell is the only one that can ever fail.

**The bug is not specific to streaming uploads.** Any GET of an object larger
than 128 KiB is exposed; the streaming test is merely the only one that checks
content that would reveal it.

### Fix

Do not serialize the reads; that would cost the pipelining. Keep the reads
concurrent and reorder on completion: hold the in-flight `chimera_s3_io` entries
in submission order (the struct already carries a `next` pointer), mark each
complete as its callback fires, and drain from the head, appending only the
contiguous completed prefix. `chimera_s3_io_alloc`/`free` and `io_pending`
already give most of the bookkeeping.

While in there, note the GET loop also advances `file_cur_offset` and
`file_left` by the *requested* length and ignores the returned `count`, so a
short read would drop bytes the same way. See the separate short-write audit
below.

### Related latent bug found while auditing (NOT the cause of this failure)

The VFS has no written contract on short writes, and several callers ignore the
`length` the backend reports:

| Caller | behaviour |
|---|---|
| S3 PUT body (`s3_put.c:196-224`) | ignores `length`; advances offset at submit time |
| S3 UploadPart (`s3_multipart.c:619-647`) | same |
| SMB2 WRITE (`smb_proc_write.c:520`) | replies with the *requested* count regardless |
| POSIX layer (`posix_write.c`, `posix_pwrite.c`, `posix_writev.c`) | returns the requested count; `posix_writev.c:111` advances the fd offset by it |

NFSv3, NFSv4 and FUSE are correct (they propagate the count and the client or
kernel reissues). `linux`, `io_uring` and the nfs/smb proxy backends can all
return a short write; memfs, cairn and diskfs cannot. The proxy backends
fundamentally cannot promise otherwise, so the caller-side fix is unavoidable
for at least those.

### B. `diskfs_evict` — ROOT CAUSED: not a hang (PR #1670)

Timeout at 900 s on `Test ubuntu24 arm64 Debug`, both backends. But:

- `Test devcontainer arm64 Debug posix`: **passes, 194 s**
- `Test rocky10 amd64 Debug` (full extended tier, same contention): **passes, 235 s**
- `Test rocky10 arm64 Debug`: **passes**

So the "deterministic hang" is gone; what remains is confined to
ubuntu24 + arm64 + Debug. The transcript still stops at the same place:

```
sweep ok: 20000 files
sweep ok: 13333 files
sweep ok: 13333 files
unlinking the rest...
<hang>
```

The cheap next step from the old notes still applies and is now much better
targeted: one `fprintf` every N iterations in the post-remount unlink loop
(`src/posix/tests/test_diskfs_evict.c`, ~line 244), then run only that cell on
ubuntu24 arm64.

### C. KVM `nfstest_alloc` — a timeout budget, not a bug (FIXED in PR #1667)

`nfstest_alloc_diskfs_io_uring_nfs4.2` and `_diskfs_aio_nfs4.2` time out at
**exactly 300 s** in all four nfstest shards (both images, both configs).

`kvm/tests/CMakeLists.txt:516` gives `TIMEOUT 900` to `nfstest_lock` and
`nfstest_dio` only, so alloc inherits the CI-wide `--timeout 300`. The captured
output shows it **still emitting PASS lines when killed** — it is progressing,
just slower than the budget on diskfs. memfs passes.

PR #1650, which carried the `TIMEOUT 900` widening, was closed and conflicting.
**PR #1667 re-raises just the `set_tests_properties` change** for the whole
nfstest family, which should clear 8 of the 9 KVM failures.

### D. `nfstest_dio_diskfs_aio_nfs4.2` — the one genuine KVM hang

Timed out at **900.002 s**, so it already has the widened budget and blew
through it. Only in `amd64 Debug kvm-ubuntu2404`; the other three shards passed
the same cell. Treat this, not alloc, as the real KVM defect.

### E. Two single-cell flakes

- `libevpl/tls/rand_full_duplex_stream_tls_select` — Timeout, no output, on
  `arm64 Debug rest`. The same cell passes in the arm64 Release and both amd64
  rest shards.
- `smbtorture_smb2_bench_cairn` — Failed at 40.9 s on `arm64 Debug smbtorture`;
  passes on the other three smbtorture shards. **The cause is not recoverable
  from the artifact**: `smb2.bench.session-setup` fills the entire 65536-byte
  ctest output cap with NTLM auth lines, so the failure message is truncated
  away. If this recurs, split bench.session-setup into its own cell or quiet
  that log line before trying to diagnose it.

### F. smbtorture matrix — one "newly failing" cell, and it is a scoring artifact

The informational matrix reports `smb2.maxfid [TIME]` on diskfs_io_uring as a
regression. **The cell log ends with `smbtorture: PASSED` / `success:
smb2.maxfid`.** `tools/smbtorture/regen.sh:253` gives maxfid a 600 s budget and
the cell took ~680 s under full-matrix contention; `timeout --signal=KILL` kills
only the direct child, so the orphaned test script kept the log fd and wrote its
success banner after the kill — the same orphan class as the old qemu bug.

So: raise the maxfid cell budget, or fix the matrix runner to kill the process
group. Do not chase it as a functional regression.

Matrix totals: 3930 cells, **106 newly passing**, 1 "newly failing" (the above).

## Scoreboard

| | 34126509828 (09-07) | 34315998434 (09-09) |
|---|---|---|
| wall clock | 6h04m | 1h40m |
| failing jobs | 13 | 9 |
| failing test instances | whole suites | 14 |
| distinct root issues | 8 | 6 |
| KVM shards completing | 0 of 2 | 8 of 8 |
| distro test jobs green | — | 20 of 22 |


---

## PR #1667 (raised 2026-09-09)

`s3: assemble the GET response body in file order` + `kvm: give the whole
nfstest family the 900 s timeout`.

**The S3 fix.** Reads stay concurrent; each hangs on the request's `read_queue`
in submission order, its callback marks it ready, and the queue's completed
prefix is drained head first. `io_pending` decrements as an entry drains rather
than as its callback fires, so the request is not finished until every byte has
reached the response.

**Regression coverage.** The 35 MB object in the GET test was a repeated byte
checked only for length -- invisible to a permutation, as is every other payload
in the S3 suite. It now carries a distinct counter per 4 KiB page and the body
is compared. Verified as a real detector, not just an assertion:

| | unfixed server | with the fix |
|---|---|---|
| GET test under a page-cache-dropping loop | caught 5 of 5 | 0 of 5 |
| 300000-byte PUT/drop/GET reproduction | 33 of 40 corrupt | 0 of 40 |

**Local verification env.** Container on kernel 6.8 aarch64. The passthrough
backends need `name_to_handle_at`, so `/build/test` must be a real Linux
filesystem: virtiofs and overlayfs both fail the mount with EOPNOTSUPP. A docker
volume works (it is ext4 on the colima VM disk); a loopback ext4 image is the
other option but the colima disk was full. Full S3 suite green on all five
backends at SigV4 plus the SigV2 subset CI registers.

**Not addressed by the PR, still open:**

- The GET loop advances `file_cur_offset` / `file_left` by the *requested*
  length and ignores the returned `count`, so a short read would drop bytes.
- `nfstest_dio` on `diskfs_aio` at NFSv4.2 exceeded **900 s** in one of the four
  shards. That is a real hang, not a budget problem, and the timeout widening
  does nothing for it.
- The short-write audit in the section above (S3 PUT, S3 UploadPart, SMB2 WRITE,
  the POSIX layer).


---

## PR #1670 (raised 2026-09-09) — diskfs_evict was never hanging

**Root cause.** The diskfs workload runs 4-6x slower under the ubuntu24 arm64
Debug image than under any other image, and has for as long as the per-test
timings go back. Run 34315998434, same commit, same runner pool, all
Debug/ASan:

| test | rocky10 arm64 | ubuntu26 arm64 | ubuntu24 arm64 | ratio |
|---|---|---|---|---|
| `diskfs/mbt/batch` | 34.5 s | 37.7 s | 204.3 s | 5.9x |
| `diskfs_reclaim` (io_uring) | 71.5 s | 73.5 s | 312.6 s | 4.4x |
| `diskfs_evict` (io_uring) | 192.5 s | 198.6 s | killed at 900 s | — |
| `smb/mbt/batch_memfs` | 78.4 s | 73.8 s | 132.9 s | 1.7x |

The last row is the control: general tests are ~1.7x slower there, diskfs 4-6x.
192 s scaled by that is ~900 s, which is the cap. Long-standing, not a
regression: reclaim has been 394-452 s on that job in every run back to 09-06.

**The trap that cost weeks.** `posix_test_success()` prints nothing, so a clean
run and a wedged run emit byte-identical output ending at `unlinking the
rest...`. Every previous note (including mine, further up this file) inferred
"hangs in the post-remount unlink loop" purely from where the output stopped.
That inference was never supported. **Do not read a stopping point as a hang
location unless the success path prints something after it.**

**PR #1670** raises evict 900 -> 1800 and reclaim 600 -> 1200 (reclaim shows the
same ratio and had only 25% headroom; it did time out before libevpl#166), and
makes every phase after the last sweep announce itself.

### Ruled out, with evidence

- **Block-cache swap-park deadlock.** `diskfs_block_defer_retry` self-defers via
  `evpl_defer`, and evpl's drain (`ext/libevpl/src/core/evpl.c:758`) is
  `while (num_active_deferrals)` and clears `armed` *before* the callback -- so a
  re-armed deferral re-runs in the SAME pass, not "next loop iteration" as the
  comment at `diskfs_block.c:1246` claims. The worker spins inside
  `evpl_continue` without servicing its own completions. **Real contract
  violation, worth a separate PR**, but a counter showed it firing **zero** times
  in this test at the cache size CI uses. Not this bug.
- **Local reproduction.** Kernel 6.8 arm64 + ASan: passes in 96-144 s single,
  and 4 concurrent copies all pass.

### Traps for next time

- `block_cache_blocks` is **floored** at 1.5x the intent log
  (`DISKFS_BLOCK_CACHE_MIN_BLOCKS`), so anything below ~24576 is silently
  ignored. My first cache-shrink experiment proved nothing because of this.
  To shrink the cache you must shrink `intent_log_size` too.
- The distro test jobs upload **no ctest artifact** -- only the devcontainer
  shards do. Distro failures are raw-log only.
- `drop_caches` value 3 breaks the passthrough/diskfs open-by-handle lookups;
  use 1.
- Branch name `diskfs-evict-timeout` on the fork belongs to merged PR #768.

### Still open

Why that image is 4-6x slower for diskfs specifically, and only for diskfs. The
images differ in ASan runtime (ubuntu24 `libclang-rt-18-dev` vs ubuntu26's 21),
which would fit an allocation-heavy workload, but this is unconfirmed.


---

## PR #1671 (raised 2026-09-09) — WPTS replay-create: made diagnosable, NOT fixed

**Status: still unresolved.** This PR adds observability only; it changes no
behaviour. Read this before re-deriving anything.

**Corrections to the notes above (section 6), which were wrong:**
- The park is **not** unbounded. It carries a 30 s deadline armed with
  `evpl_add_oneshot_timer` -> `chimera_smb_create_park_deadline_cb`
  (`smb_proc_create.c:3427`); the earlier `grep -c evpl_add_timer` missed it
  because the call is `evpl_add_oneshot_timer`.
- Trees are **per-session, not per-connection** (`session->trees[i]`, and
  `smb_internal.h:2593` explicitly handles a tree disconnect on another
  channel), so the live-open replay scan in `chimera_smb_create_guid_replay`
  is *not* connection-scoped. The "parking is per-connection" theory needs a
  narrower statement: `conn->parked_requests` and
  `chimera_smb_create_resume_parked_conn` are per-connection, but the replay
  *lookup* is not.
- `chimera_smb_durable_claim_by_guid` does compare `create_conn != req_conn`
  (`smb_durable.c:713`) — but that arm returns DUPLICATE_OBJECTID, which is a
  **reply**. The observed symptom is no reply at all, so it is not that path.

**The number that matters.** `CHIMERA_VFS_CLAIM_DEFAULT_BREAK_DEADLINE_MS` is
**30000**; the WPTS adapter gives up at **20 s**. So any create that has to wait
out the deadline has already failed the test 10 s before the server recovers.
A park in this suite can never merely be slow — it is always fatal.

**What the new logging showed immediately** (oplock+lease+dirlease memfs, 78
tests, all passing): 76 creates park, 73 resume on an ack, and the deadline
**genuinely expires** in `smb2.lease.v2_complex1` and `smb2.oplock.batch21`
without failing them. A client is entitled not to ack, so a forced revoke is
normal — that is why the deadline line is info, not error. Do not read the
first one you see as a bug.

**Why it is still open.** WPTS cannot run on this mac (testhost SIGBUS on
arm64, see [[wpts-arm64-sigbus]]), and the CI server log is captured at info
while all the park/break detail was debug — so there has never been any
evidence about what the server did. The three hypotheses (never parked; parked
and no ack; ack arrived on another channel and was not matched) have different
fixes and the logs now separate them. Wait for the next natural occurrence in
the nightly extended run.

**Do not** shorten the break deadline to "fix" this. It is a decision about
what the server does on expiry and must stay consistent with MS-SMB2; getting
it wrong turns an intermittent hang into a systematic wrong answer.


---

## WPTS replay-create: REPRODUCED LOCALLY AND ROOT CAUSED (2026-09-09, later)

**WPTS runs fine on native arm64 in the devcontainer image.** The recorded
blocker was wrong, or rather it was about the wrong thing: the CMake gate
(`src/server/smb/tests/CMakeLists.txt`, `WPTS_ARCH_SUPPORTED`) says "the arm64
CI builds run under EMULATION, where the managed test host crashes". This
container is a *native* aarch64 colima VM, the image ships the .NET 8 SDK for
`linux-arm64` and `/opt/wpts/Bin`, and the suite runs. Note CI's arm64 jobs now
use `ubuntu-24.04-arm` (native runners), so that gate looks stale and enabling
WPTS on arm64 would roughly double its coverage. See [[wpts-arm64-sigbus]],
which this supersedes.

### Local reproduction recipe

```
docker run -d --name wpts2 --privileged \
  -v /Users/bjarvis/sandboxen/chimera:/Users/bjarvis/sandboxen/chimera \
  -v <a docker volume>:/build -w <worktree> <devcontainer image> sleep infinity

# the 236 green cases for this config, in CI's 50-case shards
awk -F, '$2=="multichannel_persistent_encryption" && $3=="green" {print $1}' \
    src/server/smb/tests/wpts/smb2model_cases.csv | split -l 50

WPTS_BIN_DIR=/opt/wpts/Bin DOTNET=/usr/local/bin/dotnet WPTS_SUITE=MS-SMB2Model \
CHIMERA_SMB_MULTICHANNEL=1 CHIMERA_SMB_PERSISTENT=1 CHIMERA_SMB_PERSHARE_ENCRYPTION=1 \
bash scripts/wpts_smb_test_wrapper.sh build/DebugCtr/src/daemon/chimera memfs "<csv list>"
```

Serially the whole 236-case set passes in 22 s with **zero parks** and never
failed in 40 runs. **Run the five shards CONCURRENTLY** (as ctest does) and it
reproduces in ~3 rounds.

### Two distinct failure modes, same client symptom

Round 3 produced two failures in different shards:

| shard | failing case | parks | resumes | deadlines |
|---|---|---|---|---|
| 1 | `ReplayCreateDurableHandleV2PersistentTestCaseS1483` | **1** | 0 | 0 |
| 2 | `ReplayCreateDurableHandleV2PersistentTestCaseS694` | **0** | 0 | 0 |

Both fail as `System.TimeoutException` on `CreateRequest` after 20 s. So the
symptom has (at least) two causes and they are not the same bug.

### Mode 1 (shard 1) — ROOT CAUSE

```
12:53:37.377  channel 1 established
12:53:37.418  channel 2 established
12:53:37.448  CREATE parked on a caching break
12:53:37.448  channel 1 DISCONNECTS  <-- 0.4 ms after the park
12:53:57.477  channel 2 disconnects  <-- exactly 20 s later, the client gave up
```

The CREATE parked on a break of a caching holder that was still live when the
decision was made; 0.4 ms later the holder's channel went away. It was never
resumed, and the daemon died ~20.3 s after the park so the 30 s deadline never
fired either.

**The bug: nothing re-evaluates an already-parked CREATE when the holder it is
waiting on can no longer ack.** Every resume trigger lives in
`smb_proc_oplock_break.c` (`chimera_smb_create_resume_parked` /
`_broadcast`, lines 452/522/691/784/790) and is driven by an INBOUND break ack.
Connection teardown, open parking and close do not resume anyone.

The code already knows a parked holder cannot ack --
`chimera_smb_create_purge_parked_writers` (`smb_proc_create.c:605`) exists
precisely to evict such a holder "before a conflicting open breaks" it, because
"a doomed break would only mark its lease BREAKING (the parked handle has no
connection to ack it)". That check runs **before** the break and never again.
Win the 0.4 ms race the other way and the create is stuck for the full deadline.

That 0.4 ms window is exactly why this is intermittent, why it needs
multichannel (the model switches channels, so a channel drops mid-create), and
why the deadline never rescues it in WPTS (30 s deadline vs a 20 s client).

**Fix direction:** when an open is parked/torn down on connection loss, settle
the breaks it was holding and resume any CREATE parked on those files -- i.e.
apply the `purge_parked_writers` reasoning post-hoc, not only as a pre-check.

### Mode 2 (shard 2) — NOT characterised

Zero parks, so it is not the above. Could not be diagnosed because
**`scripts/wpts_smb_test_wrapper.sh` keeps only the last 80 lines of daemon
stderr** (`CHIMERA_LOG` lives in `SESSION_DIR`, which cleanup `rm -rf`s), and
for a 50-case shard the failing case has usually scrolled past. Mode 1 was only
diagnosable because its park happened to land inside the final 80 lines.
Preserving the full daemon log (or the window around the failure) is a
prerequisite for mode 2.


---

## WPTS replay-create: TRUE ROOT CAUSE + two failed fix attempts (2026-09-09, later still)

**Supersedes the "mode 1 / mode 2" section above, which was wrong.** There is
ONE mode. The apparent second mode (a failing shard with zero parks) was the
wrapper keeping only the **last 80 lines** of daemon stderr -- the park had
scrolled away. With the full log, every failing shard shows the identical
signature: **exactly one CREATE parks and never resumes**, while every other
park in the run pairs with its ack.

### Root cause

An SMB break notification is **pinned to the connection that created the open**
(`open_file->create_conn`; the notification is queued on that conn's thread in
`chimera_smb_lease_break_cb`, `smb_proc_oplock_break.c:362+`).

Under multichannel the **session outlives any single channel**. When a channel
drops, `chimera_smb_conn_free` (`smb_internal.h:2598+`) walks the session's
opens and clears the now-stale `create_conn`, but **leaves any break already in
flight unresolved**. The client is still there on another channel, but it was
never told about the break and can no longer be told on the dead one. The claim
sits at `CHIMERA_CLAIM_BREAK_BREAKING` until the 30 s deadline while a
conflicting CREATE parks on it; the WPTS adapter gives up at 20 s.

Evidence: at teardown the wedged claim reads `break_state 1` (BREAKING) while
every healthy completion reads `3` (REVOKED); the park and the client's 20 s
timeout bracket exactly.

### Reproduction (reliable, ~1 round in 3)

WPTS runs **natively on arm64** here -- see the recipe in the section above and
[[wpts-arm64-sigbus]]. Run the config's five 50-case shards CONCURRENTLY.
Serially it never fails. Use a wrapper copy with `tail -80` replaced by `cat`
(and set `WPTS_PTFCONFIG_DIR`, or the copy cannot find its ptfconfigs).

### Two fixes tried, both rejected by evidence

| attempt | WPTS | `smb2.durable-open.*_race` (65 tests) |
|---|---|---|
| **A.** settle the break when the notification is DROPPED because the holder conn is already tearing down (`undeliverable` flag in `chimera_smb_lease_break_cb`) | **still fails** (round 4) | **clean 65/65** |
| **B.** at `conn_free`, `chimera_vfs_claim_revoke_breaks()` on opens whose in-flight break this channel carried | **20/20 rounds clean** | **3 failures** |
| **C.** same as B but a new `chimera_vfs_claim_settle_breaks()` acking to `break_needed_mode` instead of revoking | not re-run | **6 failures (worse)** |

A is a real hole but covers only the ordering where the channel dies BEFORE the
break is raised; the dominant ordering is the channel dying just after. It is
regression-clean and is saved at
`<scratchpad>/candidate_undeliverable.c` if someone wants it on its own merits.

**Why B and C regress:** `smb2.durable-open.lease-disconnect-race` asserts that
after a mid-break disconnect and reconnect the holder gets its **full** lease
back -- `lease_response.lease_state was 3 (0x3), expected 7 (0x7)`
(`source4/torture/smb2/durable_open.c:2262`). So on a mid-break channel loss the
break must NOT be applied to the holder. That is in direct tension with
resuming a conflicting CREATE that is waiting on exactly that break.

### The design question that has to be answered first

On a mid-break channel loss, chimera must either abandon the break (durable
reconnect keeps RWH -- what the smbtorture race tests demand) or apply it (the
waiting CREATE proceeds -- what WPTS demands). Doing one unconditionally breaks
the other. The likely reconciliation is to distinguish **whether a conflicting
acquirer is actually parked on that break**: abandon it when nobody is waiting,
apply it when someone is. Neither B nor C made that distinction, which is why
each traded one suite for the other.

**Do not** re-attempt this without running BOTH
`ctest -R 'smbtorture_smb2_durable_.*_memfs'` (65) and the concurrent-shard
WPTS loop. Either one alone will happily report success.

### Left in place

PR #1671 (the park/resume/deadline logging) only -- it is green, it changes no
behaviour, and it is what produced every fact above. All fix attempts are
reverted; `smbtorture_smb2_durable_.*_memfs` is back to 65/65 and
lease/oplock/dirlease to 78/78.


---

## Extended run 34349397050 (2026-09-09 12:09, first run carrying merged #1667)

| item | result |
|---|---|
| **S3 GET reorder fix** | **CONFIRMED.** `boto3_v4_io_uring_streaming` is gone from the failure set. |
| **nfstest family TIMEOUT 900** | applied (cells now report `***Timeout 900.00 sec`, not 300) **but nfstest_alloc on both diskfs backends still exceeds it**, all four shards |
| `diskfs_evict` x2 | still failing (PR #1670 not merged yet) |
| WPTS `..._encryption_s1` | still failing |
| `smb/mbt/oplock_probe_memfs` | **new**, first sighting |

**nfstest_alloc needs a different answer.** Widening to 900 s was right -- it was
being killed at 300 s while still emitting PASS lines -- but 900 s is not
enough either, so this is either a genuine hang or a workload that needs to be
made smaller. The likely cause is already recorded in
[[extended-failures-sqpoll-rootcause]]: the wrapper gives memfs a 16 MiB
filesystem but diskfs ~180 MiB fillable at wsize=4096, so the ENOSPC fill is
roughly 10x the work. Shrinking diskfs device 1 for this test is probably the
real fix; raising the cap again is not.


---

## WPTS replay-create: fourth fix attempt also FAILED (2026-09-09, end of session)

**Attempt D:** at `chimera_smb_conn_free`, collect opens whose in-flight break
the dying channel was carrying and mark those breaks "orphaned" -- a new
`break_orphaned` flag on the claim plus a `chimera_vfs_claim_orphan_breaks()`
verb, with `chimera_vfs_claim_ack_pending()` skipping orphaned breaks. The point
was to release a parked CREATE WITHOUT resolving the break, so the holder keeps
its mode and a durable reconnect still sees its lease (which is what killed
attempts B and C).

It passes both smbtorture suites (durable 65/65 x3, lease/oplock/dirlease
78/78) -- but **it does not fix the bug**: with the orphan path firing 8 times
in a round, the WPTS failure still reproduced.

### MEASUREMENT TRAPS THAT WASTED MOST OF THIS ATTEMPT -- READ BEFORE RE-TRYING

1. **The default wrapper truncates the daemon log to the last 80 lines**
   (`tail -80 "$CHIMERA_LOG"`, and `SESSION_DIR` is `rm -rf`'d). Any park /
   probe count taken from it is meaningless. Use a copy with `cat`, AND set
   `WPTS_PTFCONFIG_DIR` or the copy cannot find its ptfconfigs.
2. **A loop that deletes logs on clean rounds** leaves nothing to grep
   afterwards. Count per round, inside the loop.
3. **Park-rounds are BURSTY**: a round has either ~10 parks or none. Baseline
   fails on the first round that produces parks (observed rounds 1, 2 and 3 in
   separate runs). **A clean round with parks=0 is NOT evidence of a fix** --
   I recorded "20/20 clean" and then "25/25 clean" on runs that contained no
   park-rounds at all. Judge a fix only on rounds where parks > 0.
4. Always run a **control** that keeps the incidental changes (stack buffers,
   scan loops) but removes the actual logic. ~1.2 KB of extra stack in the
   teardown path under ASan is enough to move this race. My control (buffers +
   collection kept, predicate + orphan call removed) failed on round 2, which
   is what ruled the perturbation out.

### Where that leaves the diagnosis

Confirmed: exactly one CREATE parks and never resumes in a failing round, and
the break is still `BREAKING` when the connection finally tears down. Orphaning
the breaks that the dying channel carried is NOT sufficient, so the
unanswerable break is not (only) the one pinned to the channel that dropped.
The next step is to log, at the moment a break is raised, WHICH connection it
was addressed to and whether that connection is still live, and correlate that
with the one park that never resumes -- the park/resume logging in PR #1671
gives the second half of that already.

All four attempts are reverted; the branch carries only the logging commit.
