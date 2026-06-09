<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: LGPL-2.1-only
-->

# S3 compliance: ceph/s3-tests

This documents wiring the [ceph/s3-tests](https://github.com/ceph/s3-tests)
compatibility suite into chimera and the current state of S3 compliance it
reveals. The suite is boto3-based and exercises ~840 cases in `test_s3.py`
covering the full AWS S3 surface (versioning, ACLs, tagging, lifecycle,
encryption, object lock, policy, CORS, website, listing semantics, multipart,
conditional requests, …).

How to run it: `src/server/s3/tests/ceph/README.md`.

## What had to be built just to run the suite

chimera previously served only **statically configured** buckets and had no
service/bucket-level operations, so the suite could not get past its own
session fixture (which lists and creates buckets). The following were added —
these are *enabling* changes, not full feature work:

- **Runtime bucket lifecycle** (`s3_bucket.c`): `CreateBucket` (PUT `/bucket`),
  `DeleteBucket` (DELETE `/bucket`), `ListBuckets` (GET `/`), `HeadBucket`
  (HEAD `/bucket`). New buckets are materialized as directories under a
  configurable **bucket root** (`server.s3_bucket_root`, default = the first
  configured mount) and registered in the bucket map at runtime.
- **Bucket-root request routing** (`s3.c`): requests with an empty object key
  are dispatched to the bucket handlers (and a bare `GET /bucket` is treated as
  ListObjects v1).
- **`ListObjectVersions`** (GET `/bucket?versions`): chimera has no versioning,
  so every object is reported as a single latest `null` version. This is what
  the suite's per-test cleanup uses to enumerate objects.
- **Recursive `DeleteBucket`**: S3 keys are flat but chimera stores `a/b/c`
  as a real directory tree, so deleting objects leaves empty key-path
  directories behind. DeleteBucket now walks the bucket, returns
  `BucketNotEmpty` if any object (file) remains, and otherwise removes the
  leftover empty directories before the bucket itself.

### Bugs found and fixed (blocking the run)

- **Crash on bucket-root GET/HEAD**: a request for an empty object key drove the
  object GET path, which fatally asserted building an ETag from a directory's
  (absent) attributes (`s3_etag.h`). The whole daemon aborted. Fixed by routing
  bucket-root requests away from object GET.
- **`ListObjects` returned a leading slash** on every key (`/foo` instead of
  `foo`): `chimera_vfs_find` roots walked paths with `/`, and the S3 list
  handler emitted them verbatim. Real clients then address/delete the wrong key.
  Fixed in `s3_list.c`.
- **`s3_port` config was ignored** (the daemon always bound 5000). Now honored
  (`server.s3_port`).

## Scope

chimera exposes S3 as a view of a real filesystem shared with NFS and SMB, not a
standalone object store. The goal is not 100% AWS compatibility — only the
behaviours that are coherent for a filesystem-backed, multi-protocol bucket. So
listing (which maps to directory walks), object data/range I/O, multipart, copy,
and bucket lifecycle are implemented; object-store-only features (versioning,
object lock, lifecycle expiration, S3 bucket policy/ACL/encryption enforcement,
CORS, website, replication) are not, and S3 requests that configure them are
accepted-and-ignored or rejected rather than honoured.

## Listing (implemented)

ListObjects V1/V2 and ListObjectVersions now support `prefix`, `delimiter` with
`CommonPrefixes` rollup (folder-style for `/`, arbitrary delimiters over keys),
lexicographic ordering, `max-keys` with `IsTruncated`, V1 `marker`/`NextMarker`,
V2 `continuation-token`/`NextContinuationToken` (opaque base64url) and
`start-after`, `KeyCount`, and `encoding-type=url`. Pagination is stateless
(each page re-walks and re-sorts the matched subtree, then slices past the
token) — O(n) per page; acceptable for the in-memory-first stage, revisit with a
cursor if a huge flat bucket bites.

## Known compliance gaps (genuine chimera behaviours the suite flags)

- **SigV4 canonicalization of special characters**: object keys containing
  spaces, `+`, `%`, or non-ASCII fail with `SignatureDoesNotMatch` — the
  server's canonical-URI percent-encoding does not match the AWS rules clients
  sign with. (Affects many object/listing/copy cases that use such keys.)
- **Odd keys break storage/cleanup**: keys with a trailing slash (`dir/`) or a
  `.`/`..` path segment are stored/looked-up inconsistently (`BucketNotEmpty` /
  `InternalError` on cleanup).
- **Bucket sub-resources are accepted but not enforced**: PUT of an
  ACL/policy/lifecycle/encryption/etc. config returns 200 and is silently
  ignored (a filesystem bucket has no place to honour them). This keeps S3
  clients working but means security-relevant configs are **not** enforced —
  flagged for review.
- **Whole feature areas unimplemented**: versioning, ACLs, tagging, lifecycle,
  SSE/encryption, object lock & retention, bucket policy enforcement, CORS,
  website, POST-object, most metadata/conditional-request semantics.

## Results

Full `test_s3.py` run (838 cases, memfs backend). Because several requests
**crash or wedge the whole daemon** (see below), a naive single-daemon run is
useless — the first bad test kills the daemon and every later test fails on
connection-refused. The numbers below come from a **per-test-isolated** run: each
test gets its own freshly started daemon in its own network namespace, with a
hard 50 s timeout, so a crash/hang only loses that one test.

| Outcome  | Count | % |
|----------|------:|----:|
| passed   |  201  | 24% |
| failed   |  540  | 64% |
| skipped  |   94  | 11% |
| hung     |    3  | <1% |
| **total**| **838** | |

After the crash/hang fixes below, a full run reports **no daemon crashes**, and
the only remaining "hangs" are 3 lifecycle tests that `time.sleep(~70 s)` waiting
for object expiration chimera doesn't implement (the daemon is idle) — a feature
gap, not a server hang.

Per feature area (pass / fail / hang / skip):

| Area | pass | fail | hang | skip |
|------|----:|----:|----:|----:|
| encryption / SSE      | 26 | 124 | 0 | 0 |
| listing               | 66 |  24 | 0 | 1 |
| ACL                   |  5 |  55 | 0 | 0 |
| versioning            |  0 |  51 | 0 | 3 |
| object lock/retention |  3 |  41 | 0 | 0 |
| multipart             | 11 |  23 | 0 | 1 |
| object basics         | 17 |  16 | 0 | 0 |
| post-object (form)    | 12 |  19 | 0 | 0 |
| tagging               |  3 |  27 | 0 | 0 |
| bucket basics         | 15 |  14 | 0 | 0 |
| bucket policy         |  5 |  19 | 0 | 2 |
| lifecycle             |  7 |  17 | 3 | 11 |
| copy                  |  8 |  11 | 0 | 0 |
| conditional requests  |  7 |   7 | 0 | 0 |
| CORS                  |  0 |  12 | 0 | 0 |
| ranged get            |  4 |   5 | 0 | 0 |
| checksum              |  0 |   9 | 0 | 0 |
| (other / misc)        | ~17| ~80 | 0 | ~76 |

What passes is the core object/bucket data path: Create/Delete/Head bucket,
PUT/GET/HEAD/DELETE object, ranged and atomic I/O, multipart lifecycle, CopyObject,
DNS-style bucket-name validation, and the bulk of the listing surface
(delimiter/prefix/pagination/v1+v2/encoding — 66/91). Everything that needs an
unimplemented object-store feature fails.

### Daemon crashes / hangs found (robustness bugs) — fixed

Getting the suite to run end-to-end surfaced several requests that aborted or
wedged the entire daemon — each a remote-triggerable DoS. All are fixed; a full
run now reports zero daemon crashes and zero server-side hangs.

- **ETag-on-non-object assert (crash).** `GET`/`HEAD` on a key that resolves to
  a directory, or to any entry whose lookup lacks size/mtime, fatally asserted
  in `chimera_s3_compute_etag`. Fixed: `chimera_s3_get_lookup_callback` now
  returns `NoSuchKey` for non-regular-file / attr-incomplete lookups.
- **Open-ended / suffix range infinite loop (hang).** A ranged GET with an
  open-ended `bytes=N-` parsed to `length = -1`, and a suffix `bytes=-N` to
  `offset = -1`; these sentinels were never resolved against the object size, so
  `file_left` wrapped to a huge unsigned value and `chimera_s3_get_send` looped
  forever issuing reads at ever-growing offsets (spinning one thread at 100%).
  Fixed: `chimera_s3_get_lookup_callback` resolves both forms against the object
  size and clamps to EOF once the size is known. This hit `boto3`'s
  `download_fileobj` (≥ 8 MB switches to ranged/multipart download — hence the
  `atomic_*_8mb` cases hung while 1/4 MB passed) and the explicit
  `ranged_request_skip_leading_bytes` / `_return_trailing_bytes` tests — 5 cases,
  now passing.

The 3 remaining timeouts (`lifecycle*_expiration`) are **not** daemon hangs:
the test sleeps ~70 s waiting for object expiration that needs the unimplemented
lifecycle feature.

### Top non-crash failure reasons

- **SigV4 special-character signatures**: keys with spaces, `+`, `%`, or
  non-ASCII → `SignatureDoesNotMatch` (canonical-URI encoding mismatch). This
  alone fails many object/listing/copy cases that use such keys.
- **Listing edge cases still open**: `max-keys=blah` should be `400
  InvalidArgument`, the `allow-unordered` RGW extension, and a few owner/ACL-in-
  listing expectations (need ACL support).
- **Unimplemented features return wrong/oversimplified responses** (or `501`):
  versioning, ACLs (always returns a fixed owner/grant or none), tagging,
  lifecycle, SSE, object lock, bucket policy, CORS, website, most metadata and
  conditional semantics.

### Reproducing

The committed ctest (`chimera/server/s3/ceph_s3tests`) runs the
`passing_tests.txt` allowlist against a single daemon as a regression guard.
198 of the 201 isolated-passing cases also pass in a shared-daemon session and
make up the allowlist; the other 3 (`test_object_put_authenticated`,
`test_object_write_with_chunked_transfer_encoding`,
`test_lifecycle_expiration_tags1`) pass only in isolation — they depend on
cross-test state that leaks because chimera has no per-credential/account
isolation (all access keys share one namespace), so they are excluded from the
gate. The full breakdown above is reproduced with the per-test-isolated harness
described in `src/server/s3/tests/ceph/README.md`.
