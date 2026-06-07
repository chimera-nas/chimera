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

## Known compliance gaps (surfaced by the suite, not fixed here)

These are genuine chimera behaviours the suite flags:

- **SigV4 canonicalization of special characters**: object keys containing
  spaces, `+`, `%`, or non-ASCII fail with `SignatureDoesNotMatch` — the
  server's canonical-URI percent-encoding does not match the AWS rules clients
  sign with.
- **Listing semantics**: no `Delimiter`/`CommonPrefixes`, no pagination
  (`MaxKeys`/`Marker`/`ContinuationToken` are ignored, `IsTruncated` is always
  false), no `KeyCount`, no `EncodingType`.
- **Odd keys break storage/cleanup**: keys with a trailing slash (`dir/`) or a
  `.`/`..` path segment are stored/looked-up inconsistently (`BucketNotEmpty` /
  `InternalError` on cleanup).
- **Whole feature areas unimplemented**: versioning, ACLs, tagging, lifecycle,
  SSE/encryption, object lock & retention, bucket policy, CORS, website,
  POST-object, most metadata/conditional-request semantics.

## Results

Full `test_s3.py` run (838 cases, memfs backend). Because several requests
**crash or wedge the whole daemon** (see below), a naive single-daemon run is
useless — the first bad test kills the daemon and every later test fails on
connection-refused. The numbers below come from a **per-test-isolated** run: each
test gets its own freshly started daemon in its own network namespace, with a
hard 50 s timeout, so a crash/hang only loses that one test.

| Outcome  | Count | % |
|----------|------:|----:|
| passed   |  146  | 17% |
| failed   |  590  | 70% |
| skipped  |   94  | 11% |
| hung     |    8  |  1% |
| **total**| **838** | |

Per feature area (pass / fail / hang / skip):

| Area | pass | fail | hang | skip |
|------|----:|----:|----:|----:|
| encryption / SSE      | 26 | 124 | 0 | 0 |
| listing               | 16 |  74 | 0 | 1 |
| ACL                   |  5 |  55 | 0 | 0 |
| versioning            |  0 |  51 | 0 | 3 |
| object lock/retention |  3 |  41 | 0 | 0 |
| multipart             | 11 |  23 | 0 | 1 |
| object basics         | 14 |  16 | 3 | 0 |
| post-object (form)    | 12 |  19 | 0 | 0 |
| tagging               |  3 |  27 | 0 | 0 |
| bucket basics         | 16 |  13 | 0 | 0 |
| bucket policy         |  5 |  19 | 0 | 2 |
| lifecycle             |  7 |  17 | 3 | 11 |
| copy                  |  8 |  11 | 0 | 0 |
| conditional requests  |  7 |   7 | 0 | 0 |
| CORS                  |  0 |  12 | 0 | 0 |
| ranged get            |  2 |   5 | 2 | 0 |
| checksum              |  0 |   9 | 0 | 0 |
| (other / misc)        | ~17| ~80 | 0 | ~76 |

What passes is the core object/bucket data path: basic Create/Delete/Head
bucket, PUT/GET/HEAD/DELETE object, atomic overwrite (1–4 MB), multipart
lifecycle (create/upload/list/complete/abort), CopyObject, a chunk of
conditional-request and listing edge cases, and DNS-style bucket-name
validation. Everything that needs an unimplemented feature fails.

### Daemon crashes / hangs found (robustness bugs)

Getting the suite to even run end-to-end surfaced several requests that abort or
wedge the entire daemon — each is a remote-triggerable DoS:

- **ETag-on-non-object assert (crashed the daemon), fixed here.** `GET`/`HEAD`
  on a key that resolves to a directory, or to any entry whose lookup lacks
  size/mtime, fatally asserted in `chimera_s3_compute_etag`. Now returns
  `NoSuchKey`. (This fired from multiple call sites; all routed through the
  hardened `chimera_s3_get_lookup_callback`.)
- **Busy-loop hang (still open).** 8 cases hang until the client/timeout kills
  them — the daemon spins at ~100% on one thread and never replies:
  `test_atomic_{write,read,dual_write}_8mb` (an 8 MB object data-path hang — the
  1 MB/4 MB variants pass), `test_ranged_request_skip_leading_bytes_response_code`
  and `..._return_trailing_bytes_response_code` (ranged-GET edge cases), and the
  three `lifecycle*_expiration` cases. These need a separate fix.

### Top non-crash failure reasons

- **SigV4 special-character signatures**: keys with spaces, `+`, `%`, or
  non-ASCII → `SignatureDoesNotMatch` (canonical-URI encoding mismatch). This
  alone fails many object/listing/copy cases that use such keys.
- **Listing**: no delimiter/`CommonPrefixes`, no pagination
  (`MaxKeys`/markers/`ContinuationToken` ignored, `IsTruncated` always false),
  no `KeyCount`, no `EncodingType`.
- **Unimplemented features return wrong/oversimplified responses** (or `501`):
  versioning, ACLs (always returns a fixed owner/grant or none), tagging,
  lifecycle, SSE, object lock, bucket policy, CORS, website, most metadata and
  conditional semantics.

### Reproducing

The committed ctest (`chimera/server/s3/ceph_s3tests`) runs the
`passing_tests.txt` allowlist against a single daemon as a regression guard.
143 of the 146 isolated-passing cases also pass in a shared-daemon session and
make up the allowlist; the other 3 (`test_object_put_authenticated`,
`test_object_write_with_chunked_transfer_encoding`,
`test_lifecycle_expiration_tags1`) pass only in isolation — they depend on
cross-test state that leaks because chimera has no per-credential/account
isolation (all access keys share one namespace), so they are excluded from the
gate. The full breakdown above is reproduced with the per-test-isolated harness
described in `src/server/s3/tests/ceph/README.md`.
