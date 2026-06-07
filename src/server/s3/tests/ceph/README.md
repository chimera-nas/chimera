<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: LGPL-2.1-only
-->

# ceph/s3-tests against chimera

[ceph/s3-tests](https://github.com/ceph/s3-tests) is a broad, unofficial S3 API
compatibility suite (boto3-based, ~840 cases in `test_s3.py`). The devcontainer
clones it into a self-contained venv at `/opt/s3-tests` (see
`.devcontainer/Dockerfile`, pinned `S3TESTS_REF`).

## Running

`run_ceph_s3tests.sh` starts a chimera daemon (memfs backend, three S3
credentials, a runtime bucket root at `/share`) and drives pytest against it.

```bash
# the curated allowlist of currently-passing cases (what ctest runs)
src/server/s3/tests/ceph/run_ceph_s3tests.sh \
    --chimera build/Release/src/daemon/chimera

# a single test / a -k expression / the whole file
src/server/s3/tests/ceph/run_ceph_s3tests.sh \
    --chimera build/Release/src/daemon/chimera \
    s3tests/functional/test_s3.py::test_bucket_list_empty

# the entire suite (most cases fail; see the compliance report)
src/server/s3/tests/ceph/run_ceph_s3tests.sh \
    --chimera build/Release/src/daemon/chimera \
    s3tests/functional/test_s3.py
```

It exits 77 (ctest "skip") when the suite venv is absent.

## ctest

`CMakeLists.txt` registers `chimera/server/s3/ceph_s3tests` (gated on
`CHIMERA_NETNS_TESTING` and the suite being installed). It runs the allowlist in
`passing_tests.txt` so CI stays green and regressions in the currently-passing
subset are caught. The full suite is intentionally **not** gated — it is a
compliance-tracking tool, not a pass/fail gate.

## Compliance status

chimera implements the object data path (PUT/GET/HEAD/DELETE, ranged GET,
multipart, CopyObject, batch DeleteObjects, ListObjects v1/v2, streaming
uploads) plus the minimal bucket lifecycle this suite needs to run at all
(CreateBucket / DeleteBucket / ListBuckets / HeadBucket / ListObjectVersions).
The large majority of ceph cases still fail: chimera has no versioning, ACLs,
tagging, lifecycle, encryption, object lock, bucket policy, CORS, or website
support, and its listing lacks delimiter/CommonPrefixes, pagination
(MaxKeys/markers/ContinuationToken), and `KeyCount`. See
`docs/design/s3_ceph_compliance.md` for the full breakdown.
