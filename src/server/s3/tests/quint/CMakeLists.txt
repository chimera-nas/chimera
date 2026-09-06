# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# These are the model-based tests, so they make up the quick tier that plain
# `ctest` runs.  See the test-tier block in the top-level CMakeLists.txt.
set(CHIMERA_TEST_TIER quick)

# S3 model-based tests: the Quint specification and its trace generation live
# in the chimera-nas/specs repo (submodule ext/specs, quint/s3); this
# directory owns only the C replay harness that drives that corpus against
# the live chimera server.  The replayer (s3_mbt_replay.c) embeds the chimera
# server (S3 enabled, memfs backend) in the test process and issues each
# trace's requests through the libevpl HTTP/1.1 client over the inproc
# transport, SigV4-signed like any AWS SDK would, failing on any divergence
# from the model's expected response.  No test binds a port, needs a network
# namespace or resource lock, or spawns a daemon -- every trace runs fully
# parallel on any host.
#
# The trace corpus is resolved once, at the top level, by
# cmake/SpecsBundle.cmake into SPECS_BUNDLE_DIR (a fetched prebuilt bundle
# when the submodule is clean, else an in-tree build of ext/specs).  The
# replay ctest points --trace-dir at ${SPECS_BUNDLE_DIR}/s3; the pure-C
# smoke/deviation probe needs no corpus and always runs.

# The C replayer + probe: chimera server + libevpl HTTP client in one process
# over the inproc transport (see s3_mbt_common.h).  They build regardless of
# whether a trace corpus is available.  OpenSSL::Crypto is linked directly
# for the harness's own SigV4 signer (the same library the server verifies
# with).
add_executable(s3_mbt_probe s3_mbt_probe.c)
target_include_directories(s3_mbt_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(s3_mbt_probe chimera_server chimera_metrics OpenSSL::Crypto)

add_executable(s3_mbt_replay s3_mbt_replay.c)
target_include_directories(s3_mbt_replay PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(s3_mbt_replay chimera_server chimera_metrics OpenSSL::Crypto jansson)

# Smoke + deviation probe: walks the modeled surface once end-to-end and
# asserts each documented chimera deviation from official AWS S3 behavior
# (see the deviation registry in s3_mbt_replay.c) still reproduces.  Goes
# red when chimera is fixed -- the signal to retire a deviation.  Needs no
# trace corpus.
add_test(NAME chimera/server/s3/mbt/probe_memfs
    COMMAND $<TARGET_FILE:s3_mbt_probe>)
set_tests_properties(chimera/server/s3/mbt/probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_probe_memfs"
    LABELS "quint;s3_mbt;memfs"
    TIMEOUT 60)

if(NOT SPECS_BUNDLE_AVAILABLE)
    message(STATUS "s3 MBT replay ctests skipped (no specs trace corpus)")
    return()
endif()

# Batched replays: a single replayer process opens the server + HTTP client
# once and replays every trace in the corpus back-to-back, standing up a
# fresh, uniquely-named memfs (mkfs) per trace -- and dropping the
# bucket-map entries and aborting the in-flight uploads the trace created
# -- so no server-side state can leak across traces while the per-process
# server/client init is amortized over the whole corpus.
#
# stepMultipart traces carry multipart uploads and are replayed separately
# with a 5 MiB block size: AWS's minimum part size makes every non-empty
# part legal at that scale, which is the contract the model's EntityTooSmall
# encoding assumes.  They are excluded from the normal-block-size batches by
# filename prefix (there is no include-prefix, so the multipart batch
# excludes every OTHER flavor instead).
add_test(NAME chimera/server/s3/mbt/batch_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
            --exclude-prefix stepMultipart_)
set_tests_properties(chimera/server/s3/mbt/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_memfs"
    LABELS "quint;s3_mbt;memfs"
    TIMEOUT 300)

add_test(NAME chimera/server/s3/mbt/batch_multipart_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
            --block-size 5242880
            --exclude-prefix step_ --exclude-prefix stepData_
            --exclude-prefix stepList_ --exclude-prefix stepBucket_
            --exclude-prefix stepTag_)
set_tests_properties(chimera/server/s3/mbt/batch_multipart_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_multipart_memfs"
    LABELS "quint;s3_mbt;memfs"
    TIMEOUT 300)

# The same corpora replayed against the on-disk backends.  The corpus is
# protocol-level and backend-agnostic, so a divergence here is a genuine
# backend deviation from the model memfs already satisfies.  Each replay
# self-provisions its scratch (device image / rocksdb dir) under its own
# mkdtemp session dir.  Gated on the storage each backend needs.
#
# Withheld batches, and why:
#  - linux/io_uring (passthrough): need a name_to_handle_at-capable scratch
#    filesystem ($CHIMERA_MBT_SCRATCH; overlayfs/tmpfs skip 77) and an
#    io_uring-capable environment, and both exhibit intermittent ESTALE
#    path lookups for directories that demonstrably exist (deterministic on
#    e.g. stepList_128_0x1_3 step 13: DeleteObjects' parent lookup ESTALEs,
#    the object survives on disk).  Until the batch-delete masking fix,
#    that surfaced as ghost objects "resurrecting" after a reported
#    <Deleted>; it now surfaces as honest InternalError entries.  With the
#    caches disabled (S3_MBT_DISABLE_CACHES=1) the failure moves earlier (a
#    PUT's final getattr loses its attributes), so the fault is in the
#    passthrough module, not the cache layer.  Run manually with
#    --backend linux/io_uring; S3_MBT_KEEP_SCRATCH=1 preserves the backing
#    tree for post-mortem.
add_test(NAME chimera/server/s3/mbt/batch_diskfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
            --backend diskfs --exclude-prefix stepMultipart_)
set_tests_properties(chimera/server/s3/mbt/batch_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_diskfs"
    LABELS "quint;s3_mbt;diskfs"
    TIMEOUT 300)

# Previously withheld: PutObject's publish could double-fire when the
# body drain outran the async metadata xattr chain (or vice versa), and
# at multipart scale on diskfs the second link_at's failure path crashed
# into the first's completion (~1-in-2 on stepMultipart_64_0x11_1).
# Fixed by the exactly-once publish handshake in s3_put.c (meta_pending
# gate + published latch); debug-level replay made the window wide
# enough to pin the real cause, which was never diskfs's completion
# threading.
add_test(NAME chimera/server/s3/mbt/batch_multipart_diskfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
            --backend diskfs --block-size 5242880
            --exclude-prefix step_ --exclude-prefix stepData_
            --exclude-prefix stepList_ --exclude-prefix stepBucket_
            --exclude-prefix stepTag_)
set_tests_properties(chimera/server/s3/mbt/batch_multipart_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_multipart_diskfs"
    LABELS "quint;s3_mbt;diskfs"
    TIMEOUT 300)

if(CAIRN_ENABLED)
    add_test(NAME chimera/server/s3/mbt/batch_cairn
        COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
                --backend cairn --exclude-prefix stepMultipart_)
    set_tests_properties(chimera/server/s3/mbt/batch_cairn PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_cairn"
        LABELS "quint;s3_mbt;cairn"
        TIMEOUT 300)

    # Same corpus with cairn forced non-atomic (grouping compounds commit
    # op-by-op): proves no S3 flow relies on cairn's compound atomicity
    # (the best-effort contract's Pillai defense).
    add_test(NAME chimera/server/s3/mbt/batch_cairn_nonatomic
        COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
                --backend cairn --exclude-prefix stepMultipart_)
    set_tests_properties(chimera/server/s3/mbt/batch_cairn_nonatomic PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_cairn_nonatomic;CHIMERA_CAIRN_NONATOMIC=1"
        LABELS "quint;s3_mbt;cairn"
        TIMEOUT 300)

    add_test(NAME chimera/server/s3/mbt/batch_multipart_cairn
        COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
                --backend cairn --block-size 5242880
                --exclude-prefix step_ --exclude-prefix stepData_
                --exclude-prefix stepList_ --exclude-prefix stepBucket_
                --exclude-prefix stepTag_)
    set_tests_properties(chimera/server/s3/mbt/batch_multipart_cairn PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_multipart_cairn"
        LABELS "quint;s3_mbt;cairn"
        TIMEOUT 300)
endif()

# The same corpus signed with AWS Signature V2 ("AWS <key>:<hmac-sha1>"),
# exercising the server's V2 verifier end to end.  Multipart is excluded
# only for the block-size reason above -- V2 signing itself covers every
# op (the server's V2 canonical resource carries no subresources).
add_test(NAME chimera/server/s3/mbt/batch_sigv2_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${SPECS_BUNDLE_DIR}/s3
            --sigv2 --exclude-prefix stepMultipart_)
set_tests_properties(chimera/server/s3/mbt/batch_sigv2_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_sigv2_memfs"
    LABELS "quint;s3_mbt;memfs"
    TIMEOUT 300)
