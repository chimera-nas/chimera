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
# The trace corpus is generated from the configs in configs/: each one binds
# the s3 model to a batch list and to the set of KNOWN DEVIATIONS the server
# under test still carries, and specs_corpus() turns it into a cell with a
# trace directory of its own that the replay ctests read in full.  There are
# two, because the multipart flavors have to be replayed at the AWS minimum
# part size and the rest do not.
#
# There is no deviation registry in the harness.  chimera's five documented
# divergences from AWS S3 are branches in the model (ext/specs/quint/s3/s3.qnt,
# gated on the DEVS set), declared with citations in that family's
# corpus.schema.json, and named by each config here.  So the expectation in a
# trace generated for this cell already IS what chimera does, replay is an
# exact match, and two mechanisms keep the claims from decaying:
#
#  - each cell's STRICT TWIN (same batches, every deviation off).  Its
#    failures are the conformance debt, re-measured on every extended run
#    rather than remembered in C.  Reporting, not gating.
#  - the LIVENESS gate, which fails a cell that enables a deviation its own
#    corpus never exercises -- a deviation that has been fixed, or coverage
#    that was quietly lost.  That one IS a quick-tier gate.
#
# The pure-C probe needs no corpus and always runs.

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

# Smoke probe: the surface the trace corpus does not reach -- SigV4/SigV2
# authentication and its failure codes, virtual-host addressing, aws-chunked
# body framing, the credential cache's synthetic clock, per-key identity
# (#494), and the listing corner cases that turn on the backing filesystem
# rather than on the object namespace the model holds.  It no longer asserts
# the deviations: the strict twins and the liveness gate below do that
# generically, per cell.  Needs no trace corpus.
add_test(NAME chimera/server/s3/mbt/probe_memfs
    COMMAND $<TARGET_FILE:s3_mbt_probe>)
set_tests_properties(chimera/server/s3/mbt/probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_probe_memfs"
    LABELS "quint;s3_mbt;memfs"
    TIMEOUT 60)

if(NOT CHIMERA_MBT_AVAILABLE)
    message(STATUS "s3 MBT replay ctests skipped (no specs trace corpus)")
    return()
endif()

# The two corpora.  s3/base is the six default-block-size batches; s3/multipart
# is the two stepMultipart batches, replayed at 5 MiB.  Both are generated in
# full and replayed in full -- the split is the reason the multipart traces
# used to be carved out of one directory by filename prefix.
specs_corpus(NAME           s3/base
             FAMILY         s3
             CONFIG         ${CMAKE_CURRENT_SOURCE_DIR}/configs/base.json
             OUT_DIR        S3_BASE_TRACES
             STRICT_OUT_DIR S3_BASE_STRICT)
specs_corpus(NAME           s3/multipart
             FAMILY         s3
             CONFIG         ${CMAKE_CURRENT_SOURCE_DIR}/configs/multipart.json
             OUT_DIR        S3_MULTIPART_TRACES
             STRICT_OUT_DIR S3_MULTIPART_STRICT)

set(SPECS_DEVLIVENESS ${CMAKE_SOURCE_DIR}/ext/specs/tools/devliveness.py)

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
# encoding assumes.  That is what the s3/multipart cell is: its own corpus,
# replayed whole, rather than the other flavors filtered out of a shared
# directory by filename prefix.
add_test(NAME chimera/server/s3/mbt/batch_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_BASE_TRACES})
set_tests_properties(chimera/server/s3/mbt/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_memfs"
    LABELS "quint;s3_mbt;memfs"
    TIMEOUT 300)

add_test(NAME chimera/server/s3/mbt/batch_multipart_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_MULTIPART_TRACES}
            --block-size 5242880)
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
#    e.g. s3_stepList_128_0x1_3 step 13: DeleteObjects' parent lookup ESTALEs,
#    the object survives on disk).  Until the batch-delete masking fix,
#    that surfaced as ghost objects "resurrecting" after a reported
#    <Deleted>; it now surfaces as honest InternalError entries.  With the
#    caches disabled (S3_MBT_DISABLE_CACHES=1) the failure moves earlier (a
#    PUT's final getattr loses its attributes), so the fault is in the
#    passthrough module, not the cache layer.  Run manually with
#    --backend linux/io_uring; S3_MBT_KEEP_SCRATCH=1 preserves the backing
#    tree for post-mortem.
add_test(NAME chimera/server/s3/mbt/batch_diskfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_BASE_TRACES}
            --backend diskfs)
set_tests_properties(chimera/server/s3/mbt/batch_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_diskfs"
    LABELS "quint;s3_mbt;diskfs"
    TIMEOUT 300)

# Previously withheld: PutObject's publish could double-fire when the
# body drain outran the async metadata xattr chain (or vice versa), and
# at multipart scale on diskfs the second link_at's failure path crashed
# into the first's completion (~1-in-2 on s3mp_stepMultipart_64_0x11_1).
# Fixed by the exactly-once publish handshake in s3_put.c (meta_pending
# gate + published latch); debug-level replay made the window wide
# enough to pin the real cause, which was never diskfs's completion
# threading.
add_test(NAME chimera/server/s3/mbt/batch_multipart_diskfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_MULTIPART_TRACES}
            --backend diskfs --block-size 5242880)
set_tests_properties(chimera/server/s3/mbt/batch_multipart_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_multipart_diskfs"
    LABELS "quint;s3_mbt;diskfs"
    TIMEOUT 300)

if(CAIRN_ENABLED)
    add_test(NAME chimera/server/s3/mbt/batch_cairn
        COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_BASE_TRACES}
                --backend cairn)
    set_tests_properties(chimera/server/s3/mbt/batch_cairn PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_cairn"
        LABELS "quint;s3_mbt;cairn"
        TIMEOUT 300)

    add_test(NAME chimera/server/s3/mbt/batch_multipart_cairn
        COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_MULTIPART_TRACES}
                --backend cairn --block-size 5242880)
    set_tests_properties(chimera/server/s3/mbt/batch_multipart_cairn PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_multipart_cairn"
        LABELS "quint;s3_mbt;cairn"
        TIMEOUT 300)
endif()

# The base corpus signed with AWS Signature V2 ("AWS <key>:<hmac-sha1>"),
# exercising the server's V2 verifier end to end.  Multipart is left out
# only for the block-size reason above -- V2 signing itself covers every
# op (the server's V2 canonical resource carries no subresources).
add_test(NAME chimera/server/s3/mbt/batch_sigv2_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_BASE_TRACES}
            --sigv2)
set_tests_properties(chimera/server/s3/mbt/batch_sigv2_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_batch_sigv2_memfs"
    LABELS "quint;s3_mbt;memfs"
    TIMEOUT 300)

# ---- the deviation machinery ------------------------------------------------

# The strict twin of each cell: the identical batch list regenerated with
# DEVS = Set(), so every expectation is the AWS behavior.  These are expected
# to FAIL for as long as chimera carries the divergences listed in the two
# configs -- that is the point.  Their output is the conformance debt,
# re-measured on every run, which is what a deviation registry could never do:
# a registry entry keeps forgiving after the bug is fixed, and forgives the
# same divergence somewhere it was never meant to.  Reporting, not gating, so
# they go in the extended tier.
set(CHIMERA_TEST_TIER extended)

add_test(NAME chimera/server/s3/mbt/strict_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_BASE_STRICT})
set_tests_properties(chimera/server/s3/mbt/strict_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_strict_memfs"
    LABELS "quint;s3_mbt;memfs;strict"
    TIMEOUT 600)

add_test(NAME chimera/server/s3/mbt/strict_multipart_memfs
    COMMAND $<TARGET_FILE:s3_mbt_replay> --trace-dir ${S3_MULTIPART_STRICT}
            --block-size 5242880)
set_tests_properties(chimera/server/s3/mbt/strict_multipart_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=s3_strict_multipart_memfs"
    LABELS "quint;s3_mbt;memfs;strict"
    TIMEOUT 600)

set(CHIMERA_TEST_TIER quick)

# The liveness gate: a cell may not enable a deviation its own corpus never
# exercises.  Either the deviation is dead (retire it from the config and
# delete its branch in the model) or the generator quietly lost the coverage
# that used to reach it.  Both need a human, so both fail here -- in the quick
# tier, because a stale claim is a blindfold from the moment it goes stale.
if(Python3_FOUND)
    add_test(NAME chimera/server/s3/mbt/devliveness_base
        COMMAND ${Python3_EXECUTABLE} ${SPECS_DEVLIVENESS}
                ${CMAKE_CURRENT_SOURCE_DIR}/configs/base.json
                ${S3_BASE_TRACES})
    set_tests_properties(chimera/server/s3/mbt/devliveness_base PROPERTIES
        LABELS "quint;s3_mbt;devliveness" TIMEOUT 120)

    add_test(NAME chimera/server/s3/mbt/devliveness_multipart
        COMMAND ${Python3_EXECUTABLE} ${SPECS_DEVLIVENESS}
                ${CMAKE_CURRENT_SOURCE_DIR}/configs/multipart.json
                ${S3_MULTIPART_TRACES})
    set_tests_properties(chimera/server/s3/mbt/devliveness_multipart PROPERTIES
        LABELS "quint;s3_mbt;devliveness" TIMEOUT 120)
else()
    message(STATUS "s3 MBT deviation-liveness gate skipped (no python3)")
endif()
