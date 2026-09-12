# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# These are the model-based tests, so they make up the quick tier that plain
# `ctest` runs.  See the test-tier block in the top-level CMakeLists.txt.
set(CHIMERA_TEST_TIER quick)

# NFS model-based tests: the Quint specifications and their trace generation
# live in the chimera-nas/specs repo (submodule ext/specs); this directory owns
# only the C replay harnesses that drive that corpus against the live chimera
# server.  Four suites:
#
#   nfs3_mbt_replay.c     NFSv3           (model family nfs3, 1 config)
#   nfs4_mbt_replay.c     NFSv4.0/4.1/4.2 (model family nfs4, 7 configs: the
#                         three minor versions, the two delegation-serving
#                         variants, the two pNFS metadata-server profiles)
#   nfs_aux_mbt_replay.c  the four protocols around NFSv3 -- portmap/rpcbind,
#                         MOUNT, NLM and NSM (model family nfsaux, 1 config)
#   nfs_drc_mbt_replay.c  the duplicate-request caches of NFSv3 and NFSv4.0
#                         (model family nfsdrc, 2 configs)
#
# Each embeds the chimera server (memfs backend) in the test process and replays
# every trace over libevpl's inproc transport, failing on any divergence.  No
# test binds a port, needs a network namespace or resource lock, or spawns a
# daemon -- every trace runs fully parallel on any host.
#
# The trace corpus is GENERATED, per cell, from the configs in configs/ and the
# models in ext/specs (cmake/Specs.cmake -> specs_corpus()).  A config is the
# unit: it binds every constant its model family declares and carries the batch
# list, so one config fully determines one corpus and the ctest pointed at that
# corpus replays ALL of it.  The pure-C deviation/delegation probes need no
# corpus and always run.

# chimera_metrics is linked by every target here because nfs3_mbt_common.h can
# stand up the Prometheus scrape endpoint on request (mbt_env_opts.metrics_port),
# which the control-plane suite uses to watch protocol counters move.
#
# The C replayers + probes: chimera server + rpc2 client in one process over the
# inproc transport (see nfs3_mbt_common.h, and nfs_aux_mbt_common.h for the
# auxiliary protocols' client half).  They build regardless of whether a trace
# corpus is available.
add_executable(nfs3_mbt_replay nfs3_mbt_replay.c)
target_include_directories(nfs3_mbt_replay PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs3_mbt_replay
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs3_mbt_replay)

add_executable(nfs3_mbt_probe nfs3_mbt_probe.c)
target_include_directories(nfs3_mbt_probe PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs3_mbt_probe
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs3_mbt_probe)

add_executable(nfs4_deleg_probe nfs4_deleg_probe.c)
target_include_directories(nfs4_deleg_probe PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs4_deleg_probe
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs4_deleg_probe)

add_executable(nfs4_named_attr_probe nfs4_named_attr_probe.c)
target_include_directories(nfs4_named_attr_probe PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs4_named_attr_probe
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs4_named_attr_probe)

add_executable(nfs_aux_probe nfs_aux_probe.c)
target_include_directories(nfs_aux_probe PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs_aux_probe
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs_aux_probe)

add_executable(nfs_drc_probe nfs_drc_probe.c)
target_include_directories(nfs_drc_probe PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs_drc_probe
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs_drc_probe)

add_executable(nfs_aux_mbt_replay nfs_aux_mbt_replay.c)
target_include_directories(nfs_aux_mbt_replay PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs_aux_mbt_replay
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs_aux_mbt_replay)

add_executable(nfs_drc_mbt_replay nfs_drc_mbt_replay.c)
target_include_directories(nfs_drc_mbt_replay PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs_drc_mbt_replay
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs_drc_mbt_replay)

add_executable(nfs4_mbt_replay nfs4_mbt_replay.c)
target_include_directories(nfs4_mbt_replay PRIVATE
    ${CMAKE_BINARY_DIR}/src/nfs_common)
target_link_libraries(nfs4_mbt_replay
    chimera_server chimera_nfs_common chimera_metrics jansson)
mbt_add_krb5(nfs4_mbt_replay)

# Deviation probe: asserts each documented chimera deviation (DEVIATIONS.md)
# still reproduces, and that the two fixed deadlocks (LINK/RENAME self-alias)
# do not hang.  Goes red when chimera is fixed -- the signal to retire a
# deviation -- or if a deadlock regresses.  Needs no trace corpus.
add_test(NAME chimera/server/nfs/mbt/deviation_probe_memfs
    COMMAND $<TARGET_FILE:nfs3_mbt_probe> -b memfs)
set_tests_properties(chimera/server/nfs/mbt/deviation_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_deviation_probe_memfs"
    LABELS "quint;nfs3_mbt;memfs"
    TIMEOUT 60)

# The same probe against the on-disk backends.  Most of what it guards is in
# the NFS3 layer and backend-independent, but the stale-filehandle case (F6) is
# each backend's own: they each resolve a handle themselves, and each had to
# learn to tell a handle that no longer names anything from a name that was
# never there.
add_test(NAME chimera/server/nfs/mbt/deviation_probe_diskfs
    COMMAND $<TARGET_FILE:nfs3_mbt_probe> -b diskfs)
set_tests_properties(chimera/server/nfs/mbt/deviation_probe_diskfs PROPERTIES
    LABELS "quint;nfs3_mbt;diskfs"
    TIMEOUT 60)

if(CAIRN_ENABLED)
    add_test(NAME chimera/server/nfs/mbt/deviation_probe_cairn
        COMMAND $<TARGET_FILE:nfs3_mbt_probe> -b cairn)
    set_tests_properties(chimera/server/nfs/mbt/deviation_probe_cairn PROPERTIES
        LABELS "quint;nfs3_mbt;cairn"
        TIMEOUT 60)
endif()

# The passthrough backends (linux, io_uring) mount a host directory instead of
# a named filesystem, and their trees must live on a filesystem that supports
# name_to_handle_at -- tmpfs and overlayfs do not.  The harness roots its
# backing store at $CHIMERA_MBT_SCRATCH (default: the working directory) and
# turns an incapable filesystem into a clean SKIP, so these register
# unconditionally on capable platforms: in CI the ctest working directory is
# a docker volume on the runner's ext4, and a local overlayfs build tree
# skips rather than fails.  The probe additionally guards the
# exclusive-create-retry-exist deviation, which only a passthrough backend
# (whose host filesystem touches the atime the exclusive verifier is stashed
# in) can reproduce.
if(CHIMERA_LINUX)
    add_test(NAME chimera/server/nfs/mbt/deviation_probe_linux
        COMMAND $<TARGET_FILE:nfs3_mbt_probe> -b linux)
    set_tests_properties(chimera/server/nfs/mbt/deviation_probe_linux PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_probe_linux"
        LABELS "quint;nfs3_mbt;linux"
        SKIP_RETURN_CODE 77
        TIMEOUT 60)
endif()

if(CHIMERA_VFS_IO_URING)
    add_test(NAME chimera/server/nfs/mbt/deviation_probe_io_uring
        COMMAND $<TARGET_FILE:nfs3_mbt_probe> -b io_uring)
    set_tests_properties(chimera/server/nfs/mbt/deviation_probe_io_uring PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_probe_io_uring"
        LABELS "quint;nfs3_mbt;io_uring"
        SKIP_RETURN_CODE 77
        TIMEOUT 60)
endif()

# Auxiliary-protocol ground truth: pins which portmap/rpcbind, MOUNT, NLM and
# NSM procedures chimera registers, and the constants their replies carry --
# the facts the nfsaux model's configuration gate encodes.  Needs no corpus.
add_test(NAME chimera/server/nfs/mbtaux/probe_memfs
    COMMAND $<TARGET_FILE:nfs_aux_probe>)
set_tests_properties(chimera/server/nfs/mbtaux/probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsaux_probe_memfs"
    LABELS "quint;nfsaux_mbt;memfs"
    TIMEOUT 120)

# Duplicate-request-cache ground truth: pins the properties the nfsdrc model
# turns into a corpus -- that a retransmit replays, that a different XID,
# procedure, connection or principal does not, what a reconnect does to each of
# the two caches, and the transport facts the model's constants record (one
# address for every in-process peer, a new filehandle per incarnation).  Needs
# no corpus.
#
# Run on every backend, not just memfs.  What a file handle means once its
# object is gone is a property of the VFS, not of one module, and registering
# this against memfs alone hid a five-backends-to-two disagreement about
# exactly that for as long as it has existed.
foreach(dback memfs diskfs cairn linux io_uring)
    if(dback STREQUAL "cairn" AND NOT CAIRN_ENABLED)
        continue()
    endif()
    if(dback STREQUAL "linux" AND NOT CHIMERA_LINUX)
        continue()
    endif()
    if(dback STREQUAL "io_uring" AND NOT CHIMERA_VFS_IO_URING)
        continue()
    endif()
    if(dback STREQUAL "memfs")
        set(_drc_probe_args "")
    else()
        set(_drc_probe_args --backend ${dback})
    endif()

    add_test(NAME chimera/server/nfs/mbtdrc/probe_${dback}
        COMMAND $<TARGET_FILE:nfs_drc_probe> ${_drc_probe_args})
    set_tests_properties(chimera/server/nfs/mbtdrc/probe_${dback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsdrc_probe_${dback}"
        LABELS "quint;nfsdrc_mbt;${dback}"
        SKIP_RETURN_CODE 77
        TIMEOUT 120)
endforeach()

# Delegation policy probe: pins the deterministic grant/recall behavior the
# delegation-enabled instances encode (see nfs4_deleg_probe.c).  Needs no corpus.
add_test(NAME chimera/server/nfs/mbt4/deleg_probe_memfs
    COMMAND $<TARGET_FILE:nfs4_deleg_probe>)
set_tests_properties(chimera/server/nfs/mbt4/deleg_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_deleg_probe_memfs"
    LABELS "quint;nfs4_mbt;multiproto;memfs"
    TIMEOUT 90)

# Named-attribute (OPENATTR) probe: pins the full NFSv4 named-attribute
# lifecycle the corpus never drives -- OPENATTR, OPEN-create/WRITE/READ of an
# attribute, FATTR4_NAMED_ATTR tracking, READDIR/LOOKUP/REMOVE of the attr
# directory -- exercising the shared VFS named-stream storage that SMB ADS also
# projects onto.  Needs no corpus.
add_test(NAME chimera/server/nfs/mbt4/named_attr_probe_memfs
    COMMAND $<TARGET_FILE:nfs4_named_attr_probe>)
set_tests_properties(chimera/server/nfs/mbt4/named_attr_probe_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_named_attr_probe_memfs"
    LABELS "quint;nfs4_mbt;multiproto;memfs"
    TIMEOUT 90)
# ---------------------------------------------------------------------------
# The same ground truth again, called under Kerberos.
#
# What changes between these and the AUTH_SYS cells above is only how the call
# travels: krb5 authenticates it, krb5i signs its arguments and results, krb5p
# seals them.  What the probes assert is unchanged, which is the point -- an
# NFS server whose answers depend on the security flavor carrying them has a
# bug, and nothing else in this tree would notice.
#
# The probes rather than the trace corpora, because a trace names the uid each
# operation is made as and under RPCSEC_GSS the identity is the authenticated
# principal instead; the corpus cells that can run are registered separately
# below.  Every cell self-skips (77) where the host has no usable MIT krb5.
#
# krb5p is in the default tier because it is the flavor that reframes the
# payload -- it is where a size or an iovec is most likely to be got wrong --
# and krb5/krb5i join it in the extended tier.
foreach(sec krb5 krb5i krb5p)
    if(sec STREQUAL "krb5p")
        set(_sec_cfg "")
    else()
        set(_sec_cfg CONFIGURATIONS extended)
    endif()

    add_test(NAME chimera/server/nfs/mbt/deviation_probe_memfs_${sec}
             ${_sec_cfg}
             COMMAND $<TARGET_FILE:nfs3_mbt_probe> -b memfs --sec ${sec})
    set_tests_properties(chimera/server/nfs/mbt/deviation_probe_memfs_${sec}
        PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_deviation_probe_memfs_${sec}"
        LABELS "quint;nfs3_mbt;memfs;gss"
        SKIP_RETURN_CODE 77
        TIMEOUT 120)

    add_test(NAME chimera/server/nfs/mbtaux/probe_memfs_${sec}
             ${_sec_cfg}
             COMMAND $<TARGET_FILE:nfs_aux_probe> --sec ${sec})
    set_tests_properties(chimera/server/nfs/mbtaux/probe_memfs_${sec}
        PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsaux_probe_memfs_${sec}"
        LABELS "quint;nfsaux_mbt;memfs;gss"
        SKIP_RETURN_CODE 77
        TIMEOUT 180)

    add_test(NAME chimera/server/nfs/mbtdrc/probe_memfs_${sec}
             ${_sec_cfg}
             COMMAND $<TARGET_FILE:nfs_drc_probe> --sec ${sec})
    set_tests_properties(chimera/server/nfs/mbtdrc/probe_memfs_${sec}
        PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsdrc_probe_memfs_${sec}"
        LABELS "quint;nfsdrc_mbt;memfs;gss"
        SKIP_RETURN_CODE 77
        TIMEOUT 180)

    add_test(NAME chimera/server/nfs/mbt4/deleg_probe_memfs_${sec}
             ${_sec_cfg}
             COMMAND $<TARGET_FILE:nfs4_deleg_probe> --sec ${sec})
    set_tests_properties(chimera/server/nfs/mbt4/deleg_probe_memfs_${sec}
        PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_deleg_probe_memfs_${sec}"
        LABELS "quint;nfs4_mbt;multiproto;memfs;gss"
        SKIP_RETURN_CODE 77
        TIMEOUT 150)
endforeach()

if(NOT CHIMERA_MBT_AVAILABLE)
    message(STATUS "nfs MBT replay ctests skipped (no specs trace corpus)")
    return()
endif()

# One corpus for NFSv3.  The model declares no feature or policy knobs -- an
# NFSv3 server has no optional surface to advertise and no minor version to
# pick -- so besides the batch list the config carries the KNOWN DEVIATIONS
# chimera still has and the places RFC 1813 permits more than one answer.
# Every cell below replays the whole of it.
#
# There is no deviation registry in nfs3_mbt_replay.c any more, and as of the
# #1771 fix (the missing search check on CREATE's existing-entry path, now
# enforced by memfs/diskfs/cairn before the directory is examined) the cell
# carries no deviations at all: the model states RFC 1813 and chimera meets it.
# Any divergence that does turn up gets a branch in ext/specs/quint/nfs/nfs3.qnt
# gated on the DEVS set, declared with its citation in that family's
# corpus.schema.json and named by the config here -- so the expectation in a
# trace already IS what chimera does and replay is an exact match.  The two
# places the STANDARD permits several answers are separate: those are
# TOLERANCES, and the model emits a `statusAccept` set beside the status it
# predicts.
#
# STRICT_OUT_DIR registers the strict twin: the same batches with every
# deviation off.  Its failures are the conformance debt, re-measured on every
# run rather than remembered in C -- which is what stops a deviation outliving
# its fix.  The liveness gate below closes the same loop from the other side.
specs_corpus(NAME           nfs3/ref
             FAMILY         nfs3
             CONFIG         ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs3_ref.json
             OUT_DIR        NFS3_TRACES
             STRICT_OUT_DIR NFS3_STRICT_TRACES)

# One batched replay per suite: a single replayer process opens the server +
# rpc2 client once and replays every trace in the corpus back-to-back, standing
# up a fresh, uniquely-named memfs (mkfs) per trace so the server's attr/name/
# open caches cannot leak state across traces -- isolation is preserved while
# the dominant per-process server/client init is amortized over the whole
# corpus.  The NFS4 replayer additionally cycles a fresh client identity per
# trace.  --trace-dir consumes every *.itf.json the specs build/bundle produced
# for the suite.
add_test(NAME chimera/server/nfs/mbt/batch_memfs
    COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_TRACES})
set_tests_properties(chimera/server/nfs/mbt/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_batch_memfs"
    LABELS "quint;nfs3_mbt;memfs"
    TIMEOUT 300)

# The same NFS3 corpus replayed against the on-disk backends.  The corpus is
# protocol-level (RFC 1813) and backend-agnostic, so a divergence here is a
# genuine backend deviation from the model memfs already satisfies.  Each
# replay process self-provisions its scratch (device image / rocksdb dir) under
# its own mkdtemp session dir, so the batches still run fully parallel and clean
# up with the temp dir.  diskfs takes whichever libevpl block backend the
# platform has (CHIMERA_DISKFS_DEVICE_TYPE); cairn is gated on rocksdb.
# The NFS3 corpus under krb5p.  Only the traces that never name a uid can run:
# under RPCSEC_GSS the identity is the authenticated principal, so an operation
# asking to be made as some other user has no expression, and the replayer
# reports how many traces it set aside for that reason rather than replaying
# them as root against an oracle that expected otherwise.
#
# The applicable subset is thin -- this corpus is DAC-heavy -- so what this
# cell adds over the probes is the protocol surface a random walk reaches,
# carried under a sealed payload.  A fixed-identity profile on the specs side
# would widen it; see DEVIATIONS-NFS.md.
add_test(NAME chimera/server/nfs/mbt/batch_memfs_krb5p
    COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_TRACES}
            --sec krb5p)
set_tests_properties(chimera/server/nfs/mbt/batch_memfs_krb5p PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_batch_memfs_krb5p"
    LABELS "quint;nfs3_mbt;memfs;gss"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)

add_test(NAME chimera/server/nfs/mbt/batch_diskfs
    COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_TRACES}
            --backend diskfs)
set_tests_properties(chimera/server/nfs/mbt/batch_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_batch_diskfs"
    LABELS "quint;nfs3_mbt;diskfs"
    TIMEOUT 300)

if(CAIRN_ENABLED)
    add_test(NAME chimera/server/nfs/mbt/batch_cairn
        COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_TRACES}
                --backend cairn)
    set_tests_properties(chimera/server/nfs/mbt/batch_cairn PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_batch_cairn"
        LABELS "quint;nfs3_mbt;cairn"
        TIMEOUT 300)
endif()

# The corpus against the passthrough backends (scratch/skip semantics as for
# the deviation probes above).  --max-attr-skip-rate is raised: NFS3 post-op
# attributes are RFC-optional, and the passthrough backends omit them where
# fetching would cost an extra host round trip -- measured up to ~25% of
# replies on the step flavors, against ~0% for the in-engine backends.  The
# model tolerates an omitted optional attribute; the cap only bounds how much
# of the corpus may go unchecked before the replay is judged vacuous.
if(CHIMERA_LINUX)
    add_test(NAME chimera/server/nfs/mbt/batch_linux
        COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_TRACES}
                --backend linux --max-attr-skip-rate 0.5)
    set_tests_properties(chimera/server/nfs/mbt/batch_linux PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_batch_linux"
        LABELS "quint;nfs3_mbt;linux"
        SKIP_RETURN_CODE 77
        TIMEOUT 300)
endif()

if(CHIMERA_VFS_IO_URING)
    add_test(NAME chimera/server/nfs/mbt/batch_io_uring
        COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_TRACES}
                --backend io_uring --max-attr-skip-rate 0.5)
    set_tests_properties(chimera/server/nfs/mbt/batch_io_uring PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_batch_io_uring"
        LABELS "quint;nfs3_mbt;io_uring"
        SKIP_RETURN_CODE 77
        TIMEOUT 300)
endif()

# The same corpus again, carried over RPC-over-RDMA instead of plain RPC record
# marking (--rdma; see mbt_env_opts.rdma).  The inproc transport's
# DATAGRAM_INPROC protocol reports itself RDMA-capable and performs real
# one-sided transfers against a registered-memory table, so this is not an
# emulation: rpc2 selects RFC 8166 framing, WRITE hands its payload over in a
# Read chunk for the server to pull, and READ advertises a Write chunk the
# server places the reply into -- the same two things chimera's own NFS client
# asks for (vfs/nfs/nfs4_write.c, nfs4_read.c).
#
# Measured on memfs: 73 RDMA reads and 26 RDMA writes across the corpus,
# against zero for the stream cells above.  That count is the check that keeps
# these honest -- without the chunk requests an RDMA run still passes, having
# exercised the framing but never a one-sided transfer.
foreach(rback memfs diskfs cairn linux io_uring)
    if(rback STREQUAL "cairn" AND NOT CAIRN_ENABLED)
        continue()
    endif()
    if(rback STREQUAL "linux" AND NOT CHIMERA_LINUX)
        continue()
    endif()
    if(rback STREQUAL "io_uring" AND NOT CHIMERA_VFS_IO_URING)
        continue()
    endif()
    set(rextra "")
    if(rback STREQUAL "linux" OR rback STREQUAL "io_uring")
        set(rextra --max-attr-skip-rate 0.5)
    endif()
    add_test(NAME chimera/server/nfs/mbt/batch_rdma_${rback}
        COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_TRACES}
                --backend ${rback} --rdma ${rextra})
    set_tests_properties(chimera/server/nfs/mbt/batch_rdma_${rback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_batch_rdma_${rback}"
        LABELS "quint;nfs3_mbt;rdma;${rback}"
        SKIP_RETURN_CODE 77
        TIMEOUT 300)
endforeach()

# ---- the NFSv3 deviation machinery ----------------------------------------

set(SPECS_DEVLIVENESS ${CMAKE_SOURCE_DIR}/ext/specs/tools/devliveness.py)

# The strict twin of nfs3/ref: the same batches regenerated with DEVS = Set(),
# so every expectation is the RFC-correct one.  With the cell's deviation list
# empty it should now PASS, and it keeps running so that the day a deviation is
# added the debt it stands for is re-measured on every run -- which is what a
# deviation registry could never do: a registry entry keeps forgiving after the
# bug is fixed, and forgives the same divergence somewhere it was never meant
# to.
# Reporting, not gating, so it goes in the extended tier.
set(CHIMERA_TEST_TIER extended)

add_test(NAME chimera/server/nfs/mbt/strict_memfs
    COMMAND $<TARGET_FILE:nfs3_mbt_replay> --trace-dir ${NFS3_STRICT_TRACES})
set_tests_properties(chimera/server/nfs/mbt/strict_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs3_strict_memfs"
    LABELS "quint;nfs3_mbt;memfs;strict"
    TIMEOUT 300)

set(CHIMERA_TEST_TIER quick)

# The liveness gate: a cell may not enable a deviation its own corpus never
# exercises.  Either the deviation is dead (retire it from the config and
# delete its branch in the model) or the generator quietly lost the coverage
# that used to reach it.  Both need a human, so both fail here -- in the quick
# tier, because a stale claim is a blindfold from the moment it goes stale.
if(Python3_FOUND)
    add_test(NAME chimera/server/nfs/mbt/devliveness_nfs3
        COMMAND ${Python3_EXECUTABLE} ${SPECS_DEVLIVENESS}
                ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs3_ref.json
                ${NFS3_TRACES})
    set_tests_properties(chimera/server/nfs/mbt/devliveness_nfs3 PROPERTIES
        LABELS "quint;nfs3_mbt;devliveness" TIMEOUT 120)
else()
    message(STATUS "nfs3 MBT deviation-liveness gate skipped (no python3)")
endif()

# The auxiliary protocols (portmap/rpcbind, MOUNT, NLM, NSM) replayed from the
# nfsaux corpus.  One batch: the four protocols share one model because they
# share state (a granted lock monitors its holder, a notify drops its locks),
# and the mixed trace flavors are what reach those seams.
#
# --paranoid is on deliberately.  Comparing replies alone does not just
# MISLOCATE a lock-table divergence -- it can miss one entirely, because a lock
# the server failed to grant or release is invisible until some later operation
# happens to collide with it, and nothing guarantees one does.  The flag probes
# each file's ranges after every step against the model's own lock set, so a
# divergence is both caught and reported where it is created.  It costs roughly
# a quarter more wall time; the timeout is set for an instrumented build, where
# it is several times slower than the release run it was measured in.
#
# Run against every backend, as the NFS3 and NFS4 batches are.  The auxiliary
# protocols look server-side, but three of the four reach the VFS: MOUNT
# resolves an export path to a filehandle, NLM's lock table hangs off the open
# file the backend handed out, and NSM's notify drops locks held against it.
# --paranoid re-probes each file's ranges through the backend after every step,
# so a backend whose handles or open state differ from memfs's diverges here
# rather than staying invisible until some later collision.  The passthrough
# pair replays the corpus whole (42/42, no capability skips): nothing in it
# reaches the host semantics the nfs3/nfs4 corpora need gates for.
# The auxiliary corpus under krb5p.  It carries no per-operation credential, so
# unlike the NFS3 corpus every trace in it runs.  MOUNT travels under a context
# of its own; portmap, NLM and NSM stay on AUTH_SYS, which is what a real
# deployment does with them.
specs_corpus(NAME    nfsaux/ref
             FAMILY  nfsaux
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfsaux_ref.json
             OUT_DIR NFSAUX_TRACES)

add_test(NAME chimera/server/nfs/mbtaux/batch_memfs_krb5p
    COMMAND $<TARGET_FILE:nfs_aux_mbt_replay> --paranoid
            --trace-dir ${NFSAUX_TRACES} --sec krb5p)
set_tests_properties(chimera/server/nfs/mbtaux/batch_memfs_krb5p PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsaux_batch_memfs_krb5p"
    LABELS "quint;nfsaux_mbt;memfs;gss"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

foreach(aback memfs diskfs cairn linux io_uring)
    if(aback STREQUAL "cairn" AND NOT CAIRN_ENABLED)
        continue()
    endif()
    if(aback STREQUAL "linux" AND NOT CHIMERA_LINUX)
        continue()
    endif()
    if(aback STREQUAL "io_uring" AND NOT CHIMERA_VFS_IO_URING)
        continue()
    endif()
    add_test(NAME chimera/server/nfs/mbtaux/batch_${aback}
        COMMAND $<TARGET_FILE:nfs_aux_mbt_replay> --paranoid
                --trace-dir ${NFSAUX_TRACES}
                --backend ${aback})
    set_tests_properties(chimera/server/nfs/mbtaux/batch_${aback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsaux_batch_${aback}"
        LABELS "quint;nfsaux_mbt;${aback}"
        TIMEOUT 600)

    # Over RPC-over-RDMA, as the NFS3 corpus runs above.  Only the NFS service
    # moves: MOUNT, NLM, NSM and portmap keep the stream endpoint, which is how
    # a real NFS/RDMA deployment is arranged.  This corpus has no READ or WRITE
    # in it, so unlike the NFS3 pair these cells cover the RFC 8166 framing and
    # the auxiliary protocols' behavior beside it, not the chunk paths.
    add_test(NAME chimera/server/nfs/mbtaux/batch_rdma_${aback}
        COMMAND $<TARGET_FILE:nfs_aux_mbt_replay> --paranoid
                --trace-dir ${NFSAUX_TRACES}
                --backend ${aback} --rdma)
    set_tests_properties(chimera/server/nfs/mbtaux/batch_rdma_${aback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsaux_batch_rdma_${aback}"
        LABELS "quint;nfsaux_mbt;rdma;${aback}"
        TIMEOUT 600)
endforeach()

# The duplicate-request caches, replayed from the nfsdrc corpus.  Two batches
# because there are two servers: the NFSv3 cache is off by default and on in the
# other, and "the switch does nothing" and "the switch works" are
# indistinguishable without a corpus for each.  The NFSv4.0 cache has no switch
# and is under test in both.
#
# --paranoid is on for the same reason the auxiliary suite has it on.  Comparing
# replies alone cannot see a cache misbehaving for every operation: a replayed
# SETATTR and a re-executed one both answer NFS3_OK, and the difference is a
# size in the directory.  The flag re-reads the working directory after every
# step and compares it against the model's own.
#
# Both batches run against every backend.  A duplicate-request cache is
# server-side, but what it caches is a reply the backend produced, and
# --paranoid's re-read of the working directory after every step is what tells a
# genuine replay from a re-execution -- so the property under test ("the second
# copy of this request changed nothing") is only as strong as the backend it is
# checked against.  Adding the passthrough pair found a real one: this corpus is
# the only one that sets a size on a symlink, and NFS3 SETATTR answered
# NFS3ERR_SERVERFAULT there instead of NFS3ERR_INVAL (see
# chimera_nfs3_setattr_open_callback).
#
# Two configs, two corpora, two directories.  Every cell below replays ALL of
# the directory it is pointed at: the two servers used to share one corpus that
# each cell carved by filename (--exclude-prefix nfsdrcNoV3 / nfsdrcRef), which
# meant every cell read traces it then discarded.
specs_corpus(NAME    nfsdrc/ref
             FAMILY  nfsdrc
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/drc_ref.json
             OUT_DIR DRC_REF_TRACES)
specs_corpus(NAME    nfsdrc/nov3
             FAMILY  nfsdrc
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/drc_nov3.json
             OUT_DIR DRC_NOV3_TRACES)

foreach(rback memfs diskfs cairn linux io_uring)
    if(rback STREQUAL "cairn" AND NOT CAIRN_ENABLED)
        continue()
    endif()
    if(rback STREQUAL "linux" AND NOT CHIMERA_LINUX)
        continue()
    endif()
    if(rback STREQUAL "io_uring" AND NOT CHIMERA_VFS_IO_URING)
        continue()
    endif()
    # And under krb5p on memfs.  This is the suite whose whole subject is
    # replaying a cached reply, so it is the one that says a cached reply is
    # still correct when the service reframes what carries it.
    if(rback STREQUAL "memfs")
        add_test(NAME chimera/server/nfs/mbtdrc/batch_memfs_krb5p
            COMMAND $<TARGET_FILE:nfs_drc_mbt_replay> --paranoid
                    --trace-dir ${DRC_REF_TRACES}
                    --backend memfs --sec krb5p)
        set_tests_properties(chimera/server/nfs/mbtdrc/batch_memfs_krb5p
            PROPERTIES
            ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsdrc_batch_memfs_krb5p"
            LABELS "quint;nfsdrc_mbt;memfs;gss"
            SKIP_RETURN_CODE 77
            TIMEOUT 600)
    endif()

    add_test(NAME chimera/server/nfs/mbtdrc/batch_${rback}
        COMMAND $<TARGET_FILE:nfs_drc_mbt_replay> --paranoid
                --trace-dir ${DRC_REF_TRACES}
                --backend ${rback})
    set_tests_properties(chimera/server/nfs/mbtdrc/batch_${rback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsdrc_batch_${rback}"
        LABELS "quint;nfsdrc_mbt;${rback}"
        TIMEOUT 600)

    add_test(NAME chimera/server/nfs/mbtdrc/batch_nodrc_${rback}
        COMMAND $<TARGET_FILE:nfs_drc_mbt_replay> --paranoid --no-nfs3-drc
                --trace-dir ${DRC_NOV3_TRACES}
                --backend ${rback})
    set_tests_properties(chimera/server/nfs/mbtdrc/batch_nodrc_${rback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsdrc_batch_nodrc_${rback}"
        LABELS "quint;nfsdrc_mbt;${rback}"
        TIMEOUT 600)

    # Over RPC-over-RDMA.  This suite is the one that most needs the transport
    # varied: both duplicate-request caches used to skip RDMA connections
    # outright, on the grounds that the cached reply was the TCP on-wire form,
    # which silently turned every non-idempotent retransmit over RDMA into a
    # second execution.  The caches now store the RPC reply without its framing
    # and serve RDMA like any other transport; these cells are what says so.
    add_test(NAME chimera/server/nfs/mbtdrc/batch_rdma_${rback}
        COMMAND $<TARGET_FILE:nfs_drc_mbt_replay> --paranoid
                --trace-dir ${DRC_REF_TRACES}
                --backend ${rback} --rdma)
    set_tests_properties(chimera/server/nfs/mbtdrc/batch_rdma_${rback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsdrc_batch_rdma_${rback}"
        LABELS "quint;nfsdrc_mbt;rdma;${rback}"
        TIMEOUT 600)

    add_test(NAME chimera/server/nfs/mbtdrc/batch_nodrc_rdma_${rback}
        COMMAND $<TARGET_FILE:nfs_drc_mbt_replay> --paranoid --no-nfs3-drc
                --trace-dir ${DRC_NOV3_TRACES}
                --backend ${rback} --rdma)
    set_tests_properties(chimera/server/nfs/mbtdrc/batch_nodrc_rdma_${rback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfsdrc_batch_nodrc_rdma_${rback}"
        LABELS "quint;nfsdrc_mbt;rdma;${rback}"
        TIMEOUT 600)
endforeach()

# SKIP_RETURN_CODE 77 still applies to NFS4: a trace whose capability profile
# does not match the live server exits 77 individually; the batch reports SKIP
# only if every trace skips, otherwise the runnable traces gate it.
# The NFSv4 corpus under krb5p.  It names no per-operation credential, so the
# whole corpus runs -- and it is the one suite that retransmits enough to work
# the session reply cache, which is where a cached reply and a service that
# reframes the payload meet.  That combination was broken until the cache was
# taught to keep the results rather than the wire message; this is the cell
# that would have caught it.
# Eight configs, eight corpora.  Three minor versions of the same server
# (4.2 with every RFC 7862/8276 data operation required, 4.1 and 4.0 with none
# of them), the delegation-serving variants of 4.2 and 4.1, and the two pNFS
# metadata-server profiles.  They were one directory each ctest read whole and
# then skipped most of at replay time -- the plain batches skipped every Deleg
# trace, the delegation batch skipped everything else, and the pNFS profiles
# needed a suite of their own because they require the opposite advertisement
# from the same server.  Now each ctest is pointed at exactly the corpora it
# can replay.
specs_corpus(NAME    nfs4/memfs42
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_memfs42.json
             OUT_DIR NFS4_MEMFS42_TRACES
             STRICT_OUT_DIR NFS4_MEMFS42_STRICT)
specs_corpus(NAME    nfs4/memfs41
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_memfs41.json
             OUT_DIR NFS4_MEMFS41_TRACES
             STRICT_OUT_DIR NFS4_MEMFS41_STRICT)
specs_corpus(NAME    nfs4/memfs40
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_memfs40.json
             OUT_DIR NFS4_MEMFS40_TRACES
             STRICT_OUT_DIR NFS4_MEMFS40_STRICT)
# The 4.1 cell again, exported as "/" rather than under a name.  Same protocol,
# different place in the server's NFSv4 namespace -- and the two shapes give
# LOOKUPP at the exported root different, both conformant, answers (RFC 7530
# 16.14.4): parent-less here, a walk onto the pseudo-root in memfs41.  chimera
# answers them from different arms of chimera_nfs4_lookupp_continue, so one
# cell could only ever cover one of them.  Deliberately narrow -- the flavors
# that walk the tree -- because the rest of the surface is memfs41's already.
specs_corpus(NAME    nfs4/memfs41_root
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_memfs41_root.json
             OUT_DIR NFS4_MEMFS41_ROOT_TRACES
             STRICT_OUT_DIR NFS4_MEMFS41_ROOT_STRICT)
specs_corpus(NAME    nfs4/memfs42_deleg
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_memfs42_deleg.json
             OUT_DIR NFS4_MEMFS42_DELEG_TRACES
             STRICT_OUT_DIR NFS4_MEMFS42_DELEG_STRICT)
specs_corpus(NAME    nfs4/memfs41_deleg
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_memfs41_deleg.json
             OUT_DIR NFS4_MEMFS41_DELEG_TRACES
             STRICT_OUT_DIR NFS4_MEMFS41_DELEG_STRICT)
specs_corpus(NAME    nfs4/pnfs42
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_pnfs42.json
             OUT_DIR NFS4_PNFS42_TRACES
             STRICT_OUT_DIR NFS4_PNFS42_STRICT)
specs_corpus(NAME    nfs4/pnfs41
             FAMILY  nfs4
             CONFIG  ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_pnfs41.json
             OUT_DIR NFS4_PNFS41_TRACES
             STRICT_OUT_DIR NFS4_PNFS41_STRICT)

# The argument groups the ctests below use.  A replayer accepts --trace-dir
# more than once (mbt_trace_dir.h), so a cell stays one corpus even where a
# ctest replays several of them.
#
# ORDER IS ASCENDING BY MINOR VERSION, AND THAT IS NOT COSMETIC.  These traces
# used to share one directory, where they sorted alphabetically -- nfs4Memfs40,
# then 41, then 42 -- and the split reproduces that order exactly, because the
# suite does not survive the other one.  Handing the replayer 4.2 first and 4.0
# last fails in nfs4Memfs40_step_80_0x7_7 at step 64: a READ against a
# write-only open answers NFS4ERR_BAD_STATEID (10025) where the model, and a
# 4.0-first run, get NFS4ERR_OPENMODE (10038).  The trace bytes are identical
# either way (verified), and memfs40 on its own passes, so what carries between
# the groups is state -- a stateid or clientid the previous minor version's
# session left behind.
#
# That is a real defect and a good find; it was invisible while alphabetical
# order happened to put 4.0 first.  It is NOT this change's to fix: pinning the
# order here keeps the migration a faithful refactor, and the bug is filed
# separately.  Do not "tidy" this into descending order.
set(NFS4_TRACE_ARGS
    --trace-dir ${NFS4_MEMFS40_TRACES}
    --trace-dir ${NFS4_MEMFS41_TRACES}
    --trace-dir ${NFS4_MEMFS42_TRACES})
set(NFS4_DELEG_TRACE_ARGS
    --trace-dir ${NFS4_MEMFS41_DELEG_TRACES}
    --trace-dir ${NFS4_MEMFS42_DELEG_TRACES})
set(NFS4_PNFS_TRACE_ARGS
    --trace-dir ${NFS4_PNFS41_TRACES}
    --trace-dir ${NFS4_PNFS42_TRACES})

# The root-export cell replays in a process of its own: it is the one cell whose
# server-side namespace differs, and a separate ctest keeps that difference from
# interleaving with the minor-version ordering pinned above.
add_test(NAME chimera/server/nfs/mbt4/batch_memfs41_root
    COMMAND $<TARGET_FILE:nfs4_mbt_replay>
            --trace-dir ${NFS4_MEMFS41_ROOT_TRACES})
set_tests_properties(chimera/server/nfs/mbt4/batch_memfs41_root PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_memfs41_root"
    LABELS "quint;nfs4_mbt;memfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)

add_test(NAME chimera/server/nfs/mbt4/batch_memfs_krb5p
    COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_TRACE_ARGS}
            --sec krb5p)
set_tests_properties(chimera/server/nfs/mbt4/batch_memfs_krb5p PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_memfs_krb5p"
    LABELS "quint;nfs4_mbt;memfs;gss"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

add_test(NAME chimera/server/nfs/mbt4/batch_memfs
    COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_TRACE_ARGS})
set_tests_properties(chimera/server/nfs/mbt4/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_memfs"
    LABELS "quint;nfs4_mbt;memfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)

# The same corpus -- every minor version instance it carries (4.0, 4.1 and
# 4.2) -- against the other named-filesystem backends, as the NFS3 suite runs
# above: diskfs/cairn self-provision scratch under their mkdtemp session dirs,
# and a trace whose capability profile the live backend cannot honor skips
# individually.  Its first runs surfaced three diskfs clone bugs (beyond-EOF
# ranges accepted, source holes leaving stale destination data, the walk
# reprocessing its own split piece) and the D4-17 real-holes seek refinement
# the hole-tracking backends need.
add_test(NAME chimera/server/nfs/mbt4/batch_diskfs
    COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_TRACE_ARGS}
            --backend diskfs)
set_tests_properties(chimera/server/nfs/mbt4/batch_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_diskfs"
    LABELS "quint;nfs4_mbt;diskfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

if(CAIRN_ENABLED)
    add_test(NAME chimera/server/nfs/mbt4/batch_cairn
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_TRACE_ARGS}
                --backend cairn)
    set_tests_properties(chimera/server/nfs/mbt4/batch_cairn PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_cairn"
        LABELS "quint;nfs4_mbt;cairn"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

# The passthrough backends (container-only, like the NFS3 pair above: the
# scratch mount needs name_to_handle_at and root).  Host-kernel semantics the
# passthrough cannot mask are excused ONLY under the replayer's passthrough
# gate -- symlink modes (no lchmod), kernel-tick change granularity, the
# refused link of an unlinked source, the relatime-clobbered EXCLUSIVE4
# verifier -- so the native backends above keep exact enforcement.
if(CHIMERA_LINUX)
    add_test(NAME chimera/server/nfs/mbt4/batch_linux
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_TRACE_ARGS}
                --backend linux)
    set_tests_properties(chimera/server/nfs/mbt4/batch_linux PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_linux"
        LABELS "quint;nfs4_mbt;linux"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

if(CHIMERA_VFS_IO_URING)
    add_test(NAME chimera/server/nfs/mbt4/batch_io_uring
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_TRACE_ARGS}
                --backend io_uring)
    set_tests_properties(chimera/server/nfs/mbt4/batch_io_uring PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_io_uring"
        LABELS "quint;nfs4_mbt;io_uring"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

# The same corpus over RPC-over-RDMA, as the NFS3 suite runs above.  A WRITE
# compound sends its payload in a Read chunk and a READ compound advertises a
# Write chunk, so the chunk paths are covered here and not just the framing.
#
# These cells are also the only test of a retransmit over RDMA, which is where
# the NFSv4.1 session reply cache used to abort the server outright: it stored
# the reply with its transport framing, and the replay path could not parse an
# RPC-over-RDMA header.  Both that and the double release of a Read chunk's
# iovecs on the replay path are fixed; either one regressing takes these red.
foreach(rback memfs diskfs cairn linux io_uring)
    if(rback STREQUAL "cairn" AND NOT CAIRN_ENABLED)
        continue()
    endif()
    if(rback STREQUAL "linux" AND NOT CHIMERA_LINUX)
        continue()
    endif()
    if(rback STREQUAL "io_uring" AND NOT CHIMERA_VFS_IO_URING)
        continue()
    endif()
    add_test(NAME chimera/server/nfs/mbt4/batch_rdma_${rback}
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_TRACE_ARGS}
                --backend ${rback} --rdma)
    set_tests_properties(chimera/server/nfs/mbt4/batch_rdma_${rback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_rdma_${rback}"
        LABELS "quint;nfs4_mbt;rdma;${rback}"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endforeach()

# The delegation corpora, replayed against a server that SERVES delegations.
# Their own configs and so their own directories: a trace that assumes a grant
# and a trace that assumes none describe different servers, and the two used to
# share one directory that each batch read whole and then discarded most of --
# the plain batches above skipped every Deleg trace, this one skipped
# everything else.  Until this batch existed the Deleg instances had no
# consumer at all: they were generated, skipped silently in the default batch,
# and the delegation wire paths were covered only by the deterministic
# deleg_probe.
#
# The passthrough backends are in this list for the same reason they are in the
# plain nfs4 list above: a delegation is recalled on a conflicting open, and
# which opens conflict is decided against the handle the backend produced.
# Measured identical to the native backends -- 14 Deleg traces replay and 51
# skip on every one of memfs/diskfs/cairn/linux/io_uring -- so the passthrough
# cells carry the same delegation coverage rather than a thinner one.
foreach(dback memfs diskfs cairn linux io_uring)
    if(dback STREQUAL "cairn" AND NOT CAIRN_ENABLED)
        continue()
    endif()
    if(dback STREQUAL "linux" AND NOT CHIMERA_LINUX)
        continue()
    endif()
    if(dback STREQUAL "io_uring" AND NOT CHIMERA_VFS_IO_URING)
        continue()
    endif()
    add_test(NAME chimera/server/nfs/mbt4/batch_deleg_${dback}
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_DELEG_TRACE_ARGS}
                --backend ${dback} --delegations)
    set_tests_properties(chimera/server/nfs/mbt4/batch_deleg_${dback} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_batch_deleg_${dback}"
        LABELS "quint;nfs4_mbt;${dback}"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endforeach()

# pNFS.  --pnfs N makes the replayer stand up a whole metadata-server cluster
# inside the test process: the server it already ran becomes the MDS, plus N
# more chimera servers as flex-files data servers, each on its own inproc
# service name and in data_server mode so they can share the process.  The MDS
# reaches them through the nfs client module, also over inproc -- so a LAYOUTGET
# exercises the real steer / backing-file-create / blob-persist path with no
# ports, namespaces, daemons or VMs, unlike the KVM pNFS matrix.  Two data
# servers, so round-robin steering and both arms of the flex-files device
# encoder (an NFSv3 DS and an NFSv4.1 one) are covered.
#
# The two pNFS configs are corpora of their own: they require the server to
# advertise itself as a metadata server, which the plain nfs4 configs above
# require it not to.
add_test(NAME chimera/server/nfs/mbt4/batch_pnfs_memfs
    COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_PNFS_TRACE_ARGS}
            --pnfs 2)
set_tests_properties(chimera/server/nfs/mbt4/batch_pnfs_memfs PROPERTIES
    LABELS "quint;nfs4_mbt;pnfs;memfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)

# The same cluster with an on-disk metadata server: diskfs persists the pNFS
# layout blob as its own b+tree record, so this is the only test that covers
# that record's write/read-back path.  The data servers stay memfs -- what is
# under test here is the MDS.
#
# Registered unconditionally, like every other diskfs variant: diskfs takes
# whichever libevpl block backend the platform has, and libaio is only the
# Linux-preferred one (see CHIMERA_DISKFS_DEVICE_TYPE at the top level).  This
# was the last cell still carrying the old if(HAVE_LIBAIO) gate, which had the
# effect of leaving the layout record untested on every platform without
# libaio -- macOS among them, where it runs green.
add_test(NAME chimera/server/nfs/mbt4/batch_pnfs_diskfs
    COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_PNFS_TRACE_ARGS}
            --pnfs 2 --backend diskfs)
set_tests_properties(chimera/server/nfs/mbt4/batch_pnfs_diskfs PROPERTIES
    LABELS "quint;nfs4_mbt;pnfs;diskfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)

# And with a cairn metadata server, which persists the layout blob as its own
# keyed record (CAIRN_KEY_PNFS).  Until cairn learned to store it, it did not
# advertise CHIMERA_VFS_CAP_LAYOUT and every trace here skipped with
# NFS4ERR_LAYOUTUNAVAILABLE -- the whole suite reported SKIP, which reads the
# same as "registered and passing" in a ctest summary.
if(CAIRN_ENABLED)
    add_test(NAME chimera/server/nfs/mbt4/batch_pnfs_cairn
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> ${NFS4_PNFS_TRACE_ARGS}
                --pnfs 2 --backend cairn)
    set_tests_properties(chimera/server/nfs/mbt4/batch_pnfs_cairn PROPERTIES
        LABELS "quint;nfs4_mbt;pnfs;cairn"
        SKIP_RETURN_CODE 77
        TIMEOUT 300)
endif()

# ---- the NFSv4 deviation machinery ----------------------------------------
#
# The strict twins: each cell's config regenerated with DEVS = Set(), so every
# expectation is the RFC-correct answer rather than chimera's.  These are
# EXPECTED to fail, and their output is the conformance debt -- re-measured on
# every run instead of remembered in a registry.  That is the whole point: a
# registry entry keeps forgiving after the bug behind it is fixed, and keeps
# forgiving the same divergence somewhere it was never meant to.  Reporting,
# not gating, so they live in the extended tier.
#
# One test per cell rather than one over all of them, and deliberately so: the
# order dependence documented above NFS4_TRACE_ARGS only bites when several
# minor versions share a replayer process, and a per-cell test cannot hit it.
set(CHIMERA_TEST_TIER extended)

foreach(scell memfs40 memfs41 memfs41_root memfs42)
    string(TOUPPER ${scell} SCELL)
    add_test(NAME chimera/server/nfs/mbt4/strict_${scell}
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> --trace-dir ${NFS4_${SCELL}_STRICT})
    set_tests_properties(chimera/server/nfs/mbt4/strict_${scell} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_strict_${scell}"
        LABELS "quint;nfs4_mbt;memfs;strict"
        SKIP_RETURN_CODE 77
        TIMEOUT 300)
endforeach()

foreach(scell memfs41_deleg memfs42_deleg)
    string(TOUPPER ${scell} SCELL)
    add_test(NAME chimera/server/nfs/mbt4/strict_${scell}
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> --trace-dir ${NFS4_${SCELL}_STRICT}
                --delegations)
    set_tests_properties(chimera/server/nfs/mbt4/strict_${scell} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_strict_${scell}"
        LABELS "quint;nfs4_mbt;memfs;strict"
        SKIP_RETURN_CODE 77
        TIMEOUT 300)
endforeach()

foreach(scell pnfs41 pnfs42)
    string(TOUPPER ${scell} SCELL)
    add_test(NAME chimera/server/nfs/mbt4/strict_${scell}
        COMMAND $<TARGET_FILE:nfs4_mbt_replay> --trace-dir ${NFS4_${SCELL}_STRICT}
                --pnfs 2)
    set_tests_properties(chimera/server/nfs/mbt4/strict_${scell} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=nfs4_strict_${scell}"
        LABELS "quint;nfs4_mbt;pnfs;strict"
        SKIP_RETURN_CODE 77
        TIMEOUT 300)
endforeach()

set(CHIMERA_TEST_TIER quick)

# The liveness gate, one per cell: a cell may not enable a deviation its own
# corpus never exercises.  Either the deviation is dead -- retire it from the
# config and delete its branch in the model -- or the generator quietly lost
# the coverage that used to reach it.  Both need a human, so both fail here,
# and in the QUICK tier, because a stale claim is a blindfold from the moment
# it goes stale.  This is also what keeps the seven configs honest about which
# divergences each flavor actually reaches: the delegation cells turn all six
# of the parent's deviations off because stepDeleg touches none of them.
if(Python3_FOUND)
    foreach(dcell memfs40 memfs41 memfs42 memfs41_deleg memfs42_deleg
                  pnfs41 pnfs42)
        string(TOUPPER ${dcell} DCELL)
        add_test(NAME chimera/server/nfs/mbt4/devliveness_${dcell}
            COMMAND ${Python3_EXECUTABLE} ${SPECS_DEVLIVENESS}
                    ${CMAKE_CURRENT_SOURCE_DIR}/configs/nfs4_${dcell}.json
                    ${NFS4_${DCELL}_TRACES})
        set_tests_properties(chimera/server/nfs/mbt4/devliveness_${dcell}
            PROPERTIES
            LABELS "quint;nfs4_mbt;devliveness" TIMEOUT 120)
    endforeach()
else()
    message(STATUS "nfs4 MBT deviation-liveness gates skipped (no python3)")
endif()
