# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# These are the model-based tests, so they make up the quick tier that plain
# `ctest` runs.  See the test-tier block in the top-level CMakeLists.txt.
set(CHIMERA_TEST_TIER quick)

# POSIX model-based tests: the Quint specification (posix.qnt and friends) and
# its trace generation live in the chimera-nas/specs repo (submodule ext/specs).
# This directory owns the replay harness -- posix_mbt_replay.c (a pure-C
# replayer that runs the model oracle in-process against chimera's POSIX client
# over an in-process memfs mount) and posix_driver.c / posix_replay.py (the
# JSON line-protocol driver + Python replayer used for the NFS-loopback
# backends) -- which drive that corpus against chimera and fail on any
# unregistered divergence.
#
# The trace corpus is resolved once, at the top level, by cmake/SpecsBundle.cmake
# into SPECS_BUNDLE_DIR.  The specs `posix` suite generates every backend
# profile into one directory, distinguished by filename prefix (memfs has none;
# nfs3_/nfs4_/disk_ for the loopback and diskfs/cairn profiles); the replay
# selects a profile with --trace-dir plus --exclude-prefix.

add_executable(posix_quint_driver posix_driver.c)
target_link_libraries(posix_quint_driver
    chimera_posix chimera_client chimera_server chimera_metrics jansson)

# Pure-C replayer: reuses posix_driver.c's op engine in-process (no Python, no
# line-protocol subprocess) to load ITF traces and run the model oracle.
add_executable(posix_mbt_replay posix_mbt_replay.c)
target_include_directories(posix_mbt_replay PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(posix_mbt_replay
    chimera_posix chimera_client chimera_server chimera_metrics jansson)

# Ground-truth probe for the SMB2 loopback.  Corpus-free (a fixed script, not a
# model replay), so it runs whether or not a trace bundle is available -- and it
# is what actually executes src/vfs/smb in the quick tier while the MBT batch
# below stays parked.  See the SD1-SD5 block further down, and the file header
# for what its pinned-defect assertions mean.
add_executable(smb_loopback_probe smb_loopback_probe.c)
target_include_directories(smb_loopback_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smb_loopback_probe
    chimera_posix chimera_client chimera_server chimera_metrics jansson)

add_test(NAME chimera/posix/smb/loopback_probe
    COMMAND $<TARGET_FILE:smb_loopback_probe>)
set_tests_properties(chimera/posix/smb/loopback_probe PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_probe"
    LABELS "quint;posix_mbt;memfs;smb_mbt"
    TIMEOUT 300)

# The same probe once per SMB dialect the client can carry.  Without a pinned
# vers= the server selects the highest dialect in common, which against our own
# server is always 3.1.1 -- so 2.1, 3.0 and 3.0.2 were dead code in practice no
# matter how many tests ran.  Pinning is the only way to reach them.
#
# A pinned mount offers exactly ONE dialect, so a mount that comes up is proof
# the connection is speaking that dialect: the server can only select the one
# offered or fail to find one in common.  There is no readback to trust.
foreach(vers 2.1 3.0 3.0.2 3.1.1)
    add_test(NAME chimera/posix/smb/loopback_probe_${vers}
        COMMAND $<TARGET_FILE:smb_loopback_probe> --vers=${vers})
    set_tests_properties(chimera/posix/smb/loopback_probe_${vers} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_probe_${vers}"
        LABELS "quint;posix_mbt;memfs;smb_mbt"
        TIMEOUT 300)
endforeach()

# 2.0.2 is the dialect the client does NOT carry: it is absent from
# CHIMERA_SMB_CLIENT_DIALECTS, and chimera_smb_client_negotiate_reply refuses it
# outright.  The server does speak it, so this is a real negotiation that the
# CLIENT declines -- and it is what proves vers= is honoured at all.  A vers=
# that was silently ignored would negotiate 3.1.1, mount happily, and fail this
# test.  If 2.0.2 support is ever added, this test starts failing and says so.
add_test(NAME chimera/posix/smb/loopback_dialect_refused_2.0.2
    COMMAND $<TARGET_FILE:smb_loopback_probe>
            --vers=2.0.2 --expect-mount-failure)
set_tests_properties(chimera/posix/smb/loopback_dialect_refused_2.0.2 PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_refused_2.0.2"
    LABELS "quint;posix_mbt;memfs;smb_mbt"
    TIMEOUT 300)

# ---------------------------------------------------------------------------
# Wire policy: what the client does when the SERVER demands something.
#
# These vary the server's configuration, not the client's capabilities, which
# is what makes them interesting -- the smb client VFS module carries signing
# but NO encryption and NO compression (there is no transform header, no
# AEAD, and no codec anywhere in src/vfs/smb).  So each cell answers "what
# happens when the far end insists", and the answer is only sometimes "it
# works".
#
#   name                       server demands            expected
#   sign_required              SIGNING_REQUIRED          mount + probe (3.x signs)
#   encrypt_enabled            encryption = 1 (offer)    mount + probe: the
#                                                        client offers no cipher
#                                                        and mode 1 does not
#                                                        insist, which is the
#                                                        spec-correct degrade
#   encrypt_required           encryption = 2 (require)  mount REFUSED
#   compress_enabled           compression advertised    mount + probe: the
#                                                        client never negotiates
#                                                        it, so the share flag is
#                                                        inert
#   leases                     oplocks + leases granted  mount + probe
foreach(cell "sign_required|--sign-required"
             "encrypt_enabled|--encrypt"
             "compress_enabled|--compress"
             "leases|--leases"
             "all_features|--sign-required;--encrypt;--compress;--leases")
    string(REPLACE "|" ";" parts "${cell}")
    list(GET parts 0 cname)
    list(GET parts 1 cflags)
    add_test(NAME chimera/posix/smb/loopback_wire_${cname}
        COMMAND $<TARGET_FILE:smb_loopback_probe> ${cflags})
    set_tests_properties(chimera/posix/smb/loopback_wire_${cname} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_wire_${cname}"
        LABELS "quint;posix_mbt;memfs;smb_mbt"
        TIMEOUT 300)
endforeach()

# Sealed sessions: the whole POSIX probe replayed over SMB3 transport
# encryption.  seal=yes makes the client offer a cipher, so these are the cells
# where the client actually encrypts rather than merely tolerating a demand.
#
# The dialect split is load-bearing, not redundant: 3.0/3.0.2 negotiate no
# encryption context and can only use AES-128-CCM with the "ServerIn "/
# "ServerOut" KDF contexts, while 3.1.1 negotiates AES-128-GCM keyed by
# SMBC2SCipherKey/SMBS2CCipherKey over the preauth hash.  Different cipher,
# different mode, different key schedule -- covered here as 0x0001 and 0x0002
# respectively.  A mistake in either would not corrupt data, it would fail every
# AEAD tag check, so these tests either pass or the mount dies.
foreach(vers 3.0 3.0.2 3.1.1)
    add_test(NAME chimera/posix/smb/loopback_sealed_${vers}
        COMMAND $<TARGET_FILE:smb_loopback_probe>
                --seal --encrypt --vers=${vers})
    set_tests_properties(chimera/posix/smb/loopback_sealed_${vers} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_sealed_${vers}"
        LABELS "quint;posix_mbt;memfs;smb_mbt"
        TIMEOUT 300)
endforeach()

# Sealed against a server that REQUIRES encryption -- the pairing the previous
# commit's enforcement exists for: the demand is now satisfiable rather than
# only refusable.
add_test(NAME chimera/posix/smb/loopback_sealed_required
    COMMAND $<TARGET_FILE:smb_loopback_probe> --seal --encrypt-required)
set_tests_properties(chimera/posix/smb/loopback_sealed_required PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_sealed_required"
    LABELS "quint;posix_mbt;memfs;smb_mbt"
    TIMEOUT 300)

# seal= is a security request, so the two ways it cannot be met are refusals,
# not silent downgrades to cleartext:
#
#   sealed_unsupported_by_server  the server has encryption off.  It still
#                                 SELECTS a cipher for any 3.1.1 client that
#                                 offers one, so a negotiated cipher is not
#                                 evidence the peer expects ciphertext --
#                                 encrypting anyway sends traffic it drops.
#   sealed_2.1                    encryption does not exist below 3.0.
foreach(cell "sealed_unsupported_by_server|--seal"
             "sealed_2.1|--seal;--encrypt;--vers=2.1")
    string(REPLACE "|" ";" parts "${cell}")
    list(GET parts 0 cname)
    list(GET parts 1 cflags)
    add_test(NAME chimera/posix/smb/loopback_${cname}_refused
        COMMAND $<TARGET_FILE:smb_loopback_probe> ${cflags}
                --expect-mount-failure)
    set_tests_properties(chimera/posix/smb/loopback_${cname}_refused PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_${cname}"
        LABELS "quint;posix_mbt;memfs;smb_mbt"
        TIMEOUT 300)
endforeach()

# SMB3 transport compression.  The probe's compressible round trip (a 16 KiB
# repeating pattern written, read back in one pread, compared byte for byte) is
# what makes these cells mean anything: chimera compresses only a READ reply,
# only when the client set SMB2_READFLAG_REQUEST_COMPRESSED, and only when the
# payload actually shrinks -- so a suite whose largest read is three bytes would
# negotiate compression and never execute a byte of the decoder.
#
# Measured on the wire: 464 bytes expanding to 12,368, and zero decompressions
# in the client_silent cell below.  If that ever inverts, the cells are lying.
foreach(cell "compressed|--client-compress;--compress"
             "compressed_sealed|--client-compress;--compress;--seal;--encrypt"
             "compressed_server_declines|--client-compress"
             "compressed_client_silent|--compress")
    string(REPLACE "|" ";" parts "${cell}")
    list(GET parts 0 cname)
    list(GET parts 1 cflags)
    add_test(NAME chimera/posix/smb/loopback_${cname}
        COMMAND $<TARGET_FILE:smb_loopback_probe> ${cflags})
    set_tests_properties(chimera/posix/smb/loopback_${cname} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_${cname}"
        LABELS "quint;posix_mbt;memfs;smb_mbt"
        TIMEOUT 300)
endforeach()

# The one cell whose answer is a refusal.  smb_encryption=2 REQUIRES encryption;
# this client negotiates none, so the session setup must be denied.  Before the
# enforcement fix that ships with this test, the knob was read as a boolean
# everywhere in the server and mode 2 behaved exactly like mode 1 -- the whole
# session was served in cleartext while configured to require encryption.
add_test(NAME chimera/posix/smb/loopback_wire_encrypt_required_refused
    COMMAND $<TARGET_FILE:smb_loopback_probe>
            --encrypt-required --expect-mount-failure)
set_tests_properties(chimera/posix/smb/loopback_wire_encrypt_required_refused
    PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=smb_loopback_wire_encrypt_required"
    LABELS "quint;posix_mbt;memfs;smb_mbt"
    TIMEOUT 300)

if(NOT SPECS_BUNDLE_AVAILABLE)
    message(STATUS "posix MBT replay ctest skipped (no specs trace corpus)")
    return()
endif()

# One batched replay: a single posix_mbt_replay runs the model oracle in-process
# across the whole memfs corpus, cycling a fresh uniquely-named filesystem per
# trace ("newfs"), so the dominant per-trace cost -- driver server/client init +
# mount -- is paid once instead of once per trace.  Traces whose LInit profile
# does not match the backend skip individually; the batch reports SKIP (77) only
# if every trace skips.
#
# The specs `posix` corpus carries per-backend profiles distinguished by
# filename prefix: memfs (no prefix, and also what diskfs replays), the NFS
# loopbacks (nfs3_/nfs4_), the no-clone profile (disk_, shared by cairn and
# the linux/io_uring passthroughs) and FUSE (fuse_).  Each registered batch
# selects its profile with --exclude-prefix.
add_test(NAME chimera/posix/mbt/batch_memfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend memfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix nfs3_ --exclude-prefix nfs4_ --exclude-prefix disk_
            --exclude-prefix fuse_)
set_tests_properties(chimera/posix/mbt/batch_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_memfs"
    LABELS "quint;posix_mbt;memfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 300)

# The memfs-profile corpus against diskfs: a chimera-native backend
# (model-conformant DAC, no host-kernel deviations), so it replays the memfs
# profile rather than the disk_ one, and the per-trace newfs recycle
# mkfs-cycles it exactly like memfs.  The replay process self-provisions its
# scratch device image under its own mkdtemp session dir (posix_env_setup) and
# takes whichever libevpl block backend the platform has
# (CHIMERA_DISKFS_DEVICE_TYPE).  Five traces are declined BY NAME in
# posix_mbt_declines (posix_mbt_replay.c), each naming the diskfs bug that
# retires the entry.
#
add_test(NAME chimera/posix/mbt/batch_diskfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend diskfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix nfs3_ --exclude-prefix nfs4_ --exclude-prefix disk_
            --exclude-prefix fuse_)
set_tests_properties(chimera/posix/mbt/batch_diskfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_diskfs"
    LABELS "quint;posix_mbt;diskfs"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

# cairn, the other on-disk backend, completing the engine-backend matrix.  It
# is the one engine backend the memfs profile does not fit: cairn advertises
# neither CHIMERA_VFS_CAP_COPY_RANGE nor CHIMERA_VFS_CAP_CLONE_RANGE, and
# while the VFS supplies a generic copy fallback for the former there is no
# clone fallback -- clone answers ENOTSUP.  So cairn replays the `disk_`
# profile (posix_run.qnt's posixDisk instance, F_CLONE_RANGE = FeatOff), the
# same profile the linux/io_uring passthrough batches below replay, and
# selects it the same way: exclude the unprefixed memfs traces (they all
# begin "step") and the three prefixed profiles, leaving disk_ alone.
#
# diskfs stays on the memfs profile above rather than joining cairn here: it
# does implement clone, so holding it to the model-conformant profile is what
# exercises that path (and is what found the clone defects behind its two
# remaining declines).
add_test(NAME chimera/posix/mbt/batch_cairn
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend cairn
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix step --exclude-prefix nfs3_
            --exclude-prefix nfs4_ --exclude-prefix fuse_)
set_tests_properties(chimera/posix/mbt/batch_cairn PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_cairn"
    LABELS "quint;posix_mbt;cairn"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

# The same corpus replayed through the NFS loopback: an in-process chimera
# server exports a memfs filesystem over the inproc transport and the posix
# client mounts it through the nfs proxy module (src/vfs/nfs) with vers=3 --
# the whole client stack, wire included, under the POSIX model.  The nfs3_
# profile pins the NFS-flavored capabilities (strict atime, no copy/clone/
# seek offload); the client-behavior deviations a real NFS client cannot
# avoid (silly rename and friends) reconcile in the replayer as the ND*
# registry.  The per-trace recycle unmounts the client, cycles the
# server-side filesystem, and remounts, so every trace also exercises the
# proxy's mount/umount paths.
add_test(NAME chimera/posix/mbt/batch_nfs3_memfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend nfs3_memfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix step --exclude-prefix disk_
            --exclude-prefix fuse_ --exclude-prefix nfs4_)
set_tests_properties(chimera/posix/mbt/batch_nfs3_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs3_memfs"
    LABELS "quint;posix_mbt;memfs;nfs3_mbt"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

# The same POSIX surface through the NFSv4.1 loopback: stateful opens,
# sessions/slots, and the pseudo-fs export junction all sit on the path.
# The nfs4_ trace profile models search permissions, so the DAC answers
# are asserted directly rather than reconciled.
add_test(NAME chimera/posix/mbt/batch_nfs4_memfs
    COMMAND $<TARGET_FILE:posix_mbt_replay>
            --backend nfs4_memfs
            --trace-dir ${SPECS_BUNDLE_DIR}/posix
            --exclude-prefix step --exclude-prefix disk_
            --exclude-prefix fuse_ --exclude-prefix nfs3_)
set_tests_properties(chimera/posix/mbt/batch_nfs4_memfs PROPERTIES
    ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs4_memfs"
    LABELS "quint;posix_mbt;memfs;nfs4_mbt"
    SKIP_RETURN_CODE 77
    TIMEOUT 600)

# The same corpus through the SMB2 loopback: the posix client mounts the
# in-process chimera SMB server through the smb proxy VFS module (src/vfs/smb).
# The nfs3_ profile is the closest pinned match to what `posix_replay.py
# --probe --backend smb_memfs` measures (they differ only in copy_file_range,
# which the model exercises through an advertised-feature knob).
#
# OFF by default, and it is not a flake: the SMB2 loopback cannot satisfy the
# POSIX model today, and the batch fails ~49 of 53 traces.  The blockers are
# properties of src/vfs/smb, not of the harness:
#
#   SD1  a session is bound to the connection that authenticated it.  A second
#        client thread opens its own connection and replays the mount's session
#        id on it, and the server answers one of the session/connection-gone
#        statuses, which the proxy maps to ESTALE -- so the first operation
#        after the mount fails whenever it lands on another thread.  The driver
#        pins core_threads=1 to work around it; SMB3 multichannel session
#        binding is what would actually fix it.
#   SD2  readdir returns exactly ONE entry per directory, whatever its size.
#        The QUERY_DIRECTORY buffer is walked until the emit callback reports
#        full, and the unconsumed remainder is freed; the resume query asks the
#        server for the NEXT batch, which is empty, so entries 2..N are lost.
#   SD3  symlink_at directly under the mount root reports SUCCESS without
#        creating anything: lstat of the new name then answers ENOENT, readlink
#        answers ELOOP, and it is absent from readdir.
#   SD4  no POSIX owner or mode.  chmod/chown are accepted and dropped, getattr
#        reports the CALLING credential as the owner and a synthesized mode
#        (0755/0644), and directory nlink is always 1.  Every EACCES/EPERM the
#        model expects therefore does not happen.
#   SD5  only the mount-root handle is re-openable by fh (chimera_smb_open_fh),
#        so every op the client routes through a resolved PARENT handle rather
#        than through its path-op strategy answers ESTALE: link and utimens at
#        any depth, and symlink/mknod below the first level.  ESTALE is also
#        the wrong answer for an operation the backend simply does not
#        implement -- mknod at the first level, which does reach the module,
#        correctly answers ENOTSUP.
#
# Turn it on with -DCHIMERA_POSIX_MBT_SMB=ON to measure progress against that
# list; it joins the quick tier only once it can pass.
option(CHIMERA_POSIX_MBT_SMB
       "Register the POSIX MBT batch over the SMB2 loopback (known failing)" OFF)
if(CHIMERA_POSIX_MBT_SMB)
    add_test(NAME chimera/posix/mbt/batch_smb_memfs
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend smb_memfs
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix disk_
                --exclude-prefix fuse_ --exclude-prefix nfs4_)
    set_tests_properties(chimera/posix/mbt/batch_smb_memfs PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_smb_memfs"
        LABELS "quint;posix_mbt;memfs;smb_mbt"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

# The disk_ profile replayed against the host-passthrough backends.  The
# backing store is a fresh directory per trace under $CHIMERA_MBT_SCRATCH
# (default: the build tree), whose filesystem must support name_to_handle_at
# -- tmpfs/overlayfs do not, and the driver turns that ENOTSUP into a clean
# SKIP.  The modules run with per-operation credential impersonation, so the
# batch needs root (CI containers; skipped otherwise via the mount failure).
if(CHIMERA_LINUX)
    add_test(NAME chimera/posix/mbt/batch_linux
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend linux
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix nfs3_
                --exclude-prefix nfs4_ --exclude-prefix fuse_)
    set_tests_properties(chimera/posix/mbt/batch_linux PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_linux"
        LABELS "quint;posix_mbt;linux"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

if(CHIMERA_VFS_IO_URING)
    add_test(NAME chimera/posix/mbt/batch_io_uring
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend io_uring
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix nfs3_
                --exclude-prefix nfs4_ --exclude-prefix fuse_)
    set_tests_properties(chimera/posix/mbt/batch_io_uring PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_io_uring"
        LABELS "quint;posix_mbt;io_uring"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endif()

# The NFS loopback profiles replayed with a passthrough backend under the
# server: the full client stack (posix client -> nfs proxy -> wire -> chimera
# NFS server) on top of the host-kernel filesystem semantics.  The backing
# store and skip behavior are as for the direct passthrough batches above.
set(POSIX_MBT_PT_LOOPBACKS "")
if(CHIMERA_LINUX)
    list(APPEND POSIX_MBT_PT_LOOPBACKS linux)
endif()
if(CHIMERA_VFS_IO_URING)
    list(APPEND POSIX_MBT_PT_LOOPBACKS io_uring)
endif()

foreach(ptmod ${POSIX_MBT_PT_LOOPBACKS})
    add_test(NAME chimera/posix/mbt/batch_nfs3_${ptmod}
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend nfs3_${ptmod}
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix disk_
                --exclude-prefix fuse_ --exclude-prefix nfs4_)
    set_tests_properties(chimera/posix/mbt/batch_nfs3_${ptmod} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs3_${ptmod}"
        LABELS "quint;posix_mbt;${ptmod};nfs3_mbt"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)

    add_test(NAME chimera/posix/mbt/batch_nfs4_${ptmod}
        COMMAND $<TARGET_FILE:posix_mbt_replay>
                --backend nfs4_${ptmod}
                --trace-dir ${SPECS_BUNDLE_DIR}/posix
                --exclude-prefix step --exclude-prefix disk_
                --exclude-prefix fuse_ --exclude-prefix nfs3_)
    set_tests_properties(chimera/posix/mbt/batch_nfs4_${ptmod} PROPERTIES
        ENVIRONMENT "CHIMERA_MBT_ARTIFACTS=posix_batch_nfs4_${ptmod}"
        LABELS "quint;posix_mbt;${ptmod};nfs4_mbt"
        SKIP_RETURN_CODE 77
        TIMEOUT 600)
endforeach()
