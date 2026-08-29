# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Control-plane tests: the REST API and the Prometheus scrape endpoint,
# driven in-process over libevpl's inproc transport (ctl_mbt_common.h).
#
# Unlike the older REST tests in the parent directory -- which shell out to
# curl against a real port and therefore run under the netns resource lock --
# nothing here binds a port, so every probe runs fully parallel on any host,
# the same way the NFS and SMB2 quint harnesses do.

find_package(PkgConfig REQUIRED)
pkg_check_modules(JANSSON REQUIRED jansson)

add_executable(ctl_smoke_probe ctl_smoke_probe.c)
target_include_directories(ctl_smoke_probe PRIVATE
    ${CMAKE_SOURCE_DIR}/src ${JANSSON_INCLUDE_DIRS})
target_link_libraries(ctl_smoke_probe
    chimera_server chimera_metrics ${JANSSON_LIBRARIES})

# Ground truth for the control plane: the routing edges, the admin lifecycle
# over HTTP, the config reconstruction, and the scrape endpoint.  Needs no
# trace corpus and always runs.
add_test(NAME chimera/server/rest/ctl/smoke_probe
    COMMAND $<TARGET_FILE:ctl_smoke_probe>)
set_tests_properties(chimera/server/rest/ctl/smoke_probe PROPERTIES
    LABELS "quint;ctl_mbt;memfs"
    TIMEOUT 120)

# The control plane's effect on the data protocols: one process running the
# NFS server, the REST API and the scrape endpoint on inproc, with an rpc2
# client and an HTTP client on one event loop.  Reuses the NFS harness
# (nfs3_mbt_common.h) for the protocol half rather than duplicating it.
add_executable(ctl_proto_probe ctl_proto_probe.c)
target_include_directories(ctl_proto_probe PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/server/nfs/tests/quint
    ${CMAKE_BINARY_DIR}/src/nfs_common
    ${JANSSON_INCLUDE_DIRS})
target_link_libraries(ctl_proto_probe
    chimera_server chimera_nfs_common chimera_metrics ${JANSSON_LIBRARIES})

add_test(NAME chimera/server/rest/ctl/proto_probe
    COMMAND $<TARGET_FILE:ctl_proto_probe>)
set_tests_properties(chimera/server/rest/ctl/proto_probe PROPERTIES
    LABELS "quint;ctl_mbt;memfs"
    TIMEOUT 120)
