# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(PYNFS_WRAPPER ${CMAKE_SOURCE_DIR}/scripts/pynfs_test_wrapper.sh)
set(PYNFS_DIR /opt/pynfs)

set(PYNFS_BACKENDS memfs linux io_uring demofs_io_uring)

if(HAVE_LIBAIO)
    list(APPEND PYNFS_BACKENDS demofs_aio)
endif()

# NFSv4.0 test flags
set(PYNFS_FLAGS_0
    access acl close commit compound create getattr getfh link lock lockt locku
    lookup lookupp nverify open openconfirm opendowngrade putfh putpubfh putrootfh
    read readdir readlink releaselockowner remove rename renew replay restorefh
    savefh secinfo setattr setclientid setclientidconfirm verify write)

# NFSv4.1 test flags
set(PYNFS_FLAGS_1
    compound courteous create_session currentstateid deleg destroy_clientid
    destroy_session exchange_id getfh lookup lookupp open putfh reclaim_complete
    rename sequence verify xattr)

# NFSv4.2 test flags (4.1 flags plus copy and sparse)
set(PYNFS_FLAGS_2 ${PYNFS_FLAGS_1} copy sparse)

set(PYNFS_MINOR_VERSIONS 0 1 2)

foreach(minor ${PYNFS_MINOR_VERSIONS})
    foreach(flag ${PYNFS_FLAGS_${minor}})
        foreach(backend ${PYNFS_BACKENDS})
            add_test(NAME chimera/pynfs/${flag}_${backend}_nfs4.${minor}
                COMMAND bash ${PYNFS_WRAPPER}
                        ${CHIMERA_BINARY} ${backend} ${minor} ${PYNFS_DIR}
                        ${flag})
            set_tests_properties(chimera/pynfs/${flag}_${backend}_nfs4.${minor}
                PROPERTIES
                    LABELS pynfs
                    ENVIRONMENT "PYTHONUNBUFFERED=1")
        endforeach()
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(minor ${PYNFS_MINOR_VERSIONS})
        foreach(flag ${PYNFS_FLAGS_${minor}})
            add_test(NAME chimera/pynfs/${flag}_cairn_nfs4.${minor}
                COMMAND bash ${PYNFS_WRAPPER}
                        ${CHIMERA_BINARY} cairn ${minor} ${PYNFS_DIR}
                        ${flag})
            set_tests_properties(chimera/pynfs/${flag}_cairn_nfs4.${minor}
                PROPERTIES
                    LABELS pynfs
                    ENVIRONMENT "PYTHONUNBUFFERED=1")
        endforeach()
    endforeach()
endif()
