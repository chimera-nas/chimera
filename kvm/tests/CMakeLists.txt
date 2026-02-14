# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(CHIMERA_BINARY ${CMAKE_BINARY_DIR}/src/daemon/chimera)
set(NFS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_nfs_test_wrapper.sh)
set(SMB_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_smb_test_wrapper.sh)

set(KVM_BACKENDS memfs linux)

if(CHIMERA_VFS_IO_URING)
    list(APPEND KVM_BACKENDS io_uring)
endif()

if(IO_URING_ENABLED)
    list(APPEND KVM_BACKENDS demofs_io_uring)
endif()

if(HAVE_LIBAIO)
    list(APPEND KVM_BACKENDS demofs_aio)
endif()

set(NFS_VERSIONS 3 4.0 4.1 4.2)

# cthon04 tests
set(CTHON_CATEGORIES basic general special)
set(CTHON_CMD_basic   "cd /opt/cthon04 && ./runtests -b -f /mnt/test")
set(CTHON_CMD_general "cd /opt/cthon04 && ./runtests -g -f /mnt/test")
set(CTHON_CMD_special "cd /opt/cthon04 && ./runtests -s -f /mnt/test")

# SMB cthon04 commands set CIFS=yes so the test suite skips NFS-specific
# sub-tests (e.g. nfsidem duplicate request check, NFS-style op_ren).
# MNTOPTIONS is checked by runtests.wrk to detect SMB version.
set(SMB_CTHON_CMD_basic   "cd /opt/cthon04 && CIFS=yes MNTOPTIONS=vers=2.1 ./runtests -b -f /mnt/test")
set(SMB_CTHON_CMD_general "cd /opt/cthon04 && CIFS=yes MNTOPTIONS=vers=2.1 ./runtests -g -f /mnt/test")
set(SMB_CTHON_CMD_special "cd /opt/cthon04 && CIFS=yes MNTOPTIONS=vers=2.1 ./runtests -s -f /mnt/test")

# xfstests programs
set(XFSTESTS_PROGRAMS fsstress fsx dirstress nametest fstest rewinddir)

set(XFSTESTS_CMD_fsstress  "mkdir -p /mnt/test && /opt/xfstests/ltp/fsstress -d /mnt/test -n 500 -p 1")
set(XFSTESTS_CMD_fsx       "mkdir -p /mnt/test && /opt/xfstests/ltp/fsx -N 10000 -l 262144 /mnt/test/fsx_testfile")
set(XFSTESTS_CMD_dirstress "mkdir -p /mnt/test && /opt/xfstests/src/dirstress -d /mnt/test -p 1 -f 50")
set(XFSTESTS_CMD_nametest  "mkdir -p /mnt/test && cd /mnt/test && /opt/xfstests/src/nametest -l /opt/xfstests/nametest_input -i 1000")
set(XFSTESTS_CMD_fstest    "mkdir -p /mnt/test && /opt/xfstests/src/fstest -p /mnt/test -n 1 -f 2 -l 5 -s 65536")
set(XFSTESTS_CMD_rewinddir "mkdir -p /mnt/test && /opt/xfstests/src/rewinddir-test /mnt/test")

# =============================================================================
# Linux KVM tests (NFS + SMB via Linux CIFS client)
# =============================================================================

if(KVM_TESTING_AVAILABLE)

foreach(image_spec ${KVM_IMAGES})
    string(REPLACE "|" ";" image_parts "${image_spec}")
    list(GET image_parts 0 IMAGE_NAME)
    set(IMAGE_DIR ${BUILD_ROOT}/kvm/${IMAGE_NAME})

    # NFS cthon04 tests
    foreach(nfsver ${NFS_VERSIONS})
        foreach(category ${CTHON_CATEGORIES})
            foreach(backend ${KVM_BACKENDS})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/cthon/${category}_${backend}_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} ${backend} ${nfsver}
                            "${CTHON_CMD_${category}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/cthon/${category}_${backend}_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(nfsver ${NFS_VERSIONS})
            foreach(category ${CTHON_CATEGORIES})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/cthon/${category}_cairn_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} cairn ${nfsver}
                            "${CTHON_CMD_${category}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/cthon/${category}_cairn_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endif()

    # NFS xfstests
    foreach(nfsver ${NFS_VERSIONS})
        foreach(prog ${XFSTESTS_PROGRAMS})
            foreach(backend ${KVM_BACKENDS})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_${backend}_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} ${backend} ${nfsver}
                            "${XFSTESTS_CMD_${prog}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_${backend}_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(nfsver ${NFS_VERSIONS})
            foreach(prog ${XFSTESTS_PROGRAMS})
                add_test(NAME chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_cairn_nfs${nfsver}
                    COMMAND bash ${NFS_WRAPPER}
                            ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                            ${CHIMERA_BINARY} cairn ${nfsver}
                            "${XFSTESTS_CMD_${prog}}")
                set_tests_properties(chimera/kvm/${IMAGE_NAME}/xfstests/${prog}_cairn_nfs${nfsver}
                    PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
            endforeach()
        endforeach()
    endif()

    # SMB cthon04 tests
    foreach(category ${CTHON_CATEGORIES})
        foreach(backend ${KVM_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_${backend}
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${backend}
                        "${SMB_CTHON_CMD_${category}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_${backend}
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(category ${CTHON_CATEGORIES})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_cairn
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} cairn
                        "${SMB_CTHON_CMD_${category}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/cthon/${category}_cairn
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
        endforeach()
    endif()

    # SMB xfstests
    foreach(prog ${XFSTESTS_PROGRAMS})
        foreach(backend ${KVM_BACKENDS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_${backend}
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} ${backend}
                        "${XFSTESTS_CMD_${prog}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_${backend}
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
        endforeach()
    endforeach()

    if(CAIRN_ENABLED)
        foreach(prog ${XFSTESTS_PROGRAMS})
            add_test(NAME chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_cairn
                COMMAND bash ${SMB_WRAPPER}
                        ${IMAGE_DIR}/vmlinuz ${IMAGE_DIR}/rootfs.qcow2
                        ${CHIMERA_BINARY} cairn
                        "${XFSTESTS_CMD_${prog}}")
            set_tests_properties(chimera/kvm/${IMAGE_NAME}/smb/xfstests/${prog}_cairn
                PROPERTIES LABELS "kvm;${IMAGE_NAME}" PROCESSORS 2)
        endforeach()
    endif()

endforeach()

endif() # KVM_TESTING_AVAILABLE

# =============================================================================
# Windows KVM tests (SMB via Windows native client)
# =============================================================================

if(KVM_WINDOWS_TESTING_AVAILABLE)

set(SMB_WINDOWS_WRAPPER ${CMAKE_SOURCE_DIR}/kvm/kvm_smb_windows_test_wrapper.sh)

# Native PowerShell tests over mapped SMB share (Z:\)
set(WINDOWS_TESTS auth_ntlm file_io dir_ops)

# auth_ntlm: share mapping in wrapper verifies NTLM authentication works
set(WINDOWS_CMD_auth_ntlm "Get-PSDrive Z")

# file_io: create file, write, read back, verify content, delete
set(WINDOWS_CMD_file_io [=[
$f = 'Z:\test_file_io.txt'
$data = 'hello chimera'
Set-Content -Path $f -Value $data
$read = Get-Content -Path $f
Remove-Item -Path $f
if ($read -ne $data) { Write-Error "mismatch: expected '$data' got '$read'"; exit 1 }
]=])

# dir_ops: create directory, add files, enumerate, verify count, cleanup
set(WINDOWS_CMD_dir_ops [=[
$d = 'Z:\test_dir_ops'
New-Item -Path $d -ItemType Directory -Force | Out-Null
1..5 | ForEach-Object { Set-Content -Path "$d\file_$_.txt" -Value "content $_" }
$count = (Get-ChildItem -Path $d -File).Count
Remove-Item -Path $d -Recurse -Force
if ($count -ne 5) { Write-Error "expected 5 files, got $count"; exit 1 }
]=])

foreach(test ${WINDOWS_TESTS})
    foreach(backend ${KVM_BACKENDS})
        add_test(NAME chimera/kvm/smb-windows/${test}_${backend}
            COMMAND bash ${SMB_WINDOWS_WRAPPER}
                    ${KVM_WINDOWS_IMAGE_DIR}/windows.qcow2
                    ${KVM_WINDOWS_IMAGE_DIR}/OVMF_VARS.fd
                    ${OVMF_CODE_PATH}
                    ${KVM_WINDOWS_IMAGE_DIR}/ssh_key
                    ${CHIMERA_BINARY} ${backend}
                    "${WINDOWS_CMD_${test}}")
        set_tests_properties(chimera/kvm/smb-windows/${test}_${backend}
            PROPERTIES LABELS "kvm;kvm-windows")
    endforeach()
endforeach()

if(CAIRN_ENABLED)
    foreach(test ${WINDOWS_TESTS})
        add_test(NAME chimera/kvm/smb-windows/${test}_cairn
            COMMAND bash ${SMB_WINDOWS_WRAPPER}
                    ${KVM_WINDOWS_IMAGE_DIR}/windows.qcow2
                    ${KVM_WINDOWS_IMAGE_DIR}/OVMF_VARS.fd
                    ${OVMF_CODE_PATH}
                    ${KVM_WINDOWS_IMAGE_DIR}/ssh_key
                    ${CHIMERA_BINARY} cairn
                    "${WINDOWS_CMD_${test}}")
        set_tests_properties(chimera/kvm/smb-windows/${test}_cairn
            PROPERTIES LABELS "kvm;kvm-windows")
    endforeach()
endif()

endif() # KVM_WINDOWS_TESTING_AVAILABLE
