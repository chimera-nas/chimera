// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "common/platform.h"
#ifdef _WIN32
#include <aclapi.h>

/* Configuration keys and metrics files use 0600/0644. Express those policies
 * with native DACLs, not the CRT read-only attribute. A distinct Unix group
 * policy cannot be mapped without an explicit identity mapping. */
static inline int chimera_host_fchmod(int fd, unsigned mode)
{
    HANDLE original = (HANDLE) _get_osfhandle(fd), handle;
    PSID owner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    PACL acl = NULL;
    EXPLICIT_ACCESSW access[2] = {0};
    union { DWORD alignment; BYTE bytes[SECURITY_MAX_SID_SIZE]; } everyone;
    DWORD sid_size = sizeof(everyone.bytes), error, count = 1;
    if (((mode >> 3) & 7) != (mode & 7) || (mode & ~0777u)) {
        errno = ENOTSUP;
        return -1;
    }
    handle = ReOpenFile(original, READ_CONTROL | WRITE_DAC,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, 0);
    if (handle == INVALID_HANDLE_VALUE) {
        errno = EACCES;
        return -1;
    }
    error = GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION,
                             &owner, NULL, NULL, NULL, &descriptor);
    if (error != ERROR_SUCCESS) {
        goto done;
    }
    access[0].grfAccessPermissions = READ_CONTROL | WRITE_DAC;
    if (mode & 0400) { access[0].grfAccessPermissions |= FILE_GENERIC_READ; }
    if (mode & 0200) { access[0].grfAccessPermissions |= FILE_GENERIC_WRITE; }
    if (mode & 0100) { access[0].grfAccessPermissions |= FILE_GENERIC_EXECUTE; }
    access[0].grfAccessMode = SET_ACCESS;
    access[0].grfInheritance = NO_INHERITANCE;
    access[0].Trustee.TrusteeForm = TRUSTEE_IS_SID;
    access[0].Trustee.ptstrName = (LPWSTR) owner;
    if (mode & 7) {
        if (!CreateWellKnownSid(WinWorldSid, NULL, everyone.bytes, &sid_size)) {
            error = GetLastError();
            goto done;
        }
        if (mode & 4) { access[1].grfAccessPermissions |= FILE_GENERIC_READ; }
        if (mode & 2) { access[1].grfAccessPermissions |= FILE_GENERIC_WRITE; }
        if (mode & 1) { access[1].grfAccessPermissions |= FILE_GENERIC_EXECUTE; }
        access[1].grfAccessMode = SET_ACCESS;
        access[1].grfInheritance = NO_INHERITANCE;
        access[1].Trustee.TrusteeForm = TRUSTEE_IS_SID;
        access[1].Trustee.ptstrName = (LPWSTR) everyone.bytes;
        count = 2;
    }
    error = SetEntriesInAclW(count, access, NULL, &acl);
    if (error == ERROR_SUCCESS) {
        error = SetSecurityInfo(handle, SE_FILE_OBJECT,
                                 DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                                 NULL, NULL, acl, NULL);
    }
done:
    LocalFree(acl);
    LocalFree(descriptor);
    CloseHandle(handle);
    if (error != ERROR_SUCCESS) {
        errno = error == ERROR_NOT_ENOUGH_MEMORY ? ENOMEM : EACCES;
        return -1;
    }
    return 0;
}
#else
#include <sys/stat.h>
#define chimera_host_fchmod fchmod
#endif
