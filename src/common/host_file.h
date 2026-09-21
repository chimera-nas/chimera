// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "common/platform.h"
#include <errno.h>
#include <string.h>
#ifdef _WIN32
#include <aclapi.h>

static inline int
chimera_host_temp_directory(char *path, size_t capacity)
{
    WCHAR wide[32768];
    DWORD length = GetTempPathW(32768, wide);
    if (!length || length >= 32768 || capacity > INT_MAX ||
        !WideCharToMultiByte(CP_UTF8, 0, wide, -1, path, (int) capacity, NULL, NULL)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

/* Configuration keys and metrics files use 0600/0644. Express those policies
 * with native DACLs, not the CRT read-only attribute. A distinct Unix group
 * policy cannot be mapped without an explicit identity mapping. */
static inline DWORD
chimera_host_permission_acl(
    PSID     owner,
    unsigned mode,
    PACL    *acl)
{
    EXPLICIT_ACCESSW access[2] = { 0 };

    union { DWORD alignment; BYTE bytes[SECURITY_MAX_SID_SIZE]; } everyone;
    DWORD            sid_size = sizeof(everyone.bytes), count = 1;
    if (((mode >> 3) & 7) != (mode & 7) || (mode & ~0777u)) {
        return ERROR_NOT_SUPPORTED;
    }
    access[0].grfAccessPermissions = READ_CONTROL | WRITE_DAC;
    if (mode & 0400) {
        access[0].grfAccessPermissions |= FILE_GENERIC_READ;
    }
    if (mode & 0200) {
        access[0].grfAccessPermissions |= FILE_GENERIC_WRITE;
    }
    if (mode & 0100) {
        access[0].grfAccessPermissions |= FILE_GENERIC_EXECUTE;
    }
    access[0].grfAccessMode       = SET_ACCESS;
    access[0].grfInheritance      = NO_INHERITANCE;
    access[0].Trustee.TrusteeForm = TRUSTEE_IS_SID;
    access[0].Trustee.ptstrName   = (LPWSTR) owner;
    if (mode & 7) {
        if (!CreateWellKnownSid(WinWorldSid, NULL, everyone.bytes, &sid_size)) {
            return GetLastError();
        }
        if (mode & 4) {
            access[1].grfAccessPermissions |= FILE_GENERIC_READ;
        }
        if (mode & 2) {
            access[1].grfAccessPermissions |= FILE_GENERIC_WRITE;
        }
        if (mode & 1) {
            access[1].grfAccessPermissions |= FILE_GENERIC_EXECUTE;
        }
        access[1].grfAccessMode       = SET_ACCESS;
        access[1].grfInheritance      = NO_INHERITANCE;
        access[1].Trustee.TrusteeForm = TRUSTEE_IS_SID;
        access[1].Trustee.ptstrName   = (LPWSTR) everyone.bytes;
        count                         = 2;
    }
    return SetEntriesInAclW(count, access, NULL, acl);
} // chimera_host_permission_acl

static inline int
chimera_host_fchmod(
    int      fd,
    unsigned mode)
{
    HANDLE               original = (HANDLE) _get_osfhandle(fd), handle;
    PSID                 owner      = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    PACL                 acl        = NULL;
    DWORD                error;

    handle = ReOpenFile(original, READ_CONTROL | WRITE_DAC,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, 0);
    if (handle == INVALID_HANDLE_VALUE) {
        errno = EACCES; return -1;
    }
    error = GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION,
                            &owner, NULL, NULL, NULL, &descriptor);
    if (error == ERROR_SUCCESS) {
        error = chimera_host_permission_acl(owner, mode, &acl);
    }
    if (error == ERROR_SUCCESS) {
        error = SetSecurityInfo(handle, SE_FILE_OBJECT,
                                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                                NULL, NULL, acl, NULL);
    }
    LocalFree(acl);
    LocalFree(descriptor);
    CloseHandle(handle);
    if (error != ERROR_SUCCESS) {
        errno = error == ERROR_NOT_ENOUGH_MEMORY ? ENOMEM :
            error == ERROR_NOT_SUPPORTED ? ENOTSUP : EACCES;
        return -1;
    }
    return 0;
} // chimera_host_fchmod

/* Set the private DACL at creation, before another process could obtain a
 * read handle. Tightening permissions on an already-created empty file would
 * not revoke handles opened before the key is written. */
static inline int
chimera_host_create_private(const char *path)
{
    HANDLE              token = NULL, file = INVALID_HANDLE_VALUE;
    TOKEN_USER         *user = NULL;
    PACL                acl  = NULL;
    SECURITY_DESCRIPTOR descriptor;
    SECURITY_ATTRIBUTES attributes = { sizeof(attributes), &descriptor, FALSE };
    WCHAR              *wide       = NULL;
    DWORD               bytes = 0, error = ERROR_ACCESS_DENIED;
    int                 length, fd = -1;

    length = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0);
    if (!length) {
        errno = EINVAL; return -1;
    }
    wide = malloc((size_t) length * sizeof(*wide));
    if (!wide) {
        errno = ENOMEM; return -1;
    }
    MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide, length);
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        goto done;
    }
    GetTokenInformation(token, TokenUser, NULL, 0, &bytes);
    if (!bytes) {
        goto done;
    }
    user = malloc(bytes);
    if (!user) {
        error = ERROR_NOT_ENOUGH_MEMORY; goto done;
    }
    if (!GetTokenInformation(token, TokenUser, user, bytes, &bytes)) {
        goto done;
    }
    error = chimera_host_permission_acl(user->User.Sid, 0600, &acl);
    if (error != ERROR_SUCCESS) {
        goto done;
    }
    if (!InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) ||
        !SetSecurityDescriptorOwner(&descriptor, user->User.Sid, FALSE) ||
        !SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) ||
        !SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED)) {
        error = GetLastError();
        goto done;
    }
    file = CreateFileW(wide, GENERIC_WRITE | READ_CONTROL | WRITE_DAC,
                       FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                       &attributes, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
    if (file == INVALID_HANDLE_VALUE) {
        error = GetLastError(); goto done;
    }
    fd = _open_osfhandle((intptr_t) file, _O_WRONLY | _O_BINARY);
    if (fd < 0) {
        CloseHandle(file); error = ERROR_TOO_MANY_OPEN_FILES;
    }

 done:
    if (token) {
        CloseHandle(token);
    }
    LocalFree(acl);
    free(user);
    free(wide);
    if (fd < 0) {
        errno = error == ERROR_FILE_EXISTS || error == ERROR_ALREADY_EXISTS ? EEXIST :
            error == ERROR_PATH_NOT_FOUND || error == ERROR_FILE_NOT_FOUND ? ENOENT :
            error == ERROR_NOT_ENOUGH_MEMORY ? ENOMEM :
            error == ERROR_TOO_MANY_OPEN_FILES ? EMFILE : EACCES;
    }
    return fd;
} // chimera_host_create_private
#else // ifdef _WIN32
#include <sys/stat.h>
#include <fcntl.h>
#define chimera_host_fchmod fchmod

static inline int
chimera_host_temp_directory(char *path, size_t capacity)
{
    if (capacity < sizeof("/tmp/")) { errno = ENAMETOOLONG; return -1; }
    memcpy(path, "/tmp/", sizeof("/tmp/"));
    return 0;
}
static inline int
chimera_host_create_private(const char *path)
{
    return open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
} // chimera_host_create_private
#endif // ifdef _WIN32
