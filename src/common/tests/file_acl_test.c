// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include "common/host_file.h"

int
main(void)
{
    WCHAR directory[MAX_PATH], path[MAX_PATH];
    int   fd;

    assert(GetTempPathW(MAX_PATH, directory));
    assert(GetTempFileNameW(directory, L"acl", 0, path));
    fd = _wopen(path, _O_RDWR | _O_BINARY);
    assert(fd >= 0);
    for (int public_read = 0; public_read < 2; public_read++) {
        PSECURITY_DESCRIPTOR        descriptor = NULL;
        PACL                        acl        = NULL;
        SECURITY_DESCRIPTOR_CONTROL control;
        DWORD                       revision;
        void                       *entry;
        int                         found_world = 0;
        assert(!chimera_host_fchmod(fd, public_read ? 0644 : 0600));
        assert(GetSecurityInfo((HANDLE) _get_osfhandle(fd), SE_FILE_OBJECT,
                               DACL_SECURITY_INFORMATION, NULL, NULL, &acl, NULL,
                               &descriptor) == ERROR_SUCCESS);
        assert(GetSecurityDescriptorControl(descriptor, &control, &revision));
        assert(control & SE_DACL_PROTECTED);
        assert(acl && acl->AceCount == (public_read ? 2 : 1));
        for (DWORD i = 0; i < acl->AceCount; i++) {
            assert(GetAce(acl, i, &entry));
            assert(((ACE_HEADER *) entry)->AceType == ACCESS_ALLOWED_ACE_TYPE);
            if (IsWellKnownSid(&((ACCESS_ALLOWED_ACE *) entry)->SidStart, WinWorldSid)) {
                assert(((ACCESS_ALLOWED_ACE *) entry)->Mask == FILE_GENERIC_READ);
                found_world++;
            }
        }
        assert(found_world == public_read);
        LocalFree(descriptor);
    }
    _close(fd);
    assert(DeleteFileW(path));
    {
        char                        utf8[MAX_PATH * 4];
        PSECURITY_DESCRIPTOR        descriptor = NULL;
        PACL                        acl        = NULL;
        PSID                        owner;
        void                       *entry;
        SECURITY_DESCRIPTOR_CONTROL control;
        DWORD                       revision;
        assert(WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, path, -1,
                                   utf8, sizeof(utf8), NULL, NULL));
        fd = chimera_host_create_private(utf8);
        assert(fd >= 0);
        assert(GetSecurityInfo((HANDLE) _get_osfhandle(fd), SE_FILE_OBJECT,
                               OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                               &owner, NULL, &acl, NULL, &descriptor) == ERROR_SUCCESS);
        assert(GetSecurityDescriptorControl(descriptor, &control, &revision));
        assert(control & SE_DACL_PROTECTED);
        assert(acl && acl->AceCount == 1);
        assert(GetAce(acl, 0, &entry));
        assert(EqualSid(owner, &((ACCESS_ALLOWED_ACE *) entry)->SidStart));
        LocalFree(descriptor);
        assert(_write(fd, "key", 3) == 3);
        assert(chimera_host_create_private(utf8) == -1 && errno == EEXIST);
        assert(!chimera_host_fchmod(fd, 0600));
        _close(fd);
        assert(DeleteFileW(path));
    }
    return 0;
} /* main */
