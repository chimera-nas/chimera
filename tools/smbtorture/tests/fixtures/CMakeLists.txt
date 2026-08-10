# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

set(SMBTORTURE_SUITES
    smb2.acls.CREATOR
    smb2.acls.DENY1
    "smb2.charset.Testing wide-a"
    smb2.dir.large-files
    smb2.setinfo
)
set(SMBTORTURE_ENABLED_memfs
    smb2.acls.CREATOR
    smb2.acls.DENY1
)
set(SMBTORTURE_ENABLED_linux
    smb2.acls.CREATOR
    smb2.setinfo
    "smb2.charset.Testing wide-a"
)
set(SMBTORTURE_DISABLED_SUBTESTS
    smb2.setinfo
)
