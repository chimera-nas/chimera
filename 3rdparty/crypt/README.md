<!-- SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors -->
<!-- SPDX-License-Identifier: Unlicense -->

`crypt_sha512.c` comes from musl's public-domain SHA-512 crypt implementation:
https://git.musl-libc.org/cgit/musl/tree/src/crypt/crypt_sha512.c

Retrieved 2026-09-17. Changes are SPDX annotations, an unsigned loop-bound constant for MSVC,
and renaming the entry point to `chimera_crypt_sha512` to avoid a reserved libc symbol. This
provides native Windows verification of existing `$6$` password hashes. Other
crypt formats are rejected on Windows. Unix builds continue to use libcrypt.
The implementation includes an internal known-answer self-test.
