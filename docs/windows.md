---
title: Native Windows
layout: default
nav_order: 3
permalink: /windows
---

<!-- SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors -->
<!-- SPDX-License-Identifier: LGPL-2.1-only -->

# Native Windows development

Chimera's Windows build uses MSVC, the Windows SDK, and libevpl's native
IOCP/Winsock backend. It does not require Cygwin or an MSYS runtime. Build
and test both Debug and Release; the CI matrix includes x64 and ARM64.

## Build

Install Visual Studio 2022 or 2026 (or its Build Tools) with the Desktop development
with C++ workload, a Windows SDK, and the compiler tools for your target
architecture. Also install CMake, Git, Python 3, Node.js 22, and
WinFlexBison 2.5.25. Use an ARM64 target and dependencies in a Windows ARM
VM on Apple silicon.

From PowerShell, starting in a recursive checkout:

```powershell
git submodule update --init --recursive
git clone https://github.com/microsoft/vcpkg _vcpkg
$baseline = (Get-Content vcpkg.json -Raw | ConvertFrom-Json).'builtin-baseline'
git -C _vcpkg checkout $baseline
./_vcpkg/bootstrap-vcpkg.bat -disableMetrics
$quintVersion = (Get-Content ext/specs/.quint-version).Trim()
npm install -g "@informalsystems/quint@$quintVersion"

# Change both values to x64 / x64-windows for an Intel/AMD target.
$arch = 'ARM64'
$triplet = 'arm64-windows'
# Use 'Visual Studio 17 2022' if that is the version installed locally.
$generator = 'Visual Studio 18 2026'
$parserTools = 'C:/tools/winflexbison'
cmake -S . -B build -G $generator -A $arch `
  "-DCMAKE_TOOLCHAIN_FILE=$pwd/_vcpkg/scripts/buildsystems/vcpkg.cmake" `
  "-DVCPKG_TARGET_TRIPLET=$triplet" "-DVCPKG_HOST_TRIPLET=$triplet" `
  "-DFLEX_EXECUTABLE=$parserTools/win_flex.exe" `
  "-DBISON_EXECUTABLE=$parserTools/win_bison.exe" `
  -DOTEL_SQLITE=ON -DREQUIRE_CTL_MBT=ON -DREQUIRE_DISKFS_MBT=ON
cmake --build build --config Debug --parallel 4
```

The manifest installs the native dependencies through vcpkg. The parser
generators are build tools, not a POSIX runtime dependency. Generated model
traces use the pinned Quint release through Node.js, without requiring Unix
symlinks or a shell. Model corpus generation can take substantially longer
than compiling the C sources.

CI uses prebuilt dependencies from the [Windows dependency publisher](/windows-dependencies):
VS2022 on `windows-2022` for x64, and VS2026 on the explicit
`windows-11-vs2026-arm` image for ARM64. It restores packages with read-only
access and `--only-binarycaching`, then passes that restriction to CMake too.
A missing package fails promptly instead of starting a source build. Rerun the
publisher on `main` when the manifest or dependency toolchain changes, then
rerun the Windows job. The restore step records its duration in the job summary.
The local build commands above still permit source builds when no feed is configured.

Executables and their dependent DLLs are placed in `build/bin/Debug` or
`build/bin/Release`. Keep the DLLs beside the executable when running it.
Pass an explicit configuration file with `chimera.exe -c <file>`; the Linux
example configuration includes backends that are unavailable on Windows.

## Test

Some shared fixtures use `/tmp` and `/build/test`. Create these directories
on the current drive before running the full suite, as the workflow does:

```powershell
New-Item -ItemType Directory -Force /tmp, /build/test | Out-Null
ctest --test-dir build -C Debug --output-on-failure --parallel 4 --timeout 300
cmake --build build --config Release --parallel 4
ctest --test-dir build -C Release --output-on-failure --parallel 4 --timeout 300
```

The runtime primitives can be built and tested independently, without
vcpkg or model generation:

```powershell
cmake -S src/common/tests/native -B runtime -G $generator -A $arch
cmake --build runtime --config Debug
ctest --test-dir runtime -C Debug --output-on-failure
```

The full suite exercises the portable VFS, protocol servers, client APIs,
and model replay harnesses. Native POSIX client tests operate on Chimera's
virtual filesystems; they do not implement POSIX semantics for arbitrary
Windows host files. Fork-based stress programs, kernel mounts, network
namespaces, and external Unix Samba tools remain Unix test paths.

## Platform choices

* Native threads, synchronization, scalar atomics, timers and secure randomness
  replace POSIX dependencies. Windows uses the domain-lock reclamation fallback
  from `common/chimera_rcu.h`, backed by native mutexes and condition variables
  with writer preference. Unix uses liburcu when available; `URCU_SUPPORT=OFF`
  exercises the same fallback there. The earlier Windows-specific QSBR
  implementation and `CHIMERA_NATIVE_RCU` option have been removed.
* `memfs`, `diskfs`, and `cairn` use the shared VFS. Windows diskfs uses
  libevpl's native implementation of its portable block backend. The Linux
  passthrough backend, io_uring, libaio, RDMA, XLIO, FUSE, and GPUDirect
  Storage are unavailable on Windows.
* Windows TLS uses libevpl's Schannel backend. SMB signing/encryption, NTLM,
  REST tokens, S3 authentication, and secure randomness use Windows CNG;
  Base64 uses Crypt32. Linux and macOS retain OpenSSL. No OpenSSL package or
  DLL is required on Windows. Without a supplied certificate/key pair, the
  Windows daemon uses libevpl's self-signed identity without writing PEM files.
  The daemon calls `evpl_cleanup()` after stopping its server and exporters,
  before Windows unloads the crypto and RPC libraries.
* The Windows authentication build supports Chimera-managed users and
  native password verification. MIT Kerberos/GSSAPI defaults off, and
  requests requiring that provider fail explicitly. NSS, Winbind, and an
  SSPI/Active Directory provider are not part of this port.
* Chimera libraries are static archives on Windows, with built-in backend
  registration. This preserves one copy of shared state across the cyclic
  VFS/server dependency graph. Loading external VFS modules through
  `module_path` is not supported by this build.
* The public client API uses `chimera_off_t`, `chimera_dev_t`,
  `chimera_dirpos_t`, and `chimera_posix_stat_t` so file offsets, device IDs, directory
  positions, and metadata remain
  full width under Windows' LLP64 ABI. On Unix these alias the corresponding
  native types. Code consuming the API should use the Chimera names.

The daemon runs as a console application. Windows Service Control Manager
integration and an installer are separate work.

The POSIX client preserves quota and stale-handle errors on Windows using
`EDQUOT` (2001) and `ESTALE` (2002), defined by Chimera because the Windows
CRT does not provide them. Compare these symbols when handling errors;
the CRT `strerror()` does not supply descriptions for these two values.

## Crypto backend validation

`Crypto backends` runs fixed digest, HMAC, CMAC, GMAC, SP800-108 KDF, RC4,
and AES-128/256 CCM/GCM vectors on x64 and ARM64 in Debug and Release,
without downloading any third-party libraries. The tests include split MAC
input, embedded NULs in KDF labels, multi-block KDF output, and rejection and
clearing of unauthenticated plaintext. The same tests exercise OpenSSL in the
normal Unix suite, and can run separately:

```sh
cmake -S src/common/tests/crypto -B crypto
cmake --build crypto
ctest --test-dir crypto --output-on-failure
```

SMB and S3 wire probes retain independent protocol encoders and use the shared
primitive interface. Windows GMAC currently gathers scatter/gather input as
contiguous AAD for CNG; the Unix backend continues streaming it to OpenSSL.
