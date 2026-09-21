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

Install Visual Studio 2022 or its Build Tools with the Desktop development
with C++ workload, a Windows SDK, and the compiler tools for your target
architecture. Also install CMake, Git, Python 3, Node.js 22, and
WinFlexBison 2.5.25. Use an ARM64 target and dependencies in a Windows ARM
VM on Apple silicon.

From PowerShell, starting in a recursive checkout:

```powershell
git submodule update --init --recursive
git clone https://github.com/microsoft/vcpkg _vcpkg
git -C _vcpkg checkout a1cae005c39be7b18ba319fced856b68d7276271
./_vcpkg/bootstrap-vcpkg.bat -disableMetrics
$quintVersion = (Get-Content ext/specs/.quint-version).Trim()
npm install -g "@informalsystems/quint@$quintVersion"

# Change both values to x64 / x64-windows for an Intel/AMD target.
$arch = 'ARM64'
$triplet = 'arm64-windows'
$parserTools = 'C:/tools/winflexbison'
cmake -S . -B build -G 'Visual Studio 17 2022' -A $arch `
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
cmake -S src/common/tests/native -B runtime -G 'Visual Studio 17 2022' -A ARM64
cmake --build runtime --config Debug
ctest --test-dir runtime -C Debug --output-on-failure
```

The full suite exercises the portable VFS, protocol servers, client APIs,
and model replay harnesses. Native POSIX client tests operate on Chimera's
virtual filesystems; they do not implement POSIX semantics for arbitrary
Windows host files. Fork-based stress programs, kernel mounts, network
namespaces, and external Unix Samba tools remain Unix test paths.

## Platform choices

* Native threads, synchronization, QSBR reclamation, scalar atomics, timers,
  and secure randomness replace dependencies on pthreads and liburcu.
  Unix retains its existing liburcu implementation; `CHIMERA_NATIVE_RCU=ON`
  lets developers exercise the native implementation there too.
* `memfs`, `diskfs`, and `cairn` use the shared VFS. Windows diskfs uses
  libevpl's native implementation of its portable block backend. The Linux
  passthrough backend, io_uring, libaio, RDMA, XLIO, FUSE, and GPUDirect
  Storage are unavailable on Windows.
* TLS and protocol cryptography use OpenSSL. Windows private-key files get
  an owner-only protected DACL when created, before key bytes are written.
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
