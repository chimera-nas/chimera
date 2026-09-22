---
title: Windows Dependency Packages
layout: default
nav_exclude: true
permalink: /windows-dependencies
---

# Windows dependency packages

The **Publish Windows dependencies** workflow builds Chimera's third-party
dependencies with MSVC and publishes native binary packages to the
organization's GitHub Packages NuGet feed. Ordinary Windows builds can then
download these packages instead of compiling RocksDB, Protobuf, OpenSSL, and
the other dependencies on every runner.

This workflow prepares dependencies only. The Windows application build and
its integration with this feed land separately.

## Publishing

After merging the workflow, run it from the default branch:

```sh
gh workflow run windows-dependencies.yml --ref main
```

It runs two jobs:

| Runner | Visual Studio | Target and host triplet | Configurations |
| --- | --- | --- | --- |
| `windows-2022` | 2022 | `x64-windows` | Debug and Release |
| `windows-11-vs2026-arm` | 2026 | `arm64-windows` | Debug and Release |

Each job checks out the vcpkg revision in `vcpkg.json`'s `builtin-baseline`,
selects the matrix's Visual Studio version, and installs that manifest. The standard triplets
already contain both configurations, so there is no separate Debug/Release
publishing matrix. The initial build is expensive; subsequent runs restore
matching packages from the feed and build only missing ones.

ARM64 uses the explicit VS2026 image because GitHub is migrating the generic
`windows-11-arm` label from VS2022 to VS2026. Publisher and consumer must select
the same image and toolchain rather than depending on that rolling alias.

Packages use the prefix `chimera-` and vcpkg's ABI-based versions. The feed is
`https://nuget.pkg.github.com/chimera-nas/index.json`. These are native C/C++
packages; NuGet is their storage and transport, not a .NET build dependency.
Package storage is independent of the repository's GitHub Actions cache.

The workflow uses its short-lived `GITHUB_TOKEN` with `contents: read` and
`packages: write`. Packages are associated with this repository through their
repository metadata. The organization must permit package creation, and the
packages must grant this repository's Actions workflows access. No personal
access token is required for this same-repository workflow. Publishing runs
only on manual dispatch of the default branch, not on pull requests.

A successful job must also restore the entire manifest into a fresh install
directory using **only the remote feed** and `--only-binarycaching`. Local
binary caches are disabled for this check. Thus an upload failure cannot be
hidden behind a successful source build. Each job records the source commit,
manifest hash, runner image, MSVC tools version, and restored package status
in a diagnostic artifact. NuGet credentials are temporary and are not uploaded.

## Consuming the packages

The Windows build workflow should use:

- The same `vcpkg.json`, vcpkg baseline, Visual Studio selection, runner families,
  and target/host triplets as the publisher.
- `X_VCPKG_NUGET_ID_PREFIX=chimera-`.
- A NuGet config authenticating to this feed using `GITHUB_TOKEN`, with package
  read access. Consumer jobs need `contents: read` and `packages: read`, not
  package write permission.
- `VCPKG_BINARY_SOURCES=clear;nugetconfig,<config-path>,read`.
- `vcpkg install --only-binarycaching`, or
  `-DVCPKG_INSTALL_OPTIONS=--only-binarycaching` with CMake's vcpkg toolchain.

A missing binary should fail promptly rather than silently compile dependencies
inside every test job. Publish the required packages first, then rerun the
consumer. Fork PR access must be verified when connecting the consumer workflow;
never give a PR package-write credentials to make a restore work.

Rerun the publisher after manifest/baseline changes or runner updates that alter
the dependency ABI. vcpkg includes compiler, features, dependencies, and build
tool details in its ABI calculation, so a hosted image update can require new
packages even when `vcpkg.json` is unchanged. Older versions remain usable by
consumers with matching ABIs.

References: [vcpkg binary caching](https://learn.microsoft.com/en-us/vcpkg/reference/binarycaching),
[GitHub's NuGet authentication](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-nuget-registry),
and [binary-only installation](https://learn.microsoft.com/en-us/vcpkg/commands/install#--only-binarycaching).
