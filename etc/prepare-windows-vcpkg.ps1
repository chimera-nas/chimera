# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense

param(
    [Parameter(Mandatory = $true)][string]$Triplet,
    [string]$VcpkgRoot = "$PSScriptRoot/../_vcpkg",
    [string]$OverlayRoot = "$PSScriptRoot/../.vcpkg-overlays"
)
$ErrorActionPreference = 'Stop'

if ($Triplet -ne 'arm64-windows') { return }

# Generate the overlay from the pinned checkout so that upstream port fixes and
# patches remain authoritative. Both publisher and consumer run this before any
# install; vcpkg includes the modified recipe in the package ABI/cache key.
$upstream = Join-Path $VcpkgRoot 'ports/openssl'
$destination = Join-Path $OverlayRoot 'openssl'
$portfile = Join-Path $upstream 'windows/portfile.cmake'
$source = Get-Content $portfile -Raw
$marker = 'set(OPENSSL_BUILD_MAKES_PDBS ON)'
if ([regex]::Matches($source, [regex]::Escape($marker)).Count -ne 1) {
    throw 'The pinned OpenSSL port changed; review the ARM64 stack-probe workaround.'
}
$replacement = @'
# MSVC 19.51 ARM64 miscompiles tls_parse_all_extensions with /O2 /Gs0:
# __chkstk overwrites LR before the function saves it. Use the normal page-size
# stack-probe threshold. /GS buffer-overrun protection remains enabled.
if(VCPKG_TARGET_ARCHITECTURE STREQUAL "arm64" AND
   VCPKG_DETECTED_CMAKE_C_COMPILER_ID STREQUAL "MSVC")
    string(APPEND VCPKG_COMBINED_C_FLAGS_RELEASE " /Gs4096")
endif()
set(OPENSSL_BUILD_MAKES_PDBS ON)
'@
if (Test-Path $destination) { Remove-Item $destination -Recurse -Force }
New-Item -ItemType Directory -Force $OverlayRoot | Out-Null
Copy-Item $upstream $destination -Recurse
$patched = $source.Replace($marker, $replacement).Replace("`r`n", "`n")
[System.IO.File]::WriteAllText((Join-Path $destination 'windows/portfile.cmake'), $patched)
$env:VCPKG_OVERLAY_PORTS = (Resolve-Path $OverlayRoot).Path
if ($env:GITHUB_ENV) {
    "VCPKG_OVERLAY_PORTS=$env:VCPKG_OVERLAY_PORTS" >> $env:GITHUB_ENV
}
Write-Output "Prepared ARM64 OpenSSL overlay: $env:VCPKG_OVERLAY_PORTS"
