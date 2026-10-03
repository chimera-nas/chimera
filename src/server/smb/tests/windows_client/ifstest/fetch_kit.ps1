# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: LGPL-2.1-only
#
# Fetch Microsoft's IFSTest from the Windows HLK and print the directory that
# holds ifstest.exe for this machine's architecture.
#
# Nothing from the HLK is stored anywhere we control: every run fetches the
# packages that carry IFSTest ("HLK Filter.Driver Content", about 25 MB with its
# cabinets, and the package with the NTLog library its logging loads) from
# Microsoft's public download location, found through the official
# HLKSetup.exe link on learn.microsoft.com/windows-hardware/test/hlk, and
# extracts them with an administrative install into the runner's temp space.
$ErrorActionPreference = 'Stop'
# Windows HLK for Windows 11 26H1, from the HLK download page.
$link = 'https://go.microsoft.com/fwlink/?linkid=2374411'
$head = Invoke-WebRequest $link -Method Head
$setup = $head.BaseResponse.RequestMessage.RequestUri.AbsoluteUri
if ($setup -notmatch '^https://download\.microsoft\.com/.+/HLKSetup\.exe$') { throw "unexpected kit location: $setup" }
$base = $setup -replace '/HLKSetup\.exe$', ''
Write-Host "kit base: $base"

$kit = "$env:RUNNER_TEMP\hlk"
$extract = "$env:RUNNER_TEMP\hlkx"
New-Item -ItemType Directory -Force $kit | Out-Null
# IFSTest itself, and the package carrying the NTLog library its
# logging loads.
foreach ($msi in 'HLK Filter.Driver Content-x86_en-us.msi', 'HLK OnecoreUAP.Filter.Driver Content-x86_en-us.msi') {
  Invoke-WebRequest "$base/Installers/$([uri]::EscapeDataString($msi))" -OutFile "$kit\$msi"

  # The cabinet names change with every kit release; read them from
  # the package's own Media table.
  $installer = New-Object -ComObject WindowsInstaller.Installer
  $db = $installer.GetType().InvokeMember('OpenDatabase', 'InvokeMethod', $null, $installer, @("$kit\$msi", 0))
  $view = $db.GetType().InvokeMember('OpenView', 'InvokeMethod', $null, $db, @('SELECT `Cabinet` FROM `Media`'))
  $view.GetType().InvokeMember('Execute', 'InvokeMethod', $null, $view, $null) | Out-Null
  $cabs = @()
  while ($record = $view.GetType().InvokeMember('Fetch', 'InvokeMethod', $null, $view, $null)) {
    $cab = $record.GetType().InvokeMember('StringData', 'GetProperty', $null, $record, 1)
    if ($cab) { $cabs += $cab }
  }
  $view.GetType().InvokeMember('Close', 'InvokeMethod', $null, $view, $null) | Out-Null
  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($db) | Out-Null
  foreach ($cab in $cabs) {
    Invoke-WebRequest "$base/Installers/$cab" -OutFile "$kit\$cab"
    Write-Host "fetched $cab ($([math]::Round((Get-Item "$kit\$cab").Length / 1MB, 1)) MB) for $msi"
  }

  $p = Start-Process msiexec.exe -ArgumentList @('/a', "`"$kit\$msi`"", '/qn', "TARGETDIR=`"$extract`"", '/l*v', "`"$env:RUNNER_TEMP\msiexec.log`"") -Wait -PassThru
  if ($p.ExitCode -ne 0) { Get-Content "$env:RUNNER_TEMP\msiexec.log" -Tail 40; throw "administrative extract of $msi failed: $($p.ExitCode)" }
}

# One ifs_test_kit per architecture; pick the one built for this
# machine from ifstest.exe's PE header.
$want = @{ AMD64 = 0x8664; ARM64 = 0xAA64 }[$env:PROCESSOR_ARCHITECTURE]
$dir = $null
foreach ($exe in Get-ChildItem $extract -Recurse -Filter ifstest.exe) {
  $bytes = [System.IO.File]::ReadAllBytes($exe.FullName)
  $pe = [BitConverter]::ToInt32($bytes, 0x3c)
  $machine = [BitConverter]::ToUInt16($bytes, $pe + 4)
  Write-Host ("{0} machine=0x{1:x4}" -f $exe.FullName, $machine)
  if ($machine -eq $want) { $dir = $exe.DirectoryName }
}
if (-not $dir) { throw "no ifstest.exe for $env:PROCESSOR_ARCHITECTURE" }

# Its logging libraries live elsewhere in the kit; the HLK's own jobs
# put them beside the binaries.  Take the build for this machine.
foreach ($lib in 'ntlog.dll', 'FbsLog.dll') {
  $found = $false
  foreach ($f in Get-ChildItem $extract -Recurse -Filter $lib) {
    $bytes = [System.IO.File]::ReadAllBytes($f.FullName)
    $pe = [BitConverter]::ToInt32($bytes, 0x3c)
    if ([BitConverter]::ToUInt16($bytes, $pe + 4) -eq $want) {
      Copy-Item $f.FullName $dir -Force; Write-Host "using $($f.FullName)"; $found = $true; break
    }
  }
  if (-not $found) { throw "no $lib for $env:PROCESSOR_ARCHITECTURE" }
}
$ini = Get-ChildItem $extract -Recurse -Filter ntlogger.ini | Select-Object -First 1
if ($ini) { Copy-Item $ini.FullName $dir -Force; Write-Host "using $($ini.FullName)" }
Write-Host (Get-ChildItem $dir | Format-Table Name, Length -AutoSize | Out-String -Width 200)
# The HLK wrapper passes "-a \datacoh.exe"; give it one at the root too.
Copy-Item "$dir\datacoh.exe" C:\ -Force
$dir
