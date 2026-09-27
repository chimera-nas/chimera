# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
from pathlib import Path
name = 'chimera/kvm/ubuntu2404/cthon/special_memfs_nfs4.2'
source = Path('/build/Debug/kvm/tests/CTestTestfile.cmake').read_text()
command = next(line.strip() for line in source.splitlines() if 'add_test(' in line and name in line)
old = 'cd /opt/cthon04 && ./runtests -s -f /mnt/test'
new = 'uname -r; cd /mnt && /opt/cthon04/special/nfsidem 10000 testdir' + '; rc=$?; if [ $rc -ne 0 ]; then echo FAILURE_DIRECTORY_CONTENTS; ls -laiR /mnt/testdir; sleep 1; echo AFTER_ONE_SECOND; ls -laiR /mnt/testdir; fi; exit $rc'
assert old in command
command = command.replace(old, new)
dest = Path('/build/nfsidem-repro')
dest.mkdir(exist_ok=True)
lines = []
for i in range(4):
    test = f'nfsidem_{i}'
    lines.append(command.replace(name, test))
    lines.append(f'set_tests_properties({test} PROPERTIES TIMEOUT 600 ENVIRONMENT "KVM_PCAP_FILE=/build/nfsidem-{i}.pcap")')
(dest / 'CTestTestfile.cmake').write_text('\n'.join(lines)+'\n')
