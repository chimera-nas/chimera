from pathlib import Path
name = 'chimera/kvm/ubuntu2604/nfstest/nfstest_delegation_memfs_nfs4.1'
source = Path('/build/Debug/kvm/tests/CTestTestfile.cmake').read_text()
command = next(line.strip() for line in source.splitlines() if 'add_test(' in line and name in line)
dest = Path('/build/delegation-repro')
dest.mkdir(exist_ok=True)
lines = []
for i in range(4):
    test = f'delegation_{i}'
    lines.append(command.replace(name, test))
    lines.append(f'set_tests_properties({test} PROPERTIES TIMEOUT 1200 ENVIRONMENT "KVM_PCAP_FILE=/build/delegation-{i}.pcap")')
(dest / 'CTestTestfile.cmake').write_text('\n'.join(lines)+'\n')
