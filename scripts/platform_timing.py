#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""Temporary controlled platform comparison; not part of the Windows PR."""
import json
import hashlib
import os
import pathlib
import platform
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

root = pathlib.Path.cwd()
out = root / 'timing-results'
out.mkdir(exist_ok=True)
cohort = json.loads((root / 'scripts/platform-timing-cohort.json').read_text())
windows = sys.platform == 'win32'
config = 'Release' if windows else 'extended'
results = {'platform': platform.platform(), 'machine': platform.machine(),
           'cpus': os.cpu_count(), 'runner_image': os.environ.get('ImageVersion'),
           'revision': os.environ.get('GITHUB_SHA'), 'build_workers': 3,
           'test_workers': 1, 'tests': cohort['tests'], 'targets': cohort['targets'],
           'steps': {}}

def run(name, command):
    start = time.monotonic()
    with (out / (name + '.log')).open('w') as log:
        process = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    results['steps'][name] = {'seconds': time.monotonic() - start,
                              'returncode': process.returncode, 'command': command}
    print(name, results['steps'][name]['seconds'], 'seconds', flush=True)
    if process.returncode:
        print((out / (name + '.log')).read_text(errors='replace')[-30000:])
        raise SystemExit(process.returncode)

try:
    if windows:
        pathlib.Path('/tmp').mkdir(exist_ok=True)
        pathlib.Path('/build/test').mkdir(parents=True, exist_ok=True)
    args = ['cmake', '-S', '.', '-B', 'timing-build',
            '-DCMAKE_BUILD_TYPE=Release', '-DCAIRN_ENABLED=OFF',
            '-DURCU_SUPPORT=OFF', '-DCHIMERA_GSSAPI=OFF',
            '-DCHIMERA_MBT_CORPUS=OFF', '-DKVM_TESTING=OFF',
            '-DIO_URING_ENABLED=OFF', '-DCHIMERA_VFS_IO_URING=OFF',
            '-DLIBAIO_ENABLED=NO', '-DCUDA_SUPPORT=OFF', '-DOTEL_SQLITE=ON',
            '-DCHIMERA_NETNS_TESTING=OFF', '-DLIBEVPL_NETNS_TESTING=OFF']
    if windows:
        arch = os.environ['BENCH_ARCH']
        triplet = arch.lower() + '-windows'
        args += ['-G', os.environ['BENCH_GENERATOR'], '-A', arch,
                 f'-DCMAKE_TOOLCHAIN_FILE={root}/_vcpkg/scripts/buildsystems/vcpkg.cmake',
                 f'-DVCPKG_TARGET_TRIPLET={triplet}', f'-DVCPKG_HOST_TRIPLET={triplet}',
                 f'-DVCPKG_INSTALLED_DIR={root}/build/vcpkg_installed',
                 '-DVCPKG_INSTALL_OPTIONS=--only-binarycaching',
                 f'-DFLEX_EXECUTABLE={os.environ["RUNNER_TEMP"]}/winflexbison/win_flex.exe',
                 f'-DBISON_EXECUTABLE={os.environ["RUNNER_TEMP"]}/winflexbison/win_bison.exe']
    else:
        args += ['-G', 'Ninja']
    run('configure', args)
    inventory = json.loads(subprocess.check_output(
        ['ctest', '--test-dir', 'timing-build', '-C', config, '--show-only=json-v1']))
    registered = {t['name'] for t in inventory['tests']}
    missing = set(cohort['tests']) - registered
    if missing:
        raise RuntimeError(f'Missing comparison tests: {sorted(missing)}')
    selection = out / 'tests.txt'
    selection.write_text('\n'.join(cohort['tests']) + '\n')
    for source in (root / 'timing-build/CMakeFiles').glob('*/CMakeCCompiler.cmake'):
        (out / 'compiler.cmake').write_bytes(source.read_bytes())
    run('corpus', ['cmake', '--build', 'timing-build', '--config', 'Release',
                   '--parallel', '3', '--target', 'diskfs_traces'])
    traces = sorted((root / 'timing-build/src/vfs/diskfs/tests/quint/traces').glob('*.itf.json'))
    results['corpus_sha256'] = {}
    for trace in traces:
        document = json.loads(trace.read_text())
        payload = {'vars': document['vars'], 'states': document['states']}
        results['corpus_sha256'][trace.name] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if len(traces) != 11:
        raise RuntimeError(f'Expected 11 identical storage traces, got {len(traces)}')
    run('cold-build', ['cmake', '--build', 'timing-build', '--config', 'Release',
                      '--parallel', '3', '--target', 'platform_timing_all'])
    run('noop-build', ['cmake', '--build', 'timing-build', '--config', 'Release',
                      '--parallel', '3', '--target', 'platform_timing_all'])
    for sample in range(1, 2):
        name = f'tests-{sample}'
        junit = out / (name + '.xml')
        run(name, ['ctest', '--test-dir', 'timing-build', '-C', config,
                   '--tests-from-file', str(selection), '--parallel', '1',
                   '--no-tests=error', '--output-on-failure', '--timeout', '120',
                   '--output-junit', str(junit)])
        cases = list(ET.parse(junit).getroot().iter('testcase'))
        if len(cases) != len(cohort['tests']) or any(t.find('skipped') is not None for t in cases):
            raise RuntimeError('Comparison must execute every selected test without skips')
        results['steps'][name]['test_seconds'] = sum(float(t.get('time', 0)) for t in cases)
finally:
    (out / 'timings.json').write_text(json.dumps(results, indent=2) + '\n')
