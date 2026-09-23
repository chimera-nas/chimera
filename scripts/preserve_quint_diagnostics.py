#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
from collections import deque
from pathlib import Path

out = Path('/workspace/quint-diagnostics')
for source in Path('/build').rglob('*.debug.log'):
    try:
        with source.open(errors='replace') as stream:
            tail = ''.join(deque(stream, maxlen=1000))
        relative = str(source.relative_to('/build')).replace('/', '__')
        (out / relative).write_text(tail)
    except OSError as error:
        print(source, error)
