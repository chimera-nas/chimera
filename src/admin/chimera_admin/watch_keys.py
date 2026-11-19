# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Apache-2.0

"""Read a single quit key from a TTY during text watch mode."""

import os
import select
import sys
import time


class WatchKeys:
    """Make q available without Enter and restore the input terminal mode."""

    def __init__(self, stream=None):
        self.stream = stream or sys.stdin
        self.fd = None
        self.original = None
        self.msvcrt = None
        if not self.stream.isatty():
            return
        if os.name == "nt":
            try:
                import msvcrt
                self.msvcrt = msvcrt
            except ImportError:
                return
            return
        try:
            import termios
            import tty
            self.fd = self.stream.fileno()
            self.original = termios.tcgetattr(self.fd)
            tty.setcbreak(self.fd)
        except (AttributeError, ImportError, OSError):
            self.close()

    @property
    def active(self):
        return self.msvcrt is not None or self.fd is not None

    def poll(self):
        if self.msvcrt is not None:
            while self.msvcrt.kbhit():
                if self.msvcrt.getwch().lower() == "q":
                    return True
            return False
        if self.fd is None:
            return False
        try:
            ready, _, _ = select.select([self.fd], [], [], 0)
            if ready:
                return os.read(self.fd, 1).lower() == b"q"
        except (OSError, ValueError):
            return False
        return False

    def wait(self, duration):
        deadline = time.monotonic() + max(0.0, duration)
        while True:
            if self.poll():
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return self.poll()
            time.sleep(min(0.1, remaining))

    def close(self):
        if self.fd is None or self.original is None:
            return
        import termios
        try:
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.original)
        except (OSError, termios.error):
            pass
        self.fd = None
        self.original = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
