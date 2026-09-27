#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

"""Forward guest serial output and bound shutdown after its final result."""

import argparse
import os
import re
import selectors
import signal
import subprocess
import sys
import time

RESULT = re.compile(rb"^(?:\[\s*\d+\.\d+\]\s*)?CHIMERA_KVM_EXIT_CODE=(\d+)\r?$")


class Interrupted(Exception):
    def __init__(self, signum):
        self.signum = signum


def interrupt(signum, _frame):
    raise Interrupted(signum)


def stop_guest(child):
    # The launcher (ip/timeout) and QEMU share a private process group. Never
    # signal a sibling CTest's VM, and reap descendants even if the launcher
    # has already exited while a child still holds the serial pipe open.
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.wait()


def run(command, shutdown_grace):
    child = subprocess.Popen(command, stdout=subprocess.PIPE, start_new_session=True)
    result = None
    deadline = None
    pending = b""
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            while selector.get_map():
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    print("Guest reported result %d but did not exit within %.1fs; "
                          "stopping its process group." % (result, shutdown_grace), flush=True)
                    break
                timeout = min(0.2, max(0, deadline - now)) if deadline else 0.2
                events = selector.select(timeout)
                if not events and child.poll() is not None:
                    break
                for key, _ in events:
                    data = os.read(key.fd, 65536)
                    if not data:
                        selector.unregister(key.fileobj)
                        break
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                    pending += data
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        match = RESULT.fullmatch(line)
                        if result is None and match and int(match[1]) <= 255:
                            result = int(match[1])
                            deadline = time.monotonic() + shutdown_grace
                    # Diagnostic output can contain very long lines; only the
                    # short result line needs to survive between reads.
                    if len(pending) > 8192:
                        pending = pending[-8192:]
            # A guest can close stdout before it exits. Its result still bounds
            # that wait; without a result the outer timeout remains in charge.
            if child.poll() is None and deadline is not None:
                try:
                    child.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
            elif child.poll() is None and result is None:
                child.wait()
        return result if result is not None else 1
    finally:
        stop_guest(child)
        child.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shutdown-grace", type=float, default=5)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command or not (0 < args.shutdown_grace < float("inf")):
        parser.error("a command and a finite positive shutdown grace are required")
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    try:
        return run(command, args.shutdown_grace)
    except Interrupted as exc:
        return 128 + exc.signum


if __name__ == "__main__":
    sys.exit(main())
