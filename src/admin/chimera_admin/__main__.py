# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Package entry point so the CLI can be run as ``python3 -m chimera_admin``."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
