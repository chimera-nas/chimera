# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Chimera Admin SDK - Python client for Chimera REST API."""

from .client import ChimeraAdminClient
from .client import ChimeraAdminError

__all__ = ["ChimeraAdminClient", "ChimeraAdminError"]
__version__ = "0.1.0"
