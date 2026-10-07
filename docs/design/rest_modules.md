<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Unlicense
-->

# REST modules: SDK and runtime loading

Status: implemented with the module naming and configuration revisions below.
The public contract and configuration are documented in [REST module SDK](../rest-module-sdk.md).

All endpoints are owned by configured shared libraries. There is no automatic
core registration. The host derives `/api/<module>/v<api_version>/...` from each
descriptor. Core, docs, and the test-only debug module use the same loader and
route registry as independently compiled third-party modules. HTTP API versions
and SDK ABI versions are separate.

The implementation separates transport/request ownership (`rest_request.c`),
module loading and registration (`rest_module.c`), the core route table
(`rest_core.c`), documentation (`rest_swagger.c`), and test mutations
(`rest_debug.c`). Existing management implementations are compiled into core;
shared authentication and response transport remain in the host.

A request wrapper outlives the underlying libevpl request after disconnect.
Handlers own one completion, and replies from any thread are copied and queued
back to the originating event loop. Shutdown drains those completions before
module state and library handles are destroyed. The libevpl HTTP teardown must
notify pending server replies of disconnect even after their request bodies are
complete, and keep each server object alive until its connections close.

The first implementation keeps fixed ABI-major layouts and module-owned worker
queues. Live reload, host scheduling/timer services, streaming, a public VFS
management service, and general role-based authorization remain future work.
Core and debug may use private LGPL management interfaces and are rebuilt with
the host; third-party modules compile solely against the public SDK.

Quick-tier probes exercise explicit module selection, two API versions with the
same entry symbol, inherited authentication, URI boundaries, body limits,
OpenAPI publication, loader errors, and cancellation during shutdown. Existing
control-plane traces continue to exercise the migrated management endpoints.
