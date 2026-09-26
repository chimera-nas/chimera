<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Unlicense
-->

# REST module SDK

REST endpoints are provided by explicitly configured, dynamically loaded modules.
The host constructs every URL as `/api/<module>/v<api_version><relative_path>`.
The module's HTTP API version is independent of the REST SDK ABI version and of
Chimera's release version. A single server can load different API versions of
the same module from different libraries.

## Platform status

The current implementation uses POSIX dynamic loading and the Unix shared-library
layout. Native Windows support is pending: the loader and module dependencies
need integration with the Windows static-library layout before this change can
build there.

## Configuration

No modules are loaded by default, including core. A listener with an empty
module list returns 404 for every request. Configuring modules without an HTTP
or HTTPS listener is an error.

```json
{
  "server": {
    "rest_http_port": 8080,
    "rest_modules": [
      {"module": "core", "allow_public_routes": true},
      {"module": "docs", "allow_public_routes": true},
      {
        "module": "example",
        "module_path": "/opt/vendor/lib/rest_example.so",
        "config": {}
      }
    ]
  }
}
```

The module name in config must match the library descriptor. Names start with a
lowercase ASCII letter and contain lowercase letters, digits, hyphens, or
underscores. When `module_path` is omitted, Chimera loads
`chimera_rest_<module><platform-module-suffix>` from the directory containing
`libchimera_rest`. Explicit paths must be absolute. There is no directory scan or
fallback to process-global module symbols. Each configured library is loaded
with `RTLD_NOW | RTLD_LOCAL`; entry-point lookup uses that library's handle.

`config` is a JSON object passed to the module's initializer. Its compact encoding
must fit the server configuration's 8192-byte field. The server accepts at most
16 REST module entries. Module paths and arbitrary module configuration are not
returned by the core configuration endpoint.

`allow_public_routes` defaults to false. A module with public routes requires
it to be true. Core needs it for login and version, and docs needs it for its
public documentation. All other routes inherit `rest_auth_enabled` (true by
default). Enabling public routes does not disable authentication on the module's
protected routes.

The standard modules are:

| Module | Routes | Distribution |
| --- | --- | --- |
| `core` | `/api/core/v1/version`, `/auth/login`, `/config`, `/users`, `/exports`, `/shares`, `/buckets`, `/mounts`, `/filesystems`, with resource suffixes as applicable | Installed shared library |
| `docs` | `/api/docs/v1` and `/api/docs/v1/`, `/api/docs/v1/openapi.json`, and Swagger JavaScript/CSS assets beneath `/api/docs/v1/` | Installed shared library |
| `debug` | `POST /api/debug/v1/fsop` | Built only when tests are enabled; not installed |

In the core row, routes after `version` are relative to `/api/core/v1`. The old
`/version`, `/api/v1/...`, `/api/docs`, and `/api/openapi.json` URLs are removed.
The old `rest_debug_fsops` switch is replaced by an explicit debug module entry.
Pynfs delegation tests load only debug; other suites load the modules they need.

Changing configuration or replacing a library requires a server restart. Module
load, ABI, configuration, or initialization errors are fatal at startup, with a
module-specific diagnostic; endpoints are never silently omitted.

## Building a module

The installed header is `include/chimera/rest/sdk/chimera_rest_sdk.h`. It contains
only declarations and constants and depends only on standard C headers. A vendor
module needs neither the Chimera source tree nor a link dependency on a Chimera
library. The host supplies all SDK operations as function pointers.

CMake consumers can use `find_package(ChimeraRestSDK CONFIG REQUIRED)` and
`target_link_libraries(my_module PRIVATE Chimera::RestSDK)`. This interface
target supplies the SDK include directory without linking a host library.
The headers, CMake package, and LGPL license text are also available through the
`rest-sdk` install component.

The example in `examples/rest_module` can build independently:

```sh
cmake -S examples/rest_module -B example-build \
    -DCHIMERA_REST_SDK_DIR=/usr/local/include/chimera/rest/sdk
cmake --build example-build
```

It demonstrates parameter access, request bodies, per-thread state, a worker
replying after cancellation, and worker joining during teardown. Its wait route
is a lifecycle demonstration, not a production long-polling implementation.

Export exactly one C entry point using `CHIMERA_REST_EXPORT`:

```c
CHIMERA_REST_EXPORT const struct chimera_rest_module *
chimera_rest_module_get_v1(void);
```

The returned descriptor specifies `CHIMERA_REST_ABI_VERSION`, its byte size,
name, positive `api_version`, an array of route pointers, and optional lifecycle
callbacks. The descriptor, routes, strings, and function pointers must remain
valid until unload. A route specifies its size, HTTP method, relative path,
flags, body limit, and handler. For example:

```c
static const struct chimera_rest_route route = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method = "GET",
    .path = "/snapshots/{id}",
    .handle = snapshot_get,
};
```

A module named `storage` with API version 2 exposes that route at
`/api/storage/v2/snapshots/{id}`. Path templates support whole-segment `{name}`
and a terminal `{path...}` that captures a nonempty suffix including slashes.
Literal and parameter patterns that overlap for the same method are rejected.
Different methods can share a path. GET, HEAD, POST, PUT, and DELETE are supported;
HEAD needs an explicit route. The host suppresses HEAD response bytes.

Query strings are separated before matching and available verbatim through
`host->query()`. Captured parameters are decoded once. Malformed escapes,
encoded slash/backslash, and NUL are rejected. No implicit trailing-slash alias
is added. The docs module explicitly registers both forms of its root.

## Request ownership and threading

Each handler receives one owned opaque `chimera_rest_request`. The handler must
call `host->reply()` exactly once, immediately or later. Returning from the
handler does not complete or release the request. Reply consumes ownership;
no access to the request, its data, or a second reply is valid afterward.

The host's immutable accessors expose method, full path, captured parameters,
raw query, copied headers, body bytes/length, and authenticated subject. Subject
is NULL when no authentication occurred (including public routes and globally
disabled authentication). Authorization and Proxy-Authorization headers are not
exposed. Authentication establishes identity, not a general administrator role;
modules implement operation-specific authorization.

Requests are dispatched only after the complete body arrives. Each route has a
body limit, at most 65536 bytes. Bodies over the limit are discarded and answered
with 413 after reception; truncated bodies are never passed to handlers. A path
has a 4096-byte limit. Response data and content type are copied by `reply()`.
The SDK exposes no transport pointers, Jansson objects, or shared allocators.

Handlers run on the owning REST event-loop thread and must not block it. An
owned request and its immutable data can be passed to a module worker;
`cancelled()` and `reply()` are thread-safe. The host marshals replies back to
the owner loop. Modules bound and manage their own worker queues. Module/thread
state must remain valid until all its requests and workers finish.

A disconnect sets the cancellation flag and detaches the underlying HTTP object.
The owned wrapper survives. The module must still reply to release it, even when
cancelled; the host discards a reply whose transport has gone away. Cancellation
does not roll back filesystem changes. A module must synchronize competing
completion paths itself; accessing a consumed request is a programming error.

## Lifecycle and ABI

1. Load and validate all configured module descriptors. Run each global init
   with the host table and borrowed config JSON. Copy configuration that will be
   retained. Initializers must not start work before they can report success.
2. Initialize each module on every core thread. `thread_init` receives global
   state and a borrowed opaque context. The current SDK provides no operations
   on that context. Without thread_init, handlers receive global state directly.
3. Start listening only after all threads have initialized.
4. At thread shutdown, quiesce module producers, close transports, and drain all
   owned requests while their completion doorbell and dependencies remain alive.
5. Destroy thread state on its owner thread. Global destruction follows after
   threads have joined and precedes destruction of supporting VFS/protocol state.
6. Close library handles only after module destruction has joined all workers.

`thread_quiesce` signals producers to stop and must return promptly. Destructors
must not submit asynchronous operations. A module that never completes an owned
request can prevent shutdown; a timeout cannot make unloading executing native
code safe. The first SDK has no host worker pool, timer service, live reload,
streaming API, or public VFS management service. Native plugins run with daemon
privileges and are not sandboxed.

ABI major 1 has fixed structure layouts guarded by byte sizes. An incompatible
layout or semantic change requires a new ABI major. Modules must test the host
ABI and size in init before using its function table. Compatibility is within a
platform's C ABI, not across operating systems or architectures. Keep C++
exceptions within module code.

The shipped modules are LGPL implementation modules built together with the host
and also use private interfaces; rebuild them with Chimera. External
modules use only the public SDK and are insulated from those private layouts.

## Documentation and licensing

The docs module serves a host-generated OpenAPI document containing only loaded
routes. The core's detailed schemas seed the document. Other routes receive
basic generated descriptions; a module can supply an `openapi_paths` JSON object
with relative paths and method operation objects to override them. The host
applies the module prefix, generates unique operation IDs and missing path
parameters, and sets effective authentication requirements. HTTP routing always
comes from route descriptors, not the document.

The SDK headers contain no substantial inline implementation. Modules can be
independently written and distributed under other licenses subject to applicable
LGPL linking/distribution requirements. Keep Chimera replaceable by an
interface-compatible modified build; provide required notices and license
copies, satisfy source obligations for any shipped Chimera code/modifications,
and preserve the rights required by LGPL section 6. A `.so` extension alone does
not establish compliance. See [LGPL 2.1](../LICENSE), especially sections 4-6.
The standalone example is MIT licensed; the SDK and host remain LGPL-2.1-only.
