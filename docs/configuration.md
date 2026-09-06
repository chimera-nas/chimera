---
title: Configuration
layout: default
nav_order: 3
permalink: /configuration
---

# Configuration
{: .no_toc }

Chimera is configured with a single JSON file. The same file format is read by
the **server** (the `chimera_server` daemon) and, for the relevant sections, by
the **client** library and benchmarking tools. This page documents every
parameter, its type, default, and effect.

All values are parsed with [jansson](https://github.com/akheron/jansson).
Unknown keys are ignored, and any key that is absent falls back to the default
listed below, so a minimal config only needs the sections you actually use.

1. TOC
{:toc}

---

## Top-level structure

The config is a single JSON object. Which sections are consulted depends on who
reads the file:

| Section | Type | Read by | Purpose |
|---|---|---|---|
| `common` | object | server + client | Shared transport, memory, and delegation settings. |
| `server` | object | server | Daemon-wide settings (threads, protocols, ports, REST, pNFS...). |
| `mounts` | object | server | VFS backends to instantiate, keyed by name. |
| `exports` | object | server | NFS exports. |
| `shares` | object | server | SMB shares. |
| `buckets` | object | server | S3 buckets. |
| `fuse_mounts` | object | server | Local kernel FUSE mounts (Linux only). |
| `users` | array | server + client | Built-in user accounts (uid/gid, passwords). |
| `s3_access_keys` | array | server | S3 access/secret key pairs, each bound to a user. |
| `s3_anon` | object | server | Identity an S3 key with no user binding acts as. |
| `config` | object | client | Client-library settings (the client's analogue of `server`). |

A typical server config wires a **mount** (a VFS backend) and then publishes it
through one or more of `exports`, `shares`, and `buckets`:

```json
{
    "common": { "tcp_flavor": "plain" },
    "server": { "threads": 16 },
    "mounts":  { "data": { "module": "memfs", "path": "/" } },
    "exports": { "/nfs":  { "path": "/data" } },
    "shares":  { "smb":   { "path": "/data" } },
    "buckets": { "s3":    { "path": "/data" } }
}
```

> **Paths starting with `/`** in `exports`/`shares`/`buckets` refer to the
> Chimera VFS namespace (i.e. `/<mount-name>`), **not** the host filesystem. The
> `path` inside a `mounts` entry is interpreted by that mount's VFS module.

### Size values

Parameters documented as a **size** accept either a byte count integer
(`2097152`) or a string with a single `K`/`M`/`G` suffix (1024-based, optional
trailing `iB`/`B`), e.g. `"2M"` or `"1G"`.

---

## `common` (shared)

Honored identically by the server and the client. The delegation keys here take
**precedence** over the per-side equivalents in `server`/`config`, so this is
the canonical place to set them.

| Key | Type | Default | Description |
|---|---|---|---|
| `tcp_flavor` | string | `"plain"` | TCP stream backend: `"plain"` (kernel sockets), `"io_uring"`, or `"xlio"` (Mellanox userspace TCP). |
| `sync_delegation` | bool | - | Enable the synchronous VFS delegation thread pool. Overrides `server.sync_delegation` / `config.sync_delegation`. |
| `sync_delegation_threads` | int | - | Size of the synchronous delegation pool. |
| `async_delegation` | bool | - | Enable the asynchronous VFS delegation thread pool. |
| `async_delegation_threads` | int | - | Size of the asynchronous delegation pool. |
| `huge_pages` | bool | libevpl default | Allocate libevpl memory from huge pages. |
| `huge_page_size` | size | libevpl default | Huge page size to request (e.g. `"2M"`, `"1G"`). |
| `slab_size` | size | libevpl default | libevpl memory slab size. |
| `preallocate_slabs` | int | libevpl default | Number of memory slabs to preallocate at startup. |
| `preallocate_threads` | int | libevpl default | Threads used to preallocate slabs in parallel. |
| `rdmacm_tos` | int | `0` | RoCEv2 traffic class stamped on every RDMA QP. ToS = DSCP x 4 (e.g. `104` for DSCP 26) so the fabric's lossless/PFC class carries Chimera traffic. |
| `metrics_file` | string | - | On shutdown, write a final Prometheus scrape to this file (so short runs keep their metrics). |
| `umount_timeout_ms` | int | `1000` | How long an unmount waits for the mount's open handles to be closed and released before giving up and returning `EBUSY`. Raise it for backends whose closes are slow; it exists so that unmounting a genuinely busy mount fails rather than hanging. |

---

## Server configuration

### `server` section

| Key | Type | Default | Description |
|---|---|---|---|
| `threads` | int | `8` | Core event-loop worker threads. |
| `max_open_files` | int | `65535` | `RLIMIT_NOFILE` the daemon raises itself to. |
| `sync_delegation` | bool | `true` | Enable the synchronous delegation pool (prefer the `common` key). |
| `sync_delegation_threads` | int | `8` | Synchronous delegation pool size. |
| `async_delegation` | bool | `false` | Enable the asynchronous delegation pool. |
| `async_delegation_threads` | int | `8` | Asynchronous delegation pool size. |
| `nfs_port` | int | `2049` | NFS (v3/v4) listen port. |
| `lockmgr_port` | int | `32803` | NLM (NFSv3 lock manager) port. |
| `rdma` | bool | `false` | Enable NFS-over-RDMA. |
| `rdma_hostname` | string | - | Address to bind the RDMA listener to. |
| `rdma_port` | int | `20049` | NFS-over-RDMA listen port. |
| `external_portmap` | bool | `false` | Register with an external `rpcbind`/portmap instead of the built-in one. |
| `portmap_hostname` | string | - | Hostname/IP to advertise in portmap registrations. |
| `nfs4_session_slots` | int | `64` | Max in-flight compounds per NFSv4.1 session. |
| `nfs4_delegations` | bool | `false` | Hand out NFSv4 read/write delegations. |
| `nfs4_lease_time` | int (s) | `90` | NFSv4 lease duration. |
| `nfs4_grace_time` | int (s) | `180` | NFSv4 grace period after restart (state reclaim window). |
| `nfs4_courtesy_time` | int (s) | `86400` | How long expired-but-courteous client state is retained. |
| `nfs_server_scope` | int | `42` | NFSv4.1 `EXCHANGE_ID` server scope. Give independent servers (e.g. an MDS and a co-located DS) distinct values so clients don't coalesce them. |
| `nfs_max_exports` | int | `4096` | Maximum number of NFS exports that may exist at once (1..65535). Distinct from the export id space, which is always 1..65535; creating an export past this cap fails. |
| `data_server` | bool | `false` | pNFS data-server mode: bind only the NFSv4 service (no portmap/mount/NLM) so a DS can share a host with its MDS. |
| `fuse_enabled` | bool | `false` | Serve the `fuse_mounts` section as local kernel FUSE mounts. Linux only; the daemon must run as root to issue the mounts. |
| `kv_module` | string | - | Key-value module used to persist server state. |
| `state_dir` | string | `<prefix>/share/state` | Directory for persisted NFS/SMB state. |
| `smb_persistent_handles` | bool | `false` | Enable SMB durable/persistent handles (needed for Continuous Availability). |
| `smb_named_streams` | bool | `false` | Enable SMB named streams (alternate data streams). |
| `smb_encryption` | string/int | `"off"` | SMB3 transport encryption: `"off"`/`"disabled"` (0), `"enabled"`/`"on"` (1), or `"required"` (2). |
| `smb_acl_inherited_canonicalize` | bool | `true` | Canonicalize inherited ACLs on SMB. |
| `smb_replay_pending_windows` | bool | `false` | Answer a replayed durable-v2 CREATE that collides with a still-deferred CREATE the way Windows servers do (`STATUS_ACCESS_DENIED`, and no replay detection while the original waits on a share conflict). The default answers `STATUS_FILE_NOT_AVAILABLE`, which clients retry until the original create completes. MS-SMB2 does not specify this race; the two profiles are mutually exclusive. |
| `metrics_port` | int | `9000` | Prometheus metrics port (`/metrics`). Make it distinct when running multiple daemons per host. |
| `rest_http_port` | int | - | HTTP port for the REST admin API. Set to enable it (examples use `8080`). |
| `rest_https_port` | int | `0` | HTTPS port for the REST API (`0` = disabled). |
| `rest_ssl_cert` | string | - | TLS certificate path. Auto-generated (self-signed) if HTTPS is enabled and this is unset. |
| `rest_ssl_key` | string | - | TLS private-key path. Auto-generated alongside the cert if unset. |
| `rest_auth_enabled` | bool | `true` | Require authentication (JWT Bearer token or HTTP Basic credentials) on all `/api/v1/*` endpoints. Set to `false` to disable auth entirely - only safe on a trusted/loopback-only management network. |
| `soft_fail_bad_req` | bool | `false` | Return a soft error on a malformed REST request instead of dropping the connection. |

See [Advanced and testing options](#advanced-and-testing-options) for a small set
of keys that exist for development and benchmarking and should not be used on a
production server.

#### `server.smb_auth`

Domain-authentication backends for SMB.

| Key | Type | Default | Description |
|---|---|---|---|
| `winbind_enabled` | bool | `false` | Authenticate via Winbind (domain-joined). |
| `winbind_domain` | string | - | Winbind domain name. |
| `kerberos_enabled` | bool | `false` | Enable Kerberos authentication. |
| `kerberos_keytab` | string | - | Path to the Kerberos keytab. |
| `kerberos_realm` | string | - | Kerberos realm. |

Winbind and Kerberos both require setup outside chimera (a domain join, a
keytab, and NSS pointed at winbind). See
[SMB Domain Authentication](smb-domain-auth) for worked procedures against
Active Directory and against an OpenLDAP directory with the Samba schema.

#### `server.smb_multichannel`

An **array** of NIC descriptors advertised to SMB clients for multichannel.

| Key | Type | Default | Description |
|---|---|---|---|
| `address` | string | required | Interface address/IP. |
| `speed` | int | required | Link speed in Mbps (e.g. `10000` for 10 GbE). |
| `rdma` | bool | `false` | Advertise this NIC as RDMA-capable. |

#### `server.pnfs`

| Key | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Act as a pNFS metadata server (MDS). |
| `data_servers` | array | `[]` | Data-server descriptors (below). |

Each entry of `data_servers`:

| Key | Type | Default | Description |
|---|---|---|---|
| `netid` | string | - | Transport: `"tcp"`, `"tcp6"`, `"rdma"`, or `"rdma6"`. |
| `uaddr` | string | required | Data server address as `host` or `host:port`. |
| `backing_path` | string | required | Chimera VFS path (an `nfs`-module mount) the MDS uses to create backing files on the DS. |
| `version` | int/string | `3` | NFS version clients use to reach the DS: `3`, `4`, `"4.0"`, or `"4.1"`. |

#### `server.vfs` - loading external VFS plugins

Built-in modules (`memfs`, `linux`, `diskfs`, `io_uring`, ...) need no
registration. A VFS module shipped as a separate shared object (e.g. `cairn`)
is registered here, keyed by module name, before it can be used in `mounts`:

```json
"server": {
    "vfs": {
        "cairn": {
            "path": "/usr/lib/chimera/libvfs_cairn.so",
            "config": { "path": "/var/lib/chimera/cairn" }
        }
    }
}
```

| Key | Type | Description |
|---|---|---|
| `path` | string | Filesystem path to the module's `.so`. |
| `config` | object | Module-specific options passed to the module at init (see [VFS module options](#vfs-module-options)). |

### `mounts`

An object keyed by mount name. Each mount instantiates a VFS backend that
becomes visible in the Chimera namespace at `/<name>`.

| Key | Type | Default | Description |
|---|---|---|---|
| `module` | string | required | VFS module name (`memfs`, `linux`, `diskfs`, `cairn`, `io_uring`, `nfs`, ...). |
| `path` | string | required | Backend-specific root. For passthrough modules this is a host path; for `nfs` it's the upstream export; for in-memory modules it's typically `/`. |
| `options` | string | - | Module-specific mount options (e.g. `"vers=4.1,rdma,port=20049"` for the `nfs` module). |

### `exports` (NFS)

An object keyed by export name. Each export publishes a VFS path
(`/<mount-name>[/subdir]`) over NFSv3/NFSv4.

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | string | required | VFS path to export; a missing or non-string value fails startup. |
| `export_id` | int | auto | Stable id (1..65535) embedded in every NFS file handle minted for this export; auto-assigned when omitted. Clustered servers exporting the same directory must pin the same id so file handles stay valid across failover. Startup fails on an out-of-range or already-used id. |
| `access` | string | `"rw"` | Access mode, `"ro"` or `"rw"`; any other value fails startup. (Renamed from `options`; the old key is rejected at startup.) |
| `squash` | string | `"none"` | Credential squashing policy: `"none"` (aka `"no_root_squash"`), `"root"` (aka `"root_squash"`), or `"all"` (aka `"all_squash"`); any other value fails startup. |
| `root_squash` / `no_root_squash` / `all_squash` | bool | - | Boolean aliases for `squash`; an explicit `squash` string takes precedence. |
| `anonuid` | int | `65534` | Anonymous uid that squashed callers are mapped to (0..4294967295; anything else fails startup). |
| `anongid` | int | `65534` | Anonymous gid that squashed callers are mapped to (0..4294967295; anything else fails startup). |
| `sec` | array | any | Allowed RPC security flavors: any of `"sys"` (aka `"auth_sys"`), `"krb5"`, `"krb5i"`, `"krb5p"`. Absent or empty permits any flavor; a non-empty list rejects others (NFSv4: `NFS4ERR_WRONGSEC`). An unknown flavor, non-string entry, or non-array value fails startup (silently dropping one would leave the export open to any flavor). |

### `shares`, `buckets`

Each is an object keyed by the published name. Both take a `path` that
points into the Chimera namespace (`/<mount-name>[/subdir]`). `shares` also
accepts one extra key.

| Section | Key | Type | Default | Description |
|---|---|---|---|---|
| `shares` (SMB) | `path` | string | required | VFS path to share. |
| `shares` (SMB) | `continuous_availability` | bool | `false` | Advertise SMB Continuous Availability (requires `smb_persistent_handles`). |
| `buckets` (S3) | `path` | string | required | VFS path backing the bucket. |

### `fuse_mounts`

Local kernel mounts of Chimera namespace paths, served by the daemon speaking
the FUSE protocol directly against `/dev/fuse`. Linux only, requires
`"fuse_enabled": true` in `server`, and the daemon must run as root (the mount
syscall is issued directly, without a `fusermount` helper). Keyed by the host
mountpoint directory, which must exist.

```json
"server":      { "fuse_enabled": true },
"mounts":      { "data": { "module": "memfs", "path": "/" } },
"fuse_mounts": { "/mnt/chimera": { "path": "/data", "options": "allow_other" } }
```

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | string | required | VFS path to mount (`/<mount-name>[/subdir]`, or `/` for the whole namespace). |
| `options` | string | - | Comma-separated: `allow_other` (other users may access the mount), `no_default_permissions` (skip kernel mode-bit enforcement), `attr_timeout_ms=<n>` / `entry_timeout_ms=<n>` (kernel attribute/entry cache lifetimes, default `1000`), `negative_timeout_ms=<n>` (kernel negative-dentry lifetime; defaults to `entry_timeout_ms` under `coherence=sync` and `0` under `coherence=ttl`), `coherence=sync\|ttl` (default `sync`), `direct_io` and `parallel_direct_writes` (default off; see below). |

Notes:

- The FUSE request header carries only uid/gid, so backends cannot see
  supplementary groups; the default `default_permissions` mode has the kernel
  do mode-bit checks with the caller's full group list.
- POSIX byte-range locks (`fcntl` `F_GETLK`/`F_SETLK`/`F_SETLKW`) are served
  remotely through the shared lease layer, so they conflict correctly with
  NLM, NFSv4, and SMB2 locks on the same files.  A blocked `F_SETLKW` is
  cancellable by signal.  `flock(2)` stays kernel-local to the mount.
- Cross-protocol cache invalidation is pushed to the kernel: a write,
  truncate, chmod/chown/utimens, remove, or rename arriving through
  NFS/SMB/S3 (or another FUSE mount of the same share) invalidates the
  kernel's cached attributes, pages, and directory entries for the affected
  objects — including files the kernel knows only from a `stat`, and the
  changed directory's own attributes.  With coverage in force, cached pages
  also survive close/open cycles (`FOPEN_KEEP_CACHE`).
- `coherence=sync` (the default) makes that invalidation **synchronous and
  ordered**: a conflicting write elsewhere does not complete until this
  kernel's caches are invalidated, a foreign namespace mutation (create,
  remove, rename, link) does not return to its caller until this kernel's
  dentries for the affected names are dropped, and replies carry cache
  lifetimes only while backed by a live invalidation grant or directory
  watch.  Once one operation returns anywhere, the very next syscall on any
  FUSE mount of the share sees its effect — which is also what makes
  negative-dentry caching safe to enable by default.  Two narrow softenings
  keep concurrent cross-mount activity off the multi-second recovery
  deadlines, both arising only when two kernels race mutations on the *same*
  directory or file (where no cross-mount ordering exists to preserve): the
  raced mount converges asynchronously (microseconds) instead of blocking.
  Recovery deadlines bound the wait if a mount stops acknowledging
  invalidations (~5s for namespace gates, the lease break deadline for
  data).
- `direct_io` makes the daemon reply `FOPEN_DIRECT_IO` on every open, so the
  kernel never stages this mount's file data in its page cache: reads and
  writes reach the VFS as exact `(offset, length)` requests built straight
  from the calling process's pinned pages.  This is the server's choice, not
  the application's — it applies to a plain `open()` and imposes none of
  `O_DIRECT`'s alignment requirements on the caller.  It removes one memory
  copy per operation in each direction, and for a page-aligned writer it is
  what lets a write reach a backend's zero-copy device path.  It also removes
  readahead, read caching, and write batching, so small or repeated I/O gets
  dramatically slower — this is a switch for bulk streaming workloads, which
  is why it is off by default.  `parallel_direct_writes` additionally allows
  concurrent writers to one file instead of serializing them on the kernel's
  inode lock; it relaxes write atomicity between overlapping concurrent
  writes, so it is separately opt-in, but without it a multi-threaded write
  benchmark measures the inode lock rather than the data path.  Shared `mmap`
  of a file on a `direct_io` mount needs `FUSE_DIRECT_IO_ALLOW_MMAP`
  (kernel 6.6 / ABI 7.39 and later), which the daemon negotiates when
  offered and logs as `(no mmap)` when it is not.
- `coherence=ttl` restores the purely asynchronous model: invalidations are
  still pushed, but mutations never wait for peer acknowledgments and the
  `attr/entry` timeouts alone bound staleness.  One extra residual there
  falls back to the entry timeout: directory-entry names dropped by an
  overflowed event ring (the directory's attributes are still refreshed).
- If a previous daemon instance crashed and left a disconnected mount on the
  mountpoint, the next start detaches it and mounts fresh.
- An externally issued `umount` disconnects the mount; the daemon logs it and
  does not remount until restart.

### `users`

An **array** of built-in accounts (used by NFS/SMB auth and ownership mapping).

| Key | Type | Default | Description |
|---|---|---|---|
| `username` | string | required | Account name (entry skipped if missing). |
| `password` | string | `""` | Plaintext password (NFS/REST auth). |
| `smbpasswd` | string | `""` | SMB NT password hash. |
| `uid` | int | `0` | POSIX user ID. |
| `gid` | int | `0` | POSIX primary group ID. |
| `gids` | array of int | `[]` | Supplementary group IDs. |

### `s3_access_keys`

An **array** of S3 credentials. `access_key` and `secret_key` are required per
entry.

| Key | Type | Description |
|---|---|---|
| `access_key` | string | S3 access key ID. |
| `secret_key` | string | S3 secret access key. |
| `username` | string | Name of a `users` entry this key acts as. |
| `user_id` | string | S3 canonical user id reported as the `<Owner>` in ACL replies. Defaults to the access key. |
| `display_name` | string | Display name reported alongside it. Defaults to `username`. |

Every request authenticated with a key runs against the store as that user's
uid/gid, and objects and buckets it creates are owned by them. Authorization is
therefore the store's own ownership and mode bits — the same ones NFS and SMB
enforce — so a key can only reach what its user could reach over any other
protocol. Give two tenants two keys bound to two users and neither can read or
write the other's buckets.

```json
"users": [
    { "username": "alice", "uid": 1001, "gid": 1001 },
    { "username": "bob",   "uid": 1002, "gid": 1002 }
],
"s3_access_keys": [
    { "access_key": "AKIAALICE", "secret_key": "...", "username": "alice" },
    { "access_key": "AKIABOB",   "secret_key": "...", "username": "bob" }
]
```

`user_id` and `display_name` are S3 identity strings with no POSIX equivalent;
they name the principal in `GET ?acl` output and nowhere else. Access is decided
by the uid/gid `username` resolves to, never by these.

A key with no `username` — or one naming a user that does not exist — is
**unbound**: it acts as the anonymous identity below rather than as root, and is
logged at startup. Such a key authenticates successfully but will be denied
anything the anonymous user may not do, so bind every key you expect to work.

### `s3_anon`

Identity an unbound S3 access key acts as. Defaults to nobody/nogroup.

| Key | Type | Default | Description |
|---|---|---|---|
| `uid` | int | `65534` | POSIX user ID for unbound keys. |
| `gid` | int | `65534` | POSIX group ID for unbound keys. |

### S3 access control

Buckets and objects are ordinary directories and files, so S3 access control is
projected onto their mode bits and enforced by the same VFS permission gate NFS
and SMB go through — there is no separate S3 ACL store to keep in agreement.

`x-amz-acl` on PutObject, CreateBucket and `PUT ?acl` maps to a mode:

| canned ACL | file | directory |
|---|---|---|
| `private` | 0600 | 0700 |
| `public-read` | 0644 | 0755 |
| `public-read-write` | 0666 | 0777 |
| `authenticated-read` | 0644 | 0755 |

An object created with no `x-amz-acl` is **private** (0600), matching S3's
default. A bucket created with none stays world-traversable (0755), because
reaching a public object requires search permission on the directory holding it.

`authenticated-read` is an approximation: a POSIX mode has no bit that
distinguishes "any authenticated principal" from "everyone", so it is treated as
`public-read` — slightly more permissive than S3.

`GET ?acl` reverses the projection, reporting the owner with FULL_CONTROL plus
an AllUsers READ or WRITE grant when the world bits are set.

### Full server example

```json
{
    "common": {
        "tcp_flavor": "plain",
        "sync_delegation": true,
        "sync_delegation_threads": 16,
        "async_delegation": false,
        "async_delegation_threads": 8
    },
    "server": {
        "threads": 16,
        "nfs4_delegations": true,
        "smb_encryption": "enabled",
        "smb_persistent_handles": true,
        "metrics_port": 9000,
        "rest_http_port": 8080
    },
    "mounts": {
        "data": { "module": "memfs", "path": "/" }
    },
    "exports": {
        "/nfs": {
            "path": "/data",
            "export_id": 1,
            "access": "rw",
            "squash": "root",
            "sec": ["sys", "krb5"]
        }
    },
    "shares":  { "data": { "path": "/data", "continuous_availability": true } },
    "buckets": { "data": { "path": "/data" } },
    "users": [
        { "username": "alice", "password": "secret", "uid": 1000, "gid": 1000 }
    ],
    "s3_access_keys": [
        { "access_key": "AKIDEXAMPLE", "secret_key": "wJalrXUtnFEMI...", "username": "alice" }
    ]
}
```

---

## Client configuration

The client library (`chimera_client_init_json`) reads the shared `common`
section, the `users` array, and a dedicated **`config`** section. It does not
read `server`/`mounts`/`exports`/`shares`/`buckets`.

### `config` section

| Key | Type | Default | Description |
|---|---|---|---|
| `core_threads` | int | `16` | Core event-loop worker threads. |
| `sync_delegation` | bool | `true` | Enable the synchronous delegation pool. |
| `sync_delegation_threads` | int | `64` | Synchronous delegation pool size. |
| `async_delegation` | bool | `false` | Enable the asynchronous delegation pool. |
| `async_delegation_threads` | int | `8` | Asynchronous delegation pool size. |
| `cache_ttl` | int (s) | `60` | Metadata (attribute) cache TTL. |
| `max_fds` | int | `1024` | Maximum concurrent open file descriptors. |
| `kv_module` | string | - | Key-value module for client-side state. |
| `vfs` | object | - | Per-module config, keyed by module name; each value is `{ "path": ..., "config": ... }` (both strings). Used to register/point at VFS modules. |

> The `common.*` delegation keys override the values in `config`, exactly as they
> override `server` on the daemon side.

### Client example

```json
{
    "common": { "tcp_flavor": "plain" },
    "config": {
        "core_threads": 8,
        "cache_ttl": 30,
        "max_fds": 4096
    },
    "users": [
        { "username": "alice", "uid": 1000, "gid": 1000 }
    ]
}
```

### fio benchmark engine

The bundled fio engine (`src/fio`) reads the same `common` section and is
otherwise driven by fio job-file options rather than the `config` section:

| fio option | Type | Default | Description |
|---|---|---|---|
| `chimera_config` | string | - | Path to a Chimera JSON config file (for its `common` section and module/mount definitions). |
| `chimera_log` | string | - | Redirect Chimera/libevpl logging to this file. |
| `chimera_debug` | bool | `false` | Enable debug-level logging. |
| `chimera_buffer_size` | int | `0` (auto) | Override the libevpl buffer-pool size; `0` auto-sizes from the job's `iodepth x bs`. |

---

## VFS module options

These keys go inside a module's `config` object - either the per-mount config or,
for plugins, the `config` under `server.vfs.<name>`.

### `memfs` (in-memory)

| Key | Type | Default | Description |
|---|---|---|---|
| `block_size` | int | `65536` | Block size in bytes (power of two, 4 KiB-1 MiB). |
| `fsid` | int/string | random | Stable filesystem ID so handles survive a restart. |
| `noatime` | bool | `false` | Disable atime updates (default keeps relatime semantics). |

### `linux` and `io_uring` (passthrough)

| Key | Type | Default | Description |
|---|---|---|---|
| `readdir_verifier` | bool | `false` | Emit/validate a readdir cookie verifier. |

### `cairn` (RocksDB-backed, plugin)

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | string | required | Directory holding the `metadb`/`datadb` RocksDB stores. |
| `cache` | int (MB) | `64` | Block-cache budget, split ~1/4 metadb / ~3/4 datadb. |
| `compression` | bool | `true` | LZ4-compress `datadb` (`metadb` is always uncompressed). |
| `bloom_filter` | bool | `true` | Enable bloom filters on both stores. |
| `statistics` | bool | `false` | Collect RocksDB statistics (diagnostics). |
| `noatime` | bool | `false` | Disable atime updates. |
| `initialize` | bool | `false` | Destroy and recreate the databases at mount. **Erases data.** |

### `diskfs` (persistent, block-device backed)

The `config` object takes a `devices` array plus filesystem-level knobs.

| Key | Type | Default | Description |
|---|---|---|---|
| `devices` | array | required | Backing devices (below). |
| `initialize` | flag | `false` | `mkfs` (format) the filesystem at mount. **Erases data.** |
| `noatime` | bool | `false` | Disable atime updates. |
| `mtime_defer_ms` | int (ms) | `1000` | Coalescing window for deferred mtime updates (`0` writes mtime on every write). |
| `intent_log_size` | int (bytes) | `1073741824` (1 GiB) | Size of the device-0 intent (redo) log; a larger log lets more redo records pipeline before the ring laps. Persisted in the superblock at format time (a remount uses the formatted value). Must fit device 0's first allocation group alongside the superblock and per-AG log; floored at 4 MiB. The block cache default scales with this. |
| `block_cache_blocks` | int | `0` (2x the intent-log block count) | Resident block-buffer cap (`0` = default; floored at 1.5x the intent-log block count). |
| `inode_cache_inodes` | int | `262144` | Resident inode cap (`0` = default). |
| `block_layout` | bool | `false` | Source RFC 5663 pNFS **block** layouts (mutually exclusive with `scsi_layout`). |
| `scsi_layout` | bool | `false` | Source RFC 8154 pNFS **SCSI** layouts (mutually exclusive with `block_layout`). |

Each entry of `devices`:

| Key | Type | Default | Description |
|---|---|---|---|
| `type` | string | required | `"io_uring"`, `"libaio"`, `"vfio"`, or `"pread"`.  The first three are Linux-only; `"pread"` runs the device from a libevpl service thread with blocking pread/pwrite and works anywhere. |
| `path` | string | required | Device path, file path, or PCI BDF (e.g. `"01:00.0"` for VFIO). |
| `size` | int | auto | Device size in bytes (auto-detected for file-backed if omitted). |
| `role` | string | `"local"` | `"local"`, or `"remote"` for a pNFS-only data device. |
| `deviceid` | string | - | 16-byte hex device ID (remote devices). |
| `signature` | object | - | SIMPLE-volume signature: `{ "offset": int, "bytes": "<hex>" }` (block layout). |
| `scsi` | object | - | SCSI designator: `{ "designator_type": "naa"\|"eui64"\|"t10", "code_set": "binary"\|"ascii", "id": "<hex>", "pr_key": int }` (SCSI layout). |

The `nfs` and `root` modules take no `config` object; the `nfs` module is
configured through mount `options` instead.

---

## Advanced and testing options

These keys exist for development, debugging, and benchmarking. They are **not**
intended for production deployments - they either weaken durability guarantees or
expose unauthenticated mutation endpoints. They are documented here for
completeness; leave them at their defaults unless you understand the trade-off.

| Section | Key | Type | Default | Description |
|---|---|---|---|---|
| `server` | `rest_debug_fsops` | bool | `false` | Enable `/api/v1/debug/fsop`, an unauthenticated endpoint that performs server-side filesystem mutations (used to drive delegation-recall tests). **Never enable on a production or network-reachable server.** |
| `diskfs` `config` | `unsafe_async` | bool | `false` | Issue block writes without FUA/sync, trading crash-consistency for throughput. A power loss or crash can corrupt the filesystem. Intended for benchmarking only. |
