<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: LGPL-2.1-only
-->

# daos

Optional Chimera VFS module for one existing DAOS POSIX container.
The exported symbol is `vfs_daos`. The loader looks for `chimera_vfs_daos`.

The module is built only when the DAOS client headers and libraries are
installed. A Chimera build without them skips this directory.

Module configuration:

```json
{
  "pool": "existing-pool",
  "container": "existing-posix-container",
  "system": null,
  "mount_uuid": "6c0f6325-07f3-46fd-97d8-7d6dfdfeac5b",
  "read_only": false,
  "registry_max_entries": 65536,
  "open_max_handles": 65536,
  "oclass": 0,
  "chunk_size": 1048576,
  "readdir_batch_entries": 64
}
```

`mount_uuid` must be the container UUID. The module mount path must be empty;
the Chimera namespace mount path is independent. Initialization calls
`dfs_init`. MOUNT connects without creating a pool or container. A second
live mount returns EBUSY. Configure a positive sync delegation thread count
before offering the share. One gateway process must be the only namespace
and metadata writer for the container.

Capabilities are FS, FS_RELATIVE_OP, BLOCKING, CREATE_GID_ENGINE,
DELEGATES_DAC and XATTR. File handles are 33 bytes. Published objects
remain in the process-local table; a full table returns EMFILE. Removed
or cold child handles return ESTALE without a namespace scan. No restart
recovery of child handles is provided. Directory scans retain every
returned cookie's anchor in one of 1024 slots; starting a new scan can
recycle the least recently used slot.

Setattr stores mode, uid, gid, size, atime and mtime. DOS attributes, ACLs,
SIDs, allocation size and layout attributes are ignored. Concrete ctime or
btime requests return ENOTSUP. Extended attributes are get, set, list and
remove, within the libdfs limits. Reverse lookup, sparse seek, hard links,
range copy, layouts, streams, persistent handle state and offloads are
unsupported. There are no build switches that enable them.

WRITE reports the submitted length after a successful blocking `dfs_write`
and always reports unstable status. COMMIT calls container-wide `dfs_sync`
and propagates failure. Neither result is a crash-durability guarantee.

`ctest -C extended -R chimera/vfs/daos` runs codec, registry, configuration, operation
and load tests. The first four use DFS and SDK doubles and require no cluster.
The load test opens the built Chimera VFS library and this module.
