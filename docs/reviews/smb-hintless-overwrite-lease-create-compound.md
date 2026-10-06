# SMB wave15: hintless and truncating regular-file lease CREATE

Regular-file RqLs CREATE now uses native command groups for all six dispositions,
with or without FILE_NON_DIRECTORY_FILE. The existing typed OVERWRITE stage runs
after ACCESS admission and both repeatable cache-recall phases. It carries the
requester's LeaseKey as its I/O actor, preserving the same-key exemption. Cache
membership and requested-mode admission still occur only at accepted publication.

Hintless requests perform a typed preflight LOOKUP requesting file type. An
actual directory defers before OPEN_AT, retaining the existing directory-lease
lifecycle. The OPEN completion also checks the actual opened type to catch a
name that became a directory after lookup. That check precedes admission,
recalls, OVERWRITE and EAs; OPEN_AT carries no TRUNCATE flag or SIZE attribute.
Existing directories do not receive create-time attributes or ALSI through this
OPEN. The empty-name root case uses OPEN_CURRENT then the same type check.
Explicit NON_DIRECTORY type errors remain native; explicit DIRECTORY requests
retain their previous boundary.

The missing-name LeaseKey guard applies only to create-capable dispositions.
Hintless noncreating OPEN/OVERWRITE retain NAME_NOT_FOUND even when the key is
bound elsewhere. Existing wrong-file key validation precedes OVERWRITE, while
CREATE/OPEN_IF/OVERWRITE_IF/SUPERSEDE with a missing wrong-key target are rejected
before creating that target. The earlier cross-request/client-session identity
reservation gap is not closed by this change.

Focused fixture additions cover all six dispositions on present/absent regular
files, both with and without the NON_DIRECTORY hint; create actions, data size
and preservation; a held accepted same-key overwrite with unchanged old grant
rights, epoch and membership; wrong-key overwrite data preservation and missing
names; and hintless directory OPEN/OPEN_IF fallback with preserved child data.
No finish rejection is injected after overwrite or namespace mutation. The
legacy EA-adapter fixture now uses FILE_OPEN_REQUIRING_OPLOCK to remain on an
explicit lifecycle boundary.

Remaining CREATE boundaries include directory and named-stream leases,
parent-key coordination, mandatory-oplock and create-time DOC, durable/reconnect/
AppInstance contexts, and actual symlinks with non-OPEN reparse dispositions.
Caching CREATE still ends its VFS run until tentative grants can be made available
to a suffix without publishing before finish. Atomic client-wide LeaseKey identity
reservation and cross-session ACK discovery remain separate correctness work.

Combined build and validation belong to the coordinator and are recorded in the
current-status document after integration.

Coordinator validation: wave15 full Debug/ASan build5 and all 51 selected CTest
entries pass, including five SMB profiles of 94 traces each. See the
[current status](smb-compound-current-status.md) for results and remaining scope.
