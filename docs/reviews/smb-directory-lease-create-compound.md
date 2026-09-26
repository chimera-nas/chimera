# Native directory lease CREATE

Wave16 validated. Combined Debug+ASan build and all 51 selected CTest entries
pass; see [the current checkpoint](smb-compound-current-status.md) for scope and logs.

SMB3 lease-v2 directory CREATE, OPEN and OPEN_IF now use the existing typed
MKDIR/OPEN, ACCESS and readiness pipeline. Explicit DIRECTORY_FILE and hintless
requests are supported when directory leasing is enabled. Parent-key contexts,
stream leases, durable/reconnect and other existing specialized boundaries remain.
Unsupported directory policy is checked against both the lookup and actual open
identity before mutation/admission; explicit unsupported requests stay legacy.

The private request reserves grant storage before executing. Accepted publication
uses a DIR_LEASE template, not the regular RqLs template: WRITE caching is masked,
and directory content changes retain the DIR_CONTENT recall rule. The existing
accepted RqLs publisher now accepts this constructor as well. Under the file lock
it joins or upgrades an existing key, preserves version/epoch, handles settled
content-break rearming, or publishes the prepared candidate without allocating or
recalling. Invalid W/H-only modes become NONE; share force-level-II policy caps to
READ. Ordinary directory legacy oplocks remain declined.

Same-key coordination recognizes directory grants when excluding the requester's
own pending break. CREATE remains terminal so later commands consume the accepted
grant. The protocol reply snapshots the final state while the grant lock is held.
No frontend completion callback publishes a FileId or modifies a live grant.

Added regressions cover all eight requested modes; explicit CREATE/OPEN_IF and
hintless OPEN_IF joins; directory CREATE sharing a prefix and ending before its
QUERY/CLOSE suffix; fresh mkdir with held accepted finish; existing directory OPEN
with one safe finish rejection while inspecting unchanged grant membership/epoch;
accepted READ-to-RH upgrade; wrong-key creation leaving no new directory; actual
child creation causing a content break; settled directory lease rearming; and
force-level-II/disabled-leases policy. The prepared-grant core test also checks the
DIR_LEASE constructor, WRITE masking, same-key NONE join and DIR_CONTENT callback.

No rejection is injected after mkdir or any other real filesystem mutation.
Client-wide LeaseKey reservations, parent-key exemptions and general backend
transaction/rollback support remain separate work.

## Fallback audit

The focused fixture additionally checks directory lease-v1 requests and a server
with directory leasing disabled: explicit DIRECTORY_FILE uses no native CREATE
submission, while hintless OPEN performs one discovery group then returns the
legacy no-oplock decision. A raw lease-v2 ParentLeaseKey request checks that the
flagged context remains wholly legacy and retains its lease reply/parent flag.
These additions follow the initial passing root smoke run and require the final
integrated rerun.

SMB2.0.2/2.1 directory fallback remains source-audited rather than a dedicated
new wire regression in this fixture. The parser treats 52-byte lease-v2 input as
v1 below SMB3, and the directory eligibility/type-revalidation checks independently
require SMB3 plus lease-v2. Existing regular lease-v1 tests do not by themselves
prove this directory/dialect boundary. The fixture also does not exercise parent
key exemption during a real parent content break; that lifecycle remains legacy.
