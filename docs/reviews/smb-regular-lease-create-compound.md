# SMB wave14: regular-file lease CREATE

Native CREATE, OPEN and OPEN_IF now accept regular-file RqLs requests at all
caching levels. This extends the wave13 READ/NONE slice to RH, RW and RWH;
W/H-only input is normalized to NONE, matching the legacy policy. The share's
force-level-II policy still caps the request to READ.

The existing two explicit coordination operations perform the H recall before
ACCESS admission and the W recall after it. They carry the LeaseKey, exempt the
requester's own grant, settle peer ACKs, and remain repeatable. A transparent
metadata OPEN does not initiate a recall to obtain more caching. Ordinary
prepare/completion callouts never publish a grant or change its mode/epoch.

At accepted publication the existing nonallocating locked RqLs helper admits
the current requested mode. It joins a same-key grant, preserving the grant's
version, rights and epoch unless an allowed strict-superset upgrade succeeds;
that upgrade advances the epoch exactly once. A grant that is breaking is not
upgraded. Fresh grants cap W, then H, then READ as live admission requires,
without recalling a peer or failing an already accepted CREATE. Publication
attaches the member, ACCESS own_cache and FileId before releasing the file lock.
The reply copies the private open before visibility and snapshots the final cache
mode/epoch/flags under that same lock. A concurrent break cannot make the CREATE
reply combine cache fields from different grant states; a suffix CLOSE cannot
free the owned response-version snapshot.

CREATE still terminates its VFS run: suffix operations need the accepted grant.
The NON_DIRECTORY hint, nontruncating disposition, nonstream and no-parent-key
limits remain explicit. Directory/stream leases, hintless lease opens,
create-time DOC, durable/reconnect/AppInstance and truncating lease CREATE still
use their established boundaries. Session-wide lease-key binding still has the
preexisting scan/publication race. The scan also sees only the current session,
whereas VFS grant ownership uses the ClientGuid-derived client_key: another
session of the same client can reuse a key on another file even sequentially.
Safe repair needs a shared client/key table with pending identity reservation
before create-capable mutation, FH binding and accepted handoff, spanning legacy
and native CREATE, streams, durable parking and teardown. Lease ACK discovery
currently scans only the current session too.

The existing create-cache fixture now asserts native submissions for v1/v2
states 0..7, CREATE and OPEN_IF joins, related QUERY/CREATE/QUERY/CLOSE grouping
for R/RH/RW/RWH, same-key READ-to-RWH upgrade with held and rejected read-only
OPEN finish, peer write-cache recall/capping, full-mode own-key reopen during an
outstanding break, and force-level-II/leases-disabled v1/v2 policy variants.
The finish inspector verifies
both the old epoch and old rights, as well as unchanged public membership.
The legacy EA-adapter fixture uses hintless RqLs to retain its specific coverage.
No finish rejection is injected after filesystem mutation.

Build and combined validation are owned by the coordinator and recorded in the
current-status document after integration.

Coordinator validation: wave14 full Debug/ASan build5 and all 51 selected CTest
entries pass, including five SMB profiles of 94 traces each. See the
[current status](smb-compound-current-status.md) for validation and remaining scope.
