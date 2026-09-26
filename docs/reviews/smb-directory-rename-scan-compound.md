# Directory rename discovery compounds

Wave16 validated. Combined Debug+ASan build and all 51 selected CTest entries
pass; see [the current checkpoint](smb-compound-current-status.md) for scope and logs.

Directory rename remains a lifecycle boundary. Its contained-open discovery now
uses typed PUTHANDLE + streaming READDIR compounds, one per backend page, for both
the initial recall scan and the final scan for racing opens. This replaces the
two direct READDIR dispatch sites without claiming that directory RENAME itself
has become one native VFS compound.

Each page owns a retained directory handle and checkpoints its starting child
count and conservative-denial flag. The streaming reset callback restores that
checkpoint before the first execution and every replay. Entry callbacks only
inspect share holders and append to request-private storage. Accepted prior pages
are preserved across a later page's retry. Recalls, further-page dispatch and
rename are reached only from the final accepted page completion, after the
frontend retry adapter has handled finish EAGAIN. Rejection exhaustion or terminal
finish failure fails the request; cancellation reports STATUS_CANCELLED. Neither
can fall through to a filesystem mutation.

Malformed or missing child FHs and allocation failures conservatively deny the
rename. Previously the initial scan's allocation-denial flag could be ignored
before the first recall wave; a subsequent scan could then clear it. A borrowed
directory handle is now retained explicitly for each compound, including while
its finish is delayed. The existing accepted backend-enumeration-error fallback
is preserved; private collection failures do not take that fallback.

The rename wire probe covers a held read-only scan rejected with EAGAIN after a
second connection closes its child. The retried scan must forget that old child:
no stale recall is allowed, the final scan accepts, and only then does rename
run. A forced one-entry-per-page case accepts the first child, rejects the second
page after its child closes, and verifies that retry preserves the first page
while discarding the second page's stale holder. The surviving first-page holder
then correctly denies rename. Additional cases cover terminal finish EIO, finish EINTR, and an accepted
scan with a surviving child that correctly denies the rename. The hooks assert
there are no recalls or namespace mutations before scan acceptance. No test
rejects a mutating compound.

Still required for full directory rename conversion: subtree-aware path/claim
coordination, descendant publication across share views, admitted-pending-open
coverage, and an atomic transition between final discovery and mutation. The
existing legacy recall driver's final-rescan race window is not closed here.
Named-stream rename still lacks a typed VFS stream-rename operation. Distinct
occupied-target replacement remains disabled until delayed legacy backend opens
cannot publish a stale destination identity after replacement acceptance.
