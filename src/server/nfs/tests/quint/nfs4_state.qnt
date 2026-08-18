// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// NFSv4 protocol state model: clients, sessions, owners, stateids, share
// reservations, byte-range locks, delegations and pNFS layouts.
//
// Like nfs4_fs.qnt this module is pure: every function maps a StateDB (plus
// arguments) to a new StateDB and an abstract outcome; the compound
// evaluator in nfs4_ops.qnt maps outcomes to NFS4ERR_* codes and the wire
// shapes.  Keeping it pure lets nfs4Test.qnt unit-test the seqid and
// conflict rules without a full model in the loop.
//
// Conventions:
//  - Model handles (ClientId, SessId, Sid) are small ints; the harness
//    learns the wire values (clientid4, sessionid4, stateid4.other) from
//    replies and maintains the mapping.
//  - Stateid seqids are modeled concretely: they start at 1 on creation
//    and advance by exactly one on each operation that modifies the
//    stateid's state (RFC 8881 8.2.2; RFC 7530 9.1.4).
//  - Byte-range locks use a per-byte map over a small lock space, which
//    makes POSIX merge/split/upgrade semantics exact by construction.
//    The harness maps model bytes onto real byte ranges and expresses a
//    lock reaching the top of the lock space as a to-EOF lock
//    (length = 0xFFFFFFFFFFFFFFFF).

module nfs4state {

    import nfs4fs.* from "./nfs4_fs"

    type ClientId = int
    type SessId = int
    type Sid = int

    // ------------------------------------------------------------------
    // Client records
    // ------------------------------------------------------------------

    // One record per model client id.  A client id is created by
    // SETCLIENTID (4.0) or EXCHANGE_ID (4.1) and starts unconfirmed; it is
    // confirmed by SETCLIENTID_CONFIRM / CREATE_SESSION.  ownerSym and
    // verfSym abstract the client-supplied owner id and boot verifier.
    type Client = {
        ownerSym: int,
        verfSym: int,
        confirmed: bool,
        // 4.0: pairing token for SETCLIENTID_CONFIRM (models the
        // setclientid_confirm4 verifier returned by SETCLIENTID).
        confirmTok: int,
        // 4.1: sequence id the next CREATE_SESSION must carry
        // (eir_sequenceid; RFC 8881 18.35.3).
        csSeq: int,
        // Callback path usable: 4.0 cb_client4 probed OK; 4.1 a session
        // with the backchannel flag exists.  Gates delegation grants.
        cbUp: bool,
        // 4.1: RECLAIM_COMPLETE has been performed (RFC 8881 18.51).
        reclaimDone: bool
    }

    // ------------------------------------------------------------------
    // Sessions and slot tables (4.1+)
    // ------------------------------------------------------------------

    // One reply-cache slot (RFC 8881 2.10.6.1).  seq is the highest
    // sequence id executed on the slot (0 = never used; first request must
    // carry 1).  reqTag identifies the request that consumed seq, so a
    // retry with the same seq and the same tag is a replay and the same
    // seq with a different tag is a false retry (NFS4ERR_SEQ_FALSE_RETRY).
    // cached records whether the reply was cached (sa_cachethis).
    type Slot = { seq: int, reqTag: int, cached: bool }

    type Session = {
        client: ClientId,
        slots: List[Slot],
        backChan: bool
    }

    // ------------------------------------------------------------------
    // Stateids
    // ------------------------------------------------------------------

    // Open stateid: one per (client, open-owner, file) triple; share bits
    // hold the current union of that owner's opens of the file
    // (RFC 8881 9.7; RFC 7530 9.1.4.1).
    type OpenState = {
        seq: int,
        client: ClientId,
        owner: int,          // open-owner opaque symbol
        file: Ino,
        access: int,         // OPEN4_SHARE_ACCESS_{READ,WRITE,BOTH} = 1,2,3
        deny: int,           // OPEN4_SHARE_DENY_{NONE,READ,WRITE,BOTH} = 0..3
        // Historical OPEN events (access/deny pairs) behind this stateid:
        // OPEN_DOWNGRADE's target must equal the union of some subset of
        // these, NOT merely a subset of the current union bits
        // (RFC 7530 16.19.4 / RFC 8881 18.18.3: one OPEN(BOTH) can never
        // be downgraded to READ).
        events: Set[{ access: int, deny: int }],
        // 4.0 only: an open stateid from a new open-owner must be
        // confirmed by OPEN_CONFIRM before further use (RFC 7530 9.1.11).
        needConfirm: bool
    }

    type LockT = LRead | LWrite

    // Lock stateid: one per (client, lock-owner, file); byte -> strongest
    // lock type held.  Per-byte representation makes POSIX upgrade/
    // downgrade/split semantics exact.
    type LockState = {
        seq: int,
        client: ClientId,
        owner: int,          // lock-owner opaque symbol
        file: Ino,
        openSid: Sid,        // the open this lock state hangs off
        bytes: int -> LockT
    }

    // Delegation stateid (RFC 7530 10; RFC 8881 10).
    type Deleg = {
        seq: int,
        client: ClientId,
        file: Ino,
        isWrite: bool,
        // A CB_RECALL for this delegation has been emitted and not yet
        // answered by DELEGRETURN.
        recalled: bool
    }

    // Layout iomode per block (RFC 8881 12.5): READ or RW segments,
    // per-block for exact partial-return arithmetic.
    type Layout = {
        seq: int,
        client: ClientId,
        file: Ino,
        // block index -> iomode is RW (true) or READ (false)
        blocks: int -> bool,
        returnOnClose: bool
    }

    // What class of state a Sid names (for generic stateid validation).
    type SidKind = KOpen | KLock | KDeleg | KLayout

    type StateDB = {
        clients: ClientId -> Client,
        nextClient: ClientId,
        sessions: SessId -> Session,
        nextSess: SessId,
        opens: Sid -> OpenState,
        locks: Sid -> LockState,
        delegs: Sid -> Deleg,
        layouts: Sid -> Layout,
        nextSid: Sid,
        // Sids that once existed and were freed (CLOSE, DELEGRETURN,
        // final LAYOUTRETURN, FREE_STATEID): using one is BAD_STATEID.
        freedSids: Set[Sid],
        // 4.0 open-owner seqid discipline (RFC 7530 9.1.7): per
        // (client, owner-symbol) request sequence and confirmation.
        // Lock-owners get the same treatment once they hold lock state.
        oo40: (ClientId, int) -> { seqid: int, confirmed: bool },
        lo40: (ClientId, int) -> { seqid: int }
    }

    pure def sdbInit: StateDB = {
        clients: Map(),
        nextClient: 0,
        sessions: Map(),
        nextSess: 0,
        opens: Map(),
        locks: Map(),
        delegs: Map(),
        layouts: Map(),
        nextSid: 0,
        freedSids: Set(),
        oo40: Map(),
        lo40: Map()
    }

    pure def smapDrop(m, k) = m.keys().exclude(Set(k)).mapBy(x => m.get(x))

    // ------------------------------------------------------------------
    // Client lifecycle: NFSv4.0 (RFC 7530 16.33, 16.34, 9.1.1)
    // ------------------------------------------------------------------

    // Find the client record (of any confirmation state) for an owner
    // symbol; -1 if none.  At most one confirmed and one unconfirmed
    // record per owner exist at a time (RFC 7530 16.33.5).
    pure def clientByOwner(sdb: StateDB, ownerSym: int, wantConfirmed: bool): ClientId =
        sdb.clients.keys().fold(-1, (acc, c) =>
            if (sdb.clients.get(c).ownerSym == ownerSym and
                sdb.clients.get(c).confirmed == wantConfirmed) c else acc)

    type SetClientIdRes = {
        sdb2: StateDB,
        clientid: ClientId,     // the (possibly new) unconfirmed record, or
                                // the confirmed record on a callback update
        confirmTok: int
    }

    // SETCLIENTID (RFC 7530 16.33.4 cases).  tok is a fresh model token
    // for the setclientid_confirm pairing (the caller supplies it so the
    // trace pins it).  Principal conflicts (NFS4ERR_CLID_INUSE) are not
    // modeled: the harness always presents one AUTH_SYS principal.
    pure def applySetclientid(sdb: StateDB, ownerSym: int, verfSym: int,
                              tok: int): SetClientIdRes = {
        val conf = clientByOwner(sdb, ownerSym, true)
        val unconf = clientByOwner(sdb, ownerSym, false)
        if (conf != -1 and sdb.clients.get(conf).verfSym == verfSym) {
            // Case: callback update of a confirmed record with the same
            // verifier.  Returns the SAME clientid with a fresh confirm
            // token; state is untouched until SETCLIENTID_CONFIRM.  The
            // server keeps at most one in-flight registration per owner
            // (RFC 7530 16.33.5: case 1 removes same-verifier unconfirmed
            // records, cases 3/4 replace them "regardless" of verifier),
            // so any pending unconfirmed record for the owner -- e.g. a
            // reboot registration awaiting confirm -- is discarded too.
            val dropped = if (unconf != -1) smapDrop(sdb.clients, unconf)
                          else sdb.clients
            { sdb2: sdb.with("clients", dropped.setBy(conf, cl =>
                  cl.with("confirmTok", tok))),
              clientid: conf, confirmTok: tok }
        } else {
            // Case: no record, unconfirmed-only, or confirmed with a
            // different verifier (client rebooted).  A fresh unconfirmed
            // record is created; any previous unconfirmed record for the
            // owner is superseded (RFC 7530 16.33.4).  A confirmed record
            // survives untouched until the new record is confirmed.
            val dropped = if (unconf != -1) smapDrop(sdb.clients, unconf)
                          else sdb.clients
            val cid = sdb.nextClient
            { sdb2: sdb
                  .with("clients", dropped.put(cid, {
                      ownerSym: ownerSym, verfSym: verfSym, confirmed: false,
                      confirmTok: tok, csSeq: 1, cbUp: false,
                      reclaimDone: false }))
                  .with("nextClient", cid + 1),
              clientid: cid, confirmTok: tok }
        }
    }

    type ConfirmOutcome = ConfOk | ConfStale

    // SETCLIENTID_CONFIRM (RFC 7530 16.34.4).  Confirming a new record for
    // an owner whose old confirmed record had a different verifier removes
    // the old record and all its state (client reboot); confirming an
    // already-confirmed record with a matching token is an idempotent
    // retransmit; an unknown clientid or a token mismatch is
    // NFS4ERR_STALE_CLIENTID.  cbUp reflects whether the callback path
    // the client registered is usable (gates delegation grants).
    pure def applySetclientidConfirm(sdb: StateDB, cid: ClientId, tok: int,
                                     cbUp: bool):
            { sdb2: StateDB, st: ConfirmOutcome } =
        if (not(sdb.clients.keys().contains(cid)) or
            sdb.clients.get(cid).confirmTok != tok)
            { sdb2: sdb, st: ConfStale }
        else {
            val cl = sdb.clients.get(cid)
            if (cl.confirmed)
                // Retransmit of a completed confirm: idempotent success.
                { sdb2: sdb, st: ConfOk }
            else {
                val old = clientByOwner(sdb, cl.ownerSym, true)
                val cleared = if (old != -1) purgeClient(sdb, old) else sdb
                { sdb2: cleared.with("clients", cleared.clients.setBy(cid,
                      x => x.with("confirmed", true).with("cbUp", cbUp))),
                  st: ConfOk }
            }
        }

    // Remove a client record and every piece of state it holds (lease
    // expiry, reboot supersession, DESTROY_CLIENTID).
    pure def purgeClient(sdb: StateDB, cid: ClientId): StateDB = {
        val deadOpens = sdb.opens.keys().filter(s => sdb.opens.get(s).client == cid)
        val deadLocks = sdb.locks.keys().filter(s => sdb.locks.get(s).client == cid)
        val deadDelegs = sdb.delegs.keys().filter(s => sdb.delegs.get(s).client == cid)
        val deadLayouts = sdb.layouts.keys().filter(s => sdb.layouts.get(s).client == cid)
        val deadSess = sdb.sessions.keys().filter(s => sdb.sessions.get(s).client == cid)
        val gone = deadOpens.union(deadLocks).union(deadDelegs).union(deadLayouts)
        sdb.with("clients", smapDrop(sdb.clients, cid))
           .with("opens", sdb.opens.keys().exclude(deadOpens)
                 .mapBy(s => sdb.opens.get(s)))
           .with("locks", sdb.locks.keys().exclude(deadLocks)
                 .mapBy(s => sdb.locks.get(s)))
           .with("delegs", sdb.delegs.keys().exclude(deadDelegs)
                 .mapBy(s => sdb.delegs.get(s)))
           .with("layouts", sdb.layouts.keys().exclude(deadLayouts)
                 .mapBy(s => sdb.layouts.get(s)))
           .with("sessions", sdb.sessions.keys().exclude(deadSess)
                 .mapBy(s => sdb.sessions.get(s)))
           .with("freedSids", sdb.freedSids.union(gone))
    }

    // ------------------------------------------------------------------
    // Client lifecycle: NFSv4.1 (RFC 8881 18.35, 18.36, 2.4)
    // ------------------------------------------------------------------

    type ExchangeIdRes = {
        sdb2: StateDB,
        clientid: ClientId,
        csSeq: int,          // eir_sequenceid the client must use
        confirmedR: bool     // EXCHGID4_FLAG_CONFIRMED_R in eir_flags
    }

    // EXCHANGE_ID (RFC 8881 18.35.4 server-behavior cases, SP4_NONE,
    // update=false).  As with 4.0, principal-mismatch branches are not
    // modeled (single test principal).
    pure def applyExchangeId(sdb: StateDB, ownerSym: int, verfSym: int):
            ExchangeIdRes = {
        val conf = clientByOwner(sdb, ownerSym, true)
        val unconf = clientByOwner(sdb, ownerSym, false)
        if (conf != -1 and sdb.clients.get(conf).verfSym == verfSym)
            // Confirmed record, same verifier: return it unchanged; the
            // client may go on to create additional sessions.
            { sdb2: sdb, clientid: conf,
              csSeq: sdb.clients.get(conf).csSeq, confirmedR: true }
        else {
            // No record / reboot (different verifier) / repeat unconfirmed:
            // produce a fresh unconfirmed record; an old confirmed record
            // survives (with its state) until the new one is confirmed by
            // CREATE_SESSION (RFC 8881 18.35.4 case 5).
            val dropped = if (unconf != -1) smapDrop(sdb.clients, unconf)
                          else sdb.clients
            val cid = sdb.nextClient
            { sdb2: sdb
                  .with("clients", dropped.put(cid, {
                      ownerSym: ownerSym, verfSym: verfSym, confirmed: false,
                      confirmTok: 0, csSeq: 1, cbUp: false,
                      reclaimDone: false }))
                  .with("nextClient", cid + 1),
              clientid: cid, csSeq: 1, confirmedR: false }
        }
    }

    type CreateSessOutcome = CsOk | CsReplay | CsMisordered | CsStale

    // CREATE_SESSION (RFC 8881 18.36.4).  csa_sequence must equal the
    // client's expected sequence (csSeq): equal -> new session, client
    // confirmed, csSeq advances; one less -> replay of the previous
    // CREATE_SESSION (the model returns the same session);
    // anything else -> NFS4ERR_SEQ_MISORDERED.  Confirming a new client
    // record whose owner had an older confirmed record with a different
    // verifier purges the old record's state (client reboot).
    pure def applyCreateSession(sdb: StateDB, cid: ClientId, csaSeq: int,
                                nSlots: int, wantBackChan: bool):
            { sdb2: StateDB, st: CreateSessOutcome, sess: SessId } =
        if (not(sdb.clients.keys().contains(cid)))
            { sdb2: sdb, st: CsStale, sess: -1 }
        else {
            val cl = sdb.clients.get(cid)
            if (csaSeq == cl.csSeq - 1 and cl.csSeq > 1) {
                // Replay of the previous CREATE_SESSION: the one-slot
                // reply cache returns the cached result (the session it
                // created).  The cache is seeded so that csa_sequence ==
                // eir_sequenceid - 1 BEFORE any CREATE_SESSION executed
                // yields NFS4ERR_SEQ_MISORDERED (RFC 8881 18.36.4), hence
                // the csSeq > 1 guard.
                val last = sdb.sessions.keys()
                    .filter(s => sdb.sessions.get(s).client == cid)
                    .fold(-1, (acc, s) => imax(acc, s))
                { sdb2: sdb, st: CsReplay, sess: last }
            }
            else if (csaSeq != cl.csSeq)
                { sdb2: sdb, st: CsMisordered, sess: -1 }
            else {
                val old = if (not(cl.confirmed))
                              clientByOwner(sdb, cl.ownerSym, true)
                          else -1
                val cleared = if (old != -1) purgeClient(sdb, old) else sdb
                val sid = cleared.nextSess
                val slots = range(0, nSlots).foldl([], (acc, _) =>
                    acc.append({ seq: 0, reqTag: -1, cached: false }))
                { sdb2: cleared
                      .with("sessions", cleared.sessions.put(sid, {
                          client: cid, slots: slots,
                          backChan: wantBackChan }))
                      .with("nextSess", cleared.nextSess + 1)
                      .with("clients", cleared.clients.setBy(cid, x => x
                          .with("confirmed", true)
                          .with("csSeq", x.csSeq + 1)
                          .with("cbUp", x.cbUp or wantBackChan))),
                  st: CsOk, sess: sid }
            }
        }

    type SeqOutcome =
        | SeqNew
        | SeqReplay
        | SeqFalseRetry
        | SeqMisordered
        | SeqBadSession
        | SeqBadSlot

    // SEQUENCE (RFC 8881 18.46.3, 2.10.6.1).  reqTag identifies the whole
    // compound so a same-seq retry can be classified as replay (same tag)
    // or false retry (different tag).  On SeqNew the slot advances and
    // records the tag; on errors and replays the table is unchanged.
    pure def applySequence(sdb: StateDB, sess: SessId, slotId: int, seq: int,
                           reqTag: int, cacheThis: bool):
            { sdb2: StateDB, st: SeqOutcome } =
        if (not(sdb.sessions.keys().contains(sess)))
            { sdb2: sdb, st: SeqBadSession }
        else {
            val sn = sdb.sessions.get(sess)
            if (slotId < 0 or slotId >= sn.slots.length())
                { sdb2: sdb, st: SeqBadSlot }
            else {
                val slot = sn.slots[slotId]
                if (seq == slot.seq + 1)
                    { sdb2: sdb.with("sessions", sdb.sessions.setBy(sess, s =>
                          s.with("slots", s.slots.replaceAt(slotId, {
                              seq: seq, reqTag: reqTag, cached: cacheThis })))),
                      st: SeqNew }
                else if (seq == slot.seq and slot.seq > 0)
                    { sdb2: sdb,
                      st: if (slot.reqTag == reqTag) SeqReplay
                          else SeqFalseRetry }
                else
                    { sdb2: sdb, st: SeqMisordered }
            }
        }

    // DESTROY_SESSION (RFC 8881 18.37): session state goes away; locking
    // and other client state survives (it belongs to the clientid).
    pure def applyDestroySession(sdb: StateDB, sess: SessId):
            { sdb2: StateDB, ok: bool } =
        if (not(sdb.sessions.keys().contains(sess)))
            { sdb2: sdb, ok: false }
        else
            { sdb2: sdb.with("sessions", smapDrop(sdb.sessions, sess)), ok: true }

    // DESTROY_CLIENTID (RFC 8881 18.50): only legal once the client has no
    // sessions and no state (NFS4ERR_CLIENTID_BUSY otherwise).
    pure def clientBusy(sdb: StateDB, cid: ClientId): bool =
        sdb.sessions.keys().exists(s => sdb.sessions.get(s).client == cid) or
        sdb.opens.keys().exists(s => sdb.opens.get(s).client == cid) or
        sdb.locks.keys().exists(s => sdb.locks.get(s).client == cid) or
        sdb.delegs.keys().exists(s => sdb.delegs.get(s).client == cid) or
        sdb.layouts.keys().exists(s => sdb.layouts.get(s).client == cid)

    // ------------------------------------------------------------------
    // Share reservations and opens (RFC 8881 9.7; RFC 7530 9.1.4.1)
    // ------------------------------------------------------------------

    pure val ACC_READ = 1
    pure val ACC_WRITE = 2
    pure val ACC_BOTH = 3
    pure val DENY_NONE = 0
    pure val DENY_READ = 1
    pure val DENY_WRITE = 2
    pure val DENY_BOTH = 3

    pure def bitsOverlap(a: int, b: int): bool =
        (a % 2 == 1 and b % 2 == 1) or (a >= 2 and b >= 2)

    // The open stateid for (client, owner, file); -1 if none.
    pure def openByOwner(sdb: StateDB, cid: ClientId, owner: int, f: Ino): Sid =
        sdb.opens.keys().fold(-1, (acc, s) => {
            val o = sdb.opens.get(s)
            if (o.client == cid and o.owner == owner and o.file == f) s else acc
        })

    // A proposed open (access, deny) conflicts iff its access intersects
    // the aggregate deny of ALL current opens of the file, or its deny
    // intersects the aggregate access (RFC 7530 9.9 / RFC 8881 9.7).
    // The aggregate deliberately includes the requester's own opens: an
    // owner holding (READ, deny WRITE) that upgrades with access WRITE
    // self-conflicts and gets NFS4ERR_SHARE_DENIED.
    pure def shareConflict(sdb: StateDB, cid: ClientId, owner: int, f: Ino,
                           access: int, deny: int): bool =
        sdb.opens.keys().exists(s => {
            val o = sdb.opens.get(s)
            o.file == f and
            (bitsOverlap(access, o.deny) or bitsOverlap(deny, o.access))
        })

    type OpenOutcome = {
        sdb2: StateDB,
        denied: bool,
        sid: Sid,
        seq: int,
        // 4.0: the open-owner is new, so the result carries
        // OPEN4_RESULT_CONFIRM and the client must OPEN_CONFIRM.
        needConfirm: bool
    }

    // Grant an open (share-reservation part only; the caller has already
    // resolved the name to file f and handled creation).  Same-owner
    // reopens combine access/deny unions into the one stateid with a seqid
    // bump; a fresh (client,owner,file) triple materializes a new stateid
    // with seqid 1.  needConfirm40 tells whether 4.0 owner-confirmation
    // applies (the caller passes false for 4.1+).
    pure def doOpen(sdb: StateDB, cid: ClientId, owner: int, f: Ino,
                    access: int, deny: int, needConfirm40: bool): OpenOutcome =
        if (shareConflict(sdb, cid, owner, f, access, deny))
            { sdb2: sdb, denied: true, sid: -1, seq: 0, needConfirm: false }
        else {
            val existing = openByOwner(sdb, cid, owner, f)
            if (existing != -1) {
                // Upgrade: union into the one stateid, seqid bumped even
                // when the union is unchanged (RFC 8881 9.9).
                val o = sdb.opens.get(existing)
                { sdb2: sdb.with("opens", sdb.opens.setBy(existing, x => x
                      .with("access", orBits(x.access, access))
                      .with("deny", orBits(x.deny, deny))
                      .with("events", x.events.union(Set({ access: access,
                                                          deny: deny })))
                      .with("seq", x.seq + 1))),
                  denied: false, sid: existing, seq: o.seq + 1,
                  needConfirm: false }
            } else {
                val s = sdb.nextSid
                { sdb2: sdb
                      .with("opens", sdb.opens.put(s, {
                          seq: 1, client: cid, owner: owner, file: f,
                          access: access, deny: deny,
                          events: Set({ access: access, deny: deny }),
                          needConfirm: needConfirm40 }))
                      .with("nextSid", s + 1),
                  denied: false, sid: s, seq: 1, needConfirm: needConfirm40 }
            }
        }

    // Bitwise OR over the 2-bit access/deny masks.
    pure def orBits(a: int, b: int): int = {
        val lo = if (a % 2 == 1 or b % 2 == 1) 1 else 0
        val hi = if (a >= 2 or b >= 2) 2 else 0
        lo + hi
    }

    // a's bits are a subset of b's.
    pure def bitsSubset(a: int, b: int): bool =
        (a % 2 <= b % 2) and (a / 2 <= b / 2)

    // OPEN_CONFIRM (RFC 7530 16.18): first use of a new open-owner's
    // stateid; bumps the stateid seqid and marks the owner confirmed.
    pure def doOpenConfirm(sdb: StateDB, sid: Sid): { sdb2: StateDB, seq: int } = {
        val o = sdb.opens.get(sid)
        // Confirmation is per-OWNER (RFC 7530 9.1.11): confirming the owner
        // through this open clears the confirm requirement on every open the
        // owner already holds.  A second unconfirmed open thus becomes usable
        // without its own OPEN_CONFIRM, and is no longer separately confirmable
        // (a later OPEN_CONFIRM of it hits the already-confirmed BAD_STATEID).
        { sdb2: sdb.with("opens", sdb.opens.keys().mapBy(s => {
              val x = sdb.opens.get(s)
              if (s == sid) x.with("needConfirm", false).with("seq", x.seq + 1)
              else if (x.client == o.client and x.owner == o.owner)
                  x.with("needConfirm", false)
              else x
          })),
          seq: o.seq + 1 }
    }

    // OPEN_DOWNGRADE (RFC 7530 16.19.4 / RFC 8881 18.18.3): the new modes
    // must exactly equal the union of some subset of the historical OPEN
    // events behind the stateid, else NFS4ERR_INVAL.  Standard trick: the
    // best achievable union under (a, d) is the union of every event
    // fitting inside (a, d); the target is expressible iff that union is
    // exactly (a, d).  On success the event history collapses to the
    // downgraded value (the model's sound simplification; the RFCs are
    // silent on post-downgrade history).
    pure def doOpenDowngrade(sdb: StateDB, sid: Sid, access: int, deny: int):
            { sdb2: StateDB, ok: bool, seq: int } = {
        val o = sdb.opens.get(sid)
        val fitting = o.events.filter(e =>
            bitsSubset(e.access, access) and bitsSubset(e.deny, deny))
        val best = fitting.fold({ access: 0, deny: 0 }, (acc, e) =>
            { access: orBits(acc.access, e.access),
              deny: orBits(acc.deny, e.deny) })
        if (access == 0 or best.access != access or best.deny != deny)
            { sdb2: sdb, ok: false, seq: o.seq }
        else
            { sdb2: sdb.with("opens", sdb.opens.setBy(sid, x => x
                  .with("access", access)
                  .with("deny", deny)
                  .with("events", Set({ access: access, deny: deny }))
                  .with("seq", x.seq + 1))),
              ok: true, seq: o.seq + 1 }
    }

    // CLOSE (RFC 7530 16.2 / RFC 8881 18.2): the open stateid dies, and
    // every lock stateid hanging off it dies with it (RFC 8881 18.2.4
    // requires the server to release the locks; 4.0 servers MAY instead
    // refuse with NFS4ERR_LOCKS_HELD, which the caller checks first).
    pure def doClose(sdb: StateDB, sid: Sid): StateDB = {
        val deadLocks = sdb.locks.keys().filter(s =>
            sdb.locks.get(s).openSid == sid)
        sdb.with("opens", smapDrop(sdb.opens, sid))
           .with("locks", sdb.locks.keys().exclude(deadLocks)
                 .mapBy(s => sdb.locks.get(s)))
           .with("freedSids", sdb.freedSids.union(Set(sid)).union(deadLocks))
    }

    pure def locksUnderOpen(sdb: StateDB, sid: Sid): Set[Sid] =
        sdb.locks.keys().filter(s => sdb.locks.get(s).openSid == sid)

    // ------------------------------------------------------------------
    // Byte-range locks (RFC 7530 9 / RFC 8881 9)
    // ------------------------------------------------------------------

    // The lock stateid for (client, lock-owner, file); -1 if none.
    pure def lockByOwner(sdb: StateDB, cid: ClientId, owner: int, f: Ino): Sid =
        sdb.locks.keys().fold(-1, (acc, s) => {
            val l = sdb.locks.get(s)
            if (l.client == cid and l.owner == owner and l.file == f) s else acc
        })

    // Does (client, lock-owner, file) currently *hold* any byte-range lock?
    // A lock stateid that exists but has been fully LOCKU'd holds none, so
    // open_to_lock_owner may still re-establish it (RFC 7530 16.10.5: only
    // held byte-range lock state makes a duplicate new-owner BAD_SEQID).
    pure def lockHeldByOwner(sdb: StateDB, cid: ClientId, owner: int,
                             f: Ino): bool =
        sdb.locks.keys().exists(s => {
            val l = sdb.locks.get(s)
            l.client == cid and l.owner == owner and l.file == f and
            l.bytes.keys().size() > 0
        })

    // Does a lock of type wr over [lo, hi) by (cid, owner) conflict with
    // any existing lock on file f held by a DIFFERENT lock-owner?
    // WRITE conflicts with everything; READ conflicts with WRITE
    // (RFC 8881 9.1.5).  Same-owner locks never conflict (they are
    // replaced/merged instead).
    pure def lockConflict(sdb: StateDB, cid: ClientId, owner: int, f: Ino,
                          lo: int, hi: int, wr: bool): bool =
        sdb.locks.keys().exists(s => {
            val l = sdb.locks.get(s)
            l.file == f and
            not(l.client == cid and l.owner == owner) and
            range(lo, hi).foldl(false, (acc, b) =>
                acc or (l.bytes.keys().contains(b) and
                        (wr or l.bytes.get(b) == LWrite)))
        })

    // One conflicting lock for the NFS4ERR_DENIED body: the model reports
    // the owner of *a* conflicting lock; which conflicting lock a server
    // reports when several exist is discretionary, so the harness checks
    // only that the reported conflict is real.
    pure def lockConflictOwner(sdb: StateDB, cid: ClientId, owner: int, f: Ino,
                               lo: int, hi: int, wr: bool): int =
        sdb.locks.keys().fold(-1, (acc, s) => {
            val l = sdb.locks.get(s)
            if (l.file == f and
                not(l.client == cid and l.owner == owner) and
                range(lo, hi).foldl(false, (accB, b) =>
                    accB or (l.bytes.keys().contains(b) and
                             (wr or l.bytes.get(b) == LWrite))))
                l.owner
            else acc
        })

    // Install a lock over [lo, hi): per-byte assignment implements the
    // POSIX upgrade/downgrade/merge/split semantics exactly
    // (RFC 8881 9.1.5: the new lock replaces whatever the owner held on
    // those bytes).  Creates the lock state (seqid 1) on first use.
    pure def doLock(sdb: StateDB, cid: ClientId, owner: int, f: Ino,
                    openSid: Sid, lo: int, hi: int, wr: bool):
            { sdb2: StateDB, sid: Sid, seq: int } = {
        val existing = lockByOwner(sdb, cid, owner, f)
        val newBytes = range(lo, hi).foldl(Map(), (acc, b) =>
            acc.put(b, if (wr) LWrite else LRead))
        if (existing != -1) {
            val l = sdb.locks.get(existing)
            val merged = range(lo, hi).foldl(l.bytes, (acc, b) =>
                acc.put(b, if (wr) LWrite else LRead))
            { sdb2: sdb.with("locks", sdb.locks.setBy(existing, x => x
                  .with("bytes", merged)
                  .with("seq", x.seq + 1))),
              sid: existing, seq: l.seq + 1 }
        } else {
            val s = sdb.nextSid
            { sdb2: sdb
                  .with("locks", sdb.locks.put(s, {
                      seq: 1, client: cid, owner: owner, file: f,
                      openSid: openSid, bytes: newBytes }))
                  .with("nextSid", s + 1),
              sid: s, seq: 1 }
        }
    }

    // Remove the owner's locks over [lo, hi) (RFC 8881 18.12: LOCKU of a
    // range that is not held still succeeds; partial unlocks split).
    pure def doLocku(sdb: StateDB, sid: Sid, lo: int, hi: int):
            { sdb2: StateDB, seq: int } = {
        val l = sdb.locks.get(sid)
        val kept = l.bytes.keys().filter(b => b < lo or b >= hi)
            .mapBy(b => l.bytes.get(b))
        { sdb2: sdb.with("locks", sdb.locks.setBy(sid, x => x
              .with("bytes", kept)
              .with("seq", x.seq + 1))),
          seq: l.seq + 1 }
    }

    // ------------------------------------------------------------------
    // Delegations (RFC 7530 10 / RFC 8881 10)
    // ------------------------------------------------------------------

    pure def delegByClient(sdb: StateDB, cid: ClientId, f: Ino): Sid =
        sdb.delegs.keys().fold(-1, (acc, s) => {
            val d = sdb.delegs.get(s)
            if (d.client == cid and d.file == f) s else acc
        })

    pure def delegsOnFile(sdb: StateDB, f: Ino): Set[Sid] =
        sdb.delegs.keys().filter(s => sdb.delegs.get(s).file == f)

    // Would an open (access, deny) by client cid require recalling any
    // delegation on f?  A write delegation conflicts with any open by
    // another client; a read delegation conflicts with an open for write
    // (RFC 8881 10.4.3).  The holder's own opens never require a recall.
    pure def delegConflicts(sdb: StateDB, cid: ClientId, f: Ino,
                            access: int): Set[Sid] =
        delegsOnFile(sdb, f).filter(s => {
            val d = sdb.delegs.get(s)
            d.client != cid and (d.isWrite or access >= 2)
        })

    pure def doGrantDeleg(sdb: StateDB, cid: ClientId, f: Ino, isWrite: bool):
            { sdb2: StateDB, sid: Sid } = {
        val s = sdb.nextSid
        { sdb2: sdb
              .with("delegs", sdb.delegs.put(s, {
                  seq: 1, client: cid, file: f, isWrite: isWrite,
                  recalled: false }))
              .with("nextSid", s + 1),
          sid: s }
    }

    pure def doRecallDeleg(sdb: StateDB, sid: Sid): StateDB =
        sdb.with("delegs", sdb.delegs.setBy(sid, d => d.with("recalled", true)))

    pure def doDelegReturn(sdb: StateDB, sid: Sid): StateDB =
        sdb.with("delegs", smapDrop(sdb.delegs, sid))
           .with("freedSids", sdb.freedSids.union(Set(sid)))

    // ------------------------------------------------------------------
    // pNFS layouts (RFC 8881 12, 18.42-18.44)
    // ------------------------------------------------------------------

    pure def layoutByClient(sdb: StateDB, cid: ClientId, f: Ino): Sid =
        sdb.layouts.keys().fold(-1, (acc, s) => {
            val l = sdb.layouts.get(s)
            if (l.client == cid and l.file == f) s else acc
        })

    // LAYOUTGET (RFC 8881 18.43.3): merge the granted block range into the
    // client's layout for the file.  The layout stateid is created on the
    // first grant (seqid 1) and its seqid advances on every LAYOUTGET /
    // partial LAYOUTRETURN (RFC 8881 12.5.3).  rw=true grants RW iomode;
    // an RW grant subsumes a READ grant on the same blocks.
    pure def doLayoutGet(sdb: StateDB, cid: ClientId, f: Ino,
                         lo: int, hi: int, rw: bool):
            { sdb2: StateDB, sid: Sid, seq: int } = {
        val existing = layoutByClient(sdb, cid, f)
        if (existing != -1) {
            val l = sdb.layouts.get(existing)
            val merged = range(lo, hi).foldl(l.blocks, (acc, b) =>
                acc.put(b, rw or (acc.keys().contains(b) and acc.get(b))))
            { sdb2: sdb.with("layouts", sdb.layouts.setBy(existing, x => x
                  .with("blocks", merged)
                  .with("seq", x.seq + 1))),
              sid: existing, seq: l.seq + 1 }
        } else {
            val s = sdb.nextSid
            val blocks = range(lo, hi).foldl(Map(), (acc, b) => acc.put(b, rw))
            { sdb2: sdb
                  .with("layouts", sdb.layouts.put(s, {
                      seq: 1, client: cid, file: f, blocks: blocks,
                      returnOnClose: false }))
                  .with("nextSid", s + 1),
              sid: s, seq: 1 }
        }
    }

    // LAYOUTRETURN of [lo, hi) (RFC 8881 18.44.3): blocks in range go
    // away; if nothing remains the layout stateid is retired and the next
    // LAYOUTGET starts over, otherwise the seqid advances and the stateid
    // stays valid.
    pure def doLayoutReturn(sdb: StateDB, sid: Sid, lo: int, hi: int):
            { sdb2: StateDB, remains: bool, seq: int } = {
        val l = sdb.layouts.get(sid)
        val kept = l.blocks.keys().filter(b => b < lo or b >= hi)
            .mapBy(b => l.blocks.get(b))
        if (kept.keys().size() == 0)
            { sdb2: sdb.with("layouts", smapDrop(sdb.layouts, sid))
                       .with("freedSids", sdb.freedSids.union(Set(sid))),
              remains: false, seq: l.seq }
        else
            { sdb2: sdb.with("layouts", sdb.layouts.setBy(sid, x => x
                  .with("blocks", kept)
                  .with("seq", x.seq + 1))),
              remains: true, seq: l.seq + 1 }
    }

    // ------------------------------------------------------------------
    // Generic stateid classification (RFC 8881 8.2.4)
    // ------------------------------------------------------------------

    type SidCheck = SidOk | SidOld | SidBad

    pure def sidSeqOf(sdb: StateDB, sid: Sid, kind: SidKind): int =
        match kind {
            | KOpen => sdb.opens.get(sid).seq
            | KLock => sdb.locks.get(sid).seq
            | KDeleg => sdb.delegs.get(sid).seq
            | KLayout => sdb.layouts.get(sid).seq
        }

    pure def sidLive(sdb: StateDB, sid: Sid, kind: SidKind): bool =
        match kind {
            | KOpen => sdb.opens.keys().contains(sid)
            | KLock => sdb.locks.keys().contains(sid)
            | KDeleg => sdb.delegs.keys().contains(sid)
            | KLayout => sdb.layouts.keys().contains(sid)
        }

    // Validate a stateid presented with sequence value seq.  seqid 0 in an
    // argument stateid means "use the current seqid, no staleness check"
    // in BOTH 4.0 and 4.1 (RFC 7530 9.1.4.4 / RFC 8881 8.2.2).  A freed or
    // unknown sid is BAD_STATEID; a stale seq is OLD_STATEID and a future
    // seq BAD_STATEID.
    pure def checkSid(sdb: StateDB, sid: Sid, kind: SidKind, seq: int): SidCheck =
        if (not(sidLive(sdb, sid, kind))) SidBad
        else {
            val cur = sidSeqOf(sdb, sid, kind)
            if (seq == 0 or seq == cur) SidOk
            else if (seq < cur) SidOld
            else SidBad
        }

    pure def imax2(a: int, b: int): int = if (a > b) a else b

    // ------------------------------------------------------------------
    // Structural invariants
    // ------------------------------------------------------------------

    // Every piece of state belongs to a live client.
    pure def sdbClientsLive(sdb: StateDB): bool =
        sdb.opens.keys().forall(s =>
            sdb.clients.keys().contains(sdb.opens.get(s).client)) and
        sdb.locks.keys().forall(s =>
            sdb.clients.keys().contains(sdb.locks.get(s).client)) and
        sdb.delegs.keys().forall(s =>
            sdb.clients.keys().contains(sdb.delegs.get(s).client)) and
        sdb.layouts.keys().forall(s =>
            sdb.clients.keys().contains(sdb.layouts.get(s).client)) and
        sdb.sessions.keys().forall(s =>
            sdb.clients.keys().contains(sdb.sessions.get(s).client))

    // Lock state always hangs off a live open of the same (client, file).
    pure def sdbLocksAnchored(sdb: StateDB): bool =
        sdb.locks.keys().forall(s => {
            val l = sdb.locks.get(s)
            sdb.opens.keys().contains(l.openSid) and
            sdb.opens.get(l.openSid).client == l.client and
            sdb.opens.get(l.openSid).file == l.file
        })

    // No two opens share a (client, owner, file) triple, and share unions
    // never conflict pairwise (the grant-time check keeps this).
    pure def sdbOpensConsistent(sdb: StateDB): bool =
        sdb.opens.keys().forall(s1 =>
            sdb.opens.keys().forall(s2 => {
                val a = sdb.opens.get(s1)
                val b = sdb.opens.get(s2)
                (s1 == s2 or
                 not(a.client == b.client and a.owner == b.owner and
                     a.file == b.file)) and
                (s1 == s2 or a.file != b.file or
                 (not(bitsOverlap(a.access, b.deny)) and
                  not(bitsOverlap(a.deny, b.access))))
            }))

    // Conflicting byte-range locks never coexist.
    pure def sdbLocksDisjoint(sdb: StateDB): bool =
        sdb.locks.keys().forall(s1 =>
            sdb.locks.keys().forall(s2 => {
                val a = sdb.locks.get(s1)
                val b = sdb.locks.get(s2)
                s1 == s2 or a.file != b.file or
                a.bytes.keys().intersect(b.bytes.keys()).forall(bb =>
                    a.bytes.get(bb) == LRead and b.bytes.get(bb) == LRead)
            }))

    // At most one confirmed and one unconfirmed client record per owner.
    pure def sdbClientRecordsUnique(sdb: StateDB): bool =
        sdb.clients.keys().forall(c1 =>
            sdb.clients.keys().forall(c2 => {
                val a = sdb.clients.get(c1)
                val b = sdb.clients.get(c2)
                c1 == c2 or a.ownerSym != b.ownerSym or
                a.confirmed != b.confirmed
            }))

    // Freed sids never come back.
    pure def sdbFreedStayDead(sdb: StateDB): bool =
        sdb.freedSids.intersect(
            sdb.opens.keys().union(sdb.locks.keys())
                .union(sdb.delegs.keys()).union(sdb.layouts.keys())) == Set()

    pure def sdbInv(sdb: StateDB): bool =
        sdbClientsLive(sdb) and sdbLocksAnchored(sdb) and
        sdbOpensConsistent(sdb) and sdbLocksDisjoint(sdb) and
        sdbClientRecordsUnique(sdb) and sdbFreedStayDead(sdb)
}
