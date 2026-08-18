// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: Unlicense

/**
 * portmap.qnt -- a Quint model of chimera's portmap/rpcbind server
 * (src/server/nfs/nfs_portmap.c), used for model-based conformance testing.
 *
 * Chimera's portmapper is a static registry: the service table is fixed at
 * startup and there is no PMAPPROC_SET/UNSET.  Each modeled procedure is a
 * pure request -> reply function; the only state is the last (call, reply)
 * event, which is exactly what the conformance driver replays against a
 * live server (portmap_quint_replay.py).
 *
 * Modeled procedures:
 *   PMAP v2:      NULL (0), GETPORT (3), DUMP (4)
 *   rpcbind v3/4: GETADDR (3), modeled as the port encoded in the returned
 *                 universal address ("" -> 0)
 *
 * Deliberate chimera semantics captured here (see portmap_lookup_port):
 * GETPORT/GETADDR match the exact (prog, vers, prot) triple or return 0.
 * Classic BSD portmap instead falls back to the port of *some* registered
 * version of the program on a version mismatch; chimera follows the strict
 * reading of RFC 1833.  The repliesExact invariant and the
 * noVersionFallbackTest run pin that divergence down.
 *
 * Standalone usage (requires quint, https://quint-lang.org):
 *   quint test portmap.qnt                            # scenario tests
 *   quint run portmap.qnt --invariant=repliesExact    # random simulation
 *   ./regen_traces.sh                                 # refresh traces/
 *
 * The SERVICES table below must mirror the table chimera builds with
 * external_portmap=false and the default lockmgr_port (32803) and NSM port
 * (32765) from server.c -- the configuration the ctest wrapper starts.
 */
module chimera_portmap {

    // ------------------------------------------------------------------
    // Domain
    // ------------------------------------------------------------------

    /// Transport protocol numbers as they appear on the wire.
    pure val PROT_TCP = 6
    pure val PROT_UDP = 17

    /// One row of the portmap service table (struct pmap in RFC 1833).
    type Mapping = { prog: int, vers: int, prot: int, port: int }

    /// rpcbind v3/v4 network identifiers offered by the client.  Chimera
    /// maps "tcp"/"udp" to protocol numbers and treats anything else as
    /// unserved (portmap_netid_to_prot returns 0).
    type Netid =
        | NetTcp
        | NetUdp
        | NetTcp6

    type Call =
        | NullV2
        | GetportV2({ prog: int, vers: int, prot: int })
        | DumpV2
        | GetaddrV3({ prog: int, vers: int, netid: Netid })
        | GetaddrV4({ prog: int, vers: int, netid: Netid })

    type Reply =
        | AckR
        | PortR(int)
        | MapsR(List[Mapping])
        | AddrR(int)

    /// Chimera's service table, in table order (DUMP returns it verbatim):
    /// the six static portmap_services entries, then NLM and NSM appended
    /// at startup (nfs.c calls portmap_set_nlm_port before
    /// portmap_set_nsm_port).  TCP only -- chimera serves no UDP transports.
    pure val SERVICES: List[Mapping] = List(
        { prog: 100000, vers: 2, prot: 6, port: 111 },
        { prog: 100000, vers: 3, prot: 6, port: 111 },
        { prog: 100000, vers: 4, prot: 6, port: 111 },
        { prog: 100003, vers: 3, prot: 6, port: 2049 },
        { prog: 100003, vers: 4, prot: 6, port: 2049 },
        { prog: 100005, vers: 3, prot: 6, port: 20048 },
        { prog: 100021, vers: 4, prot: 6, port: 32803 },
        { prog: 100024, vers: 1, prot: 6, port: 32765 }
    )

    // Query universes for the random client: every registered program, one
    // unknown program, versions beyond the served range, both transports.
    pure val PROGS = Set(100000, 100003, 100005, 100021, 100024, 300000)
    pure val VERSIONS = 1.to(5)
    pure val WIRE_PROTS = Set(PROT_TCP, PROT_UDP)
    pure val NETIDS = Set(NetTcp, NetUdp, NetTcp6)

    // ------------------------------------------------------------------
    // State: the last RPC exchanged, which the conformance driver replays
    // ------------------------------------------------------------------

    var lastEvent: { call: Call, reply: Reply }

    // ------------------------------------------------------------------
    // Reply functions
    // ------------------------------------------------------------------

    /// First-match scan of the service table, 0 when the exact triple is
    /// not served -- mirrors portmap_lookup_port (ports are never 0, so 0
    /// doubles as "not found" exactly as it does in the C code).
    pure def lookupPort(prog: int, vers: int, prot: int): int =
        SERVICES.foldl(0, (acc, m) =>
            if (acc != 0) acc
            else if (m.prog == prog and m.vers == vers and m.prot == prot) m.port
            else acc)

    /// portmap_netid_to_prot: only "tcp" and "udp" are recognized.
    pure def netidProt(n: Netid): int = match n {
        | NetTcp => PROT_TCP
        | NetUdp => PROT_UDP
        | NetTcp6 => 0
    }

    // ------------------------------------------------------------------
    // RPC procedures
    // ------------------------------------------------------------------

    action doNull = all {
        lastEvent' = { call: NullV2, reply: AckR }
    }

    action doGetport(prog: int, vers: int, prot: int): bool = all {
        lastEvent' = {
            call: GetportV2({ prog: prog, vers: vers, prot: prot }),
            reply: PortR(lookupPort(prog, vers, prot))
        }
    }

    action doDump = all {
        lastEvent' = { call: DumpV2, reply: MapsR(SERVICES) }
    }

    action doGetaddr3(prog: int, vers: int, netid: Netid): bool = all {
        lastEvent' = {
            call: GetaddrV3({ prog: prog, vers: vers, netid: netid }),
            reply: AddrR(lookupPort(prog, vers, netidProt(netid)))
        }
    }

    action doGetaddr4(prog: int, vers: int, netid: Netid): bool = all {
        lastEvent' = {
            call: GetaddrV4({ prog: prog, vers: vers, netid: netid }),
            reply: AddrR(lookupPort(prog, vers, netidProt(netid)))
        }
    }

    // ------------------------------------------------------------------
    // Transition relation: a client fires some call with some arguments
    // ------------------------------------------------------------------

    action init = doNull

    action anyGetport = {
        nondet prog = PROGS.oneOf()
        nondet vers = VERSIONS.oneOf()
        nondet prot = WIRE_PROTS.oneOf()
        doGetport(prog, vers, prot)
    }

    action anyGetaddr3 = {
        nondet prog = PROGS.oneOf()
        nondet vers = VERSIONS.oneOf()
        nondet netid = NETIDS.oneOf()
        doGetaddr3(prog, vers, netid)
    }

    action anyGetaddr4 = {
        nondet prog = PROGS.oneOf()
        nondet vers = VERSIONS.oneOf()
        nondet netid = NETIDS.oneOf()
        doGetaddr4(prog, vers, netid)
    }

    action step = any {
        doNull,
        anyGetport,
        doDump,
        anyGetaddr3,
        anyGetaddr4
    }

    // ------------------------------------------------------------------
    // Invariants -- quantified restatements of the reply semantics,
    // independent of the foldl scan that implements them above
    // ------------------------------------------------------------------

    /// A port reply is sound iff: 0 means the exact triple is absent from
    /// the table (even when sibling versions of the program are served --
    /// the no-BSD-fallback property), and a nonzero port names an entry
    /// carrying exactly that triple.
    pure def soundPort(prog: int, vers: int, prot: int, p: int): bool =
        if (p == 0)
            SERVICES.indices().forall(i => not(
                SERVICES[i].prog == prog and
                SERVICES[i].vers == vers and
                SERVICES[i].prot == prot))
        else
            SERVICES.indices().exists(i =>
                SERVICES[i].prog == prog and
                SERVICES[i].vers == vers and
                SERVICES[i].prot == prot and
                SERVICES[i].port == p)

    val repliesExact = match lastEvent.call {
        | NullV2 => lastEvent.reply == AckR
        | DumpV2 => lastEvent.reply == MapsR(SERVICES)
        | GetportV2(q) => match lastEvent.reply {
            | PortR(p) => soundPort(q.prog, q.vers, q.prot, p)
            | _ => false
        }
        | GetaddrV3(q) => match lastEvent.reply {
            | AddrR(p) => soundPort(q.prog, q.vers, netidProt(q.netid), p)
            | _ => false
        }
        | GetaddrV4(q) => match lastEvent.reply {
            | AddrR(p) => soundPort(q.prog, q.vers, netidProt(q.netid), p)
            | _ => false
        }
    }

    // ------------------------------------------------------------------
    // Scenario tests (quint test portmap.qnt)
    // ------------------------------------------------------------------

    run getportExactTest =
        init
            .then(doGetport(100003, 3, PROT_TCP))
            .expect(lastEvent.reply == PortR(2049))
            .then(doGetport(100003, 3, PROT_UDP))
            .expect(lastEvent.reply == PortR(0))
            .then(doGetport(300000, 1, PROT_TCP))
            .expect(lastEvent.reply == PortR(0))

    /// Where chimera deliberately diverges from classic BSD portmap: an
    /// unregistered version of a registered program yields 0, not the
    /// port of a sibling version.
    run noVersionFallbackTest =
        init
            .then(doGetport(100003, 2, PROT_TCP))
            .expect(lastEvent.reply == PortR(0))
            .then(doGetport(100021, 3, PROT_TCP))
            .expect(lastEvent.reply == PortR(0))

    run dumpTest =
        init
            .then(doDump)
            .expect(lastEvent.reply == MapsR(SERVICES))

    run getaddrTest =
        init
            .then(doGetaddr3(100005, 3, NetTcp))
            .expect(lastEvent.reply == AddrR(20048))
            .then(doGetaddr4(100000, 4, NetTcp))
            .expect(lastEvent.reply == AddrR(111))
            .then(doGetaddr3(100003, 3, NetTcp6))
            .expect(lastEvent.reply == AddrR(0))
            .then(doGetaddr4(100003, 3, NetUdp))
            .expect(lastEvent.reply == AddrR(0))
}
