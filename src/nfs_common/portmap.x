/*
 * SPDX-FileCopyrightText: IETF Trust and the persons identified
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * pmap_prot.x
 * Port Mapper Protocol (version 2) in RPC Language (XDR)
 * (RFC 1833, Section 3.1)
 */

/* portmapper port number */
const PMAP_PORT = 111;

/* Supported values for the "prot" field */
const IPPROTO_TCP = 6;   /* protocol number for TCP/IP */
const IPPROTO_UDP = 17;  /* protocol number for UDP/IP */

/* A mapping of (program, version, protocol) to port number */
struct mapping {
    unsigned int prog;
    unsigned int vers;
    unsigned int prot;
    unsigned int port;
};

struct pmaplist {
    mapping  map;
    pmaplist *next;
};

/* Arguments to callit */
struct call_args {
    unsigned int prog;
    unsigned int vers;
    unsigned int proc;
    opaque args<>;
};

/* Results of callit */
struct call_result {
    unsigned int port;
    opaque res<>;
};

/* Port mapper procedures */
program NFS_PORTMAP {
    version NFS_PORTMAP_V2 {
        void PMAPPROC_NULL(void) = 0;
        bool PMAPPROC_SET(mapping) = 1;
        bool PMAPPROC_UNSET(mapping) = 2;
        unsigned int PMAPPROC_GETPORT(mapping) = 3;
        pmaplist PMAPPROC_DUMP(void) = 4;
        call_result PMAPPROC_CALLIT(call_args) = 5;
    } = 2;
} = 100000;