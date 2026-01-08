/*
 * SPDX-FileCopyrightText: IETF Trust and the persons identified
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * pmap_prot.x
 * Port Mapper Protocol (version 2) in RPC Language (XDR)
 * (RFC 1833, Section 3.1)
 */

 /*
 * pmap_rpcbind.x
 *
 * Combined Port Mapper (version 2) and RPCBIND (versions 3 and 4)
 * definitions from RFC 1833.
 *
 * Program number: 100000
 * Well-known port (TCP/UDP): 111
 */

/* =========================
 * RPCBIND (versions 3 and 4)
 * ========================= */

/* rpcbind address for TCP/UDP */
const RPCB_PORT = 111;

/*
 * A mapping of (program, version, network ID) to address
 */
struct rpcb {
    unsigned long r_prog;   /* program number */
    unsigned long r_vers;   /* version number */
    string r_netid<>;       /* network id */
    string r_addr<>;        /* universal address */
    string r_owner<>;       /* owner of this service */
};

struct rp__list {
    rpcb rpcb_map;
    rp__list *next;
};

/*
 * Arguments of remote calls
 */
struct rpcb_rmtcallargs {
    unsigned long prog;     /* program number */
    unsigned long vers;     /* version number */
    unsigned long proc;     /* procedure number */
    opaque args<>;          /* argument */
};

/*
 * Results of the remote call
 */
struct rpcb_rmtcallres {
    string addr<>;          /* remote universal address */
    opaque results<>;       /* result */
};

/*
 * rpcb_entry contains a merged address of a service on a particular
 * transport, plus associated netconfig information.
 */
struct rpcb_entry {
    string r_maddr<>;           /* merged address of service */
    string r_nc_netid<>;        /* netid field */
    unsigned long r_nc_semantics; /* semantics of transport */
    string r_nc_protofmly<>;    /* protocol family */
    string r_nc_proto<>;        /* protocol name */
};

/*
 * A list of addresses supported by a service.
 */
struct rpcb_entry_list {
    rpcb_entry rpcb_entry_map;
    rpcb_entry_list *next;
};

/*
 * rpcbind statistics
 *
 * NOTE: RFC 1833 defines these in terms of procedure identifiers
 * (e.g., RPCBPROC_CALLIT).  For rpcgen compatibility across toolchains,
 * we use the explicit numeric values.
 */
const rpcb_highproc_2 = 5;   /* CALLIT */
const rpcb_highproc_3 = 8;   /* TADDR2UADDR */
const rpcb_highproc_4 = 12;  /* GETSTAT */

const RPCBSTAT_HIGHPROC = 13; /* # of procs in rpcbind V4 plus one */

const RPCBVERS_STAT = 3;      /* provide only for rpcbind V2, V3 and V4 */
const RPCBVERS_4_STAT = 2;
const RPCBVERS_3_STAT = 1;
const RPCBVERS_2_STAT = 0;

/* Link list of all the stats about getport and getaddr */
struct rpcbs_addrlist {
    unsigned long prog;
    unsigned long vers;
    int success;
    int failure;
    string netid<>;
    rpcbs_addrlist *next;
};

/* Link list of all the stats about rmtcall */
struct rpcbs_rmtcalllist {
    unsigned long prog;
    unsigned long vers;
    unsigned long proc;
    int success;
    int failure;
    int indirect;        /* whether callit or indirect */
    string netid<>;
    rpcbs_rmtcalllist *next;
};

typedef int rpcbs_proc[RPCBSTAT_HIGHPROC];

typedef rpcbs_addrlist *rpcbs_addrlist_ptr;
typedef rpcbs_rmtcalllist *rpcbs_rmtcalllist_ptr;

struct rpcb_stat {
    rpcbs_proc info;
    int setinfo;
    int unsetinfo;
    rpcbs_addrlist_ptr addrinfo;
    rpcbs_rmtcalllist_ptr rmtinfo;
};

/*
 * netbuf structure, used to store the transport specific form of
 * a universal transport address.
 */
struct netbuf {
    unsigned int maxlen;
    opaque buf<>;
};

/* portmapper port number */
const PMAP_PORT = 111;

/* protocol numbers for TCP/IP and UDP/IP */
const IPPROTO_TCP = 6;
const IPPROTO_UDP = 17;

/* A mapping of (program, version, protocol) to port number */
struct mapping {
    unsigned int prog;
    unsigned int vers;
    unsigned int prot;
    unsigned int port;
};

struct pmaplist {
    mapping map;
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
program PORTMAP {
    version PORTMAP_V2 {
        void PMAPPROC_NULL(void) = 0;
        bool PMAPPROC_SET(mapping) = 1;
        bool PMAPPROC_UNSET(mapping) = 2;
        unsigned int PMAPPROC_GETPORT(mapping) = 3;
        pmaplist PMAPPROC_DUMP(void) = 4;
        call_result PMAPPROC_CALLIT(call_args) = 5;
    } = 2;

    version PORTMAP_V3 {
        bool rpcbproc_set(rpcb) = 1;
        bool rpcbproc_unset(rpcb) = 2;
        string rpcbproc_getaddr(rpcb) = 3;
        rp__list *rpcbproc_dump(void) = 4;
        rpcb_rmtcallres rpcbproc_callit(rpcb_rmtcallargs) = 5;
        unsigned int rpcbproc_gettime(void) = 6;
        netbuf rpcbproc_uaddr2taddr(string) = 7;
        string rpcbproc_taddr2uaddr(netbuf) = 8;
    } = 3;

    version PORTMAP_V4 {
        bool RPCBPROC_SET(rpcb) = 1;
        bool RPCBPROC_UNSET(rpcb) = 2;
        string RPCBPROC_GETADDR(rpcb) = 3;
        rp__list *RPCBPROC_DUMP(void) = 4;

        /*
         * NOTE: RPCBPROC_BCAST has the same functionality as CALLIT.
         * The new name is intended to indicate that this procedure should
         * be used for broadcast RPC, and RPCBPROC_INDIRECT should be used
         * for indirect calls.
         */
        rpcb_rmtcallres RPCBPROC_BCAST(rpcb_rmtcallargs) = 5;

        unsigned int RPCBPROC_GETTIME(void) = 6;
        netbuf RPCBPROC_UADDR2TADDR(string) = 7;
        string RPCBPROC_TADDR2UADDR(netbuf) = 8;

        string RPCBPROC_GETVERSADDR(rpcb) = 9;
        rpcb_rmtcallres RPCBPROC_INDIRECT(rpcb_rmtcallargs) = 10;
        rpcb_entry_list *RPCBPROC_GETADDRLIST(rpcb) = 11;
        rpcb_stat[RPCBVERS_STAT] RPCBPROC_GETSTAT(void) = 12;
    } = 4;
} = 100000;