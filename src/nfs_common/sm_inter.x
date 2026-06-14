
/*
 * SPDX-FileCopyrightText: 2026 'IETF Trust and the persons identified
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* NSM (Network Status Monitor) protocol -- RFC 1813 Appendix / the de-facto
 * "sm_inter" interface implemented by rpc.statd.  This is the reboot-monitor
 * service that drives NLM (program 100021) lock recovery: peers' statd send us
 * SM_NOTIFY when they reboot so we drop their stale locks, and on our own
 * reboot we send SM_NOTIFY to every host we were monitoring so they reclaim. */

const SM_MAXSTRLEN = 1024;  /* Maximum bytes in a monitored-host name */
const SM_PRIV_SIZE = 16;    /* Opaque cookie handed back to the monitorer */

enum res {
    STAT_SUCC = 0,          /* NSM agrees to monitor */
    STAT_FAIL = 1           /* NSM cannot monitor     */
};

struct sm_name {
    string mon_name<SM_MAXSTRLEN>;
};

struct my_id {
    string my_name<SM_MAXSTRLEN>; /* hostname of the monitorer (the local NLM) */
    int    my_prog;               /* RPC program to call back on a status change */
    int    my_vers;
    int    my_proc;
};

struct mon_id {
    string mon_name<SM_MAXSTRLEN>; /* hostname of the monitored peer */
    my_id  id;
};

struct mon {
    mon_id id;
    opaque priv[SM_PRIV_SIZE];     /* opaque cookie returned in the callback */
};

struct sm_stat_res {
    res res_stat;
    int state;                     /* NSM state number of this host */
};

struct sm_stat {
    int state;
};

/* SM_NOTIFY argument: "host <mon_name> has restarted with state <state>". */
struct stat_chge {
    string mon_name<SM_MAXSTRLEN>;
    int    state;
};

program SM_INTER {
    version SM_INTER_V1 {
        void         SM_NULL(void)             = 0;
        sm_stat_res  SM_STAT(sm_name)          = 1;
        sm_stat_res  SM_MON(mon)               = 2;
        sm_stat      SM_UNMON(mon_id)          = 3;
        sm_stat      SM_UNMON_ALL(my_id)       = 4;
        void         SM_SIMU_CRASH(void)       = 5;
        void         SM_NOTIFY(stat_chge)      = 6;
    } = 1;
} = 100024;
