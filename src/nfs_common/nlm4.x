
/*
 * SPDX-FileCopyrightText: 2026 'IETF Trust and the persons identified
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* NLM (Network Lock Manager) v4 -- RFC 1813 Appendix II */

const LM_MAXSTRLEN = 1024;  /* Maximum bytes in a lock-related string */

typedef opaque netobj<LM_MAXSTRLEN>;
typedef uint64_t nlm4_uint64;

enum nlm4_stats {
    NLM4_GRANTED             = 0,
    NLM4_DENIED              = 1,
    NLM4_DENIED_NOLOCKS      = 2,
    NLM4_BLOCKED             = 3,
    NLM4_DENIED_GRACE_PERIOD = 4,
    NLM4_DEADLCK             = 5,
    NLM4_ROFS                = 6,
    NLM4_STALE_FH            = 7,
    NLM4_FBIG                = 8,
    NLM4_FAILED              = 9
};

struct nlm4_holder {
    bool         exclusive;
    int          svid;
    netobj       oh;
    nlm4_uint64  l_offset;
    nlm4_uint64  l_len;
};

struct nlm4_lock {
    string       caller_name<LM_MAXSTRLEN>;
    netobj       fh;
    netobj       oh;
    int          svid;
    nlm4_uint64  l_offset;
    nlm4_uint64  l_len;
};

enum fsh4_mode {
    FSM_DN  = 0,
    FSM_DR  = 1,
    FSM_DW  = 2,
    FSM_DRW = 3
};

enum fsh4_access {
    FSA_NONE = 0,
    FSA_R    = 1,
    FSA_W    = 2,
    FSA_RW   = 3
};

struct nlm4_share {
    string       caller_name<LM_MAXSTRLEN>;
    netobj       fh;
    netobj       oh;
    fsh4_mode    mode;
    fsh4_access  access;
};

struct nlm4_testargs {
    netobj     cookie;
    bool       exclusive;
    nlm4_lock  alock;
};

struct nlm4_res {
    netobj     cookie;
    nlm4_stats stat;
};

union nlm4_testrply switch (nlm4_stats stat) {
    case NLM4_DENIED:
        nlm4_holder holder;
    default:
        void;
};

struct nlm4_testres {
    netobj        cookie;
    nlm4_testrply test_stat;
};

struct nlm4_lockargs {
    netobj     cookie;
    bool       block;
    bool       exclusive;
    nlm4_lock  alock;
    bool       reclaim;
    int        state;
};

struct nlm4_cancargs {
    netobj     cookie;
    bool       block;
    bool       exclusive;
    nlm4_lock  alock;
};

struct nlm4_unlockargs {
    netobj     cookie;
    nlm4_lock  alock;
};

struct nlm4_shareargs {
    netobj      cookie;
    nlm4_share  share;
    bool        reclaim;
};

struct nlm4_shareres {
    netobj     cookie;
    nlm4_stats stat;
    int        sequence;
};

struct nlm4_notify {
    string  name<LM_MAXSTRLEN>;
    int     state;
};

program NLM {
    version NLM_V4 {
        void           NLMPROC4_NULL(void)                   = 0;
        nlm4_testres   NLMPROC4_TEST(nlm4_testargs)          = 1;
        nlm4_res       NLMPROC4_LOCK(nlm4_lockargs)          = 2;
        nlm4_res       NLMPROC4_CANCEL(nlm4_cancargs)        = 3;
        nlm4_res       NLMPROC4_UNLOCK(nlm4_unlockargs)      = 4;
        nlm4_res       NLMPROC4_GRANTED(nlm4_testargs)       = 5;
        void           NLMPROC4_TEST_MSG(nlm4_testargs)      = 6;
        void           NLMPROC4_LOCK_MSG(nlm4_lockargs)      = 7;
        void           NLMPROC4_CANCEL_MSG(nlm4_cancargs)    = 8;
        void           NLMPROC4_UNLOCK_MSG(nlm4_unlockargs)  = 9;
        void           NLMPROC4_GRANTED_MSG(nlm4_testargs)   = 10;
        void           NLMPROC4_TEST_RES(nlm4_testres)       = 11;
        void           NLMPROC4_LOCK_RES(nlm4_res)           = 12;
        void           NLMPROC4_CANCEL_RES(nlm4_res)         = 13;
        void           NLMPROC4_UNLOCK_RES(nlm4_res)         = 14;
        void           NLMPROC4_GRANTED_RES(nlm4_res)        = 15;
        void           NLMPROC4_RESERVED_16(void)            = 16;
        void           NLMPROC4_RESERVED_17(void)            = 17;
        void           NLMPROC4_RESERVED_18(void)            = 18;
        void           NLMPROC4_RESERVED_19(void)            = 19;
        nlm4_shareres  NLMPROC4_SHARE(nlm4_shareargs)        = 20;
        nlm4_shareres  NLMPROC4_UNSHARE(nlm4_shareargs)      = 21;
        nlm4_res       NLMPROC4_NM_LOCK(nlm4_lockargs)       = 22;
        void           NLMPROC4_FREE_ALL(nlm4_notify)        = 23;
    } = 4;
} = 100021;
