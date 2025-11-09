/*
 * SPDX-FileCopyrightText: IETF Trust and the persons identified
 * SPDX-License-Identifier: BSD-3-Clause
 */

struct mapping {
    unsigned int prog;
    unsigned int vers;
    unsigned int prot;
    unsigned int port;
};

struct port {
    unsigned int port;
};

program NFS_PORTMAP {
    version NFS_PORTMAP_V2 {
        void PMAPPROC_NULL(void)         = 0;
        port PMAPPROC_GETPORT(mapping)   = 3;
    } = 2;
} = 100000;
