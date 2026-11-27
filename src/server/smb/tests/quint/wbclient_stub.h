// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* The one domain account wbclient_stub.c answers for, shared with the wire
 * probe that logs on as it.  The user and password match the account
 * scripts/ad_test_wrapper.sh creates, so the probe's winbind profiles run
 * unchanged against either. */
#define WBSTUB_DOMAIN         "WBSTUB"
#define WBSTUB_DNS_DOMAIN     "wbstub.test"
#define WBSTUB_NETBIOS_NAME   "WBSTUBSRV"
#define WBSTUB_USER           "testuser1"
#define WBSTUB_PASSWORD       "Password1!"

/* Distinct from the wire probe's local account (uid/gid 1000) and from each
 * other.  A primary group SID that failed to map would leave the server with
 * gid = uid, and WB15 would read the group back as the uid's algorithmic SID
 * (S-1-5-88-2-20001) instead of WBSTUB_GROUP_SID. */
#define WBSTUB_UID            20001u
#define WBSTUB_GID            20513u
#define WBSTUB_USER_SID       "S-1-5-21-1-2-3-1001"
#define WBSTUB_GROUP_SID      "S-1-5-21-1-2-3-513"

/* wbcAuthenticateUserEx calls so far.  Zero after a WB run means the server
 * never reached the stub: its libwbclient calls bound to the real library. */
extern unsigned int wbstub_auth_calls;

#define WBSTUB_DOMAIN_SID     "S-1-5-21-1-2-3"
#define WBSTUB_GROUP_NAME     "Domain Users"

/* "user<rid>" for WBSTUB_POOL_FIRST_RID..WBSTUB_POOL_LAST_RID: accounts that
 * exist in the domain but never log on. */
#define WBSTUB_POOL_FIRST_RID 1002u
#define WBSTUB_POOL_LAST_RID  1099u

/* Lookups of this SID fail as if winbind were down. */
#define WBSTUB_DOWN_SID       "S-1-5-21-1-2-3-1500"

/* wbcLookupName resolves this name in WBSTUB_DOMAIN to WBSTUB_HALF_SID, and
 * wbcLookupSid then knows no such SID: a domain controller that stops
 * answering between the two calls of one name lookup. */
#define WBSTUB_HALF_NAME      "halfknown"
#define WBSTUB_HALF_SID       "S-1-5-21-1-2-3-1600"

/* wbcLookupSid + wbcLookupName calls so far (atomic). */
extern unsigned int  wbstub_lookup_calls;

/* wbcLookupName calls so far that named no domain (atomic).  The stub
 * resolves such a name in WBSTUB_DOMAIN; a caller that must always qualify
 * the names it asks about keeps this at zero. */
extern unsigned int  wbstub_bare_name_calls;

/* The domain and name the last wbcLookupName on the calling thread was asked
 * for.  Thread-local: the resolver's workers call the stub concurrently, so
 * only a test that calls the handler itself, and reads these on that thread,
 * can rely on them. */
extern __thread char wbstub_last_lookup_domain[256];
extern __thread char wbstub_last_lookup_name[256];

/* WBSTUB_LOOKUP_DELAY_MS in the environment, read on every call, delays each
 * wbcLookupSid / wbcLookupName by that many milliseconds. */

/* While non-zero (atomic), every wbcLookupSid / wbcLookupName waits, after it
 * has been counted, until the test clears it: a lookup that stays in flight
 * for exactly as long as the test needs, with no timing involved. */
extern int           wbstub_hold;
