// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The DCE/RPC half of the named-pipe services (see smb_rpc_host.h): one
 * libdcerpc server and pipe per client open, the context handles opened on
 * it, and helpers the stubs share.  No chimera server header is included here.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "smb_rpc_service.h"

/* Plenty for any real client; bounds what one pipe can make us hold. */
#define CHIMERA_SMB_RPC_MAX_HANDLES_PER_PIPE 256

struct chimera_smb_rpc_handle {
    struct chimera_smb_rpc_handle *next;
    enum chimera_smb_rpc_handle_type type;
    struct dcerpc_context_handle   handle;
};

static const struct {
    const char           *name;
    struct dcerpc_server *(*endpoint)(
        void);
} chimera_smb_rpc_services[] = {
    {
        "srvsvc", chimera_smb_srvsvc_endpoint
    },
    {
        "wkssvc", chimera_smb_wkssvc_endpoint
    },
    {
        "lsarpc", chimera_smb_lsarpc_endpoint
    },
    {
        "samr", chimera_smb_samr_endpoint
    },
};

int
chimera_smb_rpc_state_init(void)
{
    struct dcerpc_context *dce;

    /* dcerpc_create_context() registers every interface's coders in a global
     * list without locking; later calls (one per dcerpc_server_create) find
     * them registered and only read it.  Do the first one here, before the
     * SMB threads start. */
    dce = dcerpc_create_context(NULL);

    if (!dce) {
        chimera_smb_rpc_host_log(CHIMERA_SMB_RPC_LOG_ERROR,
                                 "libdcerpc: failed to create the coder context");
        return -1;
    }

    dcerpc_destroy_context(dce);
    return 0;
} /* chimera_smb_rpc_state_init */

static void
chimera_smb_rpc_log(
    void       *private_data,
    int         level,
    const char *msg)
{
    (void) private_data;

    switch (level) {
        case DCERPC_SERVER_LOG_ERROR:
            chimera_smb_rpc_host_log(CHIMERA_SMB_RPC_LOG_ERROR, msg);
            break;
        case DCERPC_SERVER_LOG_INFO:
            chimera_smb_rpc_host_log(CHIMERA_SMB_RPC_LOG_INFO, msg);
            break;
        default:
            chimera_smb_rpc_host_log(CHIMERA_SMB_RPC_LOG_DEBUG, msg);
            break;
    } /* switch */
} /* chimera_smb_rpc_log */

struct chimera_smb_rpc_state *
chimera_smb_rpc_state_open(
    const char                  *name,
    struct chimera_smb_rpc_host *host)
{
    struct chimera_smb_rpc_state *state;
    unsigned int                  i;

    for (i = 0; i < sizeof(chimera_smb_rpc_services) /
         sizeof(chimera_smb_rpc_services[0]); i++) {
        if (chimera_smb_rpc_strcasecmp(name, chimera_smb_rpc_services[i].name) == 0) {
            break;
        }
    }

    if (i == sizeof(chimera_smb_rpc_services) / sizeof(chimera_smb_rpc_services[0])) {
        return NULL;
    }

    state = calloc(1, sizeof(*state));

    if (!state) {
        return NULL;
    }

    state->host = host;
    state->srv  = chimera_smb_rpc_services[i].endpoint();

    if (!state->srv) {
        chimera_smb_rpc_state_close(state);
        return NULL;
    }

    dcerpc_server_set_log(state->srv, DCERPC_SERVER_LOG_INFO, chimera_smb_rpc_log, state);

    state->rpc = dcerpc_server_pipe_create(state->srv, state);

    if (!state->rpc) {
        chimera_smb_rpc_state_close(state);
        return NULL;
    }

    return state;
} /* chimera_smb_rpc_state_open */

void
chimera_smb_rpc_state_close(struct chimera_smb_rpc_state *state)
{
    struct chimera_smb_rpc_handle *h;

    if (!state) {
        return;
    }

    if (state->rpc) {
        dcerpc_server_pipe_destroy(state->rpc);
    }

    if (state->srv) {
        dcerpc_server_destroy(state->srv);
    }

    while ((h = state->handles) != NULL) {
        state->handles = h->next;
        free(h);
    }

    free(state);
} /* chimera_smb_rpc_state_close */

int
chimera_smb_rpc_state_write(
    struct chimera_smb_rpc_state *state,
    const void                   *buf,
    size_t                        len)
{
    return dcerpc_server_pipe_write(state->rpc, buf, len);
} /* chimera_smb_rpc_state_write */

size_t
chimera_smb_rpc_state_read(
    struct chimera_smb_rpc_state *state,
    void                         *buf,
    size_t                        max,
    int                          *more)
{
    return dcerpc_server_pipe_read(state->rpc, buf, max, more);
} /* chimera_smb_rpc_state_read */

int
chimera_smb_rpc_state_has_output(struct chimera_smb_rpc_state *state)
{
    return dcerpc_server_pipe_has_output(state->rpc);
} /* chimera_smb_rpc_state_has_output */

/* ---- helpers for the stubs ---- */

struct chimera_smb_rpc_state *
chimera_smb_rpc_state_of_pdu(struct dcerpc_pdu *pdu)
{
    struct dcerpc_server_call *call = dcerpc_pdu_server_call(pdu);
    struct dcerpc_server_pipe *rpc;

    rpc = call ? dcerpc_server_call_get_pipe(call) : NULL;

    return rpc ? dcerpc_server_pipe_get_private(rpc) : NULL;
} /* chimera_smb_rpc_state_of_pdu */

char *
chimera_smb_rpc_strdup(
    struct dcerpc_pdu *pdu,
    const char        *s)
{
    size_t len;
    char  *d;

    if (!s) {
        return NULL;
    }

    len = strlen(s) + 1;
    d   = dcerpc_alloc_data(pdu, len);

    if (d) {
        memcpy(d, s, len);
    }

    return d;
} /* chimera_smb_rpc_strdup */

static int
chimera_smb_rpc_lower(int c)
{
    return c >= 'A' && c <= 'Z' ? c - 'A' + 'a' : c;
} /* chimera_smb_rpc_lower */

int
chimera_smb_rpc_strncasecmp(
    const char *a,
    const char *b,
    size_t      n)
{
    for (; n; n--, a++, b++) {
        int ca = chimera_smb_rpc_lower((unsigned char) *a);
        int cb = chimera_smb_rpc_lower((unsigned char) *b);

        if (ca != cb || ca == 0) {
            return ca - cb;
        }
    }

    return 0;
} /* chimera_smb_rpc_strncasecmp */

int
chimera_smb_rpc_strcasecmp(
    const char *a,
    const char *b)
{
    return chimera_smb_rpc_strncasecmp(a, b, (size_t) -1);
} /* chimera_smb_rpc_strcasecmp */

int
chimera_smb_rpc_handle_open(
    struct chimera_smb_rpc_state    *state,
    enum chimera_smb_rpc_handle_type type,
    struct dcerpc_context_handle    *handle)
{
    struct chimera_smb_rpc_handle *h;
    uint64_t                       r;

    if (state->num_handles >= CHIMERA_SMB_RPC_MAX_HANDLES_PER_PIPE) {
        return -1;
    }

    h = calloc(1, sizeof(*h));

    if (!h) {
        return -1;
    }

    /* A per-pipe serial, so handles are unique, plus random bytes, so they
     * cannot be guessed. */
    h->type                          = type;
    h->handle.context_handle_uuid.v1 = ++state->next_handle;
    r                                = chimera_smb_rpc_host_random();
    h->handle.context_handle_uuid.v2 = (uint16_t) r;
    h->handle.context_handle_uuid.v3 = (uint16_t) (r >> 16);
    r                                = chimera_smb_rpc_host_random();
    memcpy(h->handle.context_handle_uuid.v4, &r, sizeof(h->handle.context_handle_uuid.v4));

    h->next        = state->handles;
    state->handles = h;
    state->num_handles++;

    *handle = h->handle;
    return 0;
} /* chimera_smb_rpc_handle_open */

static struct chimera_smb_rpc_handle **
chimera_smb_rpc_handle_find(
    struct chimera_smb_rpc_state       *state,
    enum chimera_smb_rpc_handle_type    type,
    const struct dcerpc_context_handle *handle)
{
    struct chimera_smb_rpc_handle **pp;

    if (!state) {
        return NULL;
    }

    for (pp = &state->handles; *pp; pp = &(*pp)->next) {
        if ((*pp)->type == type &&
            memcmp(&(*pp)->handle.context_handle_uuid, &handle->context_handle_uuid,
                   sizeof(handle->context_handle_uuid)) == 0) {
            return pp;
        }
    }

    return NULL;
} /* chimera_smb_rpc_handle_find */

int
chimera_smb_rpc_handle_valid(
    struct chimera_smb_rpc_state       *state,
    enum chimera_smb_rpc_handle_type    type,
    const struct dcerpc_context_handle *handle)
{
    return chimera_smb_rpc_handle_find(state, type, handle) != NULL;
} /* chimera_smb_rpc_handle_valid */

int
chimera_smb_rpc_handle_close(
    struct chimera_smb_rpc_state       *state,
    enum chimera_smb_rpc_handle_type    type,
    const struct dcerpc_context_handle *handle)
{
    struct chimera_smb_rpc_handle **pp = chimera_smb_rpc_handle_find(state, type, handle);
    struct chimera_smb_rpc_handle  *h;

    if (!pp) {
        return 0;
    }

    h   = *pp;
    *pp = h->next;
    free(h);
    state->num_handles--;
    return 1;
} /* chimera_smb_rpc_handle_close */

/* ---- SIDs ---- */

void
chimera_smb_rpc_account_sid(
    RPC_SID                           *sid,
    const struct chimera_smb_rpc_host *host)
{
    uint32_t sub[3];

    chimera_smb_rpc_host_machine_sid(host, sub);

    memset(sid, 0, sizeof(*sid));
    sid->Revision               = 1;
    sid->IdentifierAuthority[5] = 5;
    sid->SubAuthorityCount      = 4;
    sid->SubAuthority[0]        = 21;
    sid->SubAuthority[1]        = sub[0];
    sid->SubAuthority[2]        = sub[1];
    sid->SubAuthority[3]        = sub[2];
} /* chimera_smb_rpc_account_sid */

void
chimera_smb_rpc_builtin_sid(RPC_SID *sid)
{
    memset(sid, 0, sizeof(*sid));
    sid->Revision               = 1;
    sid->IdentifierAuthority[5] = 5;
    sid->SubAuthorityCount      = 1;
    sid->SubAuthority[0]        = 32;
} /* chimera_smb_rpc_builtin_sid */

int
chimera_smb_rpc_sid_to_string(
    const RPC_SID *sid,
    char          *buf,
    size_t         buflen)
{
    uint64_t auth = 0;
    int      n, i;

    for (i = 0; i < 6; i++) {
        auth = (auth << 8) | sid->IdentifierAuthority[i];
    }

    n = snprintf(buf, buflen, "S-%u-%llu", sid->Revision, (unsigned long long) auth);

    for (i = 0; i < sid->SubAuthorityCount && i < MAXSUBAUTH &&
         n > 0 && (size_t) n < buflen; i++) {
        n += snprintf(buf + n, buflen - n, "-%u", sid->SubAuthority[i]);
    }

    return n;
} /* chimera_smb_rpc_sid_to_string */

int
chimera_smb_rpc_string_to_sid(
    const char *str,
    RPC_SID    *sid)
{
    unsigned int       rev;
    unsigned long long auth;
    int                pos = 0, i;
    const char        *p;

    memset(sid, 0, sizeof(*sid));

    if (sscanf(str, "S-%u-%llu%n", &rev, &auth, &pos) < 2) {
        return -1;
    }

    sid->Revision = (uint8_t) rev;

    for (i = 5; i >= 0; i--) {
        sid->IdentifierAuthority[i] = (uint8_t) (auth & 0xff);
        auth                      >>= 8;
    }

    p = str + pos;
    i = 0;

    while (*p == '-' && i < MAXSUBAUTH) {
        unsigned int sa;
        int          adv = 0;

        if (sscanf(p, "-%u%n", &sa, &adv) < 1) {
            break;
        }
        sid->SubAuthority[i++] = sa;
        p                     += adv;
    }

    sid->SubAuthorityCount = (uint8_t) i;
    return 0;
} /* chimera_smb_rpc_string_to_sid */

int
chimera_smb_rpc_sid_equal(
    const RPC_SID *a,
    const RPC_SID *b)
{
    if (a->Revision != b->Revision ||
        a->SubAuthorityCount != b->SubAuthorityCount ||
        a->SubAuthorityCount > MAXSUBAUTH ||
        memcmp(a->IdentifierAuthority, b->IdentifierAuthority, 6) != 0) {
        return 0;
    }

    return memcmp(a->SubAuthority, b->SubAuthority,
                  a->SubAuthorityCount * sizeof(uint32_t)) == 0;
} /* chimera_smb_rpc_sid_equal */

RPC_SID *
chimera_smb_rpc_sid_dup(
    struct dcerpc_pdu *pdu,
    const RPC_SID     *sid)
{
    RPC_SID *d = dcerpc_alloc_data(pdu, sizeof(*d));

    if (d) {
        *d = *sid;
    }

    return d;
} /* chimera_smb_rpc_sid_dup */
