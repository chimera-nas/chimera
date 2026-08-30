/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * An HTTP client for the in-process test harnesses.
 *
 * Chimera's REST API and its Prometheus scrape endpoint are HTTP servers, and
 * both follow the server's configured transport flavor, so under
 * CHIMERA_TCP_FLAVOR_INPROC they can be reached from the same process with no
 * port bound -- exactly as the NFS harness reaches nfsd over inproc rpc2 and
 * the SMB2 harness reaches smbd over an inproc stream.  A "port" is then only
 * the name of an inproc endpoint ("chimera-inproc-<port>"); see
 * common/tcp_flavor.h.
 *
 * This header is deliberately server-agnostic: it borrows a caller-owned evpl
 * loop and http agent rather than creating any.  A control-plane-only probe
 * gets both from ctl_mbt_common.h; a probe that also drives NFS, SMB2 or S3
 * attaches an agent to THAT harness's loop instead, so one process can issue
 * an administrative change and watch its protocol-visible consequence without
 * two event loops or two servers.
 *
 * Requests are synchronous: ctl_http() dispatches one and pumps the loop until
 * the response callback lands, which is the same shape as the NFS harness's
 * per-RPC wrappers.
 */

#ifndef CTL_HTTP_H
#define CTL_HTTP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#include "common/tcp_flavor.h"

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"

/* Endpoint "ports".  Under inproc these are names, not bindable ports, so
 * they need not be free on the host -- but they must be the names the server
 * derived from its own configuration, so each is the value handed to the
 * corresponding chimera_server_config_set_*_port(). */
#define CTL_REST_PORT    8080
#define CTL_METRICS_PORT 9000

/* Response bodies: the largest is GET /metrics, which carries the whole
 * Prometheus exposition (chimera_* plus evpl_*).  The endpoint itself
 * scrapes into a 2 MiB buffer, so match it and refuse to truncate silently. */
#define CTL_BODY_MAX     (2 << 20)

/* How long a single in-process request may take before the harness calls it
 * wedged rather than slow.  Generous next to the microseconds these actually
 * take, and well inside the ctest timeout so the failure names the request
 * instead of arriving as an anonymous kill. */
#define CTL_HANG_MS      20000

static inline uint64_t
ctl_now_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000 + (uint64_t) ts.tv_nsec / 1000000;
} /* ctl_now_ms */

/* evpl_http_request_get_datav takes a BYTE count and fills as many iovecs as
 * that spans, so a request larger than the array can overrun it.  Clamp each
 * call to the array size: an iovec is at least one byte, so that bound cannot
 * be exceeded.  (rest.c's own POST body reader clamps the same way.) */
#define CTL_MAX_IOV 256

/* One HTTP exchange's result. */
struct ctl_res {
    int   done;
    int   status;              /* HTTP status, or EVPL_HTTP_ERROR_* (< 0) */
    int   overflow;            /* body exceeded CTL_BODY_MAX */
    int   body_len;
    char *body;                /* NUL-terminated; points at ctl_conn::buf */
};

/* An HTTP connection to one of the in-process endpoints.  Kept separate from
 * the env so a test can hold several at once -- the REST API and the metrics
 * endpoint are different servers, and a test that wants to prove a
 * connection-scoped property needs more than one to the same server. */
struct ctl_conn {
    struct evpl            *evpl;
    struct evpl_http_agent *agent;
    struct evpl_http_conn  *conn;
    int                     port;
    char                   *buf;  /* CTL_BODY_MAX + 1, reused per request */
    /* Bearer token sent on every request when non-empty (REST auth). */
    char                    token[1024];
};

/* ---- HTTP client ------------------------------------------------------- */

static inline void
ctl_drain(
    struct evpl              *evpl,
    struct evpl_http_request *request,
    struct ctl_res           *res)
{
    struct evpl_iovec iov[CTL_MAX_IOV];
    uint64_t          avail;
    int               niov, i, want;

    avail = evpl_http_request_get_data_avail(request);

    while (avail > 0) {
        want = avail > CTL_MAX_IOV ? CTL_MAX_IOV : (int) avail;
        niov = evpl_http_request_get_datav(evpl, request, iov, want);

        if (niov <= 0) {
            break;
        }

        for (i = 0; i < niov; i++) {
            int len = iov[i].length;

            if (res->body_len + len > CTL_BODY_MAX) {
                len           = CTL_BODY_MAX - res->body_len;
                res->overflow = 1;
            }
            if (len > 0) {
                memcpy(res->body + res->body_len, iov[i].data, len);
                res->body_len += len;
            }
            evpl_iovec_release(evpl, &iov[i]);
        }

        avail = evpl_http_request_get_data_avail(request);
    }
} /* ctl_drain */

static inline void
ctl_client_notify(
    struct evpl                *evpl,
    struct evpl_http_agent     *agent,
    struct evpl_http_request   *request,
    enum evpl_http_notify_type  notify_type,
    enum evpl_http_request_type request_type,
    const char                 *uri,
    void                       *notify_data,
    void                       *private_data)
{
    struct ctl_res *res = notify_data;

    switch (notify_type) {
        case EVPL_HTTP_NOTIFY_RESPONSE_HEADERS:
            res->status = evpl_http_request_status(request);
            break;
        case EVPL_HTTP_NOTIFY_RECEIVE_DATA:
            ctl_drain(evpl, request, res);
            break;
        case EVPL_HTTP_NOTIFY_RECEIVE_COMPLETE:
            ctl_drain(evpl, request, res);
            res->done = 1;
            break;
        /* A request the transport abandoned.  Reported rather than fatal:
         * dropping the connection under an in-flight request is a thing the
         * control plane suite deliberately does. */
        case EVPL_HTTP_NOTIFY_FAILED:
            res->status = evpl_http_request_status(request);
            res->done   = 1;
            break;
        default:
            break;
    } /* switch */
} /* ctl_client_notify */

/*
 * Issue one request and wait for its response.
 *
 * `body` is an optional NUL-terminated request body (JSON, for the POST
 * endpoints); a NULL body sends Content-Length: 0.  The response body is
 * NUL-terminated in res->body, which points into the connection's own buffer
 * and stays valid until the next request on that connection.
 */
static inline void
ctl_http(
    struct ctl_conn            *c,
    enum evpl_http_request_type method,
    const char                 *url,
    const char                 *body,
    struct ctl_res             *res)
{
    struct evpl_http_request *request;
    struct evpl_iovec         iov;
    struct evpl              *evpl    = c->evpl;
    size_t                    bodylen = body ? strlen(body) : 0;

    memset(res, 0, sizeof(*res));
    res->body = c->buf;

    request = evpl_http_request_create(c->conn, method, url);

    if (!request) {
        fprintf(stderr, "ctl: request line too long for %s\n", url);
        exit(1);
    }

    evpl_http_request_add_header(request, "Host", "localhost");

    if (body) {
        evpl_http_request_add_header(request, "Content-Type",
                                     "application/json");
    }

    if (c->token[0]) {
        char hdr[1100];

        snprintf(hdr, sizeof(hdr), "Bearer %s", c->token);
        evpl_http_request_add_header(request, "Authorization", hdr);
    }

    evpl_http_client_set_request_length(request, bodylen);

    if (bodylen) {
        evpl_iovec_alloc(evpl, bodylen, 0, 1, 0, &iov);
        memcpy(evpl_iovec_data(&iov), body, bodylen);
        evpl_iovec_set_length(&iov, bodylen);
        evpl_http_request_add_datav(request, &iov, 1);
    }

    evpl_http_request_dispatch(request, ctl_client_notify, res);

    /* Bounded, like the SMB harness's SMB2C_HANG_MS: a response that never
     * lands is a finding, and without a deadline it surfaces as a bare ctest
     * timeout that says nothing about which request wedged.  The budget is a
     * wedge guard, not a latency knob -- every request here is answered by a
     * server in this same process, so any of them taking seconds is already
     * the bug. */
    {
        uint64_t deadline = ctl_now_ms() + CTL_HANG_MS;

        while (!res->done) {
            evpl_continue(c->evpl);
            if (ctl_now_ms() >= deadline) {
                fprintf(stderr,
                        "ctl: WEDGED waiting for %s %s after %d ms -- "
                        "no response from the server in this process\n",
                        method == EVPL_HTTP_REQUEST_TYPE_GET ? "GET"
                        : method == EVPL_HTTP_REQUEST_TYPE_POST ? "POST"
                        : method == EVPL_HTTP_REQUEST_TYPE_PUT ? "PUT"
                        : "DELETE", url, CTL_HANG_MS);
                exit(4);
            }
        }
    }

    res->body[res->body_len] = '\0';

    if (res->overflow) {
        fprintf(stderr, "ctl: response body for %s exceeded %d bytes\n",
                url, CTL_BODY_MAX);
        exit(1);
    }
} /* ctl_http */

static inline void
ctl_get(
    struct ctl_conn *c,
    const char      *url,
    struct ctl_res  *res)
{
    ctl_http(c, EVPL_HTTP_REQUEST_TYPE_GET, url, NULL, res);
} /* ctl_get */

static inline void
ctl_post(
    struct ctl_conn *c,
    const char      *url,
    const char      *body,
    struct ctl_res  *res)
{
    ctl_http(c, EVPL_HTTP_REQUEST_TYPE_POST, url, body, res);
} /* ctl_post */

static inline void
ctl_delete(
    struct ctl_conn *c,
    const char      *url,
    struct ctl_res  *res)
{
    ctl_http(c, EVPL_HTTP_REQUEST_TYPE_DELETE, url, NULL, res);
} /* ctl_delete */

/* ---- connection lifecycle ---------------------------------------------- */

static inline struct ctl_conn *
ctl_conn_open(
    struct evpl            *evpl,
    struct evpl_http_agent *agent,
    int                     port)
{
    struct ctl_conn      *c = calloc(1, sizeof(*c));
    struct evpl_endpoint *ep;

    c->evpl  = evpl;
    c->agent = agent;
    c->port  = port;
    c->buf   = malloc(CTL_BODY_MAX + 1);

    ep = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                            "127.0.0.1", port);

    /* HTTP/1.1 explicitly: inproc carries no ALPN, so AUTO would settle on
     * HTTP/1.1 anyway -- saying so keeps the corpus comparable if that
     * default ever changes. */
    c->conn = evpl_http_client_connect(agent, EVPL_STREAM_INPROC, ep,
                                       EVPL_HTTP_VERSION_HTTP1, c);

    if (!c->conn) {
        fprintf(stderr, "ctl: failed to connect to in-process endpoint %d\n",
                port);
        exit(1);
    }

    return c;
} /* ctl_conn_open */

static inline void
ctl_conn_close(struct ctl_conn *c)
{
    evpl_http_client_close(c->agent, c->conn);
    free(c->buf);
    free(c);
} /* ctl_conn_close */


#endif /* CTL_HTTP_H */
