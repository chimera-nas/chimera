// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include <curl/curl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "common/thread.h"

/* The same loopback requests on every host, without invoking a shell. */
static evpl_once_t chimera_test_curl_once = EVPL_ONCE_INIT;
static void chimera_test_curl_init(void)
{
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK || atexit(curl_global_cleanup)) {
        abort();
    }
}
struct chimera_test_http_response {
    char *buffer;
    size_t capacity, length;
};
static size_t chimera_test_http_write(char *data, size_t size, size_t count, void *context)
{
    struct chimera_test_http_response *response = context;
    size_t length = size * count;
    if (!response->buffer) { return length; }
    if (length >= response->capacity - response->length) { return 0; }
    memcpy(response->buffer + response->length, data, length);
    response->length += length;
    response->buffer[response->length] = 0;
    return length;
}
static inline int
chimera_test_http(int port, const char *method, const char *path, const char *body,
                  const char *bearer, const char *basic, char *output, size_t capacity,
                  long *status)
{
    CURL *curl;
    struct curl_slist *headers = NULL, *appended;
    struct chimera_test_http_response response = { output, capacity, 0 };
    char url[2048], *authorization = NULL;
    int result = -1, length;
    if (output && !capacity) { return -1; }
    if (output) { output[0] = 0; }
    *status = 0;
    length = snprintf(url, sizeof(url), "http://127.0.0.1:%d%s", port, path);
    if (length < 0 || (size_t) length >= sizeof(url)) { return -1; }
    evpl_once(&chimera_test_curl_once, chimera_test_curl_init);
    curl = curl_easy_init();
    if (!curl) { return -1; }
    headers = curl_slist_append(NULL, "Content-Type: application/json");
    if (!headers) { goto done; }
    if (bearer && *bearer) {
        size_t bytes = strlen(bearer) + sizeof("Authorization: Bearer ");
        authorization = malloc(bytes);
        if (!authorization) { goto done; }
        snprintf(authorization, bytes, "Authorization: Bearer %s", bearer);
        appended = curl_slist_append(headers, authorization);
        if (!appended) { goto done; }
        headers = appended;
    }
#define HTTP_OPTION(option, value) do { if (curl_easy_setopt(curl, option, value) != CURLE_OK) { goto done; } } while (0)
    HTTP_OPTION(CURLOPT_URL, url);
    HTTP_OPTION(CURLOPT_NOSIGNAL, 1L);
    HTTP_OPTION(CURLOPT_NOPROXY, "*");
    HTTP_OPTION(CURLOPT_TIMEOUT_MS, 10000L);
    HTTP_OPTION(CURLOPT_CONNECTTIMEOUT_MS, 2000L);
    HTTP_OPTION(CURLOPT_HTTP_VERSION, (long) CURL_HTTP_VERSION_1_1);
    HTTP_OPTION(CURLOPT_HTTPHEADER, headers);
    HTTP_OPTION(CURLOPT_WRITEFUNCTION, chimera_test_http_write);
    HTTP_OPTION(CURLOPT_WRITEDATA, &response);
    if (body) {
        HTTP_OPTION(CURLOPT_POSTFIELDS, body);
        HTTP_OPTION(CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t) strlen(body));
    }
    if (basic) {
        HTTP_OPTION(CURLOPT_HTTPAUTH, (long) CURLAUTH_BASIC);
        HTTP_OPTION(CURLOPT_USERPWD, basic);
    }
    HTTP_OPTION(CURLOPT_CUSTOMREQUEST, method);
#undef HTTP_OPTION
    if (curl_easy_perform(curl) == CURLE_OK &&
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, status) == CURLE_OK) {
        result = 0;
    }
done:
    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);
    free(authorization);
    return result;
}
