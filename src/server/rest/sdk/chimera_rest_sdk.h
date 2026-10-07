// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stddef.h>
#include <stdint.h>

/* This header is the complete external module interface. It deliberately
 * contains no implementation or dependencies on Chimera's private layouts.
 * See docs/rest-module-sdk.md for ownership, threading and distribution rules. */
#define CHIMERA_REST_ABI_VERSION  1
#define CHIMERA_REST_MODULE_ENTRY "chimera_rest_module_get_v1"
#define CHIMERA_REST_PUBLIC       1U
#define CHIMERA_REST_MAX_BODY     65536U

#if defined(_WIN32)
#define CHIMERA_REST_EXPORT       __declspec(dllexport)
#elif defined(__GNUC__)
#define CHIMERA_REST_EXPORT       __attribute__((visibility("default")))
#else // if defined(__GNUC__)
#define CHIMERA_REST_EXPORT
#endif // if defined(__GNUC__)

#ifdef __cplusplus
extern "C" {
#endif // ifdef __cplusplus

struct chimera_rest_request;
struct chimera_rest_context;

struct chimera_rest_host {
    uint32_t     abi_version;
    uint32_t     struct_size;
    const char * (*method)(
        const struct chimera_rest_request *request);
    const char * (*path)(
        const struct chimera_rest_request *request);
    const char * (*param)(
        const struct chimera_rest_request *request,
        const char                        *name);
    const char * (*query)(
        const struct chimera_rest_request *request);
    const char * (*header)(
        const struct chimera_rest_request *request,
        const char                        *name);
    const char * (*subject)(
        const struct chimera_rest_request *request);
    const void * (*body)(
        const struct chimera_rest_request *request,
        size_t                            *length);
    int32_t      (*cancelled)(
        const struct chimera_rest_request *request);
    /* Exactly one reply consumes the handler's request ownership, even after
     * cancellation. Callable from any thread; copies all supplied bytes.
     * No request access is permitted after reply returns. */
    void         (*reply)(
        struct chimera_rest_request *request,
        uint32_t                     status,
        const char                  *content_type,
        const void                  *body,
        size_t                       length);
};

struct chimera_rest_route {
    uint32_t    struct_size;
    uint32_t    flags;
    const char *method;
    /* Relative path: literal segments, {name}, or terminal {path...}.
     * The host prepends /api/<module-name>/v<api_version>. */
    const char *path;
    uint32_t    max_body;
    void        (*handle)(
        struct chimera_rest_request *request,
        void                        *thread_state);
};

struct chimera_rest_module {
    uint32_t                                abi_version;
    uint32_t                                struct_size;
    const char                             *name;
    uint32_t                                api_version;
    const struct chimera_rest_route *const *routes;
    uint32_t                                num_routes;
    /* Optional OpenAPI paths object, using relative route paths. */
    const char                             *openapi_paths;
    int32_t                                 (*init)(
        const struct chimera_rest_host *host,
        const char                     *config_json,
        void                          **state);
    int32_t                                 (*thread_init)(
        void                        *state,
        struct chimera_rest_context *context,
        void                       **thread_state);
    /* Signal background producers to stop. Called on each owning loop before
     * draining requests. Must not block on completion callbacks. */
    void                                    (*thread_quiesce)(
        void *thread_state);
    void                                    (*thread_destroy)(
        void *thread_state);
    void                                    (*destroy)(
        void *state);
};

typedef const struct chimera_rest_module * (*chimera_rest_module_get_fn)(
    void);

#ifdef __cplusplus
}
#endif // ifdef __cplusplus
