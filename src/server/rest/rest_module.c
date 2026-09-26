// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <ctype.h>
#ifndef _WIN32
#include <dlfcn.h>
#endif /* ifndef _WIN32 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rest_internal.h"
#include "server/server.h"
#include "rest_services.h"

extern const unsigned char openapi_json[];
extern const unsigned int  openapi_json_len;

/* Pattern matching is segment based; a terminal {name...} captures a path.
 * Percent decoding happens only in captures and never creates separators. */
static char *
decode_param(
    const char *input,
    size_t      length)
{
    char  *value = malloc(length + 1);
    size_t i, out = 0;
    int    hi, lo;

    chimera_rest_abort_if(!value, "REST parameter allocation failed");
    for (i = 0; i < length; i++) {
        if (input[i] == '%') {
            if (i + 2 >= length || !isxdigit((unsigned char) input[i + 1]) ||
                !isxdigit((unsigned char) input[i + 2])) {
                free(value);
                return NULL;
            }
            hi = isdigit((unsigned char) input[i + 1]) ? input[i + 1] - '0' :
                tolower((unsigned char) input[i + 1]) - 'a' + 10;
            lo = isdigit((unsigned char) input[i + 2]) ? input[i + 2] - '0' :
                tolower((unsigned char) input[i + 2]) - 'a' + 10;
            value[out] = (hi << 4) | lo;
            if (!value[out] || value[out] == '/' || value[out] == '\\') {
                free(value);
                return NULL;
            }
            i += 2;
            out++;
        } else {
            value[out++] = input[i];
        }
    }
    value[out] = '\0';
    return value;
} /* decode_param */

int
chimera_rest_match(
    const char                  *pattern,
    const char                  *path,
    struct chimera_rest_request *request)
{
    const char  *end, *stop;
    size_t       name_length, length;
    int          tail;
    unsigned int index;

    while (*pattern) {
        if (*pattern != '{') {
            if (*pattern++ != *path++) {
                return 0;
            }
            continue;
        }
        end = strchr(pattern, '}');
        if (!end) {
            return 0;
        }
        name_length = end - pattern - 1;
        tail        = name_length > 3 && !strncmp(end - 3, "...", 3);
        if (tail) {
            name_length -= 3;
        }
        stop   = tail ? NULL : strchr(path, '/');
        length = stop ? (size_t) (stop - path) : strlen(path);
        if (!length) {
            return 0;
        }
        if (request) {
            index                        = request->num_params++;
            request->param_names[index]  = strndup(pattern + 1, name_length);
            request->param_values[index] = decode_param(path, length);
            if (!request->param_values[index]) {
                return 0;
            }
        }
        path   += length;
        pattern = end + 1;
    }
    return !*path;
} /* chimera_rest_match */

static int
valid_name(const char *name)
{
    if (!name || !(*name >= 'a' && *name <= 'z')) {
        return 0;
    }
    for (; *name; name++) {
        if (!(*name >= 'a' && *name <= 'z') && !(*name >= '0' && *name <= '9') &&
            *name != '-' && *name != '_') {
            return 0;
        }
    }
    return 1;
} /* valid_name */

static int
valid_pattern(const char *path)
{
    const char  *p = path, *end;
    unsigned int count = 0;

    if (!p || (p[0] && p[0] != '/') || strlen(p) > 1024) {
        return 0;
    }
    while (*p) {
        if (*p == '{') {
            if (p == path || p[-1] != '/' || ++count > CHIMERA_REST_MAX_PARAMS) {
                return 0;
            }
            end = strchr(p, '}');
            if (!end || end == p + 1 || (end[1] && end[1] != '/')) {
                return 0;
            }
            if (end - p > 3 && !strncmp(end - 3, "...", 3)) {
                if (end[1] || end == p + 4) {
                    return 0;
                }
                end -= 3;
            }
            for (p++; p < end; p++) {
                if (!isalnum((unsigned char) *p) && *p != '_') {
                    return 0;
                }
            }
            p = strchr(p, '}') + 1;
        } else {
            if (*p == '}' || *p == '?' || *p == '#' || *p == '%' ||
                *p == '\\' || (unsigned char) *p <= 32) {
                return 0;
            }
            p++;
        }
    }
    return 1;
} /* valid_pattern */

/* Conservative overlap check: literals disambiguate; parameters do not. */
static int
patterns_overlap(
    const char *a,
    const char *b)
{
    const char *ae, *be;

    while (*a && *b) {
        if (*a == '{' || *b == '{') {
            ae = *a == '{' ? strchr(a, '}') + 1 : strchr(a, '/');
            be = *b == '{' ? strchr(b, '}') + 1 : strchr(b, '/');
            if (!ae) {
                ae = a + strlen(a);
            }
            if (!be) {
                be = b + strlen(b);
            }
            if ((*a == '{' && ae - a > 4 && !strncmp(ae - 4, "...}", 4)) ||
                (*b == '{' && be - b > 4 && !strncmp(be - 4, "...}", 4))) {
                return 1;
            }
            a = ae;
            b = be;
        } else if (*a++ != *b++) {
            return 0;
        }
    }
    return !*a && !*b;
} /* patterns_overlap */

static void
validate_module(
    const struct chimera_rest_module               *module,
    const struct chimera_server_rest_module_config *config)
{
    const struct chimera_rest_route *route, *other;
    uint32_t                         i, j;

    chimera_rest_abort_if(!module || module->abi_version != CHIMERA_REST_ABI_VERSION ||
                          module->struct_size < sizeof(*module),
                          "REST module %s has an incompatible ABI", config->name);
    chimera_rest_abort_if(!module->name || strcmp(module->name, config->name) ||
                          !module->api_version || module->api_version > 9999 ||
                          !module->num_routes || module->num_routes > 1024 || !module->routes,
                          "Invalid REST module descriptor: %s", config->name);
    for (i = 0; i < module->num_routes; i++) {
        route = module->routes[i];
        chimera_rest_abort_if(!route || route->struct_size < sizeof(*route) ||
                              !route->method || !valid_pattern(route->path) || !route->handle ||
                              route->max_body > CHIMERA_REST_MAX_BODY ||
                              (route->flags & ~CHIMERA_REST_PUBLIC),
                              "Invalid route in REST module %s", module->name);
        chimera_rest_abort_if(strcmp(route->method, "GET") && strcmp(route->method, "POST") &&
                              strcmp(route->method, "PUT") && strcmp(route->method, "DELETE") &&
                              strcmp(route->method, "HEAD"), "Unsupported REST method %s", route->method);
        chimera_rest_abort_if((route->flags & CHIMERA_REST_PUBLIC) && !config->allow_public_routes,
                              "REST module %s needs allow_public_routes=true", module->name);
        for (j = 0; j < i; j++) {
            other = module->routes[j];
            chimera_rest_abort_if(!strcmp(route->method, other->method) &&
                                  patterns_overlap(route->path, other->path),
                                  "Overlapping routes in REST module %s: %s and %s",
                                  module->name, route->path, other->path);
        }
    }
} /* validate_module */

static void
build_openapi(struct chimera_rest_server *rest)
{
    json_t                           *base = json_loadb((const char *) openapi_json, openapi_json_len, 0, NULL);
    json_t                           *paths = json_object(), *fragment, *operation, *item, *builtin_paths;
    const struct chimera_rest_module *module;
    const struct chimera_rest_route  *route;
    uint32_t                          i, j;
    char                              path[1200], method[16];
    size_t                            k;

    chimera_rest_abort_if(!base, "Invalid embedded OpenAPI document");
    builtin_paths = json_object_get(base, "paths");
    for (i = 0; i < rest->num_modules; i++) {
        module   = rest->modules[i].module;
        fragment = module->openapi_paths ? json_loads(module->openapi_paths, JSON_REJECT_DUPLICATES, NULL) : NULL;
        chimera_rest_abort_if(module->openapi_paths && !json_is_object(fragment),
                              "Invalid OpenAPI paths in module %s", module->name);
        for (j = 0; j < module->num_routes; j++) {
            route = module->routes[j];
            snprintf(path, sizeof(path), "%s%s", rest->modules[i].prefix, route->path);
            /* OpenAPI path parameters use {name}, including a path capture. */
            char *tail = strstr(path, "...}");
            if (tail) {
                memmove(tail, tail + 3, strlen(tail + 3) + 1);
            }
            snprintf(method, sizeof(method), "%s", route->method);
            for (k = 0; method[k]; k++) {
                method[k] = tolower((unsigned char) method[k]);
            }
            operation = NULL;
            if (fragment) {
                operation = json_object_get(json_object_get(fragment, route->path), method);
            }
            if (!operation) {
                operation = json_object_get(json_object_get(builtin_paths, path), method);
            }
            operation = operation ? json_deep_copy(operation) :
                json_pack("{s:s,s:{s:{s:s}}}", "summary", route->path,
                          "responses", "200", "description", "Successful response");
            chimera_rest_abort_if(!json_is_object(operation), "OpenAPI operation must be an object");
            /* Namespace operation IDs across independently versioned modules. */
            char        operation_id[128];
            snprintf(operation_id, sizeof(operation_id), "%s_v%u_route%u",
                     module->name, module->api_version, j);
            json_object_set_new(operation, "operationId", json_string(operation_id));
            json_t     *parameters = json_object_get(operation, "parameters");
            if (!parameters) {
                parameters = json_array();
                json_object_set_new(operation, "parameters", parameters);
            }
            const char *parameter = path;
            while ((parameter = strchr(parameter, '{'))) {
                const char *end = strchr(++parameter, '}');
                char        name[1025];
                size_t      n;
                json_t     *existing;
                int         found = 0;
                snprintf(name, sizeof(name), "%.*s", (int) (end - parameter), parameter);
                json_array_foreach(parameters, n, existing)
                {
                    const char *key   = json_string_value(json_object_get(existing, "name"));
                    const char *where = json_string_value(json_object_get(existing, "in"));

                    if (key && where && !strcmp(key, name) && !strcmp(where, "path")) {
                        found = 1;
                    }
                }
                if (!found) {
                    json_array_append_new(parameters, json_pack("{s:s,s:s,s:b,s:{s:s}}",
                                                                "name", name, "in", "path", "required", 1, "schema",
                                                                "type", "string"));
                }
                parameter = end + 1;
            }
            json_object_set_new(operation, "security",
                                (route->flags & CHIMERA_REST_PUBLIC) || !rest->auth_enabled ?
                                json_array() : json_pack("[{s:[]},{s:[]}]", "bearerAuth", "basicAuth"));
            item = json_object_get(paths, path);
            if (!item) {
                item = json_object();
                json_object_set_new(paths, path, item);
            }
            json_object_set_new(item, method, operation);
        }
        json_decref(fragment);
    }
    json_object_set_new(base, "paths", paths);
    json_object_set_new(base, "servers", json_pack("[{s:s}]", "url", "/"));
    rest->openapi = json_dumps(base, JSON_COMPACT);
    json_decref(base);
    chimera_rest_abort_if(!rest->openapi, "Cannot serialize OpenAPI document");
} /* build_openapi */

/* Keep native loader handles local to each configured library. */
static void *
module_symbol(
    void       *handle,
    const char *name)
{
#ifdef _WIN32
    return (void *) GetProcAddress((HMODULE) handle, name);
#else  /* ifdef _WIN32 */
    return dlsym(handle, name);
#endif /* ifdef _WIN32 */
} /* module_symbol */

static int
module_path_absolute(const char *path)
{
#ifdef _WIN32
    return (isalpha((unsigned char) path[0]) && path[1] == ':' &&
            (path[2] == '/' || path[2] == '\\')) ||
           (path[0] == '\\' && path[1] == '\\') ||
           (path[0] == '/' && path[1] == '/');
#else  /* ifdef _WIN32 */
    return path[0] == '/';
#endif /* ifdef _WIN32 */
} /* module_path_absolute */

static void
module_directory(
    char  *path,
    size_t size)
{
#ifdef _WIN32
    HMODULE owner;
    wchar_t wide[4096];
    DWORD   length;

    chimera_rest_abort_if(!GetModuleHandleExW(
                              GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                              (LPCWSTR) module_directory, &owner),
                          "Cannot locate REST module owner: %lu", GetLastError());
    length = GetModuleFileNameW(owner, wide, sizeof(wide) / sizeof(wide[0]));
    chimera_rest_abort_if(!length || length >= sizeof(wide) / sizeof(wide[0]) ||
                          !WideCharToMultiByte(CP_UTF8, 0, wide, -1, path, (int) size, NULL, NULL),
                          "Cannot locate REST module directory: %lu", GetLastError());
    /* Use a single separator for the common directory/suffix code. */
    for (char *p = path; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
#else  /* ifdef _WIN32 */
    Dl_info info;

    chimera_rest_abort_if(!dladdr((void *) module_directory, &info),
                          "Cannot locate REST module directory");
    chimera_rest_abort_if(snprintf(path, size, "%s", info.dli_fname) >= (int) size,
                          "REST module directory is too long");
#endif /* ifdef _WIN32 */
    char   *slash = strrchr(path, '/');
    chimera_rest_abort_if(!slash, "REST owner path has no directory");
    slash[1] = '\0';
} /* module_directory */

static void *
module_open(const char *path)
{
#ifdef _WIN32
    wchar_t wide[4096];
    HMODULE handle;

    chimera_rest_abort_if(!MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1,
                                               wide, sizeof(wide) / sizeof(wide[0])),
                          "Invalid REST module path: %s", path);
    handle = LoadLibraryExW(wide, NULL, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);
    chimera_rest_abort_if(!handle, "Cannot load REST module %s: Windows error %lu", path, GetLastError());
    return handle;
#else  /* ifdef _WIN32 */
    void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);

    chimera_rest_abort_if(!handle, "Cannot load REST module %s: %s", path, dlerror());
    return handle;
#endif /* ifdef _WIN32 */
} /* module_open */

static void
module_close(void *handle)
{
#ifdef _WIN32
    FreeLibrary((HMODULE) handle);
#else  /* ifdef _WIN32 */
    dlclose(handle);
#endif /* ifdef _WIN32 */
} /* module_close */

void
chimera_rest_modules_init(
    struct chimera_rest_server         *rest,
    const struct chimera_server_config *config)
{
    const struct chimera_server_rest_module_config *configs;
    struct chimera_rest_loaded_module              *loaded;
    chimera_rest_module_get_fn                      get;
    chimera_rest_bind_fn                            bind;
    char                                            path[4096];
    int                                             count, i, j;

    configs           = chimera_server_config_get_rest_modules(config, &count);
    rest->num_modules = 0;
    rest->modules     = calloc(count ? count : 1, sizeof(*rest->modules));
    chimera_rest_abort_if(!rest->modules, "REST module allocation failed");
    for (i = 0; i < count; i++) {
        chimera_rest_abort_if(!valid_name(configs[i].name), "Invalid REST module name: %s", configs[i].name);
        loaded = &rest->modules[i];
        if (configs[i].module_path[0]) {
            chimera_rest_abort_if(!module_path_absolute(configs[i].module_path), "REST module path must be absolute");
            snprintf(path, sizeof(path), "%s", configs[i].module_path);
        } else {
            module_directory(path, sizeof(path));
            size_t length = strlen(path);
            chimera_rest_abort_if(snprintf(path + length, sizeof(path) - length,
                                           "chimera_rest_%s%s", configs[i].name, CHIMERA_REST_MODULE_SUFFIX) >=
                                  (int) (sizeof(path) - length), "REST module path is too long");
        }
        loaded->handle = module_open(path);
        get            = (chimera_rest_module_get_fn) module_symbol(loaded->handle, CHIMERA_REST_MODULE_ENTRY);
        chimera_rest_abort_if(!get, "REST module %s has no %s", path, CHIMERA_REST_MODULE_ENTRY);
        loaded->module = get();
        validate_module(loaded->module, &configs[i]);
        bind = (chimera_rest_bind_fn) module_symbol(loaded->handle, CHIMERA_REST_BIND_ENTRY);
        chimera_rest_abort_if(bind && bind(&chimera_rest_host_services),
                              "REST module %s has incompatible private host services", path);
        for (j = 0; j < i; j++) {
            chimera_rest_abort_if(!strcmp(loaded->module->name, rest->modules[j].module->name) &&
                                  loaded->module->api_version == rest->modules[j].module->api_version,
                                  "Duplicate REST module/version: %s", loaded->module->name);
        }
        snprintf(loaded->prefix, sizeof(loaded->prefix), "/api/%s/v%u",
                 loaded->module->name, loaded->module->api_version);
        if (loaded->module->init) {
            chimera_rest_abort_if(loaded->module->init(&chimera_rest_host, configs[i].config_json,
                                                       &loaded->state),
                                  "REST module %s initialization failed", loaded->module->name);
        }
        rest->num_modules++;
        chimera_rest_info("Loaded REST module %s at %s", path, loaded->prefix);
    }
    build_openapi(rest);
} /* chimera_rest_modules_init */

void
chimera_rest_modules_destroy(struct chimera_rest_server *rest)
{
    struct chimera_rest_loaded_module *loaded;

    while (rest->num_modules) {
        loaded = &rest->modules[--rest->num_modules];
        if (loaded->module->destroy) {
            loaded->module->destroy(loaded->state);
        }
        module_close(loaded->handle);
    }
    free(rest->modules);
    free(rest->openapi);
} /* chimera_rest_modules_destroy */

void
chimera_rest_modules_thread_init(struct chimera_rest_thread *thread)
{
    struct chimera_rest_context        context = { .thread = thread };
    struct chimera_rest_loaded_module *loaded;
    uint32_t                           i;

    thread->module_state = calloc(thread->shared->num_modules + 1, sizeof(void *));
    chimera_rest_abort_if(!thread->module_state, "REST thread allocation failed");
    for (i = 0; i < thread->shared->num_modules; i++) {
        loaded                  = &thread->shared->modules[i];
        thread->module_state[i] = loaded->state;
        if (loaded->module->thread_init) {
            chimera_rest_abort_if(loaded->module->thread_init(loaded->state, &context,
                                                              &thread->module_state[i]),
                                  "REST module %s thread initialization failed", loaded->module->name);
        }
    }
} /* chimera_rest_modules_thread_init */

void
chimera_rest_modules_thread_quiesce(struct chimera_rest_thread *thread)
{
    uint32_t i;

    thread->stopping = 1;
    for (i = 0; i < thread->shared->num_modules; i++) {
        if (thread->shared->modules[i].module->thread_quiesce) {
            thread->shared->modules[i].module->thread_quiesce(thread->module_state[i]);
        }
    }
} /* chimera_rest_modules_thread_quiesce */

void
chimera_rest_modules_thread_destroy(struct chimera_rest_thread *thread)
{
    uint32_t i = thread->shared->num_modules;

    while (i--) {
        if (thread->shared->modules[i].module->thread_destroy) {
            thread->shared->modules[i].module->thread_destroy(thread->module_state[i]);
        }
    }
    free(thread->module_state);
} /* chimera_rest_modules_thread_destroy */
