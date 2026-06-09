// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * sqlite: a persistent, key-value-only VFS backend (CHIMERA_VFS_CAP_KV, no
 * filesystem).  Keys and values are stored as opaque BLOBs in a single table
 * with the key as the primary key:
 *
 *     CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;
 *
 * The database runs in WAL journal mode so multiple reader threads and a writer
 * can operate concurrently.  Each VFS thread owns its own sqlite3 connection
 * (sqlite connections are not safe to share across threads); the module is
 * declared CAP_BLOCKING so the synchronous sqlite calls run on delegation
 * threads rather than the event-loop core threads.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <jansson.h>

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "sqlite.h"
#include "common/logging.h"
#include "common/macros.h"

#define CHIMERA_SQLITE_BUSY_TIMEOUT_MS 5000

#define chimera_sqlite_error(...) chimera_error("sqlite", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)
#define chimera_sqlite_abort_if(cond, ...) \
        chimera_abort_if(cond, "sqlite", __FILE__, __LINE__, __VA_ARGS__)

struct sqlite_shared {
    char *path;
};

struct sqlite_thread {
    struct sqlite_shared *shared;
    sqlite3              *db;
    sqlite3_stmt         *put_stmt;
    sqlite3_stmt         *get_stmt;
    sqlite3_stmt         *del_stmt;
};

static void *
sqlite_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
    struct sqlite_shared *shared = calloc(1, sizeof(*shared));
    json_error_t          json_error;
    json_t               *cfg;
    const char           *path;
    sqlite3              *db;
    char                 *errmsg = NULL;
    int                   rc;

    chimera_sqlite_abort_if(!cfgdata || cfgdata[0] == '\0',
                            "sqlite: config required ('path' missing)");

    cfg = json_loads(cfgdata, 0, &json_error);
    chimera_sqlite_abort_if(!cfg, "sqlite: failed to parse config: %s",
                            json_error.text);

    path = json_string_value(json_object_get(cfg, "path"));
    chimera_sqlite_abort_if(!path, "sqlite: 'path' missing in config");

    shared->path = strdup(path);

    /* Open once at init to create the schema and switch the database file into
     * WAL mode (WAL is a persistent property of the file, so per-thread
     * connections opened later inherit it). */
    rc = sqlite3_open(shared->path, &db);
    chimera_sqlite_abort_if(rc != SQLITE_OK, "sqlite: cannot open '%s': %s",
                            shared->path, sqlite3_errmsg(db));

    rc = sqlite3_exec(db,
                      "PRAGMA journal_mode=WAL;"
                      "PRAGMA synchronous=NORMAL;"
                      "CREATE TABLE IF NOT EXISTS kv ("
                      "  key   BLOB PRIMARY KEY,"
                      "  value BLOB NOT NULL"
                      ") WITHOUT ROWID;",
                      NULL, NULL, &errmsg);
    chimera_sqlite_abort_if(rc != SQLITE_OK, "sqlite: schema init failed: %s",
                            errmsg ? errmsg : "(unknown)");
    sqlite3_free(errmsg);

    sqlite3_close(db);

    json_decref(cfg);

    return shared;
} /* sqlite_init */

static void
sqlite_destroy(void *private_data)
{
    struct sqlite_shared *shared = private_data;

    free(shared->path);
    free(shared);
} /* sqlite_destroy */

static void *
sqlite_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct sqlite_shared *shared = private_data;
    struct sqlite_thread *thread = calloc(1, sizeof(*thread));
    int                   rc;

    (void) evpl;
    thread->shared = shared;

    rc = sqlite3_open(shared->path, &thread->db);
    chimera_sqlite_abort_if(rc != SQLITE_OK, "sqlite: thread open '%s': %s",
                            shared->path, sqlite3_errmsg(thread->db));

    sqlite3_busy_timeout(thread->db, CHIMERA_SQLITE_BUSY_TIMEOUT_MS);
    sqlite3_exec(thread->db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);

    rc = sqlite3_prepare_v2(thread->db,
                            "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)",
                            -1, &thread->put_stmt, NULL);
    chimera_sqlite_abort_if(rc != SQLITE_OK, "sqlite: prepare put: %s",
                            sqlite3_errmsg(thread->db));

    rc = sqlite3_prepare_v2(thread->db,
                            "SELECT value FROM kv WHERE key=?",
                            -1, &thread->get_stmt, NULL);
    chimera_sqlite_abort_if(rc != SQLITE_OK, "sqlite: prepare get: %s",
                            sqlite3_errmsg(thread->db));

    rc = sqlite3_prepare_v2(thread->db,
                            "DELETE FROM kv WHERE key=?",
                            -1, &thread->del_stmt, NULL);
    chimera_sqlite_abort_if(rc != SQLITE_OK, "sqlite: prepare delete: %s",
                            sqlite3_errmsg(thread->db));

    return thread;
} /* sqlite_thread_init */

static void
sqlite_thread_destroy(void *private_data)
{
    struct sqlite_thread *thread = private_data;

    sqlite3_finalize(thread->put_stmt);
    sqlite3_finalize(thread->get_stmt);
    sqlite3_finalize(thread->del_stmt);
    sqlite3_close(thread->db);
    free(thread);
} /* sqlite_thread_destroy */

static void
sqlite_put_key(
    struct sqlite_thread       *thread,
    struct chimera_vfs_request *request)
{
    sqlite3_stmt *stmt = thread->put_stmt;
    int           rc;

    sqlite3_bind_blob(stmt, 1, request->put_key.key, request->put_key.key_len, SQLITE_STATIC);
    sqlite3_bind_blob(stmt, 2, request->put_key.value, request->put_key.value_len, SQLITE_STATIC);

    rc = sqlite3_step(stmt);

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    if (rc != SQLITE_DONE) {
        chimera_sqlite_error("put_key step failed: %s", sqlite3_errmsg(thread->db));
        request->status = CHIMERA_VFS_EIO;
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* sqlite_put_key */

static void
sqlite_get_key(
    struct sqlite_thread       *thread,
    struct chimera_vfs_request *request)
{
    sqlite3_stmt *stmt = thread->get_stmt;
    int           rc;

    sqlite3_bind_blob(stmt, 1, request->get_key.key, request->get_key.key_len, SQLITE_STATIC);

    rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        const void *value     = sqlite3_column_blob(stmt, 0);
        int         value_len = sqlite3_column_bytes(stmt, 0);

        /* The column blob pointer is only valid until the next step/reset.  Copy
         * it into the request-owned scratch so it survives the reset below and
         * remains valid for the caller's callback, which (because this op is
         * CAP_BLOCKING) runs later on the originating thread after the
         * completion is bounced back from the delegation thread. */
        if (value_len > CHIMERA_VFS_PLUGIN_DATA_SIZE) {
            chimera_sqlite_error("get_key value too large: %d > %d",
                                 value_len, CHIMERA_VFS_PLUGIN_DATA_SIZE);
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
            request->status = CHIMERA_VFS_EIO;
            request->complete(request);
            return;
        }

        memcpy(request->plugin_data, value, value_len);
        request->get_key.r_value     = request->plugin_data;
        request->get_key.r_value_len = value_len;

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    if (rc == SQLITE_DONE) {
        request->status = CHIMERA_VFS_ENOENT;
    } else {
        chimera_sqlite_error("get_key step failed: %s", sqlite3_errmsg(thread->db));
        request->status = CHIMERA_VFS_EIO;
    }

    request->complete(request);
} /* sqlite_get_key */

static void
sqlite_delete_key(
    struct sqlite_thread       *thread,
    struct chimera_vfs_request *request)
{
    sqlite3_stmt *stmt = thread->del_stmt;
    int           rc, changes;

    sqlite3_bind_blob(stmt, 1, request->delete_key.key, request->delete_key.key_len, SQLITE_STATIC);

    rc      = sqlite3_step(stmt);
    changes = sqlite3_changes(thread->db);

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    if (rc != SQLITE_DONE) {
        chimera_sqlite_error("delete_key step failed: %s", sqlite3_errmsg(thread->db));
        request->status = CHIMERA_VFS_EIO;
    } else if (changes == 0) {
        request->status = CHIMERA_VFS_ENOENT;
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* sqlite_delete_key */

static void
sqlite_search_keys(
    struct sqlite_thread       *thread,
    struct chimera_vfs_request *request)
{
    chimera_vfs_search_keys_callback_t callback  = request->search_keys.callback;
    const void                        *start     = request->search_keys.start_key;
    uint32_t                           start_len = request->search_keys.start_key_len;
    const void                        *end       = request->search_keys.end_key;
    uint32_t                           end_len   = request->search_keys.end_key_len;
    sqlite3_stmt                      *stmt      = NULL;
    const char                        *sql;
    int                                bind_idx = 1;
    int                                rc, aborted = 0;

    /* BLOB comparison in sqlite is bytewise (memcmp with the shorter blob
     * ordering first), matching the range semantics used by the other KV
     * backends.  Both bounds are inclusive. */
    if (start_len && end_len) {
        sql = "SELECT key,value FROM kv WHERE key>=? AND key<=? ORDER BY key";
    } else if (start_len) {
        sql = "SELECT key,value FROM kv WHERE key>=? ORDER BY key";
    } else if (end_len) {
        sql = "SELECT key,value FROM kv WHERE key<=? ORDER BY key";
    } else {
        sql = "SELECT key,value FROM kv ORDER BY key";
    }

    rc = sqlite3_prepare_v2(thread->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        chimera_sqlite_error("search_keys prepare failed: %s", sqlite3_errmsg(thread->db));
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    if (start_len) {
        sqlite3_bind_blob(stmt, bind_idx++, start, start_len, SQLITE_STATIC);
    }
    if (end_len) {
        sqlite3_bind_blob(stmt, bind_idx++, end, end_len, SQLITE_STATIC);
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const void *key       = sqlite3_column_blob(stmt, 0);
        int         key_len   = sqlite3_column_bytes(stmt, 0);
        const void *value     = sqlite3_column_blob(stmt, 1);
        int         value_len = sqlite3_column_bytes(stmt, 1);

        if (callback(key, key_len, value, value_len, request->proto_private_data)) {
            aborted = 1;
            break;
        }
    }

    sqlite3_finalize(stmt);

    if (!aborted && rc != SQLITE_DONE && rc != SQLITE_ROW) {
        chimera_sqlite_error("search_keys step failed: %s", sqlite3_errmsg(thread->db));
        request->status = CHIMERA_VFS_EIO;
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* sqlite_search_keys */

static void
sqlite_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct sqlite_thread *thread = private_data;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_PUT_KEY:
            sqlite_put_key(thread, request);
            break;
        case CHIMERA_VFS_OP_GET_KEY:
            sqlite_get_key(thread, request);
            break;
        case CHIMERA_VFS_OP_DELETE_KEY:
            sqlite_delete_key(thread, request);
            break;
        case CHIMERA_VFS_OP_SEARCH_KEYS:
            sqlite_search_keys(thread, request);
            break;
        default:
            chimera_sqlite_error("sqlite_dispatch: unsupported operation %d",
                                 request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* sqlite_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_sqlite = {
    .name           = "sqlite",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_SQLITE,
    .capabilities   = CHIMERA_VFS_CAP_KV | CHIMERA_VFS_CAP_BLOCKING,
    .init           = sqlite_init,
    .destroy        = sqlite_destroy,
    .thread_init    = sqlite_thread_init,
    .thread_destroy = sqlite_thread_destroy,
    .dispatch       = sqlite_dispatch,
};
