// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

/*
 * Take a reference on the open a file has on the server, recording it if this
 * is the first handle to open the file.
 *
 * `fh` is a chimera file handle; the table is keyed on the wire handle inside
 * it, which is what actually names the file to the server.  Keying on the
 * chimera handle would split a file reached through two mounts of the same
 * server into two entries, and those two share one open on the wire -- exactly
 * the aliasing this table exists to count.
 *
 * `stateid` is the one OPEN just returned, or NULL from a caller that has none
 * (chimera_nfs4_open_fh, which cannot OPEN by handle without CLAIM_FH).  When a
 * caller does supply one it wins: an OPEN of an already-open file is an upgrade
 * that bumps the seqid, so the newest stateid is the current one.
 */
void
chimera_nfs4_open_file_get(
    struct chimera_nfs_client_server *server,
    const uint8_t                    *fh,
    int                               fh_len,
    const struct stateid4            *stateid)
{
    struct chimera_nfs4_open_file *file;
    uint8_t                       *wire_fh;
    int                            wire_fh_len;

    if (!server) {
        return;
    }

    chimera_nfs4_map_fh(fh, fh_len, &wire_fh, &wire_fh_len);

    if (wire_fh_len <= 0 || wire_fh_len > CHIMERA_VFS_FH_SIZE) {
        return;
    }

    pthread_mutex_lock(&server->open_state_lock);

    HASH_FIND(hh, server->open_files, wire_fh, wire_fh_len, file);

    if (file) {
        file->refcnt++;

        if (stateid) {
            file->stateid = *stateid;
        }

        pthread_mutex_unlock(&server->open_state_lock);
        return;
    }

    file = calloc(1, sizeof(*file));

    if (!file) {
        pthread_mutex_unlock(&server->open_state_lock);
        return;
    }

    file->refcnt = 1;
    file->fh_len = wire_fh_len;
    memcpy(file->fh, wire_fh, wire_fh_len);

    if (stateid) {
        file->stateid = *stateid;
    }

    HASH_ADD_KEYPTR(hh, server->open_files, file->fh, file->fh_len, file);

    pthread_mutex_unlock(&server->open_state_lock);
} /* chimera_nfs4_open_file_get */

/*
 * Drop a handle's reference.  Returns non-zero when it was the last one, having
 * retired the entry and copied its stateid to *r_stateid for the caller to
 * CLOSE with.  Returns zero while other handles still hold the file open, in
 * which case nothing may be sent -- a CLOSE now would destroy the stateid they
 * are still using.
 */
int
chimera_nfs4_open_file_put(
    struct chimera_nfs_client_server *server,
    const uint8_t                    *fh,
    int                               fh_len,
    struct stateid4                  *r_stateid)
{
    struct chimera_nfs4_open_file *file;
    uint8_t                       *wire_fh;
    int                            wire_fh_len;

    if (!server) {
        return 0;
    }

    chimera_nfs4_map_fh(fh, fh_len, &wire_fh, &wire_fh_len);

    if (wire_fh_len <= 0 || wire_fh_len > CHIMERA_VFS_FH_SIZE) {
        return 0;
    }

    pthread_mutex_lock(&server->open_state_lock);

    HASH_FIND(hh, server->open_files, wire_fh, wire_fh_len, file);

    if (!file || --file->refcnt > 0) {
        pthread_mutex_unlock(&server->open_state_lock);
        return 0;
    }

    HASH_DEL(server->open_files, file);

    pthread_mutex_unlock(&server->open_state_lock);

    *r_stateid = file->stateid;

    free(file);

    return 1;
} /* chimera_nfs4_open_file_put */

/*
 * Release every entry left on a server at shutdown.
 *
 * Reaching here with any means handles outlived the VFS that held them, so
 * there is no one left to CLOSE on behalf of and nothing to send on: the module
 * is being torn down and the connection with it.  Free them so the leak is not
 * silent.
 */
void
chimera_nfs4_open_file_drain(struct chimera_nfs_client_server *server)
{
    struct chimera_nfs4_open_file *file, *next;

    pthread_mutex_lock(&server->open_state_lock);

    /* Take the chain, drop uthash's table in one step, and only then free the
     * entries, walking the insertion-order links with each read taken before
     * its element goes.  Deleting them one at a time instead would keep the
     * table's internals and a just-freed entry live at the same moment -- which
     * is what it looks like to a reader who cannot assume the head has no
     * predecessor, the static analyzer included. */
    file = server->open_files;

    HASH_CLEAR(hh, server->open_files);

    while (file) {
        next = file->hh.next;
        free(file);
        file = next;
    }

    pthread_mutex_unlock(&server->open_state_lock);
} /* chimera_nfs4_open_file_drain */
