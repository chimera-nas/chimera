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
 * `stateid` is the one OPEN just returned.  When a caller supplies one it
 * wins: an OPEN of an already-open file is an upgrade that bumps the seqid,
 * so the newest stateid is the current one.
 *
 * Returns 0 with a reference taken, or -1 when the OPEN raced an in-flight
 * CLOSE of the same file and its stateid is doomed -- the caller must re-send
 * the OPEN (nothing to undo here).  Detection: a freshly created server state
 * has seqid 1 and a new `other` (RFC 8881 §8.2.2), so a reply seqid of 2 or
 * more proves the OPEN coalesced into a pre-existing state (same open-owner).
 * That is normal when it is the state this entry is tracking (`other`
 * matches, other handles hold the file), and fatal otherwise: an upgrade of
 * a state this client is closing, has closed (no entry at all), or has
 * already replaced with a fresh one (`other` differs) is an upgrade of a
 * destroyed -- or about to be destroyed -- state, and I/O on the returned
 * stateid would answer BAD_STATEID.  Once the CLOSE has been processed by
 * the server, the re-sent OPEN receives a fresh seqid-1 state, so the retry
 * converges within a CLOSE round trip.
 */
int
chimera_nfs4_open_file_get(
    struct chimera_nfs_client_server *server,
    const uint8_t                    *fh,
    int                               fh_len,
    const struct stateid4            *stateid)
{
    struct chimera_nfs4_open_file *file;
    uint8_t                       *wire_fh;
    int                            wire_fh_len;
    int                            coalesced = stateid && stateid->seqid >= 2;

    if (!server) {
        return 0;
    }

    chimera_nfs4_map_fh(fh, fh_len, &wire_fh, &wire_fh_len);

    if (wire_fh_len <= 0 || wire_fh_len > CHIMERA_VFS_FH_SIZE) {
        return 0;
    }

    pthread_mutex_lock(&server->open_state_lock);

    HASH_FIND(hh, server->open_files, wire_fh, wire_fh_len, file);

    if (file) {
        int same_state = stateid &&
            memcmp(file->stateid.other, stateid->other,
                   sizeof(stateid->other)) == 0;

        if (coalesced && (!same_state || file->closing)) {
            /* An upgrade of a doomed state: either one other than the entry
             * tracks (its CLOSE already retired it, or a fresh open already
             * replaced it and this reply raced past both), or the tracked
             * state itself while its CLOSE is in flight.  Dead either way. */
            pthread_mutex_unlock(&server->open_state_lock);
            return -1;
        }

        file->refcnt++;

        if (stateid &&
            (!same_state || stateid->seqid >= file->stateid.seqid)) {
            /* Take the newest view of the state; a late reply of an older
             * upgrade must not roll the recorded seqid back. */
            file->stateid = *stateid;
        }

        pthread_mutex_unlock(&server->open_state_lock);
        return 0;
    }

    if (coalesced) {
        /* An upgrade of a state this client no longer tracks: the file's
         * CLOSE completed between our OPEN's transmit and the server
         * processing it, so the state we coalesced into is gone. */
        pthread_mutex_unlock(&server->open_state_lock);
        return -1;
    }

    file = calloc(1, sizeof(*file));

    if (!file) {
        pthread_mutex_unlock(&server->open_state_lock);
        return 0;
    }

    file->refcnt = 1;
    file->fh_len = wire_fh_len;
    memcpy(file->fh, wire_fh, wire_fh_len);

    if (stateid) {
        file->stateid = *stateid;
    }

    HASH_ADD_KEYPTR(hh, server->open_files, file->fh, file->fh_len, file);

    pthread_mutex_unlock(&server->open_state_lock);
    return 0;
} /* chimera_nfs4_open_file_get */

/*
 * Drop a handle's reference.  Returns non-zero when it was the last one,
 * having marked the entry closing and copied its stateid to *r_stateid for
 * the caller to CLOSE with; the caller must call
 * chimera_nfs4_open_file_close_done once that CLOSE completes (or when it
 * decides none is needed).  Returns zero while other handles still hold the
 * file open, in which case nothing may be sent -- a CLOSE now would destroy
 * the stateid they are still using.
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

    /* Last handle: this caller owns the wire CLOSE.  The entry stays hashed,
     * marked closing, until chimera_nfs4_open_file_close_done -- a concurrent
     * OPEN of the same file must be able to see the in-flight CLOSE (see
     * chimera_nfs4_open_file_get). */
    file->closing++;

    *r_stateid = file->stateid;

    pthread_mutex_unlock(&server->open_state_lock);

    return 1;
} /* chimera_nfs4_open_file_put */

/*
 * The wire CLOSE consuming a put's reference has completed (or was never
 * needed).  Drop the closing mark and retire the entry once nothing else
 * holds it -- a fresh OPEN may have revived it in the meantime (refcnt > 0,
 * with the fresh state's stateid already recorded), in which case it lives
 * on for that opener.
 */
void
chimera_nfs4_open_file_close_done(
    struct chimera_nfs_client_server *server,
    const uint8_t                    *fh,
    int                               fh_len)
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

    if (file && --file->closing == 0 && file->refcnt == 0) {
        HASH_DEL(server->open_files, file);
        pthread_mutex_unlock(&server->open_state_lock);
        free(file);
        return;
    }

    pthread_mutex_unlock(&server->open_state_lock);
} /* chimera_nfs4_open_file_close_done */

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
