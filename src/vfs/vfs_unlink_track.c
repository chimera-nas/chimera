// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>

#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_release.h"
#include "vfs_unlinked.h"
#include "vfs_unlink_track.h"
#include "common/atomic.h"
#include "common/macros.h"

static uint64_t chimera_vfs_unlink_pins;
static uint64_t chimera_vfs_unlink_lookups;

struct chimera_vfs_unlink_pin {
    struct chimera_vfs_thread       *thread;
    struct chimera_vfs_open_handle  *handle;
    struct chimera_vfs_cred          root;
    struct chimera_vfs_cred          cred;
    struct chimera_vfs_unlink_victim victim;
    void                             ( *done )(
        void *private_data);
    void                            *private_data;
    chimera_vfs_unlink_pin_cb        callback;
    void                            *callback_arg;
};

SYMBOL_EXPORT void
chimera_vfs_unlinked_test_stats(
    uint64_t *r_pins,
    uint64_t *r_lookups)
{
    *r_pins    = chimera_atomic_load_n(&chimera_vfs_unlink_pins, CHIMERA_MEMORY_ACQUIRE);
    *r_lookups = chimera_atomic_load_n(&chimera_vfs_unlink_lookups, CHIMERA_MEMORY_ACQUIRE);
} /* chimera_vfs_unlinked_test_stats */

int
chimera_vfs_unlink_track_wanted(
    struct chimera_vfs_thread *thread,
    const uint8_t             *parent_fh,
    int                        parent_fh_len,
    const char                *name,
    int                        namelen)
{
    if (!thread->vfs->ref_tracking) {
        return 0;
    }

    if (!chimera_vfs_pnfs_fh_is_ds_backing(thread->vfs, parent_fh, parent_fh_len)) {
        return 1;
    }

    /* A data server this server serves itself may be backed by the mount the
     * metadata files live in, so there the directory does not say that a file
     * is data: only the name the backing file was given does. */
    return chimera_vfs_pnfs_fh_is_local_ds_backing(thread->vfs, parent_fh, parent_fh_len) &&
           !chimera_vfs_pnfs_is_backing_name(name, namelen);
} /* chimera_vfs_unlink_track_wanted */

struct chimera_vfs_unlink_lookup {
    struct chimera_vfs_unlink_victim victim;
    chimera_vfs_unlink_victim_cb     callback;
    void                            *private_data;
};

static void
chimera_vfs_unlink_victim_lookup_done(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_unlink_lookup *lookup = private_data;
    struct chimera_vfs_unlink_victim *v      = &lookup->victim;
    chimera_vfs_unlink_victim_cb      callback;
    void                             *arg;
    int                               found;

    found = error_code == CHIMERA_VFS_OK && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH);

    if (found) {
        memcpy(v->fh, attr->va_fh, attr->va_fh_len);
        v->fh_len     = (uint16_t) attr->va_fh_len;
        v->type_known = !!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE);
        v->regular    = v->type_known && S_ISREG(attr->va_mode);
        v->have_ino   = !!(attr->va_set_mask & CHIMERA_VFS_ATTR_INUM);
        v->ino        = v->have_ino ? attr->va_ino : 0;

        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) && attr->va_pnfs_len <= sizeof(v->layout)) {
            v->layout_len = attr->va_pnfs_len;
            memcpy(v->layout, attr->va_pnfs, attr->va_pnfs_len);
        }
    }

    callback = lookup->callback;
    arg      = lookup->private_data;

    callback(found, v, arg);
    free(lookup);
} /* chimera_vfs_unlink_victim_lookup_done */

void
chimera_vfs_unlink_victim_lookup(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *parent_fh,
    int                            parent_fh_len,
    const char                    *name,
    int                            namelen,
    chimera_vfs_unlink_victim_cb   callback,
    void                          *private_data)
{
    struct chimera_vfs_unlink_lookup *lookup = calloc(1, sizeof(*lookup));

    lookup->callback     = callback;
    lookup->private_data = private_data;

    chimera_atomic_add_fetch(&chimera_vfs_unlink_lookups, 1, CHIMERA_MEMORY_RELEASE);

    /* The layout is asked for here, while the name still resolves, because it
     * is a property of the file and cannot be lost by the unlink that follows. */
    chimera_vfs_lookup(thread, cred, parent_fh, parent_fh_len, name, namelen,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_PNFS_LAYOUT,
                       0, chimera_vfs_unlink_victim_lookup_done, lookup);
} /* chimera_vfs_unlink_victim_lookup */

static void
chimera_vfs_unlink_pin_opened(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_unlink_pin *pin = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        /* A directory or other non-regular object refuses the open by design;
         * anything else means the file is unlinked without being followed. */
        if (pin->victim.type_known) {
            chimera_vfs_error("unlink: could not pin a file before removing it (err=%d); "
                              "its data server file will not be tracked", error_code);
        }
        pin->callback(NULL, pin->callback_arg);
        free(pin);
        return;
    }

    pin->handle = handle;
    pin->callback(pin, pin->callback_arg);
} /* chimera_vfs_unlink_pin_opened */

void
chimera_vfs_unlink_pin_take(
    struct chimera_vfs_thread              *thread,
    const struct chimera_vfs_cred          *cred,
    const struct chimera_vfs_unlink_victim *victim,
    chimera_vfs_unlink_pin_cb               callback,
    void                                   *private_data)
{
    struct chimera_vfs_unlink_pin *pin;

    if (victim->type_known && !victim->regular) {
        callback(NULL, private_data);
        return;
    }

    pin = calloc(1, sizeof(*pin));

    pin->thread       = thread;
    pin->victim       = *victim;
    pin->cred         = *cred;
    pin->callback     = callback;
    pin->callback_arg = private_data;

    /* Root, so the pin does not depend on whether the remover may read the file. */
    chimera_vfs_cred_init_unix(&pin->root, 0, 0, 0, NULL);

    chimera_atomic_add_fetch(&chimera_vfs_unlink_pins, 1, CHIMERA_MEMORY_RELEASE);

    chimera_vfs_open_fh(thread, &pin->root, pin->victim.fh, pin->victim.fh_len,
                        CHIMERA_VFS_OPEN_REGULAR_ONLY,
                        chimera_vfs_unlink_pin_opened, pin);
} /* chimera_vfs_unlink_pin_take */

static void
chimera_vfs_unlink_pin_drop(
    struct chimera_vfs_unlink_pin *pin,
    int                            expedite)
{
    struct chimera_vfs_thread *thread = pin->thread;

    void                       ( *done )(
        void *) = pin->done;
    void                      *arg = pin->private_data;

    chimera_vfs_release(thread, pin->handle);

    if (expedite) {
        chimera_vfs_open_cache_expedite_close(thread->vfs->vfs_open_file_cache,
                                              pin->victim.fh, pin->victim.fh_len,
                                              chimera_vfs_hash(pin->victim.fh, pin->victim.fh_len));
    }

    free(pin);

    done(arg);
} /* chimera_vfs_unlink_pin_drop */

static void
chimera_vfs_unlink_pin_attrs(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_unlink_pin    *pin = private_data;
    struct chimera_vfs_unlinked_facts facts;
    const uint8_t                    *deviceid, *backing_fh;
    const uint8_t                    *layout = pin->victim.layout;
    uint32_t                          layout_len = pin->victim.layout_len, backing_fh_len;
    uint64_t                          ino      = pin->victim.ino;
    int                               have_ino = pin->victim.have_ino;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_error("unlink: could not read the link count of a removed file (err=%d); "
                          "its data server file will not be tracked", error_code);
        chimera_vfs_unlink_pin_drop(pin, 0);
        return;
    }

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) || !S_ISREG(attr->va_mode) ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK) || attr->va_nlink != 0) {
        /* Not a regular file, or another link keeps it. */
        chimera_vfs_unlink_pin_drop(pin, 0);
        return;
    }

    /* The attributes the pin reads now win over those from before the unlink. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) && attr->va_pnfs_len > 0) {
        layout     = attr->va_pnfs;
        layout_len = attr->va_pnfs_len;
    }
    if (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM) {
        ino      = attr->va_ino;
        have_ino = 1;
    }

    memset(&facts, 0, sizeof(facts));
    facts.cred = pin->cred;

    if (layout_len == 0) {
        /* No data on a data server, so nothing of its own to purge.  Such a
         * file is tracked only if a holder has a recallable reference on it
         * (a block or SCSI layout, say), because that holder must be asked to
         * give it back; otherwise the backend's own close is all that is
         * needed, and it happens as it always did. */
        chimera_vfs_unlinked_track_if_recallable(pin->thread->vfs, pin->victim.fh, pin->victim.fh_len, &facts);
        chimera_vfs_unlink_pin_drop(pin, 1);
        return;
    }

    if (have_ino &&
        chimera_vfs_pnfs_blob_unpack(layout, layout_len, &deviceid, &backing_fh, &backing_fh_len) == 0) {
        facts.has_backing = 1;
        memcpy(facts.deviceid, deviceid, CHIMERA_VFS_DEVICEID_SIZE);
        /* The name LAYOUTGET and the write redirect created it under. */
        chimera_vfs_pnfs_backing_name(facts.backing_name, pin->victim.fh, ino);
    }

    chimera_vfs_unlinked_track_facts(pin->thread->vfs, pin->victim.fh, pin->victim.fh_len, &facts);

    /* The pin is one of the file's handles, so dropping it may be what lets the
    * purge begin; the sweep takes it at once rather than after the idle age. */
    chimera_vfs_unlink_pin_drop(pin, 1);
} /* chimera_vfs_unlink_pin_attrs */

void
chimera_vfs_unlink_pin_finish(
    struct chimera_vfs_unlink_pin *pin,
    enum chimera_vfs_error         unlink_status,
    void (                        *done )(void *private_data),
    void                          *private_data)
{
    pin->done         = done;
    pin->private_data = private_data;

    if (unlink_status != CHIMERA_VFS_OK) {
        chimera_vfs_unlink_pin_drop(pin, 0);
        return;
    }

    /* The layout bit is not only wanted for its value: a request for anything
     * the attribute cache does not hold keeps it from answering with the link
     * count from before the unlink. */
    chimera_vfs_getattr(pin->thread, &pin->root, pin->handle,
                        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_NLINK | CHIMERA_VFS_ATTR_INUM |
                        CHIMERA_VFS_ATTR_PNFS_LAYOUT,
                        chimera_vfs_unlink_pin_attrs, pin);
} /* chimera_vfs_unlink_pin_finish */
