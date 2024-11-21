#include <stdlib.h>

#include "vfs.h"
#include "vfs_internal.h"
#include "common/format.h"
#if 0
static inline void
chimera_vfs_dispatch(
    struct chimera_vfs         *vfs,
    struct chimera_vfs_request *request)
{
    chimera_vfs_dump_request(request);
    vfs->dispatch_cb(vfs, request);
} /* chimera_vfs_dispatch */

static inline void
chimera_vfs_complete(
    struct chimera_vfs_request *request,
    int                         status)
{
    request->status = status;
    chimera_vfs_dump_reply(request);
    request->complete_cb();
} /* chimera_vfs_complete */

struct chimera_vfs *
chimera_get_vfs_by_fh(
    const void *fh,
    uint32_t    fh_len);

struct chimera_share *
chimera_get_share_by_name(
    const char *name);

#endif /* if 0 */

struct chimera_vfs *
chimera_vfs_init(void)
{
    struct chimera_vfs *vfs;

    vfs = calloc(1, sizeof(*vfs));

    return vfs;
} /* chimera_vfs_init */

void
chimera_vfs_destroy(struct chimera_vfs *vfs)
{
    free(vfs);
} /* chimera_vfs_destroy */

void
chimera_vfs_getrootfh(
    struct chimera_vfs *vfs,
    void               *fh,
    int                *fh_len)
{
    uint8_t *fh8 = fh;

    fh8[0]  = 0;
    *fh_len = 1;

} /* chimera_vfs_getrootfh */

void
chimera_vfs_lookup(
    struct chimera_vfs           *vfs,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    chimera_vfs_lookup_callback_t callback,
    void                         *private_data)
{
    char    fhstr[80], newfhstr[80];
    uint8_t newfh = 2;

    format_hex(fhstr, sizeof(fhstr), fh, fhlen);
    format_hex(newfhstr, sizeof(newfhstr), &newfh, sizeof(newfh));

    chimera_vfs_debug("chimera_vfs_lookup: fh %s name %s newfh %s",
                      fhstr, name, newfhstr);

    callback(0, &newfh, sizeof(newfh), private_data);

} /* chimera_vfs_lookup */
