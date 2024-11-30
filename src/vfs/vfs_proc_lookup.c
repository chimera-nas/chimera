#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/format.h"
#include "common/misc.h"

static void
chimera_vfs_lookup_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_lookup_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->lookup.r_fh,
             request->lookup.r_fh_len,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_complete */

void
chimera_vfs_lookup(
    struct chimera_vfs_thread    *thread,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    uint32_t                      namelen,
    chimera_vfs_lookup_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    char                        fhstr[80];

    format_hex(fhstr, sizeof(fhstr), fh, fhlen);
    chimera_vfs_debug("chimera_vfs_lookup: fh=%s name=%s",
                      fhstr, name);

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request                       = chimera_vfs_request_alloc(thread);
    request->opcode               = CHIMERA_VFS_OP_LOOKUP;
    request->complete             = chimera_vfs_lookup_complete;
    request->lookup.fh            = fh;
    request->lookup.fh_len        = fhlen;
    request->lookup.component     = name;
    request->lookup.component_len = namelen;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_lookup */
