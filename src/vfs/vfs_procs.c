#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/format.h"
#include "common/misc.h"

static inline struct chimera_vfs_module *
chimera_vfs_get_module(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    struct chimera_vfs *vfs = thread->vfs;

    uint8_t             fh_magic;

    if (fhlen < 1) {
        return NULL;
    }

    fh_magic = *(uint8_t *) fh;

    return vfs->modules[fh_magic];
} /* chimera_vfs_get_module */

static inline void
chimera_vfs_dispatch(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_module  *module,
    struct chimera_vfs_request *request)
{
    module->dispatch(request, thread->module_private[module->fh_magic]);
} /* chimera_vfs_dispatch */

void
chimera_vfs_getrootfh(
    struct chimera_vfs_thread *thread,
    void                      *fh,
    int                       *fh_len)
{
    uint8_t *fh8 = fh;

    fh8[0]  = 0;
    *fh_len = 1;

} /* chimera_vfs_getrootfh */

static void
chimera_vfs_lookup_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_lookup_callback_t callback = request->proto_callback;
    char                          fhstr[80];

    format_hex(fhstr, sizeof(fhstr), request->lookup.r_fh, request->lookup.
               r_fh_len);

    chimera_vfs_debug("lookup_complete: fh=%s", fhstr);

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

static void
chimera_vfs_getattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_getattr_callback_t callback = request->proto_callback;

    callback(request->status,
             request->getattr.r_attr_mask,
             &request->getattr.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_getattr_complete */

void
chimera_vfs_getattr(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       req_attr_mask,
    chimera_vfs_getattr_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_GETATTR;
    request->complete           = chimera_vfs_getattr_complete;
    request->getattr.fh         = fh;
    request->getattr.fh_len     = fhlen;
    request->getattr.attr_mask  = req_attr_mask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_getattr */

static void
chimera_vfs_readdir_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_readdir_complete_t complete = request->proto_callback;

    complete(request->status,
             request->readdir.r_cookie,
             request->readdir.r_eof,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_readdir_complete */

void
chimera_vfs_readdir(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       cookie,
    chimera_vfs_readdir_callback_t callback,
    chimera_vfs_readdir_complete_t complete,
    void                          *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_READDIR;
    request->complete           = chimera_vfs_readdir_complete;
    request->readdir.fh         = fh;
    request->readdir.fh_len     = fhlen;
    request->readdir.cookie     = cookie;
    request->readdir.callback   = callback;
    request->proto_callback     = complete;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_readdir */
