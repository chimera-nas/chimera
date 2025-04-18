#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "common/macros.h"

SYMBOL_EXPORT void
chimera_vfs_getrootfh(
    struct chimera_vfs_thread *thread,
    void                      *fh,
    int                       *fh_len)
{
    uint8_t *fh8 = fh;

    /* The root file handle is always a single zero byte */
    fh8[0]  = 0;
    *fh_len = 1;

} /* chimera_vfs_getrootfh */
