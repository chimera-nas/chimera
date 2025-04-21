
#include <stdio.h>
#include <cufile.h>

#include "chimera_cufile.h"
#include "common/macros.h"
#include "common/logging.h"

#define chimera_cufile_debug(...) chimera_debug("cufile", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_cufile_info(...)  chimera_info("cufile", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_cufile_error(...) chimera_error("cufile", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_cufile_fatal(...) chimera_fatal("cufile", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_cufile_abort(...) chimera_abort("cufile", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_cufile_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "cufile", __FILE__, __LINE__, __VA_ARGS__)

SYMBOL_EXPORT const struct CUfileFSOps chimera_cufile_ops = {
    .fs_type               = chimera_cufile_fs_type,
    .getRDMADeviceList     = NULL,
    .getRDMADevicePriority = chimera_cufile_getRDMADevicePriority,
    .read                  = chimera_cufile_read,
    .write                 = chimera_cufile_write,
};

SYMBOL_EXPORT const char *
chimera_cufile_fs_type(void *handle)
{
    fprintf(stderr, "chimera_cufile_fs_type\n");
    return "chimera";
} /* chimera_cufile_fs_type */

SYMBOL_EXPORT int
chimera_cufile_getRDMADevicePriority(
    void       *handle,
    char       *name,
    size_t      len,
    loff_t      offset,
    sockaddr_t *hostaddr)
{
    fprintf(stderr, "chimera_cufile_getRDMADevicePriority\n");
    return -1;
} /* chimera_cufile_getRDMADevicePriority */

SYMBOL_EXPORT ssize_t
chimera_cufile_read(
    void             *handle,
    char             *name,
    size_t            len,
    loff_t            offset,
    cufileRDMAInfo_t *rdma_info)
{
    fprintf(stderr, "chimera_cufile_read handle %p name '%s' len %zu offset %ld\n",
            handle, name, len, offset);
    return len;
} /* chimera_cufile_read */

SYMBOL_EXPORT ssize_t
chimera_cufile_write(
    void             *handle,
    const char       *name,
    size_t            len,
    loff_t            offset,
    cufileRDMAInfo_t *rdma_info)
{
    fprintf(stderr, "chimera_cufile_write handle %p name '%s' len %zu offset %ld\n",
            handle, name, len, offset);
    return len;
} /* chimera_cufile_write */