/*
 * SPDX-FileCopyrightText: 2025 Ben Jarvis
 * SPDX-License-Identifier: Unlicense
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#include <cuda_runtime.h>
#include <cufile.h>

extern "C"
{
#include "client/client.h"
#include "common/logging.h"
#include "cufile/chimera_cufile.h"
};

int main(int argc, char **argv)
{
    int fd;
    ssize_t ret;
    void *devPtr_base;
    off_t file_offset = 0x2000;
    off_t devPtr_offset = 0x1000;
    ssize_t IO_size = 1UL << 24;
    size_t buff_size = IO_size + 0x1000;
    CUfileError_t status;
    // CUResult cuda_result;
    int cuda_result;
    CUfileDescr_t cf_descr;
    CUfileHandle_t cf_handle;
    char *testfn;

    chimera_log_init();

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <testfile>\n", argv[0]);
        return -1;
    }

    testfn = argv[1];

    fprintf(stderr, "Opening File %s\n", testfn);

    fd = open(testfn, O_CREAT | O_WRONLY | O_DIRECT, 0644);
    if (fd < 0)
    {
        fprintf(stderr, "file open %s errno %d\n", testfn, errno);
        return -1;
    }

    // the above fd could also have been opened without O_DIRECT starting CUDA toolkit 12.2
    // (gds 1.7.x version) as follows
    // fd = open(testfn, O_CREAT|O_WRONLY, 0644);

    fprintf(stderr, "Opening cuFileDriver.\n");
    status = cuFileDriverOpen();
    if (status.err != CU_FILE_SUCCESS)
    {
        fprintf(stderr, " cuFile driver failed to open\n");
        close(fd);
        return -1;
    }

    fprintf(stderr, "Registering cuFile handle to %s.\n", testfn);

    fprintf(stderr, "chimera_cudesc_init\n");
    chimera_cudesc_init(&cf_descr, &cf_descr);
    /* XXX */
    // cf_descr.handle.fd = fd;

    status = cuFileHandleRegister(&cf_handle, &cf_descr);

    if (status.err != CU_FILE_SUCCESS)
    {
        fprintf(stderr, "cuFileHandleRegister fd %d status %d\n", fd, status.err);
        close(fd);
        return -1;
    }

    fprintf(stderr, "Allocating CUDA buffer of %zu bytes.\n", buff_size);

    cuda_result = cudaMalloc(&devPtr_base, buff_size);
    if (cuda_result != CUDA_SUCCESS)
    {
        fprintf(stderr, "buffer allocation failed %d\n", cuda_result);
        cuFileHandleDeregister(cf_handle);
        close(fd);
        return -1;
    }

    fprintf(stderr, "Registering Buffer of %zu bytes.\n", buff_size);
    status = cuFileBufRegister(devPtr_base, buff_size, 0);
    if (status.err != CU_FILE_SUCCESS)
    {
        fprintf(stderr, "buffer registration failed %d\n", status.err);
        cuFileHandleDeregister(cf_handle);
        close(fd);
        cudaFree(devPtr_base);
        return -1;
    }

    // fill a pattern
    fprintf(stderr, "Filling memory.\n");

    cudaMemset((void *)devPtr_base, 0xab, buff_size);
    cuStreamSynchronize(0);

    // perform write operation directly from GPU mem to file
    fprintf(stderr, "Writing buffer to file.\n");
    ret = cuFileWrite(cf_handle, devPtr_base, IO_size, file_offset, devPtr_offset);

    if (ret < 0 || ret != IO_size)
    {
        fprintf(stderr, "cuFileWrite failed %ld\n", ret);
    }

    // release the GPU memory pinning
    fprintf(stderr, "Releasing cuFile buffer.\n");
    status = cuFileBufDeregister(devPtr_base);
    if (status.err != CU_FILE_SUCCESS)
    {
        fprintf(stderr, "buffer deregister failed\n");
        cudaFree(devPtr_base);
        cuFileHandleDeregister(cf_handle);
        close(fd);
        return -1;
    }

    fprintf(stderr, "Freeing CUDA buffer.\n");
    cudaFree(devPtr_base);
    // deregister the handle from cuFile
    fprintf(stderr, "Releasing file handle. \n");
    (void)cuFileHandleDeregister(cf_handle);
    close(fd);

    // release all cuFile resources
    fprintf(stderr, "Closing File Driver.\n");
    (void)cuFileDriverClose();

    fprintf(stderr, "\n");

    return 0;
}
