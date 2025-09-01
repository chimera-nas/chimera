// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <cufile.h>

const char *chimera_cufile_fs_type(
    void *handle);

int chimera_cufile_getRDMADevicePriority(
    void *handle,
    char *,
    size_t,
    loff_t,
    sockaddr_t *hostaddr);

ssize_t chimera_cufile_read(
    void *handle,
    char *,
    size_t,
    loff_t,
    cufileRDMAInfo_t *);

ssize_t chimera_cufile_write(
    void *handle,
    const char *,
    size_t,
    loff_t,
    cufileRDMAInfo_t *);

extern const struct CUfileFSOps chimera_cufile_ops;

#define chimera_cudesc_init(cuda_desc, chimera_handle)        \
    {                                                         \
        memset((cuda_desc), 0, sizeof(CUfileDescr_t));        \
        (cuda_desc)->handle.handle = (chimera_handle);        \
        (cuda_desc)->type = CU_FILE_HANDLE_TYPE_USERSPACE_FS; \
        (cuda_desc)->fs_ops = &chimera_cufile_ops;            \
    }
