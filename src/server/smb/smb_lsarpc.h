#pragma once

#include "smb_internal.h"

int
chimera_smb_lsarpc_transceive(
    struct evpl_iovec *input_iov,
    int                input_niov,
    struct evpl_iovec *output_iov);