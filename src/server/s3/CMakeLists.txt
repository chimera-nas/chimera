# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only


add_library(chimera_s3 ${CHIMERA_LIBRARY_TYPE} s3.c s3_get.c s3_put.c s3_copy.c s3_dump.c s3_status.c s3_delete.c s3_delete_objects.c s3_list.c s3_bucket.c s3_auth.c s3_multipart.c s3_metadata.c s3_acl.c
    s3_tagging.c)
target_link_libraries(chimera_s3 chimera_vfs evpl_http evpl chimera_crypto)

install(TARGETS chimera_s3 DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
