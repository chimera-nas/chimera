# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

find_package(OpenSSL REQUIRED)

add_library(chimera_s3 SHARED s3.c s3_get.c s3_put.c s3_copy.c s3_dump.c s3_status.c s3_delete.c s3_delete_objects.c s3_list.c s3_bucket.c s3_auth.c s3_acl.c s3_multipart.c)
target_link_libraries(chimera_s3 chimera_vfs evpl_http evpl OpenSSL::Crypto)

install(TARGETS chimera_s3 DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
