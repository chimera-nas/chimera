# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_posix SHARED
            posix.c
            posix_mount.c
            posix_umount.c
            posix_open.c
            posix_close.c
            posix_read.c
            posix_write.c
            posix_pread.c
            posix_pwrite.c
            posix_readv.c
            posix_writev.c
            posix_mkdir.c
            posix_symlink.c
            posix_link.c
            posix_unlink.c
            posix_rename.c
            posix_readlink.c
            posix_stat.c
            posix_fstat.c
            posix_lseek.c
            posix_opendir.c
            posix_readdir.c
            posix_seekdir.c
            posix_scandir.c
            posix_fopen.c
            posix_fclose.c
            posix_fread.c
            posix_fwrite.c
            posix_fseek.c
            posix_feof.c
            posix_fileno.c
            posix_fflush.c
            posix_fgetc.c
            posix_fputc.c
            posix_fgets.c
            posix_fputs.c
            posix_ungetc.c)

target_link_libraries(chimera_posix chimera_client evpl pthread)


install(TARGETS chimera_posix DESTINATION lib)

add_subdirectory(tests)
