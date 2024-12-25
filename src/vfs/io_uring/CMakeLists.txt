add_library(chimera_vfs_io_uring STATIC
    io_uring.c
)

target_link_libraries(chimera_vfs_io_uring uring)

