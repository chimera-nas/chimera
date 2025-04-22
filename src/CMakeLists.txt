include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(common)
add_subdirectory(metrics)
add_subdirectory(vfs)
add_subdirectory(server)
add_subdirectory(client)
add_subdirectory(posix)
add_subdirectory(daemon)
add_subdirectory(nfs)

if (CUDA_ENABLED)
    add_subdirectory(cufile)
endif()
