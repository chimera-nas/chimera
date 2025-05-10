
include_directories(/fio)
add_library(chimera_fio SHARED fio.c)

target_link_libraries(chimera_fio chimera_client jansson)

install(TARGETS chimera_fio DESTINATION lib)

set(PLUGIN_PATH ${CMAKE_CURRENT_BINARY_DIR}/libchimera_fio.so)


add_subdirectory(tests)