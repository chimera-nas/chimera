remove_definitions(-Wno-unused-function -fvisibility=hidden ${MARCH_FLAG} -Werror)

add_executable(cufile_test basic.cu)

target_link_libraries(cufile_test chimera_client chimera_cufile chimera_common cuda cudart cufile)

install(TARGETS cufile_test DESTINATION bin)