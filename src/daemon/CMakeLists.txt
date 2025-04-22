
add_executable(chimera daemon.c)

target_link_libraries(chimera chimera_server chimera_metrics jansson)

install(TARGETS chimera DESTINATION sbin)
install(FILES chimera.json DESTINATION etc)
