
add_library(chimera_metrics SHARED metrics.c)

target_link_libraries(chimera_metrics evpl_http prometheus-c)

install(TARGETS chimera_metrics DESTINATION lib)