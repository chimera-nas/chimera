
add_subdirectory(libevpl)

set(XDRZCC ${XDRZCC} PARENT_SCOPE)

remove_definitions(-fvisibility=hidden)

remove_definitions(-Wall)

add_subdirectory(libsmb2)