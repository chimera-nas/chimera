
add_subdirectory(libevpl)

set(XDRZCC ${XDRZCC} PARENT_SCOPE)

remove_definitions(-fvisibility=hidden)

add_subdirectory(libsmb2)