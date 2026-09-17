# SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_executable(rbtree_test rbtree_test.c)

add_test(chimera/common/rbtree_test rbtree_test)

if(CHIMERA_NATIVE_RCU)
    add_executable(rcu_test rcu_test.c)
    target_link_libraries(rcu_test chimera_common)
    if(MSVC)
        target_compile_options(rcu_test PRIVATE /UNDEBUG)
    else()
        target_compile_options(rcu_test PRIVATE -UNDEBUG)
    endif()
    add_test(NAME chimera/common/rcu COMMAND rcu_test)
    set_tests_properties(chimera/common/rcu PROPERTIES TIMEOUT 60)
endif()
