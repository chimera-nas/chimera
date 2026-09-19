// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include "common/atomic.h"
#include "common/thread.h"

static uint64_t counter;
static void *increment(void *unused)
{
    (void) unused;
    for (int i = 0; i < 100000; i++) {
        chimera_atomic_fetch_add(&counter, 1, CHIMERA_MEMORY_RELAXED);
    }
    return NULL;
}
int main(void)
{
    evpl_native_thread_t threads[4];
    uint64_t expected;
    uint8_t byte = 255;
    uint16_t half = 65535;
    int32_t signed_value = -17, signed_expected = -17;
    int object;
    void *pointer = NULL, *pointer_expected = NULL;
    assert(chimera_atomic_add_fetch(&byte, 1, CHIMERA_MEMORY_SEQ_CST) == 0);
    assert(chimera_atomic_sub_fetch(&half, 1, CHIMERA_MEMORY_SEQ_CST) == 65534);
    assert(chimera_atomic_load_n(&signed_value, CHIMERA_MEMORY_ACQUIRE) == -17);
    assert(chimera_atomic_compare_exchange_n(&signed_value, &signed_expected, -18, 0,
                                            CHIMERA_MEMORY_ACQ_REL, CHIMERA_MEMORY_ACQUIRE));
    assert(!chimera_atomic_compare_exchange_n(&signed_value, &signed_expected, 0, 0,
                                             CHIMERA_MEMORY_ACQ_REL, CHIMERA_MEMORY_ACQUIRE));
    assert(signed_expected == -18);
    assert(chimera_atomic_compare_exchange_n(&pointer, &pointer_expected, &object, 0,
                                            CHIMERA_MEMORY_ACQ_REL, CHIMERA_MEMORY_ACQUIRE));
    assert(chimera_atomic_exchange_n(&pointer, NULL, CHIMERA_MEMORY_SEQ_CST) == &object);
    assert(!chimera_atomic_load_n(&pointer, CHIMERA_MEMORY_ACQUIRE));
    chimera_atomic_store_n(&counter, UINT64_C(1) << 48, CHIMERA_MEMORY_RELEASE);
    for (int i = 0; i < 4; i++) {
        assert(!evpl_native_thread_create(&threads[i], NULL, increment, NULL));
    }
    for (int i = 0; i < 4; i++) {
        assert(!evpl_native_thread_join(threads[i], NULL));
    }
    expected = (UINT64_C(1) << 48) + 400000;
    assert(chimera_atomic_load_n(&counter, CHIMERA_MEMORY_ACQUIRE) == expected);
    assert(chimera_atomic_fetch_or(&counter, UINT64_C(1) << 60, CHIMERA_MEMORY_RELAXED) == expected);
    assert(chimera_atomic_fetch_and(&counter, UINT64_C(0xffff), CHIMERA_MEMORY_RELAXED) ==
           (expected | (UINT64_C(1) << 60)));
    return 0;
}
