// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include "common/atomic.h"

#if !defined(_WIN32) && !defined(CHIMERA_NATIVE_RCU)
#include <urcu/urcu-qsbr.h>
#else // if !defined(_WIN32) && !defined(CHIMERA_NATIVE_RCU)
#include <stddef.h>
#include <stdatomic.h>
#include "common/macros.h"

struct rcu_head {
    struct rcu_head *next;
    void             (*callback)(
        struct rcu_head *);
};

SYMBOL_EXPORT void chimera_rcu_register_thread(
    void);
SYMBOL_EXPORT void chimera_rcu_unregister_thread(
    void);
SYMBOL_EXPORT void chimera_rcu_quiescent_state(
    void);
SYMBOL_EXPORT void chimera_rcu_thread_offline(
    void);
SYMBOL_EXPORT void chimera_rcu_thread_online(
    void);
SYMBOL_EXPORT void chimera_call_rcu(
    struct rcu_head *,
    void (*)(struct rcu_head *));
SYMBOL_EXPORT void chimera_rcu_barrier(
    void);
SYMBOL_EXPORT void chimera_synchronize_rcu(
    void);
SYMBOL_EXPORT void chimera_rcu_read_lock(
    void);
SYMBOL_EXPORT void chimera_rcu_read_unlock(
    void);

#define urcu_qsbr_register_thread   chimera_rcu_register_thread
#define urcu_qsbr_unregister_thread chimera_rcu_unregister_thread
#define urcu_qsbr_quiescent_state   chimera_rcu_quiescent_state
#define urcu_qsbr_thread_offline    chimera_rcu_thread_offline
#define urcu_qsbr_thread_online     chimera_rcu_thread_online
#define urcu_qsbr_read_lock         chimera_rcu_read_lock
#define urcu_qsbr_read_unlock       chimera_rcu_read_unlock
#define urcu_qsbr_synchronize_rcu   chimera_synchronize_rcu
#define urcu_qsbr_barrier           chimera_rcu_barrier
#define rcu_read_lock               chimera_rcu_read_lock
#define rcu_read_unlock             chimera_rcu_read_unlock
#define synchronize_rcu             chimera_synchronize_rcu
#define call_rcu                    chimera_call_rcu
#define rcu_barrier                 chimera_rcu_barrier
#define caa_container_of            container_of
#ifdef _WIN32
#include <evpl/evpl_platform.h>
#define rcu_dereference(p)       ((__typeof__(p))InterlockedCompareExchangePointer((void *volatile *) &(p), NULL, NULL))
#define rcu_assign_pointer(p, v) ((void) InterlockedExchangePointer((void *volatile *) &(p), (void *) (v)))
#else // ifdef _WIN32
#define rcu_dereference(p)       chimera_atomic_load_n(&(p), CHIMERA_MEMORY_ACQUIRE)
#define rcu_assign_pointer(p, v) chimera_atomic_store_n(&(p), (v), CHIMERA_MEMORY_RELEASE)
#endif // ifdef _WIN32
#endif // if !defined(_WIN32) && !defined(CHIMERA_NATIVE_RCU)
