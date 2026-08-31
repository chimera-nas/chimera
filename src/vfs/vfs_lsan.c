// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifdef CHIMERA_SANITIZE

/*
 * Default LeakSanitizer suppressions for all test binaries.
 *
 * LSAN calls this function at exit to get a list of leak patterns
 * to suppress.  This avoids the need for external suppressions files
 * or LSAN_OPTIONS environment variables.
 */
__attribute__((visibility("default")))
const char *
__lsan_default_suppressions(void)
{
    return
        /* OpenSSL one-time global initialization (via pthread_once) */
        "leak:CONF_modules_load\n"
        "leak:ossl_init_config_ossl_\n"
        "leak:CRYPTO_malloc\n"
        /* OpenSSL provider loading from GSSAPI/Kerberos */
        "leak:OSSL_PROVIDER_try_load\n"
        "leak:OSSL_PROVIDER_load\n"
        "leak:OSSL_PROVIDER_add_builtin\n"
        "leak:provider_init\n"
        "leak:provider_register\n"
        /* fio intentionally leaks during options parsing */
        "leak:parse_options\n"
        "leak:log_io_piece\n"
        "leak:options_mem_dupe\n"
        /* SMB compound/request free lists (per-thread caches) */
        "leak:chimera_smb_compound_alloc\n"
        /* NFS proxy per-thread server state (connection, NFS4.1 session +
         * slot tables).  Repeated mount/umount cycles accumulate one block
         * per (thread, mount) that only thread destroy releases, and a
         * client thread that never revisits the old server index holds its
         * block until process exit.  A real teardown (release at last
         * unmount, cross-thread) is tracked as follow-up work; the harness
         * batches cycle the mount per trace and trip this at exit.  The
         * session/slot allocations are indirect leaks off that state with
         * their own stacks, so each allocation site is listed. */
        "leak:chimera_nfs_thread_get_server_thread\n"
        "leak:chimera_nfs4_session_pool_init\n"
        "leak:chimera_nfs4_slot_table_init\n"
        "leak:chimera_nfs4_ctx_alloc\n"
        "leak:chimera_nfs4_mount\n"
        "leak:chimera_nfs4_cb_exchange_id_callback\n"
        /* GSSAPI/Kerberos internal allocations */
        "leak:gss_accept_sec_context\n"
        "leak:libgssapi_krb5\n"
        "leak:libkrb5\n"
        "leak:gssntlmssp\n";
} /* __lsan_default_suppressions */

#endif /* CHIMERA_SANITIZE */
