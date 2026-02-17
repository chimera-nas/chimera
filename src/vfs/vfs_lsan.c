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
        /* GSSAPI/Kerberos internal allocations */
        "leak:gss_accept_sec_context\n"
        "leak:libgssapi_krb5\n"
        "leak:libkrb5\n"
        "leak:gssntlmssp\n";
} /* __lsan_default_suppressions */

#endif /* CHIMERA_SANITIZE */
