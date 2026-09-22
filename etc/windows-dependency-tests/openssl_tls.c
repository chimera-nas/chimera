// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: Unlicense

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

static void
require(
    int         condition,
    const char *operation)
{
    if (!condition) {
        fprintf(stderr, "OpenSSL dependency test failed: %s\n", operation);
        ERR_print_errors_fp(stderr);
        exit(1);
    }
} /* require */

static void
handshake(SSL *ssl)
{
    int result, error;

    if (SSL_is_init_finished(ssl)) {
        return;
    }
    ERR_clear_error();
    result = SSL_do_handshake(ssl);
    if (result != 1) {
        error = SSL_get_error(ssl, result);
        require(error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE,
                "SSL_do_handshake");
    }
} /* handshake */

int
main(
    int   argc,
    char *argv[])
{
    SSL_CTX   *client_ctx, *server_ctx;
    SSL       *client, *server;
    BIO       *client_bio, *server_bio;
    EVP_PKEY  *key;
    X509      *cert;
    X509_NAME *name;
    int        version;
    char       reply[4];

    require(argc == 2 && (!strcmp(argv[1], "1.2") || !strcmp(argv[1], "1.3")),
            "expected protocol version 1.2 or 1.3");
    version = !strcmp(argv[1], "1.2") ? TLS1_2_VERSION : TLS1_3_VERSION;
    printf("Testing %s, TLS %s\n", OpenSSL_version(OPENSSL_VERSION), argv[1]);
    client_ctx = SSL_CTX_new(TLS_client_method());
    server_ctx = SSL_CTX_new(TLS_server_method());
    require(client_ctx && server_ctx, "SSL_CTX_new");
    require(SSL_CTX_set_min_proto_version(client_ctx, version) &&
            SSL_CTX_set_max_proto_version(client_ctx, version) &&
            SSL_CTX_set_min_proto_version(server_ctx, version) &&
            SSL_CTX_set_max_proto_version(server_ctx, version), "protocol version");
    SSL_CTX_set_verify(client_ctx, SSL_VERIFY_NONE, NULL);

    key  = EVP_PKEY_Q_keygen(NULL, NULL, "RSA", (size_t) 2048);
    cert = X509_new();
    require(key && cert, "certificate allocation");
    require(X509_set_version(cert, 2) &&
            ASN1_INTEGER_set(X509_get_serialNumber(cert), 1) &&
            X509_gmtime_adj(X509_getm_notBefore(cert), 0) &&
            X509_gmtime_adj(X509_getm_notAfter(cert), 3600) &&
            X509_set_pubkey(cert, key), "certificate fields");
    name = X509_get_subject_name(cert);
    require(name && X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                               (const unsigned char *) "localhost", -1, -1, 0) &&
            X509_set_issuer_name(cert, name), "certificate name");
    require(X509_sign(cert, key, EVP_sha256()) > 0 &&
            SSL_CTX_use_certificate(server_ctx, cert) &&
            SSL_CTX_use_PrivateKey(server_ctx, key) &&
            SSL_CTX_check_private_key(server_ctx), "certificate installation");
    X509_free(cert);
    EVP_PKEY_free(key);

    client = SSL_new(client_ctx);
    server = SSL_new(server_ctx);
    require(client && server, "SSL_new");
    require(BIO_new_bio_pair(&client_bio, 0, &server_bio, 0), "BIO_new_bio_pair");
    SSL_set_bio(client, client_bio, client_bio);
    SSL_set_bio(server, server_bio, server_bio);
    SSL_set_connect_state(client);
    SSL_set_accept_state(server);
    for (int i = 0; i < 64 &&
         (!SSL_is_init_finished(client) || !SSL_is_init_finished(server)); i++) {
        handshake(client);
        handshake(server);
    }
    require(SSL_is_init_finished(client) && SSL_is_init_finished(server),
            "handshake completion");
    require(SSL_write(client, "ping", 4) == 4 &&
            SSL_read(server, reply, sizeof(reply)) == 4 &&
            !memcmp(reply, "ping", 4), "encrypted request");
    require(SSL_write(server, "pong", 4) == 4 &&
            SSL_read(client, reply, sizeof(reply)) == 4 &&
            !memcmp(reply, "pong", 4), "encrypted response");
    SSL_free(client);
    SSL_free(server);
    SSL_CTX_free(client_ctx);
    SSL_CTX_free(server_ctx);
    return 0;
} /* main */
