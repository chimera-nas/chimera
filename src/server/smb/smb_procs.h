// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

void
chimera_smb_complete_request(
    struct chimera_smb_request *request,
    unsigned int                status);

int chimera_smb_parse_negotiate(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_negotiate_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_session_setup(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_session_setup_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_tree_connect(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_tree_connect_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_create(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_create_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_close(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_close_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_write(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_write_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_read(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_read_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_flush(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_flush_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_ioctl(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_ioctl_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_query_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_query_directory(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_query_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_query_directory_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_set_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_set_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_tree_disconnect(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_tree_disconnect_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

int chimera_smb_parse_logoff(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_logoff_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_negotiate(
    struct chimera_smb_request *request);

void chimera_smb_session_setup(
    struct chimera_smb_request *request);

void chimera_smb_logoff(
    struct chimera_smb_request *request);

void chimera_smb_tree_connect(
    struct chimera_smb_request *request);

void chimera_smb_tree_disconnect(
    struct chimera_smb_request *request);

void chimera_smb_create(
    struct chimera_smb_request *request);

void chimera_smb_close(
    struct chimera_smb_request *request);

void chimera_smb_write(
    struct chimera_smb_request *request);

void chimera_smb_read(
    struct chimera_smb_request *request);

void chimera_smb_flush(
    struct chimera_smb_request *request);

void chimera_smb_ioctl(
    struct chimera_smb_request *request);

void chimera_smb_query_info(
    struct chimera_smb_request *request);

void chimera_smb_query_directory(
    struct chimera_smb_request *request);

void chimera_smb_set_info(
    struct chimera_smb_request *request);

void chimera_smb_set_security(
    struct chimera_smb_request *request);

void chimera_smb_query_security(
    struct chimera_smb_request *request);

void chimera_smb_query_security_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_parse_sd_to_attrs(
    const uint8_t            *sd_buf,
    uint32_t                  sd_len,
    struct chimera_vfs_attrs *attrs);

int chimera_smb_parse_echo(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request);

void chimera_smb_echo_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request);

void chimera_smb_echo(
    struct chimera_smb_request *request);
