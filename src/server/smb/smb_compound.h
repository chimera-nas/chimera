// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stdbool.h>
#include <stdint.h>

struct chimera_smb_compound;
struct chimera_smb_vfs_batch;

/* Called after the current command's dispatcher preamble. Returns nonzero if
 * an eligible run was submitted (completion may have occurred synchronously). */
int chimera_smb_vfs_compound_try(
    struct chimera_smb_compound *compound);
void chimera_smb_vfs_compound_cleanup(
    struct chimera_smb_compound *compound);
/* Owner-loop cancellation registry for submitted, single-command native CLOSE.
 * A multi-command batch is excluded: VFS cancellation is currently batch-wide.
 * Returns whether the target was found, including a late finish-time CANCEL
 * that the VFS safely ignores. Disconnect drains before connection recycling. */
struct chimera_smb_conn;
bool chimera_smb_vfs_cancel(
    struct chimera_smb_conn *conn,
    uint64_t                 session_id,
    uint64_t                 target_id,
    bool                     async);
void chimera_smb_vfs_cancel_drain(
    struct chimera_smb_conn *conn);

void chimera_smb_compound_complete_batch(
    struct chimera_smb_compound *compound,
    unsigned int                 count);

#include "vfs/vfs_compound.h"
#include "smb_namespace.h"

struct chimera_smb_request;
struct chimera_smb_open_file;
struct chimera_smb_file_id;
struct smb_vfs_command;

/* The overlay is private to one attempt. Handler checks may inspect/update it;
 * only core terminal completion publishes dirty fields after accepted finish. */
struct smb_vfs_open_state {
    struct chimera_smb_open_file          *open;
    struct smb_vfs_command                *producer;
    int                                    handle_from;
    int                                    available;
    int                                    closed;
    int                                    published;
    /* Borrowed attempt token: public open owns it, or producing RESERVE does. */
    struct chimera_vfs_claim_access_owner *access_owner;
    struct chimera_vfs_claim_access_owner *base_access_owner;
    struct chimera_vfs_claim_owner        *range_owner;
    int                                    range_owner_from;
    uint64_t                               position;
    uint32_t                               flags;
    uint32_t                               flags_dirty;
    uint16_t                               integrity_algo;
    uint32_t                               integrity_flags;
    uint8_t                                integrity_dirty;
    uint8_t                                lock_seq_valid[64];
    uint8_t                                lock_seq_index[64];
    uint32_t                               lock_seq_status[64];
    uint64_t                               lock_seq_dirty;
    uint16_t                               channel_sequence;
    uint8_t                                channel_sequence_valid;
    uint8_t                                position_dirty;
    uint8_t                                sequence_dirty;
    uint8_t                                end_replay;
};

/* Builders run once; prepare/complete and reset are replayable and pure.
 * publish runs after accepted finish, may transfer compound-owned results,
 * must not fail/allocate/perform filesystem I/O or send a response. release
 * destroys attempt storage once at terminal completion, not on retry. */
struct smb_vfs_command_ops {
    int                                (*eligible)(
        struct chimera_smb_request *request);
    /* Construction-only capability gate, after resolving/pinning a handle. */
    int                                (*bound_eligible)(
        struct smb_vfs_command *command);
    /* Accepted publication must precede execution of the following command. */
    bool                               (*ends_batch)(
        struct smb_vfs_command *command);
    /* A producer may admit a suffix before its optional cache grant is public.
     * Checked for every following wire command, before constructing its span.
     * Consumers use its private canonical actor until accepted publication. */
    bool                               (*private_suffix_eligible)(
        struct smb_vfs_command     *producer,
        struct chimera_smb_request *next);
    struct chimera_claim_owner (*private_actor_owner)(struct smb_vfs_command *producer);
    int                                creates_open;
    int                                (*initialize)(
        struct smb_vfs_command *command);
    struct chimera_smb_file_id (*file_id)(struct chimera_smb_request *request);
    unsigned int                       (*map_error)(
        enum chimera_vfs_error error);
    int                                (*build)(
        struct chimera_vfs_compound *,
        struct smb_vfs_command *);
    chimera_vfs_compound_op_callback_t prepare;
    chimera_vfs_compound_op_callback_t complete;
    void                               (*publish)(
        struct chimera_vfs_compound *,
        struct smb_vfs_command *);
    /* Input gathering happens once before construction; asynchronous output
     * only after acceptance. Each hook must call done exactly once, on the
     * owning event loop, and may do so inline. */
    void                               (*gather_inputs)(
        struct smb_vfs_command *,
        void ( *done )(struct smb_vfs_command *, unsigned int));
    void                               (*publish_async)(
        struct smb_vfs_command *,
        void ( *done )(struct smb_vfs_command *, unsigned int));
    void                               (*reset)(
        struct smb_vfs_command *);
    void                               (*release)(
        struct smb_vfs_command *);
    void                               (*reply_release)(
        struct smb_vfs_command *);
};

struct smb_vfs_command {
    struct chimera_smb_vfs_batch           *batch;
    const struct smb_vfs_command_ops       *ops;
    struct chimera_smb_request             *request;
    struct chimera_smb_open_file           *open;
    struct chimera_vfs_open_handle         *handle;
    struct smb_vfs_open_state              *state;
    struct chimera_claim_actor              actor;
    /* RENAME/LINK admission excludes a concurrent directory/replacement writer
     * without exposing protocol state. Released at attempt reset/terminal drain. */
    struct chimera_smb_namespace_open_token mutation_token;
    void                                   *private_data;
    unsigned int                            status;
    unsigned int                            input_status;
    int                                     publish_live;
    int                                     reply_data_owned;
    /* Accepted execution boundary: this command remains parsed for redispatch.
     * release must retain request-owned input, but free attempt-only storage. */
    int                                     deferred;
    unsigned int                            max_read;
    int                                     group;
    int                                     result;
    int                                     inherit_error;
    int                                     inherits_file;
    struct smb_vfs_open_state              *initial_state;
    struct chimera_smb_open_file           *owned_open;
    struct chimera_vfs_open_handle         *owned_handle;
    uint64_t                                saved_pid;
    uint64_t                                saved_vid;
};

extern const struct smb_vfs_command_ops chimera_smb_read_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_flush_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_lock_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_query_info_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_write_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_query_directory_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_set_info_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_query_security_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_set_security_compound_ops;

extern const struct smb_vfs_command_ops chimera_smb_create_compound_ops;
extern const struct smb_vfs_command_ops chimera_smb_close_compound_ops;
bool chimera_smb_close_compound_uses_doc(
    struct smb_vfs_command *command);
extern const struct smb_vfs_command_ops chimera_smb_rename_compound_ops;
void chimera_smb_compound_open_available(
    struct smb_vfs_command *command);
/* Pure execution-time boundary, before this command has filesystem or protocol
 * effects. Caller MUST fail its current operation to skip this group's tail;
 * later group checkpoints are skipped automatically. Only after
 * accepted finish and full attempt cleanup is it dispatched through legacy. */
void chimera_smb_compound_defer(
    struct smb_vfs_command *command);

/* Read-only execution check supplementing namespace-registry admission for
 * bounded directory rename. Every acquired handle, including private outputs,
 * is checked using the current attempt's path overlay. Unrelated identities
 * must remain root children, outside the directory being moved. */
bool chimera_smb_compound_directory_isolated(
    struct smb_vfs_command *command);

/* Additional file participant: no related-FileId or replay publication effects.
 * Pin at construction; validate/bind in operation prepare on every attempt.
 * The batch owns its lifetime through accepted/rejected terminal completion. */
struct smb_vfs_file {
    struct smb_vfs_file            *next;
    struct smb_vfs_command         *command;
    struct chimera_smb_open_file   *open;
    struct chimera_vfs_open_handle *handle;
    struct chimera_claim_actor      actor;
    struct smb_vfs_open_state      *state;
    struct chimera_smb_open_file   *owned_open;
    struct chimera_vfs_open_handle *owned_handle;
};
struct smb_vfs_file * chimera_smb_compound_pin_file(
    struct smb_vfs_command    *command,
    struct chimera_smb_file_id id);
unsigned int chimera_smb_compound_file_prepare(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_file         *file);
const struct smb_vfs_command_ops * chimera_smb_ioctl_compound_ops_for(
    struct chimera_smb_request *request);
