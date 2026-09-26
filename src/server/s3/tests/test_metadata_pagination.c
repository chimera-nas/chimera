// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise the real metadata callbacks with real compound builders and
 * controlled backend page results. No HTTP response or filesystem is needed:
 * this pins pagination, source bindings, suffix ordering and attempt reset,
 * independently of whether the available backend happens to paginate xattrs.
 * Including the implementation also lets us inspect its private staged result
 * without exporting a test-only production API. */
#include "../s3_metadata.c"

#define CHECK(c) do { if (!(c)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); \
                      } } while (0)

struct continuation {
    int calls;
    int marker;
};

static int
after_metadata(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct continuation *after = private_data;

    after->calls++;
    after->marker = chimera_vfs_compound_add_checkpoint(compound);
    return after->marker < 0 ? -1 : 0;
} /* after_metadata */

static enum chimera_vfs_error
list_result(
    struct chimera_vfs_compound *compound,
    int                          index,
    const char                  *names,
    size_t                       length,
    int                          eof,
    uint64_t                     cookie,
    enum chimera_vfs_error       status)
{
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);

    CHECK(op && op->type == CHIMERA_VFS_COMPOUND_OP_LISTXATTRS);
    CHECK(op->complete);
    op->buffer = malloc(length ? length : 1);
    CHECK(op->buffer);
    if (length) {
        memcpy(op->buffer, names, length);
    }
    op->buffer_len = length;
    op->eof        = eof;
    op->r_cookie   = cookie;
    op->complete(compound, index, &status, op->callback_private);
    return status;
} /* list_result */

static int
last_list(struct chimera_vfs_compound *compound)
{
    for (int i = (int) chimera_vfs_compound_num_ops(compound) - 1; i >= 0; i--) {
        if (chimera_vfs_compound_op(compound, i)->type == CHIMERA_VFS_COMPOUND_OP_LISTXATTRS) {
            return i;
        }
    }
    abort();
} /* last_list */

static void
get_result(
    struct chimera_vfs_compound *compound,
    int                          index,
    const char                  *value)
{
    struct chimera_vfs_compound_op *op     = chimera_vfs_compound_op_args(compound, index);
    enum chimera_vfs_error          status = CHIMERA_VFS_OK;

    CHECK(op && op->type == CHIMERA_VFS_COMPOUND_OP_GETXATTR);
    op->buffer = (uint8_t *) strdup(value);
    CHECK(op->buffer);
    op->buffer_len = strlen(value);
    op->complete(compound, index, &status, op->callback_private);
    CHECK(status == CHIMERA_VFS_OK);
} /* get_result */

static void
test_pages(
    struct chimera_vfs_thread *thread,
    int                        copying)
{
    static const char              first[]  = "user.unrelated\0user.s3.meta.first\0user.s3.tag.one\0";
    static const char              second[] = "user.s3.meta.second\0user.s3.tag.two\0";
    static const char             *values[] = { "first-value", "first-tag", "second-value", "second-tag" };
    struct chimera_vfs_open_handle source = { 0 }, destination = { 0 };
    struct chimera_s3_metadata    *metadata = chimera_s3_metadata_read_alloc();
    struct chimera_vfs_compound   *compound = chimera_vfs_compound_alloc(thread, NULL);
    struct continuation            after    = { 0 };
    int                            first_index, next;

    CHECK(metadata && compound);
    if (copying) {
        first_index = chimera_s3_metadata_append_copy(compound, metadata,
                                                      &source, &destination,
                                                      after_metadata, &after);
    } else {
        first_index = chimera_s3_metadata_append_read(compound, metadata, after_metadata, &after);
    }
    CHECK(first_index >= 0);
    CHECK(list_result(compound, first_index, first, sizeof(first) - 1, 0, 17,
                      CHIMERA_VFS_OK) == CHIMERA_VFS_OK);
    CHECK(after.calls == 0 && metadata->count == 2 && metadata->tag_count == 1);
    get_result(compound, metadata->indices[0], values[0]);
    get_result(compound, metadata->indices[1], values[1]);
    next = last_list(compound);
    CHECK(next != first_index);
    CHECK(chimera_vfs_compound_op(compound, next)->cookie == 17);
    if (copying) {
        CHECK(chimera_vfs_compound_op(compound, next)->in_handle == &source);
    } else {
        CHECK(chimera_vfs_compound_op(compound, next)->handle_from == metadata->handle_result);
    }
    /* Retrying an oversized later page must retain all earlier page values. */
    CHECK(list_result(compound, next, NULL, 0, 0, 17, CHIMERA_VFS_ERANGE) == CHIMERA_VFS_OK);
    next = last_list(compound);
    CHECK(chimera_vfs_compound_op(compound, next)->cookie == 17);
    CHECK(chimera_vfs_compound_op(compound, next)->buffer_max == CHIMERA_S3_META_LIST_SIZE * 2);
    CHECK(after.calls == 0 && metadata->count == 2 && metadata->valid[0] && metadata->valid[1]);
    CHECK(list_result(compound, next, second, sizeof(second) - 1, 1, 23,
                      CHIMERA_VFS_OK) == CHIMERA_VFS_OK);
    CHECK(after.calls == 1 && metadata->count == 4 && metadata->tag_count == 2);
    CHECK(metadata->valid[0] && metadata->valid[1]);
    get_result(compound, metadata->indices[2], values[2]);
    get_result(compound, metadata->indices[3], values[3]);
    CHECK(after.marker > metadata->indices[3]);
    if (copying) {
        int sets = 0;
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
            struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, i);
            if (op->type == CHIMERA_VFS_COMPOUND_OP_SETXATTR) {
                enum chimera_vfs_error status = CHIMERA_VFS_OK;
                op->prepare(compound, i, &status, op->prepare_private);
                CHECK(status == CHIMERA_VFS_OK && !op->skipped);
                CHECK(op->in_handle == &destination);
                CHECK(sets < 4);
                CHECK(op->xattr_value_len == strlen(values[sets]));
                CHECK(!memcmp(op->xattr_value, values[sets], op->xattr_value_len));
                sets++;
            }
        }
        CHECK(sets == 4);
    }
    chimera_vfs_compound_free(compound);

    /* A replay starts at cookie zero against a possibly different list. The
     * old page results and validity bits must not survive into this attempt. */
    compound    = chimera_vfs_compound_alloc(thread, NULL);
    first_index = chimera_s3_metadata_append_read(compound, metadata, after_metadata, &after);
    CHECK(list_result(compound, first_index, second, sizeof(second) - 1, 1, 0,
                      CHIMERA_VFS_OK) == CHIMERA_VFS_OK);
    CHECK(metadata->count == 2 && metadata->tag_count == 1 && !metadata->valid[0] && !metadata->valid[1]);
    CHECK(!strcmp(chimera_vfs_compound_op(compound, metadata->indices[0])->name, "user.s3.meta.second"));
    chimera_vfs_compound_free(compound);
    chimera_s3_metadata_free(metadata);
} /* test_pages */

static void
test_large_list_and_invalid_cookie(struct chimera_vfs_thread *thread)
{
    struct chimera_s3_metadata  *metadata = chimera_s3_metadata_read_alloc();
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(thread, NULL);
    struct continuation          after    = { 0 };
    int                          index    = chimera_s3_metadata_append_read(compound, metadata, after_metadata, &after);
    uint32_t                     size     = CHIMERA_S3_META_LIST_SIZE;
    char                        *names;
    size_t                       used = 0;

    CHECK(list_result(compound, index, NULL, 0, 0, 0, CHIMERA_VFS_ERANGE) == CHIMERA_VFS_OK);
    index = last_list(compound);
    CHECK(chimera_vfs_compound_op(compound, index)->buffer_max == size * 2);
    CHECK(after.calls == 0);
    names = malloc(size * 2);
    CHECK(names);
    while (used < size + 1024) {
        memcpy(names + used, "user.padding", sizeof("user.padding"));
        used += sizeof("user.padding");
    }
    memcpy(names + used, "user.s3.meta.last", sizeof("user.s3.meta.last"));
    used += sizeof("user.s3.meta.last");
    CHECK(list_result(compound, index, names, used, 1, 0, CHIMERA_VFS_OK) == CHIMERA_VFS_OK);
    CHECK(after.calls == 1 && metadata->count == 1);
    CHECK(!strcmp(chimera_vfs_compound_op(compound, metadata->indices[0])->name, "user.s3.meta.last"));
    free(names);
    chimera_vfs_compound_free(compound);

    compound = chimera_vfs_compound_alloc(thread, NULL);
    index    = chimera_s3_metadata_append_read(compound, metadata, after_metadata, &after);
    CHECK(list_result(compound, index, NULL, 0, 0, 0, CHIMERA_VFS_OK) == CHIMERA_VFS_EIO);
    CHECK(after.calls == 1);
    chimera_vfs_compound_free(compound);

    compound = chimera_vfs_compound_alloc(thread, NULL);
    index    = chimera_s3_metadata_append_read(compound, metadata, after_metadata, &after);
    while (size < CHIMERA_S3_META_LIST_MAX) {
        CHECK(list_result(compound, index, NULL, 0, 0, 0, CHIMERA_VFS_ERANGE) == CHIMERA_VFS_OK);
        index = last_list(compound);
        size *= 2;
        CHECK(chimera_vfs_compound_op(compound, index)->buffer_max == size);
    }
    CHECK(list_result(compound, index, NULL, 0, 0, 0, CHIMERA_VFS_ERANGE) == CHIMERA_VFS_ERANGE);
    CHECK(after.calls == 1);
    chimera_vfs_compound_free(compound);
    chimera_s3_metadata_free(metadata);
} /* test_large_list_and_invalid_cookie */

int
main(void)
{
    struct chimera_vfs_thread *thread = calloc(1, sizeof(*thread));

    CHECK(thread);
    /* No executor or thread pool is active; force free() to release builders
     * rather than retain them in this isolated thread's recycling pool. */
    thread->num_free_compounds = UINT32_MAX;
    test_pages(thread, 0);
    test_pages(thread, 1);
    test_large_list_and_invalid_cookie(thread);
    free(thread);
    puts("S3 metadata pagination tests passed");
    return 0;
} /* main */
