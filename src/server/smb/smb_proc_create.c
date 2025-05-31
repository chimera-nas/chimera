#include <sys/time.h>
#include <strings.h>
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "common/misc.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "smb_attr.h"

const uint8_t root_fh = CHIMERA_VFS_FH_MAGIC_ROOT;

static inline void
chimera_smb_create_mkdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request  = private_data;
    struct chimera_smb_compound      *compound = request->compound;
    struct chimera_server_smb_thread *thread   = compound->thread;
    struct chimera_smb_tree          *tree     = request->tree;
    struct chimera_smb_open_file     *open_file;
    uint64_t                          open_file_bucket;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    open_file = chimera_smb_open_file_alloc(thread);

    open_file->file_id.pid = ++tree->next_file_id;
    open_file->file_id.vid = chimera_rand64();
    open_file->handle      = oh;
    open_file->flags       = CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY;
    open_file->position    = 0;

    memcpy(open_file->name, request->create.name, request->create.name_len * 2);
    open_file->name_len = request->create.name_len;

    open_file_bucket = open_file->file_id.pid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_ADD(hh, tree->open_files[open_file_bucket], file_id, sizeof(open_file->file_id), open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    request->create.r_open_file = open_file;

    compound->saved_file_id = open_file->file_id;

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_create_open_parent_callback */

static inline void
chimera_smb_create_mkdir_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{

    struct chimera_smb_request       *request    = private_data;
    struct chimera_smb_compound      *compound   = request->compound;
    struct chimera_server_smb_thread *thread     = compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;

    chimera_vfs_release(vfs_thread, request->create.parent_handle);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    request->create.r_attrs.smb_attributes |= SMB2_FILE_ATTRIBUTE_DIRECTORY;

    chimera_vfs_open(
        vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH,
        chimera_smb_create_mkdir_open_callback,
        request);

} /* chimera_smb_create_mkdir_callback */

static inline void
chimera_smb_create_open_at_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_smb_compound      *compound   = request->compound;
    struct chimera_server_smb_thread *thread     = compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_smb_tree          *tree       = request->tree;
    struct chimera_smb_open_file     *open_file;
    uint64_t                          open_file_bucket;

    chimera_vfs_release(vfs_thread, request->create.parent_handle);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    open_file = chimera_smb_open_file_alloc(thread);

    open_file->file_id.pid = ++tree->next_file_id;
    open_file->file_id.vid = chimera_rand64();
    open_file->handle      = oh;
    open_file->flags       = 0;
    open_file->position    = 0;

    memcpy(open_file->name, request->create.name, request->create.name_len * 2);
    open_file->name_len = request->create.name_len;

    open_file_bucket = open_file->file_id.pid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_ADD(hh, tree->open_files[open_file_bucket], file_id, sizeof(open_file->file_id), open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    request->create.r_open_file = open_file;

    compound->saved_file_id = open_file->file_id;

    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_create_open_at_callback */

static inline void
chimera_smb_create_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    unsigned int                      flags      = 0;
    struct chimera_vfs_attrs          set_attr;
    const char                       *slash;
    const char                       *name;
    char                              path[CHIMERA_VFS_PATH_MAX];

    chimera_smb_utf16le_to_utf8(&thread->iconv_ctx,
                                request->create.name,
                                request->create.name_len,
                                path,
                                sizeof(path));

    slash = rindex(path, '\\');

    if (slash) {
        name = slash + 1;
    } else {
        name = path;
    }


    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("Open parent error_code %d", error_code);
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    request->create.parent_handle = oh;

    set_attr.va_req_mask = 0;
    set_attr.va_set_mask = 0;

    if ((request->create.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) &&
        (request->create.create_disposition == SMB2_FILE_CREATE ||
         request->create.create_disposition == SMB2_FILE_OPEN_IF)) {

        chimera_vfs_mkdir(
            vfs_thread,
            oh,
            name,
            strlen(name),
            &set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            0,
            chimera_smb_create_mkdir_callback,
            request);

    } else {
        switch (request->create.create_disposition) {
            case SMB2_FILE_SUPERSEDE:
                break;
            case SMB2_FILE_OPEN:
                break;
            case SMB2_FILE_OPEN_IF:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_CREATE:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_OVERWRITE:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_OVERWRITE_IF:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
        } /* switch */

        chimera_vfs_open_at(
            vfs_thread,
            oh,
            name,
            strlen(name),
            flags,
            &set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            0,
            chimera_smb_create_open_at_callback,
            request);
    }

} /* chimera_smb_create_open_at_callback */

static inline void
chimera_smb_create_lookup_parent_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    chimera_vfs_open(
        vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        chimera_smb_create_open_parent_callback,
        request);
} /* chimera_smb_create_lookup_parent_callback */

static inline void
chimera_smb_create_open_parent(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_smb_tree          *tree       = request->tree;
    const char                       *slash;
    char                              path[CHIMERA_VFS_PATH_MAX];
    int                               rc;

    rc = chimera_smb_utf16le_to_utf8(&thread->iconv_ctx,
                                     request->create.name,
                                     request->create.name_len,
                                     path,
                                     sizeof(path));

    if (unlikely(rc < 0)) {
        chimera_smb_error("Failed to convert create name to UTF-8");
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    slash = rindex(path, '\\');

    if (slash) {
        chimera_vfs_lookup_path(
            vfs_thread,
            tree->fh,
            tree->fh_len,
            path,
            slash - path,
            CHIMERA_VFS_ATTR_FH,
            chimera_smb_create_lookup_parent_callback,
            request);

    } else {
        chimera_vfs_open(
            vfs_thread,
            tree->fh,
            tree->fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
            chimera_smb_create_open_parent_callback,
            request);
    }
} /* chimera_smb_create_open_parent */

static void
chimera_smb_revalidate_tree_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    struct chimera_smb_tree    *tree    = request->tree;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("Revalidate error_code %d", error_code);
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    tree->fh_len = attr->va_fh_len;
    memcpy(&tree->fh, &attr->va_fh, attr->va_fh_len);

    clock_gettime(CLOCK_MONOTONIC, &tree->fh_expiration);
    tree->fh_expiration.tv_sec += 60;

    chimera_smb_create_open_parent(request);
} /* chimera_smb_revalidate_tree_callback */

static inline void
chimera_smb_revalidate_tree(
    struct chimera_smb_tree    *tree,
    struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;

    chimera_vfs_lookup_path(
        vfs_thread,
        &root_fh,
        1,
        tree->share->path,
        strlen(tree->share->path),
        CHIMERA_VFS_ATTR_FH,
        chimera_smb_revalidate_tree_callback,
        request);

} /* chimera_smb_revalidate_tree */

void
chimera_smb_create(struct chimera_smb_request *request)
{
    //struct chimera_server_smb_shared *shared = thread->shared;
    struct timespec now;

    if (unlikely(!request->tree)) {
        chimera_smb_error("Received SMB2 CREATE request for unknown tree id %u", request->smb2_hdr.sync.tree_id);
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    clock_gettime(CLOCK_MONOTONIC, &now);

    if (chimera_timespec_cmp(&now, &request->tree->fh_expiration) > 0) {
        chimera_smb_revalidate_tree(request->tree, request);
    } else {
        chimera_smb_create_open_parent(request);
    }
} /* chimera_smb_create */

void
chimera_smb_create_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_CREATE_REPLY_SIZE);

    /* Oplock level*/
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Flags */
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Create Action */

    if (request->create.create_disposition == SMB2_FILE_OPEN) {
        evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_CREATE_ACTION_OPENED);
    } else {
        evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_CREATE_ACTION_CREATED);
    }

    chimera_smb_append_network_open_info(reply_cursor, &request->create.r_attrs);

    /* File Id (persistent) */
    evpl_iovec_cursor_append_uint64(reply_cursor, request->create.r_open_file->file_id.pid);

    /* File Id (volatile) */
    evpl_iovec_cursor_append_uint64(reply_cursor, request->create.r_open_file->file_id.vid);

    /* Create Context Offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

    /* Create Context Length */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

    /* Ea Error Offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

} /* chimera_smb_create_reply */

int
chimera_smb_parse_create(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t name_offset;
    uint32_t blob_offset, blob_length;

    if (unlikely(request->request_struct_size != SMB2_CREATE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CREATE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_CREATE_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint8(request_cursor, &request->create.requested_oplock_level);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.impersonation_level);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->create.flags);
    evpl_iovec_cursor_skip(request_cursor, 8);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.desired_access);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.file_attributes);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.share_access);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.create_disposition);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.create_options);
    evpl_iovec_cursor_get_uint16(request_cursor, &name_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->create.name_len);
    evpl_iovec_cursor_get_uint32(request_cursor, &blob_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &blob_length);

    if (request->create.name_len >= sizeof(request->create.name)) {
        chimera_smb_error("Create request: UTF-16 name too long (%u bytes, buffer %zu bytes)",
                          request->create.name_len, sizeof(request->create.name));
        return -1;
    }

    evpl_iovec_cursor_copy(request_cursor, request->create.name, request->create.name_len);
    request->create.name_len >>= 1;

    return 0;
} /* chimera_smb_parse_create */
