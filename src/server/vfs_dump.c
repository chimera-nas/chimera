#include <string.h>
#include <errno.h>

#include "vfs_dump.h"
#include "vfs.h"
#include "common.h"

#define CHIMERA_VFS_OP_LOOKUP_PATH  1
#define CHIMERA_VFS_OP_LOOKUP       2
#define CHIMERA_VFS_OP_GETATTR      3
#define CHIMERA_VFS_OP_READDIR      4
#define CHIMERA_VFS_OP_READLINK     5
#define CHIMERA_VFS_OP_OPEN         6
#define CHIMERA_VFS_OP_OPEN_AT      7
#define CHIMERA_VFS_OP_CLOSE        8
#define CHIMERA_VFS_OP_READ         9
#define CHIMERA_VFS_OP_WRITE        10

static const char *
chimera_vfs_op_name(unsigned int opcode)
{
    switch (opcode) {
    case CHIMERA_VFS_OP_LOOKUP_PATH: return "LookupPath";
    case CHIMERA_VFS_OP_LOOKUP: return "Lookup";
    case CHIMERA_VFS_OP_GETATTR: return "GetAttr";
    case CHIMERA_VFS_OP_READDIR: return "ReadDir";
    case CHIMERA_VFS_OP_READLINK: return "ReadLink";
    case CHIMERA_VFS_OP_OPEN: return "Open";
    case CHIMERA_VFS_OP_OPEN_AT: return "OpenAt";
    case CHIMERA_VFS_OP_CLOSE: return "Close";
    case CHIMERA_VFS_OP_READ: return "Read";
    case CHIMERA_VFS_OP_WRITE: return "Write";
    case CHIMERA_VFS_OP_REMOVE: return "Remove";
    case CHIMERA_VFS_OP_MKDIR: return "Mkdir";
    default: return "Unknown";
    }

}

void
chimera_vfs_dump_request(struct chimera_vfs_request *req)
{
    chimera_log("VFS Request %p: %s",
                req,
                chimera_vfs_op_name(req->opcode));
}
void
chimera_vfs_dump_reply(struct chimera_vfs_request *req)
{
    chimera_log("VFS Reply   %p: %s status %d (%s)",
                req,
                chimera_vfs_op_name(req->opcode),
                req->status,
                req->status ? strerror(req->status) : "OK");
}

