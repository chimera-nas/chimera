#ifndef __VFS_DUMP_H__
#define __VFS_DUMP_H__

struct chimera_vfs_request;

void chimera_vfs_dump_request(struct chimera_vfs_request *request);
void chimera_vfs_dump_reply(struct chimera_vfs_request *request);

#endif
