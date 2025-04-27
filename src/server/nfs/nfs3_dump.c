#include <stdio.h>
#include <string.h>
#include "nfs3_dump.h"
#include "common/format.h"
#include "common/snprintf.h"
#include "nfs_internal.h"

void
_nfs3_dump_null(struct nfs_request *req)
{
    chimera_nfs_debug("NFS3 Request %p: Null", req);
} /* nfs3_dump_null */

void
_nfs3_dump_getattr(
    struct nfs_request  *req,
    struct GETATTR3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->object.data.data, args->object.data.len);
    chimera_nfs_debug("NFS3 Request %p: GetAttr fh %s", req, fhstr);
} /* nfs3_dump_getattr */

void
_nfs3_dump_setattr(
    struct nfs_request  *req,
    struct SETATTR3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->object.data.data, args->object.data.len);
    chimera_nfs_debug("NFS3 Request %p: SetAttr fh %s mode %o uid %u gid %u size %lu",
                      req,
                      fhstr,
                      args->new_attributes.mode.set_it ? args->new_attributes.mode.mode : 0,
                      args->new_attributes.uid.set_it ? args->new_attributes.uid.uid : 0,
                      args->new_attributes.gid.set_it ? args->new_attributes.gid.gid : 0,
                      args->new_attributes.size.set_it ? args->new_attributes.size.size : 0);
} /* nfs3_dump_setattr */

void
_nfs3_dump_lookup(
    struct nfs_request *req,
    struct LOOKUP3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->what.dir.data.data, args->what.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Lookup dir %s name %.*s",
                      req,
                      fhstr,
                      args->what.name.len,
                      args->what.name.str);
} /* nfs3_dump_lookup */

void
_nfs3_dump_access(
    struct nfs_request *req,
    struct ACCESS3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->object.data.data, args->object.data.len);
    chimera_nfs_debug("NFS3 Request %p: Access fh %s access %u",
                      req,
                      fhstr,
                      args->access);
} /* nfs3_dump_access */

void
_nfs3_dump_readlink(
    struct nfs_request   *req,
    struct READLINK3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->symlink.data.data, args->symlink.data.len);
    chimera_nfs_debug("NFS3 Request %p: ReadLink fh %s",
                      req,
                      fhstr);
} /* nfs3_dump_readlink */

void
_nfs3_dump_read(
    struct nfs_request *req,
    struct READ3args   *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->file.data.data, args->file.data.len);
    chimera_nfs_debug("NFS3 Request %p: Read fh %s offset %lu count %u",
                      req,
                      fhstr,
                      args->offset,
                      args->count);
} /* nfs3_dump_read */

void
_nfs3_dump_write(
    struct nfs_request *req,
    struct WRITE3args  *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->file.data.data, args->file.data.len);
    chimera_nfs_debug("NFS3 Request %p: Write fh %s offset %lu count %u stable %d",
                      req,
                      fhstr,
                      args->offset,
                      args->count,
                      args->stable);
} /* nfs3_dump_write */

void
_nfs3_dump_create(
    struct nfs_request *req,
    struct CREATE3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->where.dir.data.data, args->where.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Create dir %s name %.*s mode %o",
                      req,
                      fhstr,
                      args->where.name.len,
                      args->where.name.str,
                      args->how.mode);
} /* nfs3_dump_create */

void
_nfs3_dump_mkdir(
    struct nfs_request *req,
    struct MKDIR3args  *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->where.dir.data.data, args->where.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Mkdir dir %s name %.*s mode %o",
                      req,
                      fhstr,
                      args->where.name.len,
                      args->where.name.str,
                      args->attributes.mode.mode);
} /* nfs3_dump_mkdir */

void
_nfs3_dump_symlink(
    struct nfs_request  *req,
    struct SYMLINK3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->where.dir.data.data, args->where.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Symlink dir %s name %.*s target %.*s",
                      req,
                      fhstr,
                      args->where.name.len,
                      args->where.name.str,
                      args->symlink.symlink_data.len,
                      args->symlink.symlink_data.str);
} /* nfs3_dump_symlink */

void
_nfs3_dump_mknod(
    struct nfs_request *req,
    struct MKNOD3args  *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->where.dir.data.data, args->where.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Mknod dir %s name %.*s type %d",
                      req,
                      fhstr,
                      args->where.name.len,
                      args->where.name.str,
                      args->what.type);
} /* nfs3_dump_mknod */

void
_nfs3_dump_remove(
    struct nfs_request *req,
    struct REMOVE3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->object.dir.data.data, args->object.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Remove dir %s name %.*s",
                      req,
                      fhstr,
                      args->object.name.len,
                      args->object.name.str);
} /* nfs3_dump_remove */

void
_nfs3_dump_rmdir(
    struct nfs_request *req,
    struct RMDIR3args  *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->object.dir.data.data, args->object.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Rmdir dir %s name %.*s",
                      req,
                      fhstr,
                      args->object.name.len,
                      args->object.name.str);
} /* nfs3_dump_rmdir */

void
_nfs3_dump_rename(
    struct nfs_request *req,
    struct RENAME3args *args)
{
    char from_fhstr[80], to_fhstr[80];

    format_hex(from_fhstr, sizeof(from_fhstr), args->from.dir.data.data, args->from.dir.data.len);
    format_hex(to_fhstr, sizeof(to_fhstr), args->to.dir.data.data, args->to.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Rename from_dir %s from_name %.*s to_dir %s to_name %.*s",
                      req,
                      from_fhstr,
                      args->from.name.len,
                      args->from.name.str,
                      to_fhstr,
                      args->to.name.len,
                      args->to.name.str);
} /* nfs3_dump_rename */

void
_nfs3_dump_link(
    struct nfs_request *req,
    struct LINK3args   *args)
{
    char file_fhstr[80], dir_fhstr[80];

    format_hex(file_fhstr, sizeof(file_fhstr), args->file.data.data, args->file.data.len);
    format_hex(dir_fhstr, sizeof(dir_fhstr), args->link.dir.data.data, args->link.dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: Link file %s dir %s name %.*s",
                      req,
                      file_fhstr,
                      dir_fhstr,
                      args->link.name.len,
                      args->link.name.str);
} /* nfs3_dump_link */

void
_nfs3_dump_readdir(
    struct nfs_request  *req,
    struct READDIR3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->dir.data.data, args->dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: ReadDir dir %s cookie %lu count %u",
                      req,
                      fhstr,
                      args->cookie,
                      args->count);
} /* nfs3_dump_readdir */

void
_nfs3_dump_readdirplus(
    struct nfs_request      *req,
    struct READDIRPLUS3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->dir.data.data, args->dir.data.len);
    chimera_nfs_debug("NFS3 Request %p: ReadDirPlus dir %s cookie %lu dircount %u maxcount %u",
                      req,
                      fhstr,
                      args->cookie,
                      args->dircount,
                      args->maxcount);
} /* nfs3_dump_readdirplus */

void
_nfs3_dump_fsstat(
    struct nfs_request *req,
    struct FSSTAT3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->fsroot.data.data, args->fsroot.data.len);
    chimera_nfs_debug("NFS3 Request %p: FsStat root %s",
                      req,
                      fhstr);
} /* nfs3_dump_fsstat */

void
_nfs3_dump_fsinfo(
    struct nfs_request *req,
    struct FSINFO3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->fsroot.data.data, args->fsroot.data.len);
    chimera_nfs_debug("NFS3 Request %p: FsInfo root %s",
                      req,
                      fhstr);
} /* nfs3_dump_fsinfo */

void
_nfs3_dump_pathconf(
    struct nfs_request   *req,
    struct PATHCONF3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->object.data.data, args->object.data.len);
    chimera_nfs_debug("NFS3 Request %p: PathConf fh %s",
                      req,
                      fhstr);
} /* nfs3_dump_pathconf */

void
_nfs3_dump_commit(
    struct nfs_request *req,
    struct COMMIT3args *args)
{
    char fhstr[80];

    format_hex(fhstr, sizeof(fhstr), args->file.data.data, args->file.data.len);
    chimera_nfs_debug("NFS3 Request %p: Commit fh %s offset %lu count %u",
                      req,
                      fhstr,
                      args->offset,
                      args->count);
} /* nfs3_dump_commit */