#include "nfs4_dump.h"
#include "nfs_internal.h"

void
_nfs4_dump_null(struct nfs_request *req)
{
    chimera_nfs_debug("NFS4 Request %p: Null", req);
} /* nfs4_dump_null */

void
_nfs4_dump_compound(
    struct nfs_request   *req,
    struct COMPOUND4args *args)
{
    int i;

    chimera_nfs_debug("NFS4 Request %p: Compound", req);

    for (i = 0; i < args->num_argarray; i++) {
        switch (args->argarray[i].argop) {
            case OP_ACCESS:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Access",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_CLOSE:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Close",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_COMMIT:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Commit offset=%lu count=%u",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opcommit.offset,
                                  args->argarray[i].opcommit.count);
                break;

            case OP_READ:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Read offset=%lu count=%u",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opread.offset,
                                  args->argarray[i].opread.count);
                break;

            case OP_WRITE:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Write offset=%lu stable=%d",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opwrite.offset,
                                  args->argarray[i].opwrite.stable);
                break;

            case OP_CREATE:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Create name=%.*s",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opcreate.objname.len,
                                  args->argarray[i].opcreate.objname.data);
                break;

            case OP_OPEN:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Open access=%u deny=%u",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opopen.share_access,
                                  args->argarray[i].opopen.share_deny);
                break;

            case OP_SETATTR:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Setattr",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_RENAME:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Rename old=%.*s new=%.*s",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].oprename.oldname.len,
                                  args->argarray[i].oprename.oldname.data,
                                  args->argarray[i].oprename.newname.len,
                                  args->argarray[i].oprename.newname.data);
                break;

            case OP_REMOVE:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Remove target=%.*s",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opremove.target.len,
                                  args->argarray[i].opremove.target.data);
                break;

            case OP_PUTFH:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d PutFH",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_SAVEFH:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d SaveFH",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_READDIR:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d ReadDir count=%u",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opreaddir.maxcount);
                break;

            case OP_DELEGPURGE:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d DelegPurge clientid=%llu",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opdelegpurge.clientid);
                break;

            case OP_DELEGRETURN:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d DelegReturn",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_GETATTR:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d GetAttr",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_GETFH:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d GetFH",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_LINK:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Link name=%.*s",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].oplink.newname.len,
                                  args->argarray[i].oplink.newname.data);
                break;

            case OP_LOCK:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Lock type=%d",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].oplock.locktype);
                break;

            case OP_LOCKT:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d LockT type=%d",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].oplockt.locktype);
                break;

            case OP_LOCKU:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d LockU type=%d",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].oplocku.locktype);
                break;
            case OP_LOOKUP:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Lookup name=%.*s",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].oplookup.objname.len,
                                  args->argarray[i].oplookup.objname.data);
                break;

            case OP_LOOKUPP:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d LookupP",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_NVERIFY:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d NVerify",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_OPENATTR:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d OpenAttr createdir=%d",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opopenattr.createdir);
                break;

            case OP_OPEN_DOWNGRADE:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d OpenDowngrade access=%u deny=%u",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].opopen_downgrade.share_access,
                                  args->argarray[i].opopen_downgrade.share_deny);
                break;

            case OP_PUTPUBFH:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d PutPubFH",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_PUTROOTFH:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d PutRootFH",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_READLINK:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d ReadLink",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_VERIFY:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Verify",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_OPEN_CONFIRM:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d OpenConfirm",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_RENEW:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Renew",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_SETCLIENTID:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d SetClientID",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_SETCLIENTID_CONFIRM:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d SetClientIDConfirm",
                                  req, i + 1, args->num_argarray);
                break;

            case OP_RELEASE_LOCKOWNER:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d ReleaseLockOwner",
                                  req, i + 1, args->num_argarray);
                break;

            default:
                chimera_nfs_debug("NFS4 Request %p: %02d/%02d Unknown op=%d",
                                  req, i + 1, args->num_argarray,
                                  args->argarray[i].argop);
                break;
        } /* switch */
    }
} /* nfs4_dump_compound */

