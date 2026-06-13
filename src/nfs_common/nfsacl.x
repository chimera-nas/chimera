/*
 * SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * NFSACL: the unofficial NFSv3 ACL sideband protocol (RPC program 100227,
 * version 3) originated by Sun for Solaris and implemented by the Linux kernel
 * for interop.  It carries POSIX.1e draft ACLs (access + default) so that
 * getfacl/setfacl/ls work over NFSv3.  Never standardised in an RFC.
 *
 * xdrzcc compiles each .x independently and emits a self-contained header, so
 * the few NFSv3 types this program reuses (file handle, post-op attrs, status)
 * are redeclared here with an "nacl_" prefix to avoid colliding with the
 * identically-named declarations in nfs3_xdr.h -- both headers are co-included
 * by the server's nfs_common.h.  Layouts are byte-identical to nfs3.x; keep
 * them in sync.  (nfs4.x coexists with nfs3.x the same way, via unique names.)
 */

const NFSACL_FHSIZE = 64;

enum nacl_nfsstat3 {
 NACL_NFS3_OK             = 0,
 NACL_NFS3ERR_PERM        = 1,
 NACL_NFS3ERR_NOENT       = 2,
 NACL_NFS3ERR_IO          = 5,
 NACL_NFS3ERR_ACCES       = 13,
 NACL_NFS3ERR_INVAL       = 22,
 NACL_NFS3ERR_NOTSUPP     = 10004,
 NACL_NFS3ERR_SERVERFAULT = 10006
};

enum nacl_ftype3 {
 NACL_NF3REG  = 1,
 NACL_NF3DIR  = 2,
 NACL_NF3BLK  = 3,
 NACL_NF3CHR  = 4,
 NACL_NF3LNK  = 5,
 NACL_NF3SOCK = 6,
 NACL_NF3FIFO = 7
};

struct nacl_specdata3 {
 unsigned int specdata1;
 unsigned int specdata2;
};

struct nfsacl_fh3 {
 opaque data<NFSACL_FHSIZE>;
};

struct nacl_nfstime3 {
 unsigned int seconds;
 unsigned int nseconds;
};

struct nacl_fattr3 {
 nacl_ftype3    type;
 unsigned int   mode;
 unsigned int   nlink;
 unsigned int   uid;
 unsigned int   gid;
 uint64_t       size;
 uint64_t       used;
 nacl_specdata3 rdev;
 uint64_t       fsid;
 uint64_t       fileid;
 nacl_nfstime3  atime;
 nacl_nfstime3  mtime;
 nacl_nfstime3  ctime;
};

union nacl_post_op_attr switch (bool attributes_follow) {
 case TRUE:
  nacl_fattr3 attributes;
 case FALSE:
  void;
};

/*
 * One POSIX.1e ACL entry on the wire: three uint32 words (tag, id, perm),
 * matching Linux fs/nfs_common/nfsacl.c (xdr_nfsace_encode).  For default-ACL
 * entries the tag word additionally carries the NFS_ACL_DEFAULT (0x1000) flag.
 */
struct nfsacl_entry {
 unsigned int tag;
 unsigned int id;
 unsigned int perm;
};

/*
 * The access and default ACLs are each a count-prefixed array.  Real clients
 * (Linux, Solaris) always request entries (NFS_ACL|NFS_ACLCNT together), so the
 * leading count equals the array length, which is exactly what an xdrzcc
 * var-array encodes.  The pure count-only query (NFS_ACLCNT without NFS_ACL) is
 * not separately supported -- chimera always returns entries when either bit is
 * set and echoes the satisfied mask.
 */
struct GETACL3args {
 nfsacl_fh3   fh;
 unsigned int mask;
};

/*
 * Each ACL is encoded as Linux fs/nfs_common/nfsacl.c does: an explicit entry
 * count word, followed by an XDR counted array (which carries its own length
 * prefix).  The decoder requires the two counts to match (they differ only in
 * the count-only NFS_ACLCNT query, which real clients never issue without
 * NFS_ACL).  Hence the explicit *_count field in front of each <> array.
 */
struct GETACL3resok {
 nacl_post_op_attr attr;
 unsigned int      mask;
 unsigned int      acl_access_count;
 nfsacl_entry      acl_access<>;
 unsigned int      acl_default_count;
 nfsacl_entry      acl_default<>;
};

union GETACL3res switch (nacl_nfsstat3 status) {
 case NACL_NFS3_OK:
  GETACL3resok resok;
 default:
  nacl_post_op_attr attr;
};

struct SETACL3args {
 nfsacl_fh3   fh;
 unsigned int mask;
 unsigned int acl_access_count;
 nfsacl_entry acl_access<>;
 unsigned int acl_default_count;
 nfsacl_entry acl_default<>;
};

struct SETACL3resok {
 nacl_post_op_attr attr;
};

union SETACL3res switch (nacl_nfsstat3 status) {
 case NACL_NFS3_OK:
  SETACL3resok resok;
 default:
  void;
};

program NFSACL {
 version NFSACL_V3 {
  void          NFSACLPROC3_NULL(void)             = 0;
  GETACL3res    NFSACLPROC3_GETACL(GETACL3args)    = 1;
  SETACL3res    NFSACLPROC3_SETACL(SETACL3args)    = 2;
 } = 3;
} = 100227;
