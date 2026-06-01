
/*
 * SPDX-FileCopyrightText: IETF Trust and the persons identified
 * SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 * SPDX-License-Identifier: BSD-3-Clause
 */
 
const MNTPATHLEN = 1024;  /* Maximum bytes in a path name */
const MNTNAMLEN  = 255;   /* Maximum bytes in a name */
const FHSIZE3    = 64;

enum mountstat3 {
 MNT3_OK = 0,                 /* no error */
 MNT3ERR_PERM = 1,            /* Not owner */
 MNT3ERR_NOENT = 2,           /* No such file or directory */
 MNT3ERR_IO = 5,              /* I/O error */
 MNT3ERR_ACCES = 13,          /* Permission denied */
 MNT3ERR_NOTDIR = 20,         /* Not a directory */
 MNT3ERR_INVAL = 22,          /* Invalid argument */
 MNT3ERR_NAMETOOLONG = 63,    /* Filename too long */
 MNT3ERR_NOTSUPP = 10004,     /* Operation not supported */
 MNT3ERR_SERVERFAULT = 10006  /* A failure on the server */
};

typedef opaque fhandle3<FHSIZE3>;


struct groupnode {
 string name<MNTNAMLEN>;
 groupnode *nextgroup;
};

struct exportnode {
 string ex_dir<MNTPATHLEN>;
 groupnode *ex_groups;
 exportnode *ex_next;
};

struct exportres {
  exportnode *exports;
};

struct mountbody {
 string ml_hostname<MNTNAMLEN>;
 string ml_directory<MNTPATHLEN>;
 mountbody *ml_next;
};

/*
 * RFC 1813 declares MOUNTPROC3_DUMP as returning a bare "mountlist"
 * (an optional mountbody pointer).  xdrzcc does not emit the leading
 * "value-follows" boolean for a procedure that returns a bare optional
 * pointer, so wrap it in a one-field struct -- the wire encoding of an
 * optional pointer inside a struct is identical to RFC's mountlist
 * (bool + chained mountbody), and the wrapper also lets DUMP return an
 * empty list (mounts == NULL) without a NULL deref.
 */
struct mountdumpres {
 mountbody *mounts;
};

struct mountres3_ok {
 fhandle3   fhandle;
 int        auth_flavors<>;
};

struct mountarg3 {
    string path<MNTPATHLEN>;
};

union mountres3 switch (mountstat3 fhs_status) {
 case MNT3_OK:
  mountres3_ok  mountinfo;
 default:
  void;
};

program NFS_MOUNT {
 version NFS_MOUNT_V3 {
  void      MOUNTPROC3_NULL(void)    = 0;
  mountres3 MOUNTPROC3_MNT(mountarg3)  = 1;
  mountdumpres MOUNTPROC3_DUMP(void)  = 2;
  void      MOUNTPROC3_UMNT(mountarg3) = 3;
  void      MOUNTPROC3_UMNTALL(void) = 4;
  exportres MOUNTPROC3_EXPORT(void)  = 5;
 } = 3;
} = 100005;