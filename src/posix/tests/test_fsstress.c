// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
// SPDX-FileCopyrightText: 2000-2002 Silicon Graphics, Inc.
//
// SPDX-License-Identifier: GPL-2.0-only

/*
 * fsstress - Filesystem stress test
 *
 * A general purpose filesystem exerciser that performs random operations
 * including: create, unlink, mkdir, rmdir, rename, read, write, truncate,
 * stat, link, symlink, readdir, and more.
 *
 * Originally from xfstests/LTP, ported to Chimera POSIX userspace API.
 * This is a simplified version focusing on POSIX-portable operations.
 */

#include <sys/wait.h>
#include <sys/uio.h>
#include <dirent.h>
#include "posix_test_common.h"

/* Operation types */
typedef enum {
    OP_CHOWN,
    OP_CREAT,
    OP_FDATASYNC,
    OP_FSYNC,
    OP_GETDENTS,
    OP_LINK,
    OP_MKDIR,
    OP_READ,
    OP_READLINK,
    OP_RENAME,
    OP_RMDIR,
    OP_STAT,
    OP_SYMLINK,
    OP_TRUNCATE,
    OP_UNLINK,
    OP_WRITE,
    OP_LAST
} opty_t;

typedef long long opnum_t;

/* Forward declarations */
typedef void (*opfnc_t)(
    opnum_t,
    long);

typedef struct opdesc {
    const char *name;
    opfnc_t     func;
    int         freq;
    int         iswrite;
} opdesc_t;

/* File entry */
typedef struct fent {
    int id;
    int parent;
} fent_t;

/* File list */
typedef struct flist {
    int     nfiles;
    int     nslots;
    int     tag;
    fent_t *fents;
} flist_t;

/* Pathname structure */
typedef struct pathname {
    int   len;
    char *path;
} pathname_t;

/* File list tags */
#define FT_DIR       0
#define FT_REG       1
#define FT_SYM       2
#define FT_NTYPE     3

/* Constants */
#define FILELEN_MAX  (32 * 4096)
#define NAME_MAX_LEN 256
#define PATH_MAX_LEN 4096

/* Global state */
static flist_t flist[FT_NTYPE] = {
    { 0, 0, 'd', NULL },    /* directories */
    { 0, 0, 'f', NULL },    /* regular files */
    { 0, 0, 's', NULL },    /* symlinks */
};

/* *INDENT-OFF* */
static int     nops                  = 0;
static int     verbose               = 0;
static long    seed                  = 0;
static char    homedir[PATH_MAX_LEN] = "/test/fsstress";
static opnum_t opno                  = 0;
static int     procid                = 0;
static int     nproc                 = 1;
static int     nameseq               = 0;
/* *INDENT-ON* */

/* Operation function declarations */
static void chown_f(
    opnum_t opno,
    long    r);
static void creat_f(
    opnum_t opno,
    long    r);
static void fdatasync_f(
    opnum_t opno,
    long    r);
static void fsync_f(
    opnum_t opno,
    long    r);
static void getdents_f(
    opnum_t opno,
    long    r);
static void link_f(
    opnum_t opno,
    long    r);
static void mkdir_f(
    opnum_t opno,
    long    r);
static void read_f(
    opnum_t opno,
    long    r);
static void readlink_f(
    opnum_t opno,
    long    r);
static void rename_f(
    opnum_t opno,
    long    r);
static void rmdir_f(
    opnum_t opno,
    long    r);
static void stat_f(
    opnum_t opno,
    long    r);
static void symlink_f(
    opnum_t opno,
    long    r);
static void truncate_f(
    opnum_t opno,
    long    r);
static void unlink_f(
    opnum_t opno,
    long    r);
static void write_f(
    opnum_t opno,
    long    r);

/* *INDENT-OFF* */
/* Operation table */
static opdesc_t ops[OP_LAST] = {
    [OP_CHOWN]     = { "chown",     chown_f,     3,      1                },
    [OP_CREAT]     = { "creat",     creat_f,     4,      1                },
    [OP_FDATASYNC] = { "fdatasync", fdatasync_f, 1,      1                },
    [OP_FSYNC]     = { "fsync",     fsync_f,     1,      1                },
    [OP_GETDENTS]  = { "getdents",  getdents_f,  2,      0                },
    [OP_LINK]      = { "link",      link_f,      2,      1                },
    [OP_MKDIR]     = { "mkdir",     mkdir_f,     4,      1                },
    [OP_READ]      = { "read",      read_f,      4,      0                },
    [OP_READLINK]  = { "readlink",  readlink_f,  2,      0                },
    [OP_RENAME]    = { "rename",    rename_f,    4,      1                },
    [OP_RMDIR]     = { "rmdir",     rmdir_f,     2,      1                },
    [OP_STAT]      = { "stat",      stat_f,      2,      0                },
    [OP_SYMLINK]   = { "symlink",   symlink_f,   2,      1                },
    [OP_TRUNCATE]  = { "truncate",  truncate_f,  2,      1                },
    [OP_UNLINK]    = { "unlink",    unlink_f,    2,      1                },
    [OP_WRITE]     = { "write",     write_f,     8,      1                },
};
/* *INDENT-ON* */

/* Helper: allocate pathname */
static void
path_init(pathname_t *name)
{
    name->len  = 0;
    name->path = malloc(PATH_MAX_LEN);
    if (name->path) {
        name->path[0] = '\0';
    }
} /* path_init */

/* Helper: free pathname */
static void
path_free(pathname_t *name)
{
    if (name->path) {
        free(name->path);
        name->path = NULL;
    }
    name->len = 0;
} /* path_free */

/* Helper: append to pathname */
static int
path_append(
    pathname_t *name,
    const char *str)
{
    int len = strlen(str);

    if (name->len + len + 1 >= PATH_MAX_LEN) {
        return 0;
    }

    if (name->len > 0 && name->path[name->len - 1] != '/') {
        strcat(name->path, "/");
        name->len++;
    }
    strcat(name->path, str);
    name->len += len;
    return 1;
} /* path_append */

/* Helper: add file entry to list */
static void
add_to_flist(
    int ft,
    int id,
    int parent)
{
    flist_t *fl = &flist[ft];

    if (fl->nfiles >= fl->nslots) {
        int newslots = fl->nslots ? fl->nslots * 2 : 64;
        fl->fents  = realloc(fl->fents, newslots * sizeof(fent_t));
        fl->nslots = newslots;
    }
    fl->fents[fl->nfiles].id     = id;
    fl->fents[fl->nfiles].parent = parent;
    fl->nfiles++;
} /* add_to_flist */

/* Helper: remove file entry from list */
static void
del_from_flist(
    int ft,
    int slot)
{
    flist_t *fl = &flist[ft];

    if (slot < fl->nfiles - 1) {
        fl->fents[slot] = fl->fents[fl->nfiles - 1];
    }
    fl->nfiles--;
} /* del_from_flist */

/* Helper: get random entry from list */
static int
get_random_fent(
    int      ft,
    fent_t **fentp)
{
    flist_t *fl = &flist[ft];

    if (fl->nfiles == 0) {
        return -1;
    }
    *fentp = &fl->fents[random() % fl->nfiles];
    return 0;
} /* get_random_fent */

/* Helper: find entry in list by id */
static int
find_fent(
    int      ft,
    int      id,
    fent_t **fentp)
{
    flist_t *fl = &flist[ft];
    int      i;

    for (i = 0; i < fl->nfiles; i++) {
        if (fl->fents[i].id == id) {
            if (fentp) {
                *fentp = &fl->fents[i];
            }
            return i;
        }
    }
    return -1;
} /* find_fent */

/* Helper: build pathname for a file entry */
static void
fent_to_path(
    fent_t     *fent,
    int         ft,
    pathname_t *name)
{
    char namebuf[NAME_MAX_LEN];

    path_init(name);
    path_append(name, homedir);

    if (fent->parent != -1) {
        /* For simplicity, use flat directory structure */
        snprintf(namebuf, sizeof(namebuf), "%c%x", flist[ft].tag, fent->id);
        path_append(name, namebuf);
    } else {
        snprintf(namebuf, sizeof(namebuf), "%c%x", flist[ft].tag, fent->id);
        path_append(name, namebuf);
    }
} /* fent_to_path */

/* Helper: generate a new unique name */
static void
gen_new_name(
    pathname_t *name,
    int         ft)
{
    char namebuf[NAME_MAX_LEN];

    path_init(name);
    path_append(name, homedir);
    snprintf(namebuf, sizeof(namebuf), "%c%x", flist[ft].tag, nameseq++);
    path_append(name, namebuf);
} /* gen_new_name */

/* Operation implementations */

static void
chown_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t path;
    int        ret;

    if (get_random_fent(FT_REG, &fent) < 0 &&
        get_random_fent(FT_DIR, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_REG, &path);
    ret = chimera_posix_chown(path.path, -1, -1);
    if (verbose) {
        fprintf(stderr, "%lld: chown %s %d\n", (long long) opno, path.path, ret);
    }
    path_free(&path);
} /* chown_f */

static void
creat_f(
    opnum_t opno,
    long    r)
{
    pathname_t path;
    int        fd;
    int        id;

    gen_new_name(&path, FT_REG);
    fd = chimera_posix_open(path.path, O_CREAT | O_EXCL | O_RDWR, 0644);
    if (fd >= 0) {
        id = nameseq - 1;
        add_to_flist(FT_REG, id, -1);
        chimera_posix_close(fd);
        if (verbose) {
            fprintf(stderr, "%lld: creat %s\n", (long long) opno, path.path);
        }
    } else if (verbose) {
        fprintf(stderr, "%lld: creat %s failed: %s\n", (long long) opno, path.path, strerror(errno));
    }
    path_free(&path);
} /* creat_f */

static void
fdatasync_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t path;
    int        fd;

    if (get_random_fent(FT_REG, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_REG, &path);
    fd = chimera_posix_open(path.path, O_RDWR, 0);
    if (fd >= 0) {
        chimera_posix_fdatasync(fd);
        chimera_posix_close(fd);
        if (verbose) {
            fprintf(stderr, "%lld: fdatasync %s\n", (long long) opno, path.path);
        }
    }
    path_free(&path);
} /* fdatasync_f */

static void
fsync_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t path;
    int        fd;

    if (get_random_fent(FT_REG, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_REG, &path);
    fd = chimera_posix_open(path.path, O_RDWR, 0);
    if (fd >= 0) {
        chimera_posix_fsync(fd);
        chimera_posix_close(fd);
        if (verbose) {
            fprintf(stderr, "%lld: fsync %s\n", (long long) opno, path.path);
        }
    }
    path_free(&path);
} /* fsync_f */

static void
getdents_f(
    opnum_t opno,
    long    r)
{
    fent_t      *fent;
    pathname_t   path;
    CHIMERA_DIR *dir;
    int          count = 0;

    if (get_random_fent(FT_DIR, &fent) < 0) {
        /* Use home directory */
        path_init(&path);
        path_append(&path, homedir);
    } else {
        fent_to_path(fent, FT_DIR, &path);
    }

    dir = chimera_posix_opendir(path.path);
    if (dir) {
        while (chimera_posix_readdir(dir) != NULL) {
            count++;
        }
        chimera_posix_closedir(dir);
        if (verbose) {
            fprintf(stderr, "%lld: getdents %s (%d entries)\n", (long long) opno, path.path, count);
        }
    }
    path_free(&path);
} /* getdents_f */

static void
link_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t oldpath, newpath;

    if (get_random_fent(FT_REG, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_REG, &oldpath);
    gen_new_name(&newpath, FT_REG);

    if (chimera_posix_link(oldpath.path, newpath.path) == 0) {
        add_to_flist(FT_REG, nameseq - 1, -1);
        if (verbose) {
            fprintf(stderr, "%lld: link %s -> %s\n", (long long) opno, oldpath.path, newpath.path);
        }
    } else if (verbose) {
        fprintf(stderr, "%lld: link %s -> %s failed: %s\n",
                (long long) opno, oldpath.path, newpath.path, strerror(errno));
    }

    path_free(&oldpath);
    path_free(&newpath);
} /* link_f */

static void
mkdir_f(
    opnum_t opno,
    long    r)
{
    pathname_t path;
    int        id;

    gen_new_name(&path, FT_DIR);
    if (chimera_posix_mkdir(path.path, 0755) == 0) {
        id = nameseq - 1;
        add_to_flist(FT_DIR, id, -1);
        if (verbose) {
            fprintf(stderr, "%lld: mkdir %s\n", (long long) opno, path.path);
        }
    } else if (verbose) {
        fprintf(stderr, "%lld: mkdir %s failed: %s\n", (long long) opno, path.path, strerror(errno));
    }
    path_free(&path);
} /* mkdir_f */

static void
read_f(
    opnum_t opno,
    long    r)
{
    fent_t     *fent;
    pathname_t  path;
    int         fd;
    char        buf[FILELEN_MAX];
    struct stat statb;
    off_t       off;
    ssize_t     len;

    if (get_random_fent(FT_REG, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_REG, &path);
    fd = chimera_posix_open(path.path, O_RDONLY, 0);
    if (fd >= 0) {
        if (chimera_posix_fstat(fd, &statb) == 0 && statb.st_size > 0) {
            off = random() % statb.st_size;
            len = random() % (statb.st_size - off + 1);
            if (len > sizeof(buf)) {
                len = sizeof(buf);
            }
            chimera_posix_pread(fd, buf, len, off);
            if (verbose) {
                fprintf(stderr, "%lld: read %s [%lld, %zd]\n",
                        (long long) opno, path.path, (long long) off, len);
            }
        }
        chimera_posix_close(fd);
    }
    path_free(&path);
} /* read_f */

static void
readlink_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t path;
    char       buf[PATH_MAX_LEN];

    if (get_random_fent(FT_SYM, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_SYM, &path);
    if (chimera_posix_readlink(path.path, buf, sizeof(buf)) >= 0) {
        if (verbose) {
            fprintf(stderr, "%lld: readlink %s\n", (long long) opno, path.path);
        }
    }
    path_free(&path);
} /* readlink_f */

static void
rename_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t oldpath, newpath;
    int        ft;
    int        slot;

    ft = (random() % 2) ? FT_REG : FT_DIR;
    if (get_random_fent(ft, &fent) < 0) {
        ft = (ft == FT_REG) ? FT_DIR : FT_REG;
        if (get_random_fent(ft, &fent) < 0) {
            return;
        }
    }

    slot = find_fent(ft, fent->id, NULL);
    fent_to_path(fent, ft, &oldpath);
    gen_new_name(&newpath, ft);

    if (chimera_posix_rename(oldpath.path, newpath.path) == 0) {
        /* Update entry with new id */
        if (slot >= 0) {
            flist[ft].fents[slot].id = nameseq - 1;
        }
        if (verbose) {
            fprintf(stderr, "%lld: rename %s -> %s\n", (long long) opno, oldpath.path, newpath.path);
        }
    } else if (verbose) {
        fprintf(stderr, "%lld: rename %s -> %s failed: %s\n",
                (long long) opno, oldpath.path, newpath.path, strerror(errno));
    }

    path_free(&oldpath);
    path_free(&newpath);
} /* rename_f */

static void
rmdir_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t path;
    int        slot;

    if (get_random_fent(FT_DIR, &fent) < 0) {
        return;
    }

    slot = find_fent(FT_DIR, fent->id, NULL);
    fent_to_path(fent, FT_DIR, &path);

    if (chimera_posix_rmdir(path.path) == 0) {
        if (slot >= 0) {
            del_from_flist(FT_DIR, slot);
        }
        if (verbose) {
            fprintf(stderr, "%lld: rmdir %s\n", (long long) opno, path.path);
        }
    } else if (verbose && errno != ENOTEMPTY) {
        fprintf(stderr, "%lld: rmdir %s failed: %s\n", (long long) opno, path.path, strerror(errno));
    }
    path_free(&path);
} /* rmdir_f */

static void
stat_f(
    opnum_t opno,
    long    r)
{
    fent_t     *fent;
    pathname_t  path;
    struct stat statb;
    int         ft;

    ft = random() % FT_NTYPE;
    if (get_random_fent(ft, &fent) < 0) {
        return;
    }

    fent_to_path(fent, ft, &path);
    if (chimera_posix_stat(path.path, &statb) == 0) {
        if (verbose) {
            fprintf(stderr, "%lld: stat %s\n", (long long) opno, path.path);
        }
    }
    path_free(&path);
} /* stat_f */

static void
symlink_f(
    opnum_t opno,
    long    r)
{
    pathname_t linkpath;
    int        id;
    char       targetbuf[NAME_MAX_LEN];

    /* Create a symlink with a simple target name */
    snprintf(targetbuf, sizeof(targetbuf), "target_%d", (int) (random() % 1000));

    gen_new_name(&linkpath, FT_SYM);

    if (chimera_posix_symlink(targetbuf, linkpath.path) == 0) {
        id = nameseq - 1;
        add_to_flist(FT_SYM, id, -1);
        if (verbose) {
            fprintf(stderr, "%lld: symlink %s -> %s\n", (long long) opno, linkpath.path, targetbuf);
        }
    } else if (verbose) {
        fprintf(stderr, "%lld: symlink %s -> %s failed: %s\n",
                (long long) opno, linkpath.path, targetbuf, strerror(errno));
    }
    path_free(&linkpath);
} /* symlink_f */

static void
truncate_f(
    opnum_t opno,
    long    r)
{
    fent_t     *fent;
    pathname_t  path;
    off_t       len;
    struct stat statb;

    if (get_random_fent(FT_REG, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_REG, &path);

    /* Get current size to pick a reasonable truncate length */
    if (chimera_posix_stat(path.path, &statb) == 0) {
        len = random() % (statb.st_size + FILELEN_MAX + 1);
    } else {
        len = random() % FILELEN_MAX;
    }

    if (chimera_posix_truncate(path.path, len) == 0) {
        if (verbose) {
            fprintf(stderr, "%lld: truncate %s %lld\n", (long long) opno, path.path, (long long) len);
        }
    } else if (verbose) {
        fprintf(stderr, "%lld: truncate %s %lld failed: %s\n",
                (long long) opno, path.path, (long long) len, strerror(errno));
    }
    path_free(&path);
} /* truncate_f */

static void
unlink_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t path;
    int        slot;

    if (get_random_fent(FT_REG, &fent) < 0) {
        if (get_random_fent(FT_SYM, &fent) < 0) {
            return;
        }
        slot = find_fent(FT_SYM, fent->id, NULL);
        fent_to_path(fent, FT_SYM, &path);

        if (chimera_posix_unlink(path.path) == 0) {
            if (slot >= 0) {
                del_from_flist(FT_SYM, slot);
            }
            if (verbose) {
                fprintf(stderr, "%lld: unlink %s\n", (long long) opno, path.path);
            }
        }
        path_free(&path);
        return;
    }

    slot = find_fent(FT_REG, fent->id, NULL);
    fent_to_path(fent, FT_REG, &path);

    if (chimera_posix_unlink(path.path) == 0) {
        if (slot >= 0) {
            del_from_flist(FT_REG, slot);
        }
        if (verbose) {
            fprintf(stderr, "%lld: unlink %s\n", (long long) opno, path.path);
        }
    } else if (verbose) {
        fprintf(stderr, "%lld: unlink %s failed: %s\n", (long long) opno, path.path, strerror(errno));
    }
    path_free(&path);
} /* unlink_f */

static void
write_f(
    opnum_t opno,
    long    r)
{
    fent_t    *fent;
    pathname_t path;
    int        fd;
    char       buf[FILELEN_MAX];
    off_t      off;
    size_t     len;
    ssize_t    ret;

    if (get_random_fent(FT_REG, &fent) < 0) {
        return;
    }

    fent_to_path(fent, FT_REG, &path);
    fd = chimera_posix_open(path.path, O_RDWR, 0);
    if (fd >= 0) {
        off = random() % FILELEN_MAX;
        len = random() % sizeof(buf) + 1;

        /* Fill buffer with deterministic pattern */
        memset(buf, (char) (opno & 0xff), len);

        ret = chimera_posix_pwrite(fd, buf, len, off);
        if (verbose) {
            fprintf(stderr, "%lld: write %s [%lld, %zu] = %zd\n",
                    (long long) opno, path.path, (long long) off, len, ret);
        }
        chimera_posix_close(fd);
    }
    path_free(&path);
} /* write_f */

/* Run the stress test */
static int
do_stress(int noperations)
{
    int       total_freq = 0;
    int       i;
    long      r;
    opdesc_t *op;

    /* Calculate total frequency */
    for (i = 0; i < OP_LAST; i++) {
        total_freq += ops[i].freq;
    }

    /* Create home directory */
    chimera_posix_mkdir(homedir, 0755);

    /* Create some initial files */
    for (i = 0; i < 10; i++) {
        creat_f(opno++, random());
    }
    for (i = 0; i < 5; i++) {
        mkdir_f(opno++, random());
    }

    /* Run operations */
    for (i = 0; i < noperations; i++) {
        r = random() % total_freq;

        /* Select operation based on frequency */
        op = NULL;
        for (int j = 0; j < OP_LAST; j++) {
            r -= ops[j].freq;
            if (r < 0) {
                op = &ops[j];
                break;
            }
        }

        if (op && op->func) {
            op->func(opno++, random());
        }

        /* Print progress */
        if ((i % 100) == 0) {
            fprintf(stderr, ".");
            fflush(stderr);
        }
    }
    fprintf(stderr, "\n");

    return 0;
} /* do_stress */

/* Cleanup the home directory */
static void
do_cleanup(void)
{
    CHIMERA_DIR   *dir;
    struct dirent *de;
    char           path[PATH_MAX_LEN];
    struct stat    statb;

    dir = chimera_posix_opendir(homedir);
    if (dir) {
        while ((de = chimera_posix_readdir(dir)) != NULL) {
            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) {
                continue;
            }
            snprintf(path, sizeof(path), "%s/%s", homedir, de->d_name);
            if (chimera_posix_lstat(path, &statb) == 0) {
                if (S_ISDIR(statb.st_mode)) {
                    chimera_posix_rmdir(path);
                } else {
                    chimera_posix_unlink(path);
                }
            }
        }
        chimera_posix_closedir(dir);
    }
    chimera_posix_rmdir(homedir);
} /* do_cleanup */

static void
usage(void)
{
    fprintf(stderr,
            "Usage: test_fsstress -b <backend> [options]\n"
            "  -b backend   VFS backend (required)\n"
            "  -n nops      number of operations (default: 1000)\n"
            "  -p nprocs    number of processes (default: 1)\n"
            "  -s seed      random seed\n"
            "  -v           verbose output\n"
            "  -h           show this help\n");
    exit(1);
} /* usage */

int
main(
    int   argc,
    char *argv[])
{
    struct posix_test_env env;
    int                   c;
    int                   i, status, ret;

    nops = 1000;
    seed = time(NULL);

    posix_test_init(&env, argv, argc);

    /* Reset optind for our own option parsing */
    optind = 1;

    while ((c = getopt(argc, argv, "b:n:p:s:vh")) != -1) {
        switch (c) {
            case 'b':
                /* Already handled by posix_test_init */
                break;
            case 'n':
                nops = atoi(optarg);
                break;
            case 'p':
                nproc = atoi(optarg);
                break;
            case 's':
                seed = strtol(optarg, NULL, 0);
                break;
            case 'v':
                verbose = 1;
                break;
            case 'h':
                usage();
                break;
            default:
                usage();
        } /* switch */
    }

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "Failed to mount test filesystem\n");
        posix_test_fail(&env);
    }

    fprintf(stderr, "fsstress: backend=%s nops=%d nproc=%d seed=%ld\n",
            env.backend, nops, nproc, seed);

    srandom(seed);

    if (nproc == 1) {
        /* Single process mode */
        ret = do_stress(nops);
        do_cleanup();

        posix_test_umount();

        if (ret != 0) {
            posix_test_fail(&env);
        }

        fprintf(stderr, "fsstress completed successfully\n");
        posix_test_success(&env);
        return 0;
    }

    /* Multi-process mode */
    for (i = 0; i < nproc; i++) {
        if (fork() == 0) {
            procid = i;
            srandom(seed + i);
            snprintf(homedir + strlen(homedir), sizeof(homedir) - strlen(homedir),
                     ".%d", procid);
            exit(do_stress(nops / nproc));
        }
    }

    ret = 0;

    /* Wait for children */
    while (waitpid(0, &status, 0) == 0 || errno != ECHILD) {
        if (WEXITSTATUS(status) != 0) {
            ret = WEXITSTATUS(status);
            fprintf(stderr, "Child exited with status %d\n", ret);
        }
    }

    /* Cleanup all process directories */
    for (i = 0; i < nproc; i++) {
        char dir[PATH_MAX_LEN];
        snprintf(dir, sizeof(dir), "/test/fsstress.%d", i);
        /* Simple cleanup - remove what we can */
        chimera_posix_rmdir(dir);
    }
    chimera_posix_rmdir("/test/fsstress");

    posix_test_umount();

    if (ret != 0) {
        posix_test_fail(&env);
    }

    fprintf(stderr, "fsstress completed successfully\n");
    posix_test_success(&env);
    return ret;
} /* main */
