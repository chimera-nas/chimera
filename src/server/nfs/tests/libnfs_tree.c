#include "libnfs_test_common.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_PATH_LEN     256
#define MAX_CONTENT_SIZE 4096
#define NUM_OPERATIONS   100

struct fs_node {
    char            name[MAX_PATH_LEN];
    int             is_dir;
    char           *content;
    size_t          content_len;
    struct fs_node *next;
    struct fs_node *children;
    struct fs_node *parent;
};

static struct fs_node *root = NULL;

static struct fs_node *
create_node(
    const char     *name,
    int             is_dir,
    struct fs_node *parent)
{
    struct fs_node *node = calloc(1, sizeof(struct fs_node));

    strncpy(node->name, name, MAX_PATH_LEN - 1);
    node->is_dir = is_dir;
    node->parent = parent;
    return node;
} /* create_node */

static void
add_child(
    struct fs_node *parent,
    struct fs_node *child)
{
    if (!parent->children) {
        parent->children = child;
    } else {
        struct fs_node *curr = parent->children;
        while (curr->next) {
            curr = curr->next;
        }
        curr->next = child;
    }
} /* add_child */

static struct fs_node *
find_node(
    struct fs_node *start,
    const char     *path)
{
    char           *path_copy = strdup(path);
    char           *token     = strtok(path_copy, "/");
    struct fs_node *current   = start;

    while (token && current) {
        struct fs_node *found = NULL;
        struct fs_node *child = current->children;

        while (child) {
            if (strcmp(child->name, token) == 0) {
                found = child;
                break;
            }
            child = child->next;
        }

        current = found;
        token   = strtok(NULL, "/");
    }

    free(path_copy);
    return current;
} /* find_node */

static int
verify_fs_node(
    struct nfs_context *nfs,
    const char         *base_path,
    struct fs_node     *node)
{
    char               full_path[MAX_PATH_LEN * 2];
    struct nfsfh      *fh;
    struct nfs_stat_64 st;

    snprintf(full_path, sizeof(full_path), "%s/%s", base_path, node->name);

    if (nfs_stat64(nfs, full_path, &st) < 0) {
        return 0;
    }

    if (node->is_dir) {
        if (!S_ISDIR(st.nfs_mode)) {
            return 0;
        }
        struct nfsdir    *dir;
        struct nfsdirent *ent;

        if (nfs_opendir(nfs, full_path, &dir) < 0) {
            return 0;
        }

        while ((ent = nfs_readdir(nfs, dir)) != NULL) {
            if (strcmp(ent->name, ".") == 0 || strcmp(ent->name, "..") == 0) {
                continue;
            }
            struct fs_node *child = node->children;
            int             found = 0;
            while (child) {
                if (strcmp(child->name, ent->name) == 0) {
                    found = 1;
                    if (!verify_fs_node(nfs, full_path, child)) {
                        nfs_closedir(nfs, dir);
                        return 0;
                    }
                    break;
                }
                child = child->next;
            }
            if (!found) {
                nfs_closedir(nfs, dir);
                return 0;
            }
        }
        nfs_closedir(nfs, dir);
    } else {
        if (!S_ISREG(st.nfs_mode)) {
            return 0;
        }
        if (nfs_open(nfs, full_path, O_RDONLY, &fh) < 0) {
            return 0;
        }
        char buffer[MAX_CONTENT_SIZE];
        int  count = nfs_read(nfs, fh, node->content_len, buffer);
        nfs_close(nfs, fh);

        if (count != node->content_len ||
            memcmp(buffer, node->content, node->content_len) != 0) {
            return 0;
        }
    }
    return 1;
} /* verify_fs_node */

int
main(
    int    argc,
    char **argv)
{
    struct test_env env;
    struct nfsfh   *fh;

    libnfs_test_init(&env, argv, argc);
    srand(time(NULL));

    if (nfs_mount(env.nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    root = create_node("", 1, NULL);

    for (int i = 0; i < NUM_OPERATIONS; i++) {
        char path[80];
        char content[MAX_CONTENT_SIZE];
        int  op = rand() % 5;

        switch (op) {
            case 0: /* Create directory */
            {
                snprintf(path, sizeof(path), "dir_%d", i);
                if (nfs_mkdir(env.nfs, path) == 0) {
                    struct fs_node *node = create_node(path, 1, root);
                    add_child(root, node);
                }
                break;
            }
            case 1: /* Create file */
            {
                snprintf(path, sizeof(path), "file_%d", i);
                if (nfs_create(env.nfs, path, O_CREAT | O_RDWR, 0644, &fh) == 0) {
                    int len = rand() % 1024;
                    for (int j = 0; j < len; j++) {
                        content[j] = 'a' + (rand() % 26);
                    }

                    if (nfs_write(env.nfs, fh, len, content) == len) {
                        struct fs_node *node = create_node(path, 0, root);
                        node->content = malloc(len);
                        memcpy(node->content, content, len);
                        node->content_len = len;
                        add_child(root, node);
                    }
                    nfs_close(env.nfs, fh);
                }
                break;
            }
            case 2: /* Remove file or directory */
            {
                if (root->children) {
                    struct fs_node *prev   = NULL;
                    struct fs_node *curr   = root->children;
                    int             target = rand() % 3;
                    while (target > 0 && curr->next) {
                        prev = curr;
                        curr = curr->next;
                        target--;
                    }

                    if (curr->is_dir) {
                        if (nfs_rmdir(env.nfs, curr->name) == 0) {
                            if (prev) {
                                prev->next = curr->next;
                            } else {
                                root->children = curr->next;
                            }
                            free(curr);
                        }
                    } else {
                        if (nfs_unlink(env.nfs, curr->name) == 0) {
                            if (prev) {
                                prev->next = curr->next;
                            } else {
                                root->children = curr->next;
                            }
                            free(curr->content);
                            free(curr);
                        }
                    }
                }
                break;
            }
            case 3: /* Modify file */
            {
                struct fs_node *curr = root->children;
                while (curr && curr->is_dir) {
                    curr = curr->next;
                }

                if (curr && !curr->is_dir) {
                    if (nfs_open(env.nfs, curr->name, O_WRONLY, &fh) == 0) {
                        int len = rand() % 1024;
                        for (int j = 0; j < len; j++) {
                            content[j] = 'a' + (rand() % 26);
                        }

                        if (nfs_write(env.nfs, fh, len, content) == len) {
                            free(curr->content);
                            curr->content = malloc(len);
                            memcpy(curr->content, content, len);
                            curr->content_len = len;
                        }
                        nfs_close(env.nfs, fh);
                    }
                }
                break;
            }
            case 4: /* Verify partial tree */
            {
                if (!verify_fs_node(env.nfs, "", root)) {
                    fprintf(stderr, "Tree verification failed at operation %d\n", i);
                    nfs_umount(env.nfs);
                    libnfs_test_fail(&env);
                }
                break;
            }
        } /* switch */
    }

    if (!verify_fs_node(env.nfs, "", root)) {
        fprintf(stderr, "Final tree verification failed\n");
        nfs_umount(env.nfs);
        libnfs_test_fail(&env);
    }

    nfs_umount(env.nfs);
    libnfs_test_success(&env);
} /* main */