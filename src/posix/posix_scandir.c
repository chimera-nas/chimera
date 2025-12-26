// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_scandir(
    const char *path,
    struct dirent ***namelist,
    int ( *filter )(const struct dirent *),
    int ( *compar )(const struct dirent **, const struct dirent **))
{
    CHIMERA_DIR    *dirp;
    struct dirent  *entry;
    struct dirent **list  = NULL;
    int             count = 0;
    int             alloc = 0;
    struct dirent  *copy;

    if (!namelist) {
        errno = EINVAL;
        return -1;
    }

    dirp = chimera_posix_opendir(path);
    if (!dirp) {
        return -1;
    }

    while ((entry = chimera_posix_readdir(dirp)) != NULL) {
        // Apply filter if provided
        if (filter && !filter(entry)) {
            continue;
        }

        // Grow array if needed
        if (count >= alloc) {
            int             new_alloc = alloc ? alloc * 2 : 16;
            struct dirent **new_list  = realloc(list, new_alloc * sizeof(*list));
            if (!new_list) {
                goto error;
            }
            list  = new_list;
            alloc = new_alloc;
        }

        // Allocate and copy the entry
        copy = malloc(sizeof(*copy));
        if (!copy) {
            goto error;
        }

        *copy         = *entry;
        list[count++] = copy;
    }

    chimera_posix_closedir(dirp);

    // Sort if comparator provided
    if (compar && count > 0) {
        qsort(list, count, sizeof(*list),
              (int (*)(const void *, const void *)) compar);
    }

    *namelist = list;
    return count;

 error:
    // Free all allocated entries
    for (int i = 0; i < count; i++) {
        free(list[i]);
    }
    free(list);
    chimera_posix_closedir(dirp);
    errno = ENOMEM;
    return -1;
} /* chimera_posix_scandir */
