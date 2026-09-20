// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include "common/test_host.h"

int
main(void)
{
    char directory[] = "./fixtureXXXXXX";
    char file[256], child[256];
    int  fd;

    assert(mkdtemp(directory));
    snprintf(child, sizeof(child), "%s/child", directory);
    assert(!chimera_test_mkdir(child, 0700));
    snprintf(file, sizeof(file), "%s/fileXXXXXX", child);
    fd = mkstemp(file);
    assert(fd >= 0);
    assert(write(fd, "a\r\nb\n", 5) == 5);
    assert(!close(fd));
    {
        FILE *input = fopen(file, "rb");
        char  data[8];
        assert(input);
        assert(fread(data, 1, sizeof(data), input) == 5);
        assert(!memcmp(data, "a\r\nb\n", 5));
        fclose(input);
    }
#ifndef _WIN32
    snprintf(file, sizeof(file), "%s/outside", child);
    assert(!symlink("../..", file));
#endif /* ifndef _WIN32 */
    assert(!chimera_test_remove_tree(directory));
    assert(!opendir(directory));
    assert(!setenv("CHIMERA_FIXTURE_ENV_TEST", "first", 1));
    assert(!setenv("CHIMERA_FIXTURE_ENV_TEST", "second", 0));
    assert(!strcmp(getenv("CHIMERA_FIXTURE_ENV_TEST"), "first"));
    assert(!setenv("CHIMERA_FIXTURE_ENV_TEST", "third", 1));
    assert(!strcmp(getenv("CHIMERA_FIXTURE_ENV_TEST"), "third"));
    assert(!unsetenv("CHIMERA_FIXTURE_ENV_TEST"));
    return 0;
} /* main */
