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
        FILE  *input = fopen(file, "rb");
        char   data[8];
        assert(input);
        assert(fread(data, 1, sizeof(data), input) == 5);
        assert(!memcmp(data, "a\r\nb\n", 5));
        rewind(input);
        char  *line     = NULL;
        size_t capacity = 0;
        assert(chimera_test_getline(&line, &capacity, input) == 3);
        assert(!strcmp(line, "a\r\n"));
        assert(chimera_test_getline(&line, &capacity, input) == 2);
        assert(!strcmp(line, "b\n"));
        assert(chimera_test_getline(&line, &capacity, input) == -1);
        free(line);
        fclose(input);
        input = fopen(file, "wb");
        assert(input);
        for (int i = 0; i < 8193; i++) {
            assert(fputc('x', input) == 'x');
        }
        assert(!fclose(input));
        input = fopen(file, "rb");
        assert(input);
        line     = NULL;
        capacity = 0;
        assert(chimera_test_getline(&line, &capacity, input) == 8193);
        assert(strlen(line) == 8193 && line[8192] == 'x');
        assert(chimera_test_getline(&line, &capacity, input) == -1);
        free(line);
        fclose(input);
    }
#ifdef _WIN32
    /* Diskfs fixtures use large, mostly empty images. Resize must retain the
     * descriptor position, preserve existing data, and leave zero-filled holes
     * without allocating the entire logical image on NTFS. */
    fd = _open(file, _O_RDWR | _O_BINARY);
    assert(fd >= 0);
    assert(_lseeki64(fd, 3, SEEK_SET) == 3);
    assert(!ftruncate(fd, (INT64_C(1) << 33) + 123));
    assert(_lseeki64(fd, 0, SEEK_CUR) == 3);
    {
        char                    byte = 1;
        LARGE_INTEGER           size;
        FILE_ATTRIBUTE_TAG_INFO attrs;
        HANDLE                  handle = (HANDLE) _get_osfhandle(fd);
        assert(GetFileSizeEx(handle, &size));
        assert(size.QuadPart == (INT64_C(1) << 33) + 123);
        assert(GetFileInformationByHandleEx(handle, FileAttributeTagInfo,
                                            &attrs, sizeof(attrs)));
        assert(attrs.FileAttributes & FILE_ATTRIBUTE_SPARSE_FILE);
        assert(_read(fd, &byte, 1) == 1 && byte == 'x');
        assert(_lseeki64(fd, -1, SEEK_END) == size.QuadPart - 1);
        assert(_read(fd, &byte, 1) == 1 && byte == 0);
        assert(!ftruncate(fd, 4));
        assert(_lseeki64(fd, 0, SEEK_CUR) == size.QuadPart);
        assert(GetFileSizeEx(handle, &size) && size.QuadPart == 4);
        errno = 0;
        assert(ftruncate(fd, -1) == -1 && errno == EINVAL);
    }
    assert(!close(fd));
#endif /* ifdef _WIN32 */
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
