// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test for fopen, fclose, fread, fwrite, fseek, ftell, feof, ferror, etc.

#include <string.h>
#include "posix_test_common.h"

static void
test_fopen_fclose(void)
{
    CHIMERA_FILE *fp;

    fprintf(stderr, "Testing fopen/fclose...\n");

    // Test opening a new file for writing
    fp = chimera_posix_fopen("/test/testfile.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }

    if (chimera_posix_fclose(fp) != 0) {
        fprintf(stderr, "fclose failed: %s\n", strerror(errno));
        exit(1);
    }

    // Test opening the file for reading
    fp = chimera_posix_fopen("/test/testfile.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    if (chimera_posix_fclose(fp) != 0) {
        fprintf(stderr, "fclose failed: %s\n", strerror(errno));
        exit(1);
    }

    // Test opening a non-existent file for reading
    fp = chimera_posix_fopen("/test/nonexistent.txt", "r");
    if (fp != NULL) {
        fprintf(stderr, "fopen should have failed for non-existent file\n");
        exit(1);
    }

    fprintf(stderr, "fopen/fclose tests passed\n");
}

static void
test_fread_fwrite(void)
{
    CHIMERA_FILE *fp;
    const char   *test_data = "Hello, World! This is a test.";
    char          buf[256];
    size_t        len     = strlen(test_data);
    size_t        written, read;

    fprintf(stderr, "Testing fread/fwrite...\n");

    // Write data to file
    fp = chimera_posix_fopen("/test/fwrite_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }

    written = chimera_posix_fwrite(test_data, 1, len, fp);
    if (written != len) {
        fprintf(stderr, "fwrite failed: wrote %zu, expected %zu\n", written, len);
        exit(1);
    }

    chimera_posix_fclose(fp);

    // Read data back
    fp = chimera_posix_fopen("/test/fwrite_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    memset(buf, 0, sizeof(buf));
    read = chimera_posix_fread(buf, 1, len, fp);
    if (read != len) {
        fprintf(stderr, "fread failed: read %zu, expected %zu\n", read, len);
        exit(1);
    }

    if (memcmp(buf, test_data, len) != 0) {
        fprintf(stderr, "fread data mismatch\n");
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fread/fwrite tests passed\n");
}

static void
test_fseek_ftell(void)
{
    CHIMERA_FILE *fp;
    const char   *test_data = "0123456789ABCDEF";
    char          buf[16];
    size_t        len = strlen(test_data);
    long          pos;

    fprintf(stderr, "Testing fseek/ftell...\n");

    // Create test file
    fp = chimera_posix_fopen("/test/fseek_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }
    chimera_posix_fwrite(test_data, 1, len, fp);
    chimera_posix_fclose(fp);

    // Open for reading
    fp = chimera_posix_fopen("/test/fseek_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    // Test ftell at beginning
    pos = chimera_posix_ftell(fp);
    if (pos != 0) {
        fprintf(stderr, "ftell at start: expected 0, got %ld\n", pos);
        exit(1);
    }

    // Read 5 bytes
    chimera_posix_fread(buf, 1, 5, fp);
    pos = chimera_posix_ftell(fp);
    if (pos != 5) {
        fprintf(stderr, "ftell after read: expected 5, got %ld\n", pos);
        exit(1);
    }

    // Seek to position 10
    if (chimera_posix_fseek(fp, 10, SEEK_SET) != 0) {
        fprintf(stderr, "fseek SEEK_SET failed\n");
        exit(1);
    }
    pos = chimera_posix_ftell(fp);
    if (pos != 10) {
        fprintf(stderr, "ftell after SEEK_SET: expected 10, got %ld\n", pos);
        exit(1);
    }

    // Seek backwards by 3
    if (chimera_posix_fseek(fp, -3, SEEK_CUR) != 0) {
        fprintf(stderr, "fseek SEEK_CUR failed\n");
        exit(1);
    }
    pos = chimera_posix_ftell(fp);
    if (pos != 7) {
        fprintf(stderr, "ftell after SEEK_CUR: expected 7, got %ld\n", pos);
        exit(1);
    }

    // Seek to end
    if (chimera_posix_fseek(fp, 0, SEEK_END) != 0) {
        fprintf(stderr, "fseek SEEK_END failed\n");
        exit(1);
    }
    pos = chimera_posix_ftell(fp);
    if (pos != (long) len) {
        fprintf(stderr, "ftell after SEEK_END: expected %zu, got %ld\n", len, pos);
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fseek/ftell tests passed\n");
}

static void
test_rewind(void)
{
    CHIMERA_FILE *fp;
    const char   *test_data = "Hello";
    char          buf[16];
    long          pos;

    fprintf(stderr, "Testing rewind...\n");

    fp = chimera_posix_fopen("/test/rewind_test.txt", "w+");
    if (!fp) {
        fprintf(stderr, "fopen failed: %s\n", strerror(errno));
        exit(1);
    }

    chimera_posix_fwrite(test_data, 1, strlen(test_data), fp);
    pos = chimera_posix_ftell(fp);
    if (pos != 5) {
        fprintf(stderr, "ftell after write: expected 5, got %ld\n", pos);
        exit(1);
    }

    chimera_posix_rewind(fp);
    pos = chimera_posix_ftell(fp);
    if (pos != 0) {
        fprintf(stderr, "ftell after rewind: expected 0, got %ld\n", pos);
        exit(1);
    }

    // Read and verify
    memset(buf, 0, sizeof(buf));
    chimera_posix_fread(buf, 1, 5, fp);
    if (memcmp(buf, test_data, 5) != 0) {
        fprintf(stderr, "Data mismatch after rewind\n");
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "rewind tests passed\n");
}

static void
test_feof_ferror(void)
{
    CHIMERA_FILE *fp;
    char          buf[16];

    fprintf(stderr, "Testing feof/ferror...\n");

    // Create a small test file
    fp = chimera_posix_fopen("/test/eof_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }
    chimera_posix_fwrite("AB", 1, 2, fp);
    chimera_posix_fclose(fp);

    // Read and test feof
    fp = chimera_posix_fopen("/test/eof_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    // Initially feof should return 0
    if (chimera_posix_feof(fp) != 0) {
        fprintf(stderr, "feof should be 0 initially\n");
        exit(1);
    }

    // Read all data
    chimera_posix_fread(buf, 1, 10, fp);

    // After reading past end, feof should return non-zero
    if (chimera_posix_feof(fp) == 0) {
        fprintf(stderr, "feof should be non-zero after reading past end\n");
        exit(1);
    }

    // Test clearerr
    chimera_posix_clearerr(fp);
    if (chimera_posix_feof(fp) != 0) {
        fprintf(stderr, "feof should be 0 after clearerr\n");
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "feof/ferror tests passed\n");
}

static void
test_fileno(void)
{
    CHIMERA_FILE *fp;
    int           fd;

    fprintf(stderr, "Testing fileno...\n");

    fp = chimera_posix_fopen("/test/fileno_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen failed: %s\n", strerror(errno));
        exit(1);
    }

    fd = chimera_posix_fileno(fp);
    if (fd < 0) {
        fprintf(stderr, "fileno failed: %s\n", strerror(errno));
        exit(1);
    }

    fprintf(stderr, "fileno returned fd=%d\n", fd);

    chimera_posix_fclose(fp);

    fprintf(stderr, "fileno tests passed\n");
}

static void
test_fgetpos_fsetpos(void)
{
    CHIMERA_FILE   *fp;
    const char     *test_data = "ABCDEFGHIJ";
    chimera_fpos_t  pos;
    char            buf[16];

    fprintf(stderr, "Testing fgetpos/fsetpos...\n");

    // Create test file
    fp = chimera_posix_fopen("/test/fpos_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }
    chimera_posix_fwrite(test_data, 1, strlen(test_data), fp);
    chimera_posix_fclose(fp);

    // Test fgetpos/fsetpos
    fp = chimera_posix_fopen("/test/fpos_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    // Read 3 chars and save position
    chimera_posix_fread(buf, 1, 3, fp);
    if (chimera_posix_fgetpos(fp, &pos) != 0) {
        fprintf(stderr, "fgetpos failed\n");
        exit(1);
    }

    // Read more
    chimera_posix_fread(buf, 1, 4, fp);

    // Restore position
    if (chimera_posix_fsetpos(fp, &pos) != 0) {
        fprintf(stderr, "fsetpos failed\n");
        exit(1);
    }

    // Read from saved position
    memset(buf, 0, sizeof(buf));
    chimera_posix_fread(buf, 1, 3, fp);
    if (memcmp(buf, "DEF", 3) != 0) {
        fprintf(stderr, "Data mismatch after fsetpos: got '%s'\n", buf);
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fgetpos/fsetpos tests passed\n");
}

static void
test_fgetc_fputc(void)
{
    CHIMERA_FILE *fp;
    int           c;

    fprintf(stderr, "Testing fgetc/fputc...\n");

    // Test fputc
    fp = chimera_posix_fopen("/test/fputc_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }

    if (chimera_posix_fputc('H', fp) == EOF) {
        fprintf(stderr, "fputc failed\n");
        exit(1);
    }
    if (chimera_posix_fputc('i', fp) == EOF) {
        fprintf(stderr, "fputc failed\n");
        exit(1);
    }

    chimera_posix_fclose(fp);

    // Test fgetc
    fp = chimera_posix_fopen("/test/fputc_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    c = chimera_posix_fgetc(fp);
    if (c != 'H') {
        fprintf(stderr, "fgetc: expected 'H', got '%c' (%d)\n", c, c);
        exit(1);
    }

    c = chimera_posix_fgetc(fp);
    if (c != 'i') {
        fprintf(stderr, "fgetc: expected 'i', got '%c' (%d)\n", c, c);
        exit(1);
    }

    c = chimera_posix_fgetc(fp);
    if (c != EOF) {
        fprintf(stderr, "fgetc: expected EOF, got '%c' (%d)\n", c, c);
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fgetc/fputc tests passed\n");
}

static void
test_fgets_fputs(void)
{
    CHIMERA_FILE *fp;
    char          buf[256];
    int           rc;

    fprintf(stderr, "Testing fgets/fputs...\n");

    // Test fputs
    fp = chimera_posix_fopen("/test/fputs_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }

    rc = chimera_posix_fputs("Line 1\n", fp);
    if (rc == EOF) {
        fprintf(stderr, "fputs failed\n");
        exit(1);
    }

    rc = chimera_posix_fputs("Line 2\n", fp);
    if (rc == EOF) {
        fprintf(stderr, "fputs failed\n");
        exit(1);
    }

    chimera_posix_fclose(fp);

    // Test fgets
    fp = chimera_posix_fopen("/test/fputs_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    if (chimera_posix_fgets(buf, sizeof(buf), fp) == NULL) {
        fprintf(stderr, "fgets failed\n");
        exit(1);
    }
    if (strcmp(buf, "Line 1\n") != 0) {
        fprintf(stderr, "fgets: expected 'Line 1\\n', got '%s'\n", buf);
        exit(1);
    }

    if (chimera_posix_fgets(buf, sizeof(buf), fp) == NULL) {
        fprintf(stderr, "fgets failed\n");
        exit(1);
    }
    if (strcmp(buf, "Line 2\n") != 0) {
        fprintf(stderr, "fgets: expected 'Line 2\\n', got '%s'\n", buf);
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fgets/fputs tests passed\n");
}

static void
test_ungetc(void)
{
    CHIMERA_FILE *fp;
    int           c;

    fprintf(stderr, "Testing ungetc...\n");

    // Create test file
    fp = chimera_posix_fopen("/test/ungetc_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        exit(1);
    }
    chimera_posix_fwrite("ABC", 1, 3, fp);
    chimera_posix_fclose(fp);

    // Test ungetc
    fp = chimera_posix_fopen("/test/ungetc_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        exit(1);
    }

    // Read 'A'
    c = chimera_posix_fgetc(fp);
    if (c != 'A') {
        fprintf(stderr, "fgetc: expected 'A', got '%c'\n", c);
        exit(1);
    }

    // Push 'X' back
    if (chimera_posix_ungetc('X', fp) == EOF) {
        fprintf(stderr, "ungetc failed\n");
        exit(1);
    }

    // Read again - should get 'X'
    c = chimera_posix_fgetc(fp);
    if (c != 'X') {
        fprintf(stderr, "fgetc after ungetc: expected 'X', got '%c'\n", c);
        exit(1);
    }

    // Next should be 'B'
    c = chimera_posix_fgetc(fp);
    if (c != 'B') {
        fprintf(stderr, "fgetc: expected 'B', got '%c'\n", c);
        exit(1);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "ungetc tests passed\n");
}

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Run all tests
    test_fopen_fclose();
    test_fread_fwrite();
    test_fseek_ftell();
    test_rewind();
    test_feof_ferror();
    test_fileno();
    test_fgetpos_fsetpos();
    test_fgetc_fputc();
    test_fgets_fputs();
    test_ungetc();

    fprintf(stderr, "All stdio tests passed!\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
