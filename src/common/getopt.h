// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#if !defined(_WIN32) && !defined(CHIMERA_TEST_NATIVE_GETOPT)
#include <getopt.h>
#include <unistd.h>
#else
#include <stdio.h>
#include <string.h>

/* Native CLI parsing: POSIX short options plus GNU-style long options.
 * Like the BSD parser, stop at the first positional argument. State is private
 * to each command-line translation unit; this is not a library-wide API. */
#define no_argument 0
#define required_argument 1
#define optional_argument 2
struct option {
    const char *name;
    int has_arg;
    int *flag;
    int val;
};
static char *optarg;
static int optind = 1;
static int opterr = 1;
static int optopt;
static const char *chimera_option_next;

static inline int
chimera_option_error(const char *program, const char *message, int result)
{
    if (opterr) {
        fprintf(stderr, "%s: %s\n", program, message);
    }
    return result;
}

static inline int
getopt_long(int argc, char *const argv[], const char *options,
            const struct option *long_options, int *long_index)
{
    const char *spec;
    int option;
    optarg = NULL;
    if (optind == 0) {
        optind = 1;
        chimera_option_next = NULL;
    }
    if (options[0] == '+') {
        options++;
    }
    if (!chimera_option_next || !*chimera_option_next) {
        const char *arg;
        if (optind >= argc || argv[optind][0] != '-' || !argv[optind][1]) {
            return -1;
        }
        arg = argv[optind++];
        if (!strcmp(arg, "--")) {
            return -1;
        }
        if (arg[1] == '-' && long_options) {
            const char *name = arg + 2;
            const char *equal = strchr(name, '=');
            size_t length = equal ? (size_t) (equal - name) : strlen(name);
            int match = -1;
            for (int i = 0; long_options[i].name; i++) {
                if (strncmp(name, long_options[i].name, length)) {
                    continue;
                }
                if (strlen(long_options[i].name) == length) {
                    match = i;
                    break;
                }
                match = match == -1 ? i : -2;
            }
            optopt = 0;
            if (match < 0) {
                return chimera_option_error(argv[0], "unknown or ambiguous option", '?');
            }
            const struct option *entry = &long_options[match];
            if (equal && entry->has_arg == no_argument) {
                return chimera_option_error(argv[0], "option does not take an argument", '?');
            }
            if (equal) {
                optarg = (char *) equal + 1;
            } else if (entry->has_arg == required_argument) {
                if (optind >= argc) {
                    optopt = entry->val;
                    return options[0] == ':' ? ':' :
                        chimera_option_error(argv[0], "option requires an argument", '?');
                }
                optarg = argv[optind++];
            }
            if (long_index) {
                *long_index = match;
            }
            if (entry->flag) {
                *entry->flag = entry->val;
                return 0;
            }
            return entry->val;
        }
        chimera_option_next = arg + 1;
    }
    option = (unsigned char) *chimera_option_next++;
    optopt = option;
    spec = strchr(options, option);
    if (!spec || option == ':') {
        return chimera_option_error(argv[0], "unknown option", '?');
    }
    if (spec[1] == ':') {
        if (*chimera_option_next) {
            optarg = (char *) chimera_option_next;
        } else if (spec[2] != ':') {
            if (optind >= argc) {
                chimera_option_next = NULL;
                return options[0] == ':' ? ':' :
                    chimera_option_error(argv[0], "option requires an argument", '?');
            }
            optarg = argv[optind++];
        }
        chimera_option_next = NULL;
    }
    return option;
}

static inline int
getopt(int argc, char *const argv[], const char *options)
{
    return getopt_long(argc, argv, options, NULL, NULL);
}
#endif
