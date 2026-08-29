#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# coverage-report.sh - Merge clang source-based coverage data and print a report
#
# Usage: coverage-report.sh BUILD_DIR
#
# Expects the test suite to have already run with the binaries instrumented by a
# Coverage build (-fprofile-instr-generate -fcoverage-mapping) and with
# LLVM_PROFILE_FILE pointing into BUILD_DIR/coverage/profraw (see the Makefile's
# test_coverage target). This merges every *.profraw into a single profdata file
# and prints a per-file llvm-cov report covering the chimera source tree.
#
# Set COVERAGE_HTML=1 to additionally emit a browsable HTML report under
# BUILD_DIR/coverage/html, or COVERAGE_JSON=<path> to additionally write
# llvm-cov's machine-readable per-file summary there (what CI renders its
# coverage report from).

set -euo pipefail

BUILD_DIR="${1:?usage: coverage-report.sh BUILD_DIR}"
COV_DIR="${BUILD_DIR}/coverage"
PROFRAW_DIR="${COV_DIR}/profraw"
PROFDATA="${COV_DIR}/coverage.profdata"

PROFDATA_TOOL="${LLVM_PROFDATA:-llvm-profdata}"
COV_TOOL="${LLVM_COV:-llvm-cov}"

# System headers and vendored third-party trees are never anybody's code here,
# so they are always out.  Generated sources and the bundled ext/ projects are
# out by default -- the local `make coverage` report is about the code you are
# editing -- and each has a switch to fold it back in.  CI's quint report sets
# both: its question is how much of everything that ships the models reach, and
# generated marshallers and libevpl ship.
ignore_parts=('/usr/' '/3rdparty/' '/CMakeFiles/')

# Generated sources (the xdrzcc/ndrzcc XDR/NDR marshallers, embedded asset
# blobs, etc.) are emitted into the build tree, whereas hand-written sources
# live under the source tree -- so "compiled from under BUILD_DIR" is a reliable,
# generator-agnostic way to spot generated code.
if [[ "${COVERAGE_INCLUDE_GENERATED:-0}" == "1" ]]; then
    echo "coverage: including generated code (COVERAGE_INCLUDE_GENERATED=1)"
else
    abs_build=$(readlink -f "${BUILD_DIR}")
    # Escape regex metacharacters so the path is matched literally.
    esc_build=$(printf '%s' "${abs_build}" | sed 's/[.[\*^$()+?{|/]/\\&/g')
    ignore_parts+=("${esc_build}/")
    echo "coverage: excluding generated code under ${abs_build} (set COVERAGE_INCLUDE_GENERATED=1 to include)"
fi

if [[ "${COVERAGE_INCLUDE_EXT:-0}" == "1" ]]; then
    echo "coverage: including the bundled ext/ projects (COVERAGE_INCLUDE_EXT=1)"
else
    ignore_parts+=('/ext/')
    echo "coverage: excluding the bundled ext/ projects (set COVERAGE_INCLUDE_EXT=1 to include)"
fi

IGNORE_REGEX="($(IFS='|'; echo "${ignore_parts[*]}"))"

# A full suite run produces hundreds of thousands of .profraw files (one per
# test process / spawned daemon), which overflows ARG_MAX if globbed onto the
# command line. Build a newline-separated list file and hand it to llvm-profdata
# via --input-files instead.
raw_list="${COV_DIR}/profraw.list"
find "${PROFRAW_DIR}" -type f -name '*.profraw' > "${raw_list}"
raw_count=$(wc -l < "${raw_list}")
if [[ "${raw_count}" -eq 0 ]]; then
    echo "coverage: no .profraw files found under ${PROFRAW_DIR}" >&2
    echo "coverage: did the tests run against a Coverage build with LLVM_PROFILE_FILE set?" >&2
    exit 1
fi

echo "coverage: merging ${raw_count} raw profile(s) ..."
"${PROFDATA_TOOL}" merge -sparse --input-files="${raw_list}" -o "${PROFDATA}"

# Collect the instrumented binaries: any executable or shared object in the
# build tree that carries an __llvm_covmap section. llvm-cov fails outright if
# handed an object with no coverage mapping, so we must filter rather than glob.
objs=()
while IFS= read -r f; do
    # Deliberately not `readelf | grep -q`.  grep -q exits at its first match,
    # readelf then dies of SIGPIPE, and `set -o pipefail` reports the pipeline
    # as failed -- so the object is silently dropped.  Whether that happens is
    # a race on how much readelf has already written, so the object set, and
    # every number derived from it, varied run to run: this tree reported 3,
    # 14 and 423 instrumented binaries on three consecutive runs.  No pipe,
    # no race.
    if [[ "$(readelf -SW "$f" 2>/dev/null)" == *__llvm_covmap* ]]; then
        objs+=("$f")
    fi
done < <(find "${BUILD_DIR}" -type f \
            \( -perm -u+x -o -name '*.so' -o -name '*.so.*' \) \
            ! -path '*/CMakeFiles/*')

if [[ ${#objs[@]} -eq 0 ]]; then
    echo "coverage: no instrumented binaries found under ${BUILD_DIR}" >&2
    exit 1
fi

# llvm-cov takes the first object positionally and the rest via -object.
object_args=()
for o in "${objs[@]:1}"; do
    object_args+=(-object "$o")
done

echo "coverage: reporting over ${#objs[@]} instrumented binaries"
echo
"${COV_TOOL}" report "${objs[0]}" "${object_args[@]}" \
    -instr-profile="${PROFDATA}" \
    -ignore-filename-regex="${IGNORE_REGEX}" \
    -show-region-summary=false

# Machine-readable per-file summary for CI to render.  -summary-only drops the
# per-segment detail, which is what makes the export a few hundred KB instead of
# a few hundred MB over a whole-tree instrumented build.
if [[ -n "${COVERAGE_JSON:-}" ]]; then
    echo
    echo "coverage: exporting per-file summary to ${COVERAGE_JSON} ..."
    "${COV_TOOL}" export "${objs[0]}" "${object_args[@]}" \
        -instr-profile="${PROFDATA}" \
        -ignore-filename-regex="${IGNORE_REGEX}" \
        -summary-only > "${COVERAGE_JSON}"
fi

# Per-line hit counts for a named handful of sources, as an lcov tracefile.
# This is the full (non-summary) export, which is why it is restricted: over the
# whole tree it runs to hundreds of megabytes, but over the files one pull
# request touched it is tens of kilobytes.  scripts/ci_patch_coverage.py reads
# it to say which of the lines a PR changed were actually executed.
#
# Emitted here rather than from a second llvm-cov invocation elsewhere because
# collecting the instrumented objects above means readelf'ing every binary in
# the build tree, which costs more than the export itself.
#
# An empty source list means "no sources to measure" and must NOT fall through
# to a bare `export`: llvm-cov would then happily export the entire tree.
if [[ -n "${COVERAGE_LCOV:-}" && -s "${COVERAGE_LCOV_SOURCES:-/dev/null}" ]]; then
    mapfile -t lcov_sources < "${COVERAGE_LCOV_SOURCES}"
    echo
    echo "coverage: exporting ${#lcov_sources[@]} source(s) to ${COVERAGE_LCOV} ..."
    "${COV_TOOL}" export "${objs[0]}" "${object_args[@]}" \
        -instr-profile="${PROFDATA}" \
        -ignore-filename-regex="${IGNORE_REGEX}" \
        --format=lcov --sources "${lcov_sources[@]}" > "${COVERAGE_LCOV}"
fi

if [[ "${COVERAGE_HTML:-0}" == "1" ]]; then
    HTML_DIR="${COV_DIR}/html"
    echo
    echo "coverage: writing HTML report to ${HTML_DIR} ..."
    "${COV_TOOL}" show "${objs[0]}" "${object_args[@]}" \
        -instr-profile="${PROFDATA}" \
        -ignore-filename-regex="${IGNORE_REGEX}" \
        -format=html -output-dir="${HTML_DIR}" \
        -show-line-counts-or-regions
    echo "coverage: open ${HTML_DIR}/index.html"
fi
