# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# CHIMERA_BUILD_IN_WORKTREE: If 1 (default), worktrees use local ./build
# and ignore CHIMERA_BUILD_DIR. If 0, CHIMERA_BUILD_DIR is respected.
CHIMERA_BUILD_IN_WORKTREE ?= 1

# Determine build directory based on source location:
# - Worktrees in /worktrees use local ./build subdirectory (unless CHIMERA_BUILD_IN_WORKTREE=0)
# - Main tree (/chimera) uses /build or CHIMERA_BUILD_DIR if set
ifneq ($(filter /worktrees/%,$(CURDIR)),)
  # In a worktree
  ifeq ($(CHIMERA_BUILD_IN_WORKTREE),1)
    override CHIMERA_BUILD_DIR := build
  else
    CHIMERA_BUILD_DIR ?= /build
  endif
else
  # Not in a worktree
  CHIMERA_BUILD_DIR ?= /build
endif

CMAKE_ARGS := -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -G Ninja
CMAKE_ARGS_RELEASE := -DCMAKE_BUILD_TYPE=Release
CMAKE_ARGS_DEBUG := -DCMAKE_BUILD_TYPE=Debug
CTEST_PARALLEL := $(shell n=$$(nproc); echo $$(( n < 64 ? n : 64 )))
CTEST_ARGS := --output-on-failure -j $(CTEST_PARALLEL)

default: build_debug

.PHONY: build_release
build_release: 
	@mkdir -p ${CHIMERA_BUILD_DIR}/Release
	@cmake ${CMAKE_ARGS} ${CMAKE_ARGS_RELEASE} -S . -B ${CHIMERA_BUILD_DIR}/Release
	@ninja -C ${CHIMERA_BUILD_DIR}/Release

.PHONY: build_debug
build_debug:
	@mkdir -p ${CHIMERA_BUILD_DIR}/Debug
	@cmake ${CMAKE_ARGS} ${CMAKE_ARGS_DEBUG} -S . -B ${CHIMERA_BUILD_DIR}/Debug
	@ninja -C ${CHIMERA_BUILD_DIR}/Debug

.PHONY: test_debug
test_debug: build_debug
	cd ${CHIMERA_BUILD_DIR}/Debug && ctest ${CTEST_ARGS}

.PHONY: test_release
test_release: build_release
	cd ${CHIMERA_BUILD_DIR}/Release && ctest ${CTEST_ARGS}

.PHONY: debug
debug: build_debug test_debug

.PHONY: release
release: build_release test_release

clean:
	@rm -rf ${CHIMERA_BUILD_DIR}/*

.PHONY: syntax-check
syntax-check:
	@find src/ -type f \( -name "*.c" -o -name "*.h" \) -print0 | \
		xargs -0 -I {} sh -c 'uncrustify -c etc/uncrustify.cfg --check {} >/dev/null 2>&1 || (echo "Formatting issue in: {}" && exit 1)' || exit 1


.PHONY: syntax
syntax:
	@find src/ -type f \( -name "*.c" -o -name "*.h" \) -print0 | \
		xargs -0 -I {} sh -c 'uncrustify -c etc/uncrustify.cfg --replace --no-backup {}' >/dev/null 2>&1

.PHONY: build_clang_debug
build_clang_debug:
	@rm -rf ${CHIMERA_BUILD_DIR}/ClangDebug
	@mkdir -p ${CHIMERA_BUILD_DIR}/ClangDebug
	@LC_ALL=C scan-build --status-bugs --exclude ext/libsmb2 -o ${CHIMERA_BUILD_DIR}/ClangDebug/ScanReport \
		sh -c "cmake ${CMAKE_ARGS} ${CMAKE_ARGS_DEBUG} -S . -B ${CHIMERA_BUILD_DIR}/ClangDebug && ninja -C ${CHIMERA_BUILD_DIR}/ClangDebug"

.PHONY: build_clang_release
build_clang_release:
	@rm -rf ${CHIMERA_BUILD_DIR}/ClangRelease
	@mkdir -p ${CHIMERA_BUILD_DIR}/ClangRelease
	@LC_ALL=C scan-build --status-bugs --exclude ext/libsmb2 -o ${CHIMERA_BUILD_DIR}/ClangRelease/ScanReport \
		sh -c "cmake ${CMAKE_ARGS} ${CMAKE_ARGS_RELEASE} -S . -B ${CHIMERA_BUILD_DIR}/ClangRelease && ninja -C ${CHIMERA_BUILD_DIR}/ClangRelease"

.PHONY: build_clang
build_clang: build_clang_debug build_clang_release

BASE ?= main

.PHONY: copyright
copyright:
	@bash etc/copyright-year.sh --base $(BASE)

.PHONY: copyright-check
copyright-check:
	@bash etc/copyright-year.sh --check --base $(BASE)

.PHONY: reuse-lint
reuse-lint:
	@reuse lint

.PHONY: check
check: syntax-check build_release test_release build_debug test_debug build_clang reuse-lint copyright-check
	@echo "All checks passed!"

