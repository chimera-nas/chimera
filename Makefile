# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Default build directory:
# - /chimera (devcontainer) uses /build
# - anywhere else uses local ./build
# Override by setting CHIMERA_BUILD_DIR explicitly.
ifeq ($(CURDIR),/chimera)
  CHIMERA_BUILD_DIR ?= /build
else
  CHIMERA_BUILD_DIR ?= build
endif

CMAKE_ARGS := -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -G Ninja
CMAKE_ARGS_RELEASE := -DCMAKE_BUILD_TYPE=Release
CMAKE_ARGS_DEBUG := -DCMAKE_BUILD_TYPE=Debug
# Coverage uses clang's source-based coverage, so force the clang toolchain.
CMAKE_ARGS_COVERAGE := -DCMAKE_BUILD_TYPE=Coverage -DCMAKE_C_COMPILER=clang
CTEST_PARALLEL := $(shell n=$$(nproc); echo $$(( n < 64 ? n : 64 )))
CTEST_ARGS := --output-on-failure --timeout 30 -j $(CTEST_PARALLEL)

# Plain `make` produces a debug build only. Use `make debug`/`make release`
# to also run the test suite, or `make check` for the full CI sweep.
.DEFAULT_GOAL := build_debug

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

.PHONY: build_coverage
build_coverage:
	@mkdir -p ${CHIMERA_BUILD_DIR}/Coverage
	@cmake ${CMAKE_ARGS} ${CMAKE_ARGS_COVERAGE} -S . -B ${CHIMERA_BUILD_DIR}/Coverage
	@ninja -C ${CHIMERA_BUILD_DIR}/Coverage

# Run the suite against the instrumented build, then merge the raw profiles and
# print a coverage report. Each test (and any daemon it spawns) writes its own
# profile via the %p/%m patterns; the path is absolute so it survives the cwd
# changes and netns hops the tests make. The report runs even when tests fail.
COVERAGE_DIR := $(abspath ${CHIMERA_BUILD_DIR}/Coverage)/coverage
.PHONY: test_coverage
test_coverage: build_coverage
	@rm -rf ${COVERAGE_DIR}
	@mkdir -p ${COVERAGE_DIR}/profraw
	-cd ${CHIMERA_BUILD_DIR}/Coverage && \
		LLVM_PROFILE_FILE=${COVERAGE_DIR}/profraw/%m-%p.profraw ctest ${CTEST_ARGS}
	@bash etc/coverage-report.sh ${CHIMERA_BUILD_DIR}/Coverage

.PHONY: debug
debug: build_debug test_debug

.PHONY: release
release: build_release test_release

.PHONY: coverage
coverage: build_coverage test_coverage

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
	@LC_ALL=C scan-build --status-bugs -o ${CHIMERA_BUILD_DIR}/ClangDebug/ScanReport \
		sh -c "cmake ${CMAKE_ARGS} ${CMAKE_ARGS_DEBUG} -D IO_URING_NVME_ENABLED=OFF -S . -B ${CHIMERA_BUILD_DIR}/ClangDebug && ninja -C ${CHIMERA_BUILD_DIR}/ClangDebug"

.PHONY: build_clang_release
build_clang_release:
	@rm -rf ${CHIMERA_BUILD_DIR}/ClangRelease
	@mkdir -p ${CHIMERA_BUILD_DIR}/ClangRelease
	@LC_ALL=C scan-build --status-bugs -o ${CHIMERA_BUILD_DIR}/ClangRelease/ScanReport \
		sh -c "cmake ${CMAKE_ARGS} ${CMAKE_ARGS_RELEASE} -D IO_URING_NVME_ENABLED=OFF -S . -B ${CHIMERA_BUILD_DIR}/ClangRelease && ninja -C ${CHIMERA_BUILD_DIR}/ClangRelease"

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

.PHONY: docs
docs:
	@echo "API reference: docs/api-reference.md"
	@echo "API documentation: docs/api.html"
	@echo "OpenAPI spec: docs/openapi.json"
	@echo "Run 'cd docs && python3 -m http.server' to preview locally"

