# SPDX-FileCopyrightText: 2024 - 2025 Ben Jarvis
#
# SPDX-License-Identifier: Unlicense

# Allow override of build directory (defaults to ./build)
CHIMERA_BUILD_DIR ?= build

CMAKE_ARGS := -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -G Ninja
CMAKE_ARGS_RELEASE := -DCMAKE_BUILD_TYPE=Release
CMAKE_ARGS_DEBUG := -DCMAKE_BUILD_TYPE=Debug
CTEST_ARGS := --output-on-failure -j 8

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
		
