# SPDX-FileCopyrightText: 2024 Ben Jarvis
#
# SPDX-License-Identifier: LGPL

CMAKE_ARGS := -G Ninja
CMAKE_ARGS_RELEASE := -DCMAKE_BUILD_TYPE=Release
CMAKE_ARGS_DEBUG := -DCMAKE_BUILD_TYPE=Debug
CTEST_ARGS := --output-on-failure

default: release

.PHONY: build_release
build_release: 
	@mkdir -p build/release
	@cmake ${CMAKE_ARGS} ${CMAKE_ARGS_RELEASE} -S . -B build/release
	@ninja -C build/release

.PHONY: build_debug
build_debug:
	@mkdir -p build/debug
	@cmake ${CMAKE_ARGS} ${CMAKE_ARGS_DEBUG} -S . -B build/debug
	@ninja -C build/debug

.PHONY: test_debug
test_debug: build_debug
	cd build/debug && ctest ${CTEST_ARGS}

.PHONY: test_release
test_release: build_release
	cd build/release && ctest ${CTEST_ARGS}

.PHONY: debug
debug: build_debug test_debug

.PHONY: release
release: build_release test_release

clean:
	@rm -rf build
		
