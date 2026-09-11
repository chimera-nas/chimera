# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only
#
# Make the model-based-test corpus available to the quint suites under src/.
#
# The corpus is GENERATED, always, from the models in ext/specs and the configs
# in this repo.  There is no prebuilt bundle any more, and nothing is fetched:
# a config is what determines a corpus, chimera owns its configs, so a corpus
# built anywhere but here could not reflect them.  That also retires a whole
# class of confusion -- a bundle keyed by the submodule's committed SHA silently
# not matching a locally edited model.
#
# What replaces it: ext/specs exports specs_corpus() and specs_corpus_finalize()
# (ext/specs/cmake/SpecsCorpus.cmake).  Each quint suite registers one cell per
# config it owns, gets back that cell's trace directory, and points its replay
# ctest at it.  One directory per cell, so a cell runs ALL of the traces it is
# given -- no --exclude-prefix carving, no per-trace skipping.
#
#   CHIMERA_MBT_CORPUS=ON   generate the corpus (default)
#   CHIMERA_MBT_CORPUS=OFF  no corpus: the harnesses still build, so the static
#                           analyser still sees them, but no traces are
#                           generated and the replay ctests are not registered.
#                           This is what the scan-build jobs use -- they compile
#                           the tree only to give the analyser its generated
#                           headers and then exclude ext/ from the analysis, so
#                           elaborating every quint model there is pure cost.
#
# On success this sets, in the including scope:
#   CHIMERA_MBT_AVAILABLE   ON, and the specs_corpus() function is defined.

option(CHIMERA_MBT_CORPUS
       "Generate the model-based-test trace corpus from ext/specs + configs" ON)

set(CHIMERA_MBT_AVAILABLE OFF)
set(_specs_src ${CMAKE_CURRENT_SOURCE_DIR}/ext/specs)

if(NOT CHIMERA_MBT_CORPUS)
    message(STATUS
        "specs: CHIMERA_MBT_CORPUS=OFF; no trace corpus generated, "
        "MBT replay ctests disabled")
    return()
endif()

if(NOT EXISTS ${_specs_src}/CMakeLists.txt)
    message(STATUS
        "specs: submodule ext/specs is not checked out "
        "(git submodule update --init ext/specs); MBT trace suites disabled")
    return()
endif()

# quint and node are what generation needs.  Probed here rather than left to
# fail inside ext/specs, so a machine without them gets one clear line instead
# of a configure error in a subdirectory.
find_program(QUINT_BIN quint)
find_program(NODE_BIN NAMES node nodejs)
if(NOT QUINT_BIN OR NOT NODE_BIN)
    message(STATUS
        "specs: quint and node are required to generate the corpus "
        "(npm i -g @informalsystems/quint@$(cat ext/specs/.quint-version)); "
        "MBT trace suites disabled")
    return()
endif()

# Brings in specs_corpus() / specs_corpus_finalize().  ext/specs generates none
# of its own corpus when it is a subdirectory -- see SPECS_LEGACY_CORPUS there.
add_subdirectory(${_specs_src} ${CMAKE_BINARY_DIR}/ext/specs)

set(CHIMERA_MBT_AVAILABLE ON)
message(STATUS "specs: generating the MBT corpus from ext/specs + chimera configs")
