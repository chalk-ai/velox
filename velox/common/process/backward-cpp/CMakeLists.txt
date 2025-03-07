#
# CMakeLists.txt
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

cmake_minimum_required(VERSION 3.14)
project(backward_velox CXX)

# Introduce variables:
# * CMAKE_INSTALL_LIBDIR
# * CMAKE_INSTALL_BINDIR
# * CMAKE_INSTALL_INCLUDEDIR
include(GNUInstallDirs)

include(BackwardVeloxConfig.cmake)

###############################################################################
# OPTIONS
###############################################################################

option(BACKWARD_VELOX_SHARED "Build backward_velox as a shared library" OFF)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR AND NOT DEFINED BACKWARD_VELOX_TESTS)
    # If this is a top level CMake project, we most lixely want the tests
    set(BACKWARD_VELOX_TESTS ON CACHE BOOL "Enable tests")
else()
    set(BACKWARD_VELOX_TESTS OFF CACHE BOOL "Enable tests")
endif()

###############################################################################
# COMPILER FLAGS
###############################################################################

# check if compiler is nvcc or nvcc_wrapper
set(COMPILER_IS_NVCC false)
get_filename_component(COMPILER_NAME ${CMAKE_CXX_COMPILER} NAME)
if (COMPILER_NAME MATCHES "^nvcc")
    set(COMPILER_IS_NVCC true)
endif()

if (DEFINED ENV{OMPI_CXX} OR DEFINED ENV{MPICH_CXX})
    if ( ($ENV{OMPI_CXX} MATCHES "nvcc") OR ($ENV{MPICH_CXX} MATCHES "nvcc") )
        set(COMPILER_IS_NVCC true)
    endif()
endif()

# set CXX standard
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_STANDARD 11)
if (${COMPILER_IS_NVCC})
    # GNU CXX extensions are not supported by nvcc
    set(CMAKE_CXX_EXTENSIONS OFF)
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
    if (NOT ${COMPILER_IS_NVCC})
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic-errors")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
endif()

###############################################################################
# BACKWARD_VELOX INTERFACE
###############################################################################

add_library(backward_velox_interface INTERFACE)
set_target_properties(backward_velox_interface PROPERTIES EXPORT_NAME Interface)
target_compile_definitions(backward_velox_interface INTERFACE ${BACKWARD_VELOX_DEFINITIONS})
target_include_directories(backward_velox_interface INTERFACE ${BACKWARD_VELOX_INCLUDE_DIRS})
if(BACKWARD_VELOX_HAS_EXTERNAL_LIBRARIES)
    target_link_libraries(backward_velox_interface INTERFACE ${BACKWARD_VELOX_LIBRARIES})
endif()
add_library(BackwardVelox::Interface ALIAS backward_velox_interface)

###############################################################################
# BACKWARD_VELOX OBJECT (Includes backward_velox.cpp)
# (Note that this target is not exported, since CMake currently does not allow
# exporting an OBJECT library.)
###############################################################################

add_library(backward_velox_object OBJECT backward_velox.cpp)
set_target_properties(backward_velox_object PROPERTIES EXPORT_NAME Object)
target_link_libraries(backward_velox_object PUBLIC BackwardVelox::Interface)
add_library(BackwardVelox::Object ALIAS backward_velox_object)

###############################################################################
# BACKWARD_VELOX LIBRARY (Includes backward_velox.cpp)
# (Note that the linker will not include unused objects from a static library,
# unless the -Wl,--whole-archive option (or similar) is used.)
###############################################################################

set(libtype STATIC)
if(BACKWARD_VELOX_SHARED)
    set(libtype SHARED)
endif()
add_library(backward_velox ${libtype} backward_velox.cpp)
set_target_properties(backward_velox PROPERTIES EXPORT_NAME BackwardVelox)
target_link_libraries(backward_velox PUBLIC BackwardVelox::Interface)
add_library(BackwardVelox::BackwardVelox ALIAS backward_velox)

###############################################################################
# TESTS
###############################################################################

if(BACKWARD_VELOX_TESTS)
    enable_testing()

    add_library(test_main OBJECT test/_test_main.cpp)

    macro(backward_velox_add_test src)
        get_filename_component(name ${src} NAME_WE)
        set(test_name "test_${name}")

        add_executable(${test_name} ${src} ${ARGN} $<TARGET_OBJECTS:test_main>)

        target_link_libraries(${test_name} PRIVATE BackwardVelox::Interface)

        add_test(NAME ${name} COMMAND ${test_name})
    endmacro()

    # Tests without backward_velox.cpp
    set(TESTS
            test
            stacktrace
            rectrace
            select_signals
    )

    foreach(test ${TESTS})
        backward_velox_add_test(test/${test}.cpp)
    endforeach()

    # Tests with backward_velox.cpp
    set(TESTS
            suicide
    )

    foreach(test ${TESTS})
        backward_velox_add_test(test/${test}.cpp backward_velox.cpp)
    endforeach()
endif()

install(
        FILES "backward_velox.hpp"
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(
        FILES "BackwardVeloxConfig.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
)
# check if BackwardVelox is being used as a top-level project or included as a subproject
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    # export the targets (note that exporting backward_velox_object does not make sense)
    install(TARGETS backward_velox_interface backward_velox EXPORT BackwardVeloxTargets)
    # install a CMake file for the exported targets
    install(EXPORT BackwardVeloxTargets
            NAMESPACE BackwardVelox::
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
endif()
