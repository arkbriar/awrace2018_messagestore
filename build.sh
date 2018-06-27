#!/bin/bash
# set -e: exit immediately if a pipeline exits in a non-zero status.
# set -v: print shell input lines as they are read.
set -ev

CmakeBinary=$(command -v cmake)
MakeBinary=$(command -v make)
CoreNum=$(getconf _NPROCESSORS_ONLN)
BuildType=Debug

# Enable case insensitive string match
shopt -s nocasematch
# Read build type
if [[ $# -gt 0 ]]; then
    if [ "$1" = "Release" ]; then BuildType="Release";
    elif [ "$1" != "Debug" ]; then printf "Unrecognized build type: %s" $1; fi
fi

# Create build directory and switch to it
mkdir -p build && cd build

# Generate makefile using cmake
${CmakeBinary} -DCMAKE_BUILD_TYPE=${BuildType} ..

# Compile and link
${MakeBinary} -j${CoreNum}
