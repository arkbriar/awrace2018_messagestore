#!/bin/bash

clang-format -i --sort-includes \
    $(find . -type d -name proto -prune -o \( -name "*.cc" -o -name "*.h" \) -print)
