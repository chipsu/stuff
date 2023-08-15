#!/bin/bash
g++ int32set.cpp --std=c++20 -mtune=native -O3 -o int32set || exit 1
chmod +x int32set
./int32set
ls -lha *.bin
