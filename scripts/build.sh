#!/bin/bash

test -e ../build || mkdir -p ../build
echo "create build"

test -e ../bin || mkdir -p ../bin
echo "create bin"

cd ../build

#conan install .. -s build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .

cd ..
