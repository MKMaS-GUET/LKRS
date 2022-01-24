cmake_minimum_required(VERSION 3.15)

project(pisano LANGUAGES CXX) # CUDA
# if need to set CUDA compiler, add
# -DCMAKE_CUDA_COMPILER="C:/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v10.1/bin/nvcc.exe"

set(CMAKE_CXX_STANDARD 14)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin/)

execute_process( COMMAND conan install .. -s build_type=${CMAKE_BUILD_TYPE} )

if(EXISTS ${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    conan_basic_setup(NO_OUTPUT_DIRS)
else()
    message(WARNING "The file conanbuildinfo.cmake doesn't exist, you have to run conan install first")
endif()

include_directories(include)
add_subdirectory(src)

enable_testing()
add_subdirectory(test)
