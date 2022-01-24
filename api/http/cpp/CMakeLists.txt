cmake_minimum_required(VERSION 3.19)
project(connection_driver)

set(CMAKE_CXX_STANDARD 14)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

if(EXISTS ${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    conan_basic_setup(NO_OUTPUT_DIRS)
else()
    message(WARNING "The file conanbuildinfo.cmake doesn't exist, you have to run conan install first")
endif()

list( APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules)


include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(connection_driver STATIC connection_driver.cpp)
target_link_libraries(connection_driver ${CONAN_LIBS})
install(TARGETS connection_driver RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/lib)