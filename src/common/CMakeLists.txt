set(THIS common)

file(GLOB SOURCE_FILES  *.cpp)
source_group("Source" FILES ${SOURCE_FILES})

add_library(${THIS} STATIC ${SOURCE_FILES})
