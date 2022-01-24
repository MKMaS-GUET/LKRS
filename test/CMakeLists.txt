set(SOURCE_FILES
        test.cpp
        sparql_parser_test.cpp
        )

add_executable(unitTests ${SOURCE_FILES})
target_link_libraries(unitTests parser query database ${CONAN_LIBS})
add_test(
        NAME unitTests
        COMMAND unitTests
)