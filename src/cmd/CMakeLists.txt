add_executable(psoBuild psoBuild.cpp)
target_link_libraries(psoBuild parser database ${CONAN_LIBS})
        #                                CONAN_PKG::boost CONAN_PKG::spdlog)

add_executable(psoQuery psoQuery.cpp)
target_link_libraries(psoQuery query parser database ${CONAN_LIBS})
#                                CONAN_PKG::boost CONAN_PKG::spdlog)

add_executable(psoHttp psoHttp.cpp)
target_link_libraries(psoHttp query parser database ${CONAN_LIBS})
#                               CONAN_PKG::boost CONAN_PKG::spdlog CONAN_PKG::cpp-httplib CONAN_PKG::nlohmann_json)
