/*
 * @FileName   : psoBuild.cpp
 * @CreateAt   : 2021/6/19
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: command-line tool for handle database creation.
 */

#include <iostream>
#include <string>
#include <chrono>

#include <spdlog/spdlog.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "database/database.hpp"
#include "common/utils.hpp"

static const auto _ = []{
    spdlog::set_pattern("[%l]\t%v");
    return 0;
}();

int main (int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "./psoBuild <db_name> <raw_rdf_file_path>" << std::endl;
        return 0;
    }

    std::string dbname = argv[1];
    std::string datafile = argv[2];

    spdlog::info("create RDF database <{}> from path '{}'.", dbname, datafile);

    auto ret = inno::timeit(inno::DatabaseBuilder::Create, dbname, datafile);

    spdlog::info("Used time: {} ms.", std::get<1>(ret));

    return 0;
}