#include <set>
#include <string>
#include <sstream>
#include <fstream>
#include <iostream>

#include <spdlog/spdlog.h>

#include "common/utils.hpp"
#include "database/database.hpp"
#include "query/sparql_query.hpp"

static const auto _ = []{
    spdlog::set_pattern("[%l]\t%v");
    return 0;
}();

std::string readSPARQLFromFile(const std::string& filepath) {
    std::ifstream infile(filepath, std::ios::in);
    std::ifstream::sync_with_stdio(false);
    std::ostringstream buf;
    std::string sparql;
    char ch;
    if (infile.is_open()) {
        while (buf && infile.get(ch)) {
            buf.put(ch);
        }
        sparql = buf.str();
    } else {
        std::cerr << "cannot open file: " << filepath << std::endl;
    }
    infile.close();
    return sparql;
}

void execute_query(inno::SparqlQuery& sparqlQuery, inno::SparqlParser& parser) {
    auto result = sparqlQuery.query(parser);

    spdlog::info("Query time: {} ms.", sparqlQuery.getQueryTime());
    if (result.empty()) {
        spdlog::info("[Empty Result]");
    } else {
        spdlog::info("{} result(s).", result.size());
        auto variables = parser.getQueryVariables();

        std::cout << "\n=============================================================\n";
        std::copy(variables.begin(), variables.end(), std::ostream_iterator<std::string>(std::cout, "\t"));
        std::cout << std::endl;

        for (const auto &item : result) {
            std::copy(item.begin(), item.end(), std::ostream_iterator<std::string>(std::cout, "\t"));
            std::cout << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
    std::cout.tie(nullptr);

    if (argc == 1) {
        std::cout << "psoQuery <db_name> <query_file>" << std::endl;
        std::cout << "psoQuery <db_name>" << std::endl;
        return 1;
    }
    std::string dbname = argv[1];
    std::string query_file = argv[2];

    std::shared_ptr<inno::DatabaseBuilder::Option> db;
    double used_time = 0;

    std::string sparql = readSPARQLFromFile(query_file);
    inno::SparqlParser parser;

    if (argc >= 3) {
        parser.parse(sparql);

        std::tie(db, used_time) =
                inno::timeit(inno::DatabaseBuilder::LoadPartial, dbname, parser.getPredicateIndexedList());
        spdlog::info("<{}> loadAll done, used {} ms.", dbname, used_time);

        inno::SparqlQuery sparqlQuery(db);
        execute_query(sparqlQuery, parser);
    } else {
        std::tie(db, used_time) = inno::timeit(inno::DatabaseBuilder::LoadAll, dbname);
        spdlog::info("<{}> load done, used {} ms.", dbname, used_time);

        inno::SparqlQuery sparqlQuery(db);
        for (;;) {
            std::cout << "\nquery >  ";
            std::cin >> query_file;
            if (query_file == "exit" || query_file == "quit" || query_file == "q") {
                break;
            }

            sparql = readSPARQLFromFile(query_file);
            parser.parse(sparql);
            execute_query(sparqlQuery, parser);
        }
    }

    return 0;
}
