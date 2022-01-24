/*
 * @FileName   : sparql_query.hpp 
 * @CreateAt   : 2021/6/25
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: 
 */

#ifndef RETRIEVE_SYSTEM_SPARQL_QUERY_HPP
#define RETRIEVE_SYSTEM_SPARQL_QUERY_HPP

#include <memory>

#include "database/database.hpp"
#include "parser/sparql_parser.hpp"
#include "common/type.hpp"

namespace inno {
class SparqlQuery {

public:

public:
    explicit SparqlQuery(const std::shared_ptr<DatabaseBuilder::Option> &db);
    ~SparqlQuery();

    ResultSet query(SparqlParser &parser);
    double getQueryTime() const;

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
};

}

#endif //RETRIEVE_SYSTEM_SPARQL_QUERY_HPP
