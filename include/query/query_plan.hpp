/*
 * @FileName   : QueryPlan.hpp 
 * @CreateAt   : 2021/11/18
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: 
 */

#ifndef PISANO_QUERY_PLAN_HPP
#define PISANO_QUERY_PLAN_HPP

#include <memory>

#include "common/type.hpp"
#include "database/database.hpp"

namespace inno {

using QueryItem = std::tuple<TripletId, query_type>;// (TripletId tuple, QueryType, Join/Filter Variable Id)
using QueryQueue = std::deque<QueryItem>;

class QueryPlan {
public:
    QueryPlan();
    ~QueryPlan();

    inno::QueryQueue
    generate(const std::shared_ptr<DatabaseBuilder::Option> &db,
             const std::vector<std::tuple<std::string, std::string, std::string>> &query_triplets);

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
};

}

#endif //PISANO_QUERY_PLAN_HPP
