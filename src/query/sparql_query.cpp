/*
 * @FileName   : sparql_query.cpp 
 * @CreateAt   : 2021/10/14
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: 
 */

#include "query/sparql_query.hpp"

#include <list>
#include <queue>
#include <algorithm>
#include <functional>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/utils.hpp"

namespace inno {

class SparqlQuery::Impl {
public:
    explicit Impl(std::shared_ptr<DatabaseBuilder::Option> db)
        : db_(std::move(db)), var_idx_(0), query_time_(0) {
        using namespace std::placeholders;
        query_selector_.emplace(JOIN_S, std::bind(&SparqlQuery::Impl::join_s_, this, _1, _2));
        query_selector_.emplace(JOIN_O, std::bind(&SparqlQuery::Impl::join_o_, this, _1, _2));

        query_selector_.emplace(FILTER_S, std::bind(&SparqlQuery::Impl::filter_s_, this, _1, _2));
        query_selector_.emplace(FILTER_O, std::bind(&SparqlQuery::Impl::filter_o_, this, _1, _2));
        query_selector_.emplace(FILTER_SO, std::bind(&SparqlQuery::Impl::filter_so_, this, _1, _2));

        query_selector_.emplace(SINGLE_S, std::bind(&SparqlQuery::Impl::single_query_, this, _1, _2));
        query_selector_.emplace(SINGLE_O, std::bind(&SparqlQuery::Impl::single_query_, this, _1, _2));
        query_selector_.emplace(SINGLE_SO, std::bind(&SparqlQuery::Impl::single_query_, this, _1, _2));
    }

    ~Impl() = default;

    void initialize() {
        var_idx_ = 0;
        query_time_ = 0;
        var2id_.clear();
        id2var_.clear();
    }

    ResultSet query(SparqlParser &parser) {
        initialize();

        QueryQueue query_queue = generateQueryPlan(parser);

        TempResult result;
        std::tie(result, query_time_) =
                inno::timeit(std::bind(&SparqlQuery::Impl::execute, this, query_queue));

        return resultMapper(result, parser.getQueryVariables());
    }

    TripletId convert2TripletId(const std::string &s, const std::string &p, const std::string &o) {
        uint32_t pid = db_->getPredicateId(p);
        uint32_t sid, oid;
        if (s[0] == '?') {
            if (!var2id_.count(s)) {
                var2id_[s] = var_idx_;
                id2var_[var_idx_] = s;
                var_idx_++;
            }
            sid = var2id_[s];
        } else {
            sid = db_->getEntityId(s);
        }

        if (o[0] == '?') {
           if (!var2id_.count(o)) {
               var2id_[o] = var_idx_;
               id2var_[var_idx_] = o;
               var_idx_++;
           }
           oid = var2id_[o];
        } else {
           oid = db_->getEntityId(o);
        };

        return {sid, pid, oid};
    }

    QueryItem markAsSingle(const Triplet &triplet, std::unordered_set<std::string> &node_set) {
        std::string s, p, o;
        std::tie(s, p, o) = triplet;
//        if (p[0] == '?') {
//            spdlog::error("the 1st query triplet without predicate, cannot handle it!");
//            return {};
//        }

        TripletId triplet_id = convert2TripletId(s, p, o);

        // join or filter variable id, that's use to join or filter when execute query
        query_type type;
        if (s[0] == '?') {
            node_set.emplace(s);
            type = query_type::SINGLE_S;
        }
        if (o[0] == '?') {
            node_set.emplace(o);
            if (s[0] == '?') {
                // special case, s and o are both query variable,
                // which means the join_or_filter_var_id is not 0,
                // so left shift 8 bit so that can use bitwise OR to add oid;
                type = query_type::SINGLE_SO;
                spdlog::info("[] SINGLE_SO, size: {},  {} {} {}", db_->getPredicateCountBy(p), s, p, o);
            } else {
                type = query_type::SINGLE_O;
                spdlog::info("[] SINGLE_O, {} {} {}", db_->getPredicateCountBy(p), s, p, o);
            }
        } else {
            spdlog::info("[] SINGLE_S, {} {} {}", db_->getPredicateCountBy(p), s, p, o);
        }
        return { triplet_id, type };
    }

    QueryQueue generateQueryPlan(SparqlParser &parser) {
        auto triplet_list = parser.getQueryTriplets();
        std::string s, p, o;
        uint16_t sid, pid, oid;
//        if (triplet_list.size() == 1) {
//            std::tie(s, p, o) = triplet_list.back();
//
//            // if predicate is query variables,
//            if (p[0] == '?') {
//                spdlog::error("the 1st query triplet without predicate, cannot handle it!");
//                return  {};
//            }
//            bool is_s_var = s[0] == '?';
//            return { {convert2TripletId(db, s, p, o),
//                      is_s_var ? QueryType::FILTER_O : QueryType::FILTER_S,
//                      is_s_var ? sid : oid} };
//        }

        std::sort(triplet_list.begin(), triplet_list.end(),
                  [this](const inno::Triplet &a, const inno::Triplet &b) {
            std::string a_s, a_p, a_o, b_s, b_p, b_o;
            std::tie(a_s, a_p, a_o) = a;
            std::tie(b_s, b_p, b_o) = b;
            uint32_t a_num = db_->getPredicateCountBy(a_p);
            uint32_t b_num = db_->getPredicateCountBy(b_p);

            if (a_s[0] != '?') a_num = std::min(a_num, db_->getEntityCountBy(a_s));
            else if (a_o[0] != '?') a_num = std::min(a_num, db_->getEntityCountBy(a_o));

            if (b_s[0] != '?') b_num = std::min(b_num, db_->getEntityCountBy(b_s));
            else if (b_o[0] != '?') b_num = std::min(b_num, db_->getEntityCountBy(b_o));

            return  a_num < b_num;
        });

        // node set contains all query variables
        std::unordered_set<std::string> node_set;

        QueryQueue query_queue;

        // if there is only one query triplet, use the above join_or_filter_var_id by default.
        // But if there are more than one query triplets, when query queue have been chosen the second
        // query triplet, the value of join_or_filter_var_id of the first query triplet should be updated.
        query_queue.emplace_back(markAsSingle(triplet_list.front(), node_set));
        triplet_list.erase(triplet_list.begin());

        query_type type;
        std::vector<std::string> type_str {
            "FILTER_S",
            "FILTER_O",
            "FILTER_SO",
            "JOIN_S",
            "JOIN_O",
        };

        int idx = 1;
        while (!triplet_list.empty()) {
            auto curr = triplet_list.begin();
            size_t old_size = triplet_list.size();
            while (curr != triplet_list.end()) {
                bool match = true;

                std::tie(s, p, o) = *curr;
                std::tie(sid, pid, oid) = convert2TripletId(s, p, o);

                if (p[0] == '?') {
                    spdlog::error("the query triplet({} {} {}) without predicate, cannot handle it!", s, p, o);
                    return {};
                }

                bool is_s_var = s[0] == '?';
                bool is_o_var = o[0] == '?';
                bool is_s_in_node_set = is_s_var && node_set.count(s);
                bool is_o_in_node_set = is_o_var && node_set.count(o);

                if (is_s_var && is_o_var) {
                    if (is_s_in_node_set && is_o_in_node_set) {
                        // special case, which have two var to filter.
                        type = query_type::FILTER_SO;
                    } else if (is_s_in_node_set) {
                        type = query_type::JOIN_S;
                        node_set.emplace(o);
                    } else if (is_o_in_node_set) {
                        type = query_type::JOIN_O;
                        node_set.emplace(s);
                    } else {
                        match = false;
                    }
                } else if (is_s_var && is_s_in_node_set) {
                    type = query_type::FILTER_S;
                } else if (is_o_var && is_o_in_node_set) {
                    type = query_type::FILTER_O;
                } else {
                    match = false;
                }

                if (match) {
                    spdlog::info("[{}] {}, size: {},  {} {} {}", idx++, type_str[type], db_->getPredicateCountBy(p), s, p, o);
                    query_queue.emplace_back(convert2TripletId(s, p, o), type);
                    triplet_list.erase(curr);
                    break;
                } else {
                    ++curr;
                }
            }
            // for the situation of matching nothing for the previous triplets
//            if (curr == triplet_list.end() && !triplet_list.empty()) {
            // if the new_size is equal with the old_size, that's mean matches nothing in this turn.
            size_t new_size = triplet_list.size();
            if (old_size == new_size) {
                curr = triplet_list.begin();
                query_queue.emplace_back(markAsSingle(*curr, node_set));
                triplet_list.erase(curr);
            }
        }

        return query_queue;
    }

    TempResult execute(QueryQueue &query_queue) {
        TempResult result;
        if (query_queue.empty()) {
            return result;
        }

//        auto query_item = query_queue.front(); query_queue.pop_front();
//        result = first_query_(query_item);

//        double time = 0;
//        std::tie( result, time ) = inno::timeit(std::bind(&SparqlQuery::Impl::first_query_, this, query_item));
//        int idx = 0;
//        spdlog::info("[{}] result size: {}, used {} ms.", idx++, result.size(), time);

        while (!query_queue.empty()) {
            auto query_item = query_queue.front(); query_queue.pop_front();
            result = query_selector_.at(std::get<1>(query_item))(result, query_item);
            if (result.empty()) {
                break;
            }

//           std::tie( result, time ) = inno::timeit( query_selector_.at(std::get<1>(query_item)), result, query_item);
//            spdlog::info("[{}] result size: {}, used {} ms.", idx++, result.size(), time);
        }
        return result;
    }

    ResultSet resultMapper(const TempResult &temp_result, const std::vector<std::string> &query_variables) {
        std::vector<uint16_t> query_ids;
        query_ids.reserve(query_variables.size());
        for (const auto &var : query_variables) {
            query_ids.emplace_back(var2id_[var]);
        }

        ResultSet result;
        for (auto &item : temp_result) {
//            std::unordered_map<std::string, std::string> result_item;
            std::vector<std::string> result_item;
            result_item.reserve(query_ids.size());
            for (auto &var_id : query_ids) {
                uint32_t entity_id = item.at(var_id);
                result_item.emplace_back(db_->getEntityById(entity_id));
//                result_item.emplace(id2var_[var_id], db_->getEntityById(entity_id));
            }
            result.insert(std::move(result_item));
        }
        return result;
    }

private:
    using ResultItemType = std::unordered_map<uint32_t, uint32_t>;

private:

    TempResult
    single_query_(const TempResult &temp_result, const QueryItem &query_item) {
        TripletId tripletId;
        query_type type;
        std::tie(tripletId, type) = query_item;

        uint32_t sid, pid, oid;
        std::tie(sid, pid, oid) = tripletId;

//        auto data = db_->getSOByP(pid);
        auto data = db_->getS2OByP(pid);

        TempResult result;
        result.reserve(data.size());

        if (type == query_type::SINGLE_S) {
            for (const auto &item : data) {
                if (oid == item.second) {
                    ResultItemType result_item;
                    result_item.emplace(sid, item.first);

                    if (!temp_result.empty()) {
                        for (const auto &temp : temp_result) {
                            result_item.insert(temp.begin(), temp.end());
                        }
                    }

                    result.push_back(std::move(result_item));
                }
            }
        } else if (type == query_type::SINGLE_O) {
            for (const auto &item : data) {
                if (sid == item.first) {
                    ResultItemType result_item;
                    result_item.emplace(oid, item.second);

                    if (!temp_result.empty()) {
                        for (const auto &temp : temp_result) {
                            result_item.insert(temp.begin(), temp.end());
                        }
                    }

                    result.push_back(std::move(result_item));
                }
            }
        } else {
            for (const auto &item : data) {
                ResultItemType result_item;
                result_item.emplace(sid, item.first);
                result_item.emplace(oid, item.second);

                if (!temp_result.empty()) {
                    for (const auto &temp : temp_result) {
                        result_item.insert(temp.begin(), temp.end());
                    }
                }

                result.push_back(std::move(result_item));
            }
        }

//        for (const auto &item : data) {
//            ResultItemType result_item;
//            if (type == QueryType::SINGLE_S && oid == item.second) {
//                result_item.emplace(sid, item.first);
//            } else if (type == QueryType::SINGLE_O && sid == item.first) {
//                result_item.emplace(oid, item.second);
//            } else {
//                result_item.emplace(sid, item.first);
//                result_item.emplace(oid, item.second);
//            }
//            result.push_back(std::move(result_item));
//        }

        return result;
    }

    TempResult
    join_s_(const TempResult &temp_result, const QueryItem &query_item) {
        TripletId tripletId;
        query_type type;
        std::tie(tripletId, type) = query_item;

        uint32_t sid, pid, oid;
        std::tie(sid, pid, oid) = tripletId;

        auto data = db_->getS2OByP(pid);


        TempResult result;
        result.reserve(temp_result.size());
        for (const auto &item : temp_result) {
            auto range = data.equal_range(item.at(sid));
            for (auto it = range.first; it != range.second; ++it) {
                ResultItemType result_item = item;
                result_item.emplace(oid, it->second);
                result.emplace_back(std::move(result_item));
            }
        }

        return result;
    }

    TempResult
    join_o_(const TempResult &temp_result, const QueryItem &query_item) {
        TripletId tripletId;
        query_type type;
        std::tie(tripletId, type) = query_item;

        uint32_t sid, pid, oid;
        std::tie(sid, pid, oid) = tripletId;
        auto data = db_->getO2SByP(pid);

        TempResult result;
        result.reserve(temp_result.size());
        for (const auto &item : temp_result) {
            auto range = data.equal_range(item.at(oid));
            for (auto it = range.first; it != range.second; ++it) {
                ResultItemType result_item = item;
                result_item.emplace(sid, it->second);
                result.emplace_back(result_item);
            }
        }
        return result;
    }

    TempResult
    filter_s_(const TempResult &temp_result, const QueryItem &query_item) {
        TripletId tripletId;
        query_type type;
        std::tie(tripletId, type) = query_item;

        uint32_t sid, pid, oid;
        std::tie(sid, pid, oid) = tripletId;
        auto data = db_->getSByPO(pid, oid);

        TempResult result;
        result.reserve(temp_result.size());
        for (const auto &item : temp_result) {
            if (data.count(item.at(sid))) {
               result.emplace_back(item);
            }
        }
        return result;
    }

    TempResult
    filter_o_(const TempResult &temp_result, const QueryItem &query_item) {
        TripletId tripletId;
        query_type type;
        std::tie(tripletId, type) = query_item;

        uint32_t sid, pid, oid;
        std::tie(sid, pid, oid) = tripletId;
        auto data = db_->getOBySP(sid, pid);

        TempResult result;
        result.reserve(temp_result.size());
        for (const auto &item : temp_result) {
            if (data.count(item.at(oid))) {
                result.emplace_back(item);
            }
        }
        return result;
    }

    TempResult
    filter_so_(const TempResult &temp_result, const QueryItem &query_item) {
        TripletId tripletId;
        query_type type;
        std::tie(tripletId, type) = query_item;

        uint32_t sid, pid, oid;
        std::tie(sid, pid, oid) = tripletId;
//        auto data = db_->getSOByP(pid);

        ///////////


        auto data = db_->getS2OByP(pid);

        TempResult result;
        result.reserve(temp_result.size());
        for (const auto &item : temp_result) {
            auto range = data.equal_range(item.at(sid));
            for (auto it = range.first; it != range.second; ++it) {
                if (it->second == item.at(oid)) {
                    result.emplace_back(item);
                }
//                ResultItemType result_item(item.begin(), item.end());
//                result_item.emplace(std::get<2>(tripletId), it->second);
//                result.push_back(std::move(result_item));
            }
        }
        return result;
        //////////

//        TempResult result;
//        result.reserve(temp_result.size());
//        for (const auto &item : temp_result) {
//            if (data.count({item.at(sid), item.at(oid)})) {
//                result.emplace_back(item);
//            }
//        }
//        return result;
    }

public:
    double query_time_;

private:
    uint8_t var_idx_;
    std::unordered_map<uint16_t, std::string> id2var_;
    std::unordered_map<std::string, uint16_t> var2id_;
    std::unordered_map<query_type, std::function<TempResult(TempResult const&, QueryItem const&)>> query_selector_;
    std::shared_ptr<DatabaseBuilder::Option> db_;
};

SparqlQuery::SparqlQuery(const std::shared_ptr<DatabaseBuilder::Option> &db) : impl_(new Impl(db)) { }

SparqlQuery::~SparqlQuery() { }

//ResultSet<std::string, std::string>
//std::vector<std::unordered_map<std::string, std::string>>
inno::ResultSet
SparqlQuery::query(SparqlParser &parser) {
    return impl_->query(parser);
}

double SparqlQuery::getQueryTime() const {
    return impl_->query_time_;
}

}