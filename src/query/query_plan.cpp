/*
 * @FileName   : query_plan.cpp 
 * @CreateAt   : 2021/11/18
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: 
 */

#include "query/query_plan.hpp"

#include <algorithm>

#include <spdlog/spdlog.h>

namespace inno {

class QueryPlan::Impl {
public:
    TripletId convert2TripletId(const std::shared_ptr<DatabaseBuilder::Option> &db,
                                const std::string &s, const std::string &p, const std::string &o) {
        uint32_t pid = db->getPredicateId(p);
        uint32_t sid, oid;
        if (s[0] == '?') {
            if (!var2id_.count(s)) {
                var2id_[s] = var_idx_;
                id2var_[var_idx_] = s;
                var_idx_++;
            }
            sid = var2id_[s];
        } else {
            sid = db->getEntityId(s);
        }

        if (o[0] == '?') {
            if (!var2id_.count(o)) {
                var2id_[o] = var_idx_;
                id2var_[var_idx_] = o;
                var_idx_++;
            }
            oid = var2id_[o];
        } else {
            oid = db->getEntityId(o);
        };

        return {sid, pid, oid};
    }

    QueryQueue generate(const std::shared_ptr<DatabaseBuilder::Option> &db,
                        const std::vector<std::tuple<std::string, std::string, std::string>> &query_triplets) {

        auto triplet_list = query_triplets;
        std::string s, p, o;
        uint16_t sid, pid, oid;

        std::sort(triplet_list.begin(), triplet_list.end(),
                  [&](const inno::Triplet &a, const inno::Triplet &b) {
                      return db->getPredicateCountBy(std::get<1>(a)) < db->getPredicateCountBy(std::get<1>(b));
                  });

        // node set contains all query variables
        std::unordered_set<std::string> node_set;

        QueryQueue query_queue;

        std::tie(s, p, o) = triplet_list.front();
        if (p[0] == '?') {
            spdlog::error("the 1st query triplet without predicate, cannot handle it!");
            return {};
        }

        TripletId triplet_id = convert2TripletId(db, s, p, o);

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
                spdlog::info("[0] FIRST_SO, size: {},  {} {} {}", db->getPredicateCountBy(p), s, p, o);
            } else {
                type = query_type::SINGLE_O;
                spdlog::info("[0] FIRST_O, {} {} {}", db->getPredicateCountBy(p), s, p, o);
            }
        } else {
            spdlog::info("[0] FIRST_S, {} {} {}", db->getPredicateCountBy(p), s, p, o);
        }

        // if there is only one query triplet, use the above join_or_filter_var_id by default.
        // But if there are more than one query triplets, when query queue have been chosen the second
        // query triplet, the value of join_or_filter_var_id of the first query triplet should be updated.
        query_queue.emplace_back(triplet_id, type);
        triplet_list.erase(triplet_list.begin());

        std::vector<std::string> type_str{
                "FILTER_S",
                "FILTER_O",
                "FILTER_SO",
                "JOIN_S",
                "JOIN_O",
        };

        int idx = 1;
        while (!triplet_list.empty()) {
            auto curr = triplet_list.begin();
            while (curr != triplet_list.end()) {
                bool match = true;

                std::tie(s, p, o) = *curr;
                std::tie(sid, pid, oid) = convert2TripletId(db, s, p, o);

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
                    spdlog::info("[{}] {}, size: {},  {} {} {}", idx++, type_str[type],
                                 db->getPredicateCountBy(p), s, p, o);
                    query_queue.emplace_back(convert2TripletId(db, s, p, o), type);
                    triplet_list.erase(curr);
                    break;
                } else {
                    ++curr;
                }
            }
        }

        return query_queue;
    }

public:
    uint8_t var_idx_;
    std::unordered_map<uint16_t, std::string> id2var_;
    std::unordered_map<std::string, uint16_t> var2id_;
};


QueryPlan::QueryPlan() : impl_(new Impl()) {}

QueryPlan::~QueryPlan() {}

QueryQueue
inno::QueryPlan::generate(const std::shared_ptr<DatabaseBuilder::Option> &db,
                          const std::vector<std::tuple<std::string, std::string, std::string>> &query_triplets) {
    return impl_->generate(db, query_triplets);
}

}

