/*
 * @FileName   : type.hpp 
 * @CreateAt   : 2021/11/18
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: define some type
 */

#ifndef PISANO_TYPE_HPP
#define PISANO_TYPE_HPP

#include <set>
#include <deque>
#include <vector>
#include <string>
#include <utility>
#include <unordered_set>
#include <unordered_map>

namespace inno {

enum query_type {
    FILTER_S,  // that's O is known, one variable no need to join, filter intermediate Result by S
    FILTER_O,  // that's S is known, one variable no need to join, filter intermediate Result by O
    FILTER_SO, // S and O isn't known, but the previous query variable set contains this S and O, filter by S and O

    JOIN_S,    // S is query variable, and O is not known
    JOIN_O,    // O is query variable, and S is not known

    SINGLE_S,   // that's the first query triplet for the whole query statement
    SINGLE_O,   // that's the first query triplet for the whole query statement
    SINGLE_SO,  // that's the first query triplet for the whole query statement
};

using Triplet = std::tuple<std::string, std::string, std::string>;

using ResultSet = std::set<std::vector<std::string>>;

using TempResult = std::vector<std::unordered_map<uint32_t, uint32_t>>;
using TripletId = std::tuple<uint32_t, uint32_t, uint32_t>;  // (Subject, Predicate, Object)
using QueryItem = std::tuple<inno::TripletId, inno::query_type>;// (TripletId tuple, QueryType, Join/Filter Variable Id)
using QueryQueue = std::deque<inno::QueryItem>;

}

#endif //PISANO_TYPE_HPP
