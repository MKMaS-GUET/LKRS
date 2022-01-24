/*
 * @FileName   : sparql_parser.cpp 
 * @CreateAt   : 2021/10/28
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: 
 */

#include "parser/sparql_parser.hpp"

#include <regex>
#include <sstream>
#include <unordered_map>

#include <spdlog/spdlog.h>

//const std::regex QUERY_PATTERN(R"(SELECT\s+(DISTINCT)?(.*)[\s]?WHERE\s*\{([^}]+)\})", std::regex::icase);
//const std::regex INSERT_PATTERN(R"(INSERT\s+DATA\s*\{([^}]+)\})", std::regex::icase);
const std::regex QUERY_PATTERN(R"(SELECT\s+(DISTINCT)?(.*)[\s]*WHERE\s*\{([^}]+)\})", std::regex::icase);
const std::regex INSERT_PATTERN(R"(INSERT\s+DATA\s*\{([^}]+)\})", std::regex::icase);
const std::regex DELETE_PATTERN(R"(DELETE\s+WHERE\s*\{([^}]+)\})", std::regex::icase);

namespace inno {

class SparqlParser::Impl {
public:
    bool distinct_ = false;
    std::vector<std::string> query_variables;
    std::vector<Triplet> query_triplets_;
    std::vector<Triplet> insert_triplets_;
    std::vector<std::string> predicates_indexed_list_;

    void parse(const std::string &sparql) {
        std::smatch match;
        if (std::regex_search(sparql, match, QUERY_PATTERN)) {
            distinct_ = !match.str(1).empty();
            catchQueryVariables_(match.str(2));
            catchQueryTriplets_(match.str(3));
        } else if (std::regex_search(sparql, match, INSERT_PATTERN)) {
            catchInsertTriplets(match.str(1));
        } else {
            spdlog::error("[SPARQL parser] cannot parse it as SPARQL.");
        }
    }

private:
    void catchQueryVariables_(const std::string &raw_variables) {
        query_variables.clear();

        std::istringstream iss(raw_variables);
        std::istringstream::sync_with_stdio(false);

        using is_iter_str = std::istream_iterator<std::string>;
        auto beg = is_iter_str(iss);
        auto end = is_iter_str();
        query_variables.assign(beg, end);
    }

    void catchQueryTriplets_(const std::string &raw_triplet) {
        query_triplets_.clear();

        std::regex sep("\\.\\s+");
        std::sregex_token_iterator tokens(raw_triplet.cbegin(), raw_triplet.cend(), sep, -1);
        std::sregex_token_iterator end;

        std::string s, p, o;
        for (; tokens != end; ++ tokens) {
            std::istringstream iss(*tokens);
            iss >> s >> p >> o;
            query_triplets_.emplace_back(s, p, o);
            predicates_indexed_list_.emplace_back(p);
        }
    }

    void catchInsertTriplets(const std::string &raw_triplet) {
        insert_triplets_.clear();

        std::regex sep("\\.\\s*");
        std::sregex_token_iterator tokens(raw_triplet.cbegin(), raw_triplet.cend(), sep, -1);
        std::sregex_token_iterator end;

        std::string s, p, o;
        for (; tokens != end; ++ tokens) {
            std::istringstream iss(*tokens);
            iss >> s >> p >> o;
            insert_triplets_.emplace_back(s, p, o);
        }
    }
};

SparqlParser::SparqlParser(): impl_(new Impl()) { }

SparqlParser::~SparqlParser() { }

void SparqlParser::parse(const std::string &sparql) {
    impl_->parse(sparql);
}

std::vector<std::string> SparqlParser::getQueryVariables() {
    return impl_->query_variables;
}

std::vector<std::string> SparqlParser::getQueryVariables() const {
    return impl_->query_variables;
}

std::vector<inno::Triplet> SparqlParser::getQueryTriplets() {
    return impl_->query_triplets_;
}

std::vector<inno::Triplet> SparqlParser::getQueryTriplets() const {
    return impl_->query_triplets_;
}

std::vector<inno::Triplet> SparqlParser::getInsertTriplets() {
    return impl_->insert_triplets_;
}

std::vector<std::string> SparqlParser::getPredicateIndexedList() const {
    return impl_->predicates_indexed_list_;
}

std::vector<std::string> SparqlParser::getPredicateIndexedList() {
    return impl_->predicates_indexed_list_;
}

std::vector<inno::Triplet> SparqlParser::getInsertTriplets() const {
    return impl_->insert_triplets_;
}

bool SparqlParser::isDistinctQuery() {
    return impl_->distinct_;
}

}
