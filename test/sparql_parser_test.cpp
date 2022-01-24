#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <spdlog/spdlog.h>
#include <boost/filesystem.hpp>

#include "parser/sparql_parser.hpp"

namespace test {

namespace fs = boost::filesystem;

class SparqlParserTest : public testing::Test {
protected:
    std::string readSPARQLFromFile(const fs::path& filepath) {
        fs::ifstream infile(filepath, std::ios::in);
        fs::ifstream::sync_with_stdio(false);
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
};

TEST_F(SparqlParserTest, IndistinctSparql) {
    std::string sparql = "select ?x ?p where { ?x ?p <FullProfessor0>. }";
    inno::SparqlParser sparqlParser;
    sparqlParser.parse(sparql);

    auto distinct = sparqlParser.isDistinctQuery();
    EXPECT_FALSE(distinct);

    auto query_variables = sparqlParser.getQueryVariables();
    auto expect_variables = std::vector<std::string> {"?x", "?p"};
    EXPECT_EQ(expect_variables, query_variables);
}

TEST_F(SparqlParserTest, ParseSparqlInCRLF) {
    std::string sparql = "SELECT   ?v0 ?v1    \r\n"
                         "WHERE     {    \r\n"
                         "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v1 . \r\n"
                         "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/subscribes> <http://db.uwaterloo.ca/~galuc/wsdbm/Website36> . \r\n"
                         "}";
    inno::SparqlParser sparqlParser;
    sparqlParser.parse(sparql);

    auto distinct = sparqlParser.isDistinctQuery();
    EXPECT_FALSE(distinct);

    auto query_variables = sparqlParser.getQueryVariables();
    auto expect_variables = std::vector<std::string> {"?v0", "?v1"};
    EXPECT_EQ(expect_variables, query_variables);

    auto triplets = sparqlParser.getQueryTriplets();
    EXPECT_EQ(2, triplets.size());

    std::vector<inno::Triplet> answer = {
            {"?v0", "<http://db.uwaterloo.ca/~galuc/wsdbm/likes>", "?v1"},
            { "?v0", "<http://db.uwaterloo.ca/~galuc/wsdbm/subscribes>", "<http://db.uwaterloo.ca/~galuc/wsdbm/Website36>"},
    };
    for (auto a_beg = answer.begin(), b_beg = triplets.begin();
         a_beg != answer.end() && b_beg != triplets.end();
         a_beg++, b_beg++) {
        EXPECT_EQ(*a_beg, *b_beg);
    }
}


TEST_F(SparqlParserTest, DistinctSparql) {
    std::string sparql = "select distinct ?x ?p where { ?x ?p <FullProfessor0>. }";
    inno::SparqlParser sparqlParser;
    sparqlParser.parse(sparql);

    auto distinct = sparqlParser.isDistinctQuery();
    EXPECT_TRUE(distinct);

    auto query_variables = sparqlParser.getQueryVariables();
    auto expect_variables = std::vector<std::string> {"?x", "?p"};
    EXPECT_EQ(expect_variables, query_variables);
}

TEST_F(SparqlParserTest, ParseInsertStatement) {
    std::string sparql = "INSERT DATA { A :likes B .\n"
                         "A :likes C ."
                         "B :follows D ."
                         "D :follows E . "
                         "}";
    inno::SparqlParser parser;
    parser.parse(sparql);
    auto triplets = parser.getInsertTriplets();
    EXPECT_EQ(4, triplets.size());

    std::vector<inno::Triplet> answer = {
            {"A", ":likes", "B"},
            {"A", ":likes", "C"},
            {"B", ":follows", "D"},
            {"D", ":follows", "E"},
    };
    for (auto a_beg = answer.begin(), b_beg = triplets.begin();
              a_beg != answer.end() && b_beg != triplets.end();
              a_beg++, b_beg++) {
        EXPECT_EQ(*a_beg, *b_beg);
    }
}

} // namespace test
