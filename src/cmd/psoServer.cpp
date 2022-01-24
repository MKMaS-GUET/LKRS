/*
 * @FileName   : psoServer.cpp 
 * @CreateAt   : 2021/10/21
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: HTTP server used to handle RDF dataset for remote call
 */

#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <codecvt>
#include <unordered_map>

#include <spdlog/spdlog.h>
#include <httplib.h>
#include <nlohmann/json.hpp>

#include "parser/legacy/sparql_parser.hpp"
#include "database/legacy/database.hpp"
#include "query/legacy/sparql_query.hpp"

using ResultSet = std::vector<std::unordered_map<std::string, std::string>>;

std::unique_ptr<SparqlQuery> sparqlQuery;

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

ResultSet execute_query(std::string &sparql) {
    if (sparqlQuery == nullptr) {
        spdlog::error("database doesn't be loaded correctly.");
        return {};
    }

    SparqlParser parser(sparql);

    auto result = sparqlQuery->query(parser);
    if (result.empty()) {
        return {};
    }
    auto variables = parser.getQueryVariables();

    ResultSet ret;
    // FIXME: haven't removed duplicate item yet
    for (std::unordered_map<std::string, uint64_t> &row : result) {
        std::unordered_map<std::string, std::string> item;
        item.reserve(variables.size());
        for (std::string &variable: variables) {
            uint64_t entity_id = row.at(variable);
            std::string entity = sparqlQuery->getSOById(entity_id).substr(1);
            entity.pop_back();
            item.emplace(variable.substr(1), entity);
        }
        ret.emplace_back(std::move(item));
    }

    return ret;
}

char* G2U(const char* gb2312)
{
    int len = ::MultiByteToWideChar(CP_ACP, 0, gb2312, -1, nullptr , 0);
    wchar_t* wstr = new wchar_t[len + 1];
    memset(wstr, 0, len + 1);
    ::MultiByteToWideChar(CP_ACP, 0, gb2312, -1, wstr, len);
    len = ::WideCharToMultiByte(CP_UTF8, 0, wstr, -1, nullptr, 0, nullptr, nullptr);
    char* str = new char[len + 1];//需要在外面析构，可以改成传指针进来的方式
    memset(str, 0, len + 1);
    ::WideCharToMultiByte(CP_UTF8, 0, wstr, -1, str, len, nullptr, nullptr);
    if (wstr) delete[] wstr;
    return str;
}

void getProductWithPrice(const httplib::Request &req, httplib::Response &res) {
//    std::string sparql = "SELECT ?x where { ?x <ub:name> <FullProfessor0> . }";
     const char* sparql_ ="SELECT ?product ?price WHERE { "
                         "\"联想(lenovo)\" :商品 ?product . "
                         "?product :商品-属性-硬盘容量 \"512G固态\" . "
                         "?product :商品-属性-内存容量 \"16G\" . "
                         "?product :商品-价格 ?price . }";

#ifdef WIN32
     std::string sparql(G2U(sparql_));
#else
     std::string sparql(sparql_);
#endif

//    std::string sparql = readSPARQLFromFile(R"(D:\Projects\Python\computer_knowledge_graph\gome\Q1.txt)");
    nlohmann::json j;
    j["code"] = 1;
    j["message"] = "success";
    j["result"] = execute_query(sparql);
    res.set_content(j.dump(2), "text/plain");
}

void getProductWithReviewInfo(const httplib::Request &req, httplib::Response &res) {
    std::string sparql = readSPARQLFromFile(R"(D:\Projects\Python\computer_knowledge_graph\gome\Q2.txt)");
    nlohmann::json j;
    j["code"] = 1;
    j["message"] = "success";
    j["result"] = execute_query(sparql);
    res.set_content(j.dump(2), "text/plain");
}

int main(int argc, char** argv) {
    if (argc == 1) {
        spdlog::info("psoServer <db_name>");
        return 1;
    }

    std::string db_name = argv[1];
    spdlog::info("Load database: {}", db_name);
    sparqlQuery = std::make_unique<SparqlQuery>(db_name);
    spdlog::info("database Load done");

    httplib::Server svr;

    svr.Get("/product/price", getProductWithPrice);
    svr.Get("/product/review_info", getProductWithReviewInfo);

    spdlog::info("HTTP server starting ...");
    svr.listen("0.0.0.0", 8899);
    return 0;
}