#include "include/connection_driver.hpp"

namespace inno {

std::string Connection::DEFAULT_SERVER_IP = "127.0.0.1";
unsigned short Connection::DEFAULT_SERVER_PORT = 8998;

Connection::Connection(const std::string &db_name,
                       const std::string &ip = DEFAULT_SERVER_IP,
                       const unsigned short &port = DEFAULT_SERVER_PORT)
                        : base_url_("/" + db_name),
                      client_(std::make_unique<httplib::Client>(
                              "http://" + ip + ":" + std::to_string(port) + "/pisano")){
    if (!connect()) {
        std::cerr << "connect failed!" << std::endl;
    }
};

Connection::~Connection() {
    disconnect();
}

ResultSet Connection::query(const std::string &sparql) {
    httplib::Params params;
    params.emplace("sparql", sparql);
    auto res = client_->Post((base_url_+"/query").c_str(), params);
    ResultSet ret;
    if (res->status == 200) {
        auto json = nlohmann::json::parse(res->body.c_str());
        for (const auto &object : json) {
            std::unordered_map<std::string, std::string> temp;
            for (const auto &item: object.items()) {
                temp.emplace(item.key(), item.value());
            }
            ret.emplace_back(temp);
        }
    } else {
        std::cerr << res.error() << std::endl;
    }
    return ret;
}

bool Connection::connect() {
    auto res = client_->Get(base_url_.c_str());
    return res->status == 200;
}

void Connection::disconnect() {
    client_->Get("/stop");
}

}