/*
 * @FileName   : connection_driver.hpp
 * @CreateAt   : 2021/10/22
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description:
 */

#ifndef RETRIEVE_SYSTEM_CONNECTION_DRIVER_HPP
#define RETRIEVE_SYSTEM_CONNECTION_DRIVER_HPP

#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <unordered_map>

#include <httplib.h>
#include <nlohmann/json.hpp>

namespace inno {

using ResultSet = std::vector<std::unordered_map<std::string, std::string>>;

class Connection {

public:
    Connection(const std::string &db_name, const std::string &ip, const unsigned short &port);
    ~Connection();
    ResultSet query(const std::string &sparql);

private:
    bool connect();
    void disconnect();

private:
    static std::string DEFAULT_SERVER_IP;
    static unsigned short DEFAULT_SERVER_PORT;
    std::string base_url_;
    std::unique_ptr<httplib::Client> client_;
};

}

#endif //RETRIEVE_SYSTEM_CONNECTION_DRIVER_HPP
