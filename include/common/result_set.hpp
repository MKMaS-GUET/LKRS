/*
 * @FileName   : result_set.hpp
 * @CreateAt   : 2021/10/14
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: Just as its name implies, this is used to collect the mappings of query variables and its value.
 */

#ifndef RETRIEVE_SYSTEM_RESULT_SET_HPP
#define RETRIEVE_SYSTEM_RESULT_SET_HPP

#include <vector>
#include <unordered_map>
#include <spdlog/spdlog.h>

namespace inno {

template<typename Key, typename Value>
class ResultSet {
public:
    using key_type = Key;
    using value_type = Value;
    using item_type = std::unordered_map<key_type, value_type>;

public:
    ResultSet() = default;
    explicit ResultSet(const size_t &size): result_(size) {}
    ResultSet(const size_t &size, item_type item): result_(size, item) {}
    ResultSet(const ResultSet<key_type, value_type> &other): result_(other.result_) {}
    ResultSet(ResultSet<key_type, value_type> &&other) noexcept : result_(std::move(other.result_)) {}
    ~ResultSet() = default;

    decltype(auto) begin() { return result_.begin(); }
    decltype(auto) begin() const { return result_.begin(); }
    decltype(auto) end() { return result_.end(); }
    decltype(auto) end() const { return result_.end(); }


    decltype(auto) reserve(const std::size_t &size) {
        return result_.resize(size);
    }

    decltype(auto) emplace_back(item_type &item) {
        return result_.emplace_back(item);
    }

    decltype(auto) emplace_back(const item_type &item) {
        return result_.emplace_back(item);
    }

    decltype(auto) push_back(item_type item) {
//        spdlog::info("item size: {}", item.size());
        return result_.push_back(item);
    }

//    decltype(auto) push_back(const item_type &item) {
//        return result_.push_back(item);
//    }

    decltype(auto) erase(const item_type &item) {
        return result_.erase(item);
    }

    decltype(auto) clear() {
        return result_.clear();
    }

    std::size_t size() const {
        return result_.size();
    }

    bool empty() {
        return result_.empty();
    }

    item_type &operator[](size_t index) {
        return result_.at[index];
    }

    ResultSet<key_type, value_type> operator==(const ResultSet<key_type, value_type> &other) {
        return this->result_ == other.result_;
    }

    ResultSet<key_type, value_type> operator!=(const ResultSet<key_type, value_type> &other) {
        return this->result_ != other.result_;
    }

    ResultSet<key_type, value_type> &operator=(const ResultSet<key_type, value_type> &other) {
//        if (this != other) {
            result_.assign(other.result_.begin(), other.result_.end());
        spdlog::info("&operator= other.result first size = {}", other.result_[0].size());
//        }
        return *this;
    }

    ResultSet<key_type, value_type> &operator=(ResultSet<key_type, value_type> &&other) noexcept {
//        if (this != &other) {
            result_.swap(other.result_);
//        }
        return *this;
    }

private:
    std::vector<item_type> result_;
};

}

#endif //RETRIEVE_SYSTEM_RESULT_SET_HPP
