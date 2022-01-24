/*
 * @FileName   : skip_list.hpp
 * @CreateAt   : 2021/9/21
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: implement Skip List
 */

#ifndef RETRIEVE_SYSTEM_SKIP_LIST_HPP
#define RETRIEVE_SYSTEM_SKIP_LIST_HPP

#include <chrono>
#include <vector>
#include <random>
#include <memory>
#include <iostream>

namespace inno {

template<typename T>
struct SkipListDefaultComparator {
    int operator() (const T &a, const T &b) const {
        if (a > b) { return 1; }
        else if (a == b) { return 0; }
        else { return -1; }
    }
};

template<typename Value, typename Comparator=SkipListDefaultComparator<Value>>
class SkipList {
public:
    using value_type = Value;
    using clock = std::chrono::system_clock;

public:
    class iterator;

private:
    class Node;

public:
    SkipList()
            : head_(new Node(max_height_))
            , random_generator_(clock::now().time_since_epoch().count()) {}

    ~SkipList() {
        clear();
        delete head_;
    }

    iterator begin() { return iterator(head_->levels_[0].next); }
    iterator begin() const { return iterator(head_->levels_[0].next); }
    iterator end() { return iterator(nullptr); }
    iterator end() const { return iterator(nullptr); }

    void clear() {
        // release the heads of all levels except the lowest level
        for (int i = max_height_ - 1; i > 0; i--) {
           head_->levels_[i].next = nullptr;
        }

        // delete all element nodes
        Node *p = head_->levels_[0].next;
        while (p) {
            Node *next = p->levels_[0].next;
            delete p;
            p = next;
        }
        head_->levels_[0].next = nullptr;
        current_height_ = 1;
        element_num_ = 0;
    }

    iterator find(value_type &val) {
        return find_(val);
    }

    std::size_t count(const value_type &val) const {
        return find_(val) != end();
    }

    iterator insert(const value_type &val) {
        std::vector<Node*> cache;
        iterator iter = find_(val, &cache);
        if (iter != end()) {
            return iter;
        }

        iter = insert_(val, cache);
        return iter;
    }

    bool erase(const value_type &val) {
        iterator iter = find_(val);
        if (iter == end()) {
            return false;
        }
        return erase_(iter.node_);
    }

    iterator lower_bound(const value_type &val) const {
        std::vector<Node*> cache;
        iterator iter = find_(val, &cache);
        if (iter != end()) {
            return iter;
        }

        return iterator(cache[0]->levels_[0].next);
    }

    iterator upper_bound(const value_type &val) const {
        std::vector<Node*> cache;
        iterator iter = find_(val, &cache);
        if (iter != end()) {
            return ++iter;
        }
        return iterator(cache[0]->levels[0].next);
    }

    bool empty() const {
        return element_num_ == 0;
    }

    std::size_t size() const {
        return element_num_;
    }

private:
    iterator find_(const value_type& val, std::vector<Node*> *cache = nullptr) const {
        if (cache) {
            cache->reserve(max_height_);
        }
        Node *dummy = head_;
        int height = current_height_;

        for (int i = height - 1; i >= 0; i--) {
            for (;;) {
                if (dummy->levels_[i].next == nullptr) break;

                int res = Comparator()(val, dummy->levels_[i].next->val_);
                if (res == 0) return iterator(dummy->levels_[i].next);
                else if (res > 0) dummy = dummy->levels_[i].next;
                else if (res < 0) break;
            }
            if (cache) {
                (*cache)[i] = dummy;
            }
        }
        return end();
    }

    iterator insert_(const value_type& val, std::vector<Node*> &cache) {
        int height = random_height_();
        if (height > current_height_) {
            for (int i = height - 1; i >= current_height_; i--) {
                cache[i] = head_;
            }
            current_height_ = height;
        }

        Node *val_node = new Node(height, val);
        for (int i = height - 1; i >= 0; i--) {
            Node *prev_node = cache[i];
            val_node->levels_[i].next = prev_node->levels_[i].next;
            val_node->levels_[i].prev = prev_node;
            if (prev_node->levels_[i].next) {
                prev_node->levels_[i].next->levels_[i].prev = val_node;
            }
            prev_node->levels_[i].next = val_node;
        }
        if (val_node->levels_[0].next == nullptr) {
           tail_ = val_node;
        }
        element_num_ ++;
        return iterator(val_node);
    }

    bool erase_(Node *n) {
        for (int i = n->height_ - 1; i >= 0; i--) {
            n->levels_[i].prev->levels_[i].next = n->levels_[i].next;
            if (n->levels_[i].next) {
                n->levels_[i].next->levels_[i].prev = n->levels_[i].prev;
            }
        }

        if (n->height_ != 1 && n->height_ == current_height_) {
            while (current_height_ > 1 && head_->levels_[current_height_ - 1].next == nullptr) {
                current_height_--;
            }
        }

        if (n == tail_) {
            if (n->levels_[0].prev == head_) {
                tail_ = nullptr;
            } else {
                tail_ = n->levels_[0].prev;
            }
        }

        element_num_ --;
        delete n;
        return true;
    }

    int random_height_() {
        int height = 1;
        for (; height < max_height_ && random_generator_() % branching_ == 0; height++);

        return height;
    }

public:
   class iterator {
    public:
        friend class SkipList;
        iterator() = default;
        iterator(const iterator &other) { node_ = other.node_; }
        explicit iterator(Node *n) : node_(n) {}

        iterator &operator=(const iterator &other) {
            node_ = other.node_;
            return *this;
        }

        bool operator==(const iterator &other) const { return this->node_ == other.node_; }
        bool operator!=(const iterator &other) const { return this->node_ != other.node_; }
        value_type &operator*() const { return node_->val_; }
        value_type *operator->() const { return &(node_->val_); }
        iterator &operator++() {
           node_ = node_->levels_[0].next;
           return *this;
        }

        iterator &operator--() {
            node_ = node_->levels[0].prev;
            return *this;
        }

    private:
        Node *node_ = nullptr;
    };

private:
    struct level_item {
        Node *prev = nullptr;
        Node *next = nullptr;
    };

    class Node {
    public:
        friend class SkipList;

    public:
        explicit Node(std::size_t height)
            : height_(height), levels_(height) {}
        explicit Node(std::size_t height, const value_type &val)
            : height_(height), levels_(height), val_(val) {}

    private:
        value_type val_;
        std::size_t height_;
        std::vector<level_item> levels_;
    };

private:
    static constexpr int max_height_ = 12;
    static constexpr int branching_ = 4;
    std::size_t current_height_ = 1;
    std::size_t element_num_ = 0;
    std::mt19937 random_generator_;
    Node *head_;
    Node *tail_ = nullptr;
};

}

#endif //RETRIEVE_SYSTEM_SKIP_LIST_HPP

