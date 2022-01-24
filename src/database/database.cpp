/*
 * @FileName   : database_builder.cpp
 * @CreateAt   : 2021/6/19
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: implement `DatabaseBuilder` and its sub-class
 */


#include "database/database.hpp"

#include <set>
#include <vector>
#include <future>
#include <fstream>
#include <unordered_map>

#include <spdlog/spdlog.h>
#include <boost/filesystem.hpp>

namespace inno {

namespace fs = boost::filesystem;

class DatabaseBuilder::Impl {
private:
//    using entity_pair_set = inno::SkipList<std::pair<uint32_t, uint32_t>>;
//    using entity_pair_set = std::set<std::pair<uint32_t, uint32_t>>;
    using entity_pair_set = std::unordered_multimap<uint32_t, uint32_t>;

public:
    Impl() : info_path_("info")
           , id_predicates_path_("id_predicates")
           , id_entities_path_("id_entities")
           , triplet_path_("triplet")
           { initialize_(); }

    ~Impl() { unload(); }

    void create(const std::string &db_name, const std::string &data_file) {
        db_name_ = db_name;
        if (fs::exists(data_file)) {
            insertFromFile(data_file);
        } else {
            spdlog::info("data file path '{}' doesn't exist.", data_file);
        }
    }

    bool insertFromFile(const std::string &data_file) {
        if (db_name_.empty()) {
            spdlog::info("<{}> haven't been specify, choose one to add data.");
            return false;
        }
        std::ifstream infile(data_file, std::ifstream::binary);
        std::ifstream::sync_with_stdio(false);

        size_t affect = 0;
        if (infile.is_open()) {
            std::string s, p, o;
            while (infile >> s >> p) {
                infile.ignore();
                std::getline(infile, o);
                for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {}

                affect += insert(s, p, o) ? 1 : 0;
            }

            save();
            spdlog::info("{} triplet(s) have been inserted.", affect);
            return true;
        } else {
            spdlog::error("Cannot open RDF data file, problem occurs by path '{}'", data_file);
            return false;
        }
    }

    bool insertFromTriplets(const std::vector<std::tuple<std::string, std::string, std::string>> &triplets) {
        std::string s, p, o;
        size_t affect = 0;
        for (const auto &triplet : triplets) {
            std::tie(s, p, o) = triplet;
            affect += insert(s, p, o) ? 1 : 0;
        }
        save();
        spdlog::info("{} triplet(s) have been inserted.", affect);
        return true;
    }

    bool insert(const std::string &s, const std::string &p, const std::string &o) {
        triplet_size_ ++;

        if (!p2id_.count(p)) {
            p2id_[p] = ++ predicate_size_;
            id2p_.emplace_back(p);
            id2p_count_.emplace_back(1);
        } else {
            id2p_count_[p2id_[p]]++;
        }

        if (!so2id_.count(s)) {
            so2id_[s] = ++ entity_size_;
            id2so_.emplace_back(s);
            id2so_count_.emplace_back(1);
        } else {
            id2so_count_[so2id_[s]]++;
        }

        if (!so2id_.count(o)) {
            so2id_[o] = ++ entity_size_;
            id2so_.emplace_back(o);
            id2so_count_.emplace_back(1);
        } else {
            id2so_count_[so2id_[o]]++;
        }

        predicate_indexed_storage_[p2id_[p]].insert({so2id_[s], so2id_[o]});
        return true;
    }

    bool save() {
        if (db_name_.empty()) {
            spdlog::info("Save Failed! Haven't specified a database yet, "
                         "you should call Create or LoadAll before this operation.");
            return false;
        }
        return save(db_name_);
    }

    bool save(const std::string &db_name) {
        fs::ofstream::sync_with_stdio(false);

        fs::path db_path = fs::current_path().append(db_name + ".db");

        if (!fs::exists(db_path)) {
            fs::create_directories(db_path);
        }
        if (!fs::exists(db_path / triplet_path_)) {
            fs::create_directories(db_path / triplet_path_);
        }

        auto info_store_task = std::async(std::launch::async,
                                          &DatabaseBuilder::Impl::store_basic_info,
                                          this,
                                          db_path / info_path_);

        auto pid_store_task = std::async(std::launch::async,
                                          &DatabaseBuilder::Impl::store_predicate_ids_,
                                          this,
                                         db_path / id_predicates_path_);

        auto soid_store_task = std::async(std::launch::async,
                                         &DatabaseBuilder::Impl::store_entity_ids_,
                                         this,
                                          db_path / id_entities_path_);

        auto triplet_store_task = std::async(std::launch::async,
                                         &DatabaseBuilder::Impl::store_triplet_,
                                         this,
                                             db_path / triplet_path_);

        triplet_store_task.get();
        info_store_task.get();
        pid_store_task.get();
        soid_store_task.get();

        return true;
    }

    void loadBasic(const std::string &db_name) {
        initialize_();

        fs::path db_path = fs::current_path().append(db_name + ".db");
        if (!fs::exists(db_path)) {
            return;
        }
        db_name_ = db_name;

        load_basic_info(db_path / info_path_);

        auto pid_load_task = std::async(std::launch::async,
                                        &DatabaseBuilder::Impl::load_predicate_ids_,
                                        this,
                                        db_path / id_predicates_path_,
                                        predicate_size_);

        auto soid_load_task = std::async(std::launch::async,
                                         &DatabaseBuilder::Impl::load_entity_ids_,
                                         this,
                                         db_path / id_entities_path_,
                                         entity_size_);

        pid_load_task.get();
        soid_load_task.get();
    }

    void loadAll(const std::string &db_name) {
        initialize_();

        fs::path db_path = fs::current_path().append(db_name + ".db");
        if (!fs::exists(db_path)) {
            spdlog::info("<{}> doesn't exist, create or build it firstly please.", db_path.string());
            return;
        }
        db_name_ = db_name;

        load_basic_info(db_path / info_path_);

        auto pid_load_task = std::async(std::launch::async,
                                         &DatabaseBuilder::Impl::load_predicate_ids_,
                                         this,
                                         db_path / id_predicates_path_,
                                         predicate_size_);

        auto soid_load_task = std::async(std::launch::async,
                                          &DatabaseBuilder::Impl::load_entity_ids_,
                                          this,
                                          db_path / id_entities_path_,
                                          entity_size_);

        auto triplet_load_task = std::async(std::launch::async,
                                             &DatabaseBuilder::Impl::load_all_triplet_,
                                             this,
                                             db_path / triplet_path_,
                                             predicate_size_);

        pid_load_task.get();
        soid_load_task.get();
        triplet_load_task.get();
    }

    void loadPartial(const std::string &db_name, const std::vector<std::string> &predicate_indexed_list) {
        initialize_();

        fs::path db_path = fs::current_path().append(db_name + ".db");
        if (!fs::exists(db_path)) {
            return;
        }
        db_name_ = db_name;

        load_basic_info(db_path / info_path_);

        auto pid_load_task = std::async(std::launch::async,
                                        &DatabaseBuilder::Impl::load_predicate_ids_,
                                        this,
                                        db_path / id_predicates_path_,
                                        predicate_size_);

        auto soid_load_task = std::async(std::launch::async,
                                         &DatabaseBuilder::Impl::load_entity_ids_,
                                         this,
                                         db_path / id_entities_path_,
                                         entity_size_);

        pid_load_task.get();
        soid_load_task.get();

        std::vector<uint32_t> pid_list;
        pid_list.reserve(predicate_indexed_list.size());
        for (const auto &p : predicate_indexed_list) {
            uint32_t pid = p2id_.at(p);
            pid_list.emplace_back(pid);
        }

        // Read data
//        auto triplet_load_task = std::async(std::launch::async,
//                                            &DatabaseBuilder::Impl::load_partial_triplet_,
//                                            this,
//                                            db_path / triplet_path_,
//                                            pid_list);
//        triplet_load_task.get();

        // Read data asynchronously
        std::vector<std::future<entity_pair_set>> task_list;
        task_list.reserve(pid_list.size());
        for (const auto &pid : pid_list) {
           task_list.emplace_back(std::async(std::launch::async,
                                             &DatabaseBuilder::Impl::load_triplet_with_pid_,
                                             this,
                                             db_path/triplet_path_
                                             ,pid));
        }
        for(int i = 0; i < pid_list.size(); i++) {
            predicate_indexed_storage_.emplace(pid_list[i], std::move(task_list[i].get()));
        }
    }

        void unload() {
        db_name_.clear();
        predicate_size_ = 0;
        entity_size_ = 0;
        triplet_size_ = 0;
        id2p_count_.clear();
        so2id_.clear();
        p2id_.clear();
        id2so_.clear();
        id2p_.clear();
        predicate_indexed_storage_.clear();
    }
//
//    uint32_t getPredicateId(const std::string &p) const {
//        return p2id_.at(p);
//    }

//    std::string getPredicateById(const uint32_t &pid) const {
//        return id2p_[pid];
//    }

//    uint32_t getEntityId(const std::string &so) const {
//       return so2id_.at(so);
//    }
//
//    uint32_t getPredicateCount(const std::string &p) const {
//       return id2p_count_[p2id_.at(p)];
//    }

//    std::vector<uint32_t> getPredicateStatistics() {
//        return id2p_count_;
//    }

//    std::string getEntityById(const uint32_t entity_id) const {
//        return id2so_.at(entity_id);
//    }

//    std::unordered_set<uint32_t>
//    getSByPO(const uint32_t &pid, const uint32_t &oid) {
//        std::unordered_set<uint32_t> ret;
//        ret.reserve(static_cast<size_t>(id2p_count_[pid] * 0.75));
//        for (const auto &item : predicate_indexed_storage_[pid]) {
//            if (item.second == oid) {
//                ret.insert(item.first);
//            }
//        }
//        return ret;
//    }

//    std::unordered_set<uint32_t>
//    getOBySP(const uint32_t &sid, const uint32_t &pid) {
//        std::unordered_set<uint32_t> ret;
//        ret.reserve(static_cast<size_t>(id2p_count_[pid] * 0.75));
//        for (const auto &item : predicate_indexed_storage_[pid]) {
//            if (item.first == sid) {
//                ret.insert(item.second);
//            }
//        }
//        return ret;
//    }
//
//    const std::unordered_multimap<uint32_t, uint32_t> &
//    getS2OByP(const uint32_t &pid) {
////        std::unordered_multimap<uint32_t, uint32_t> ret;
////        ret.reserve(static_cast<size_t>(predicate_statistic_[pid] * 0.75));
////        for (const auto &item : predicate_indexed_storage_[pid]) {
////            ret.emplace(item.first, item.second);
////        }
////        return ret;
//        return predicate_indexed_storage_[pid];
//    }

//    std::unordered_multimap<uint32_t, uint32_t>
//    getO2SByP(const uint32_t &pid) {
//        std::unordered_multimap<uint32_t, uint32_t> ret;
//        ret.reserve(static_cast<size_t>(id2p_count_[pid] * 0.75));
//        for (const auto &item : predicate_indexed_storage_[pid]) {
//            ret.emplace(item.second, item.first);
//        }
//        return ret;
//    }

//    std::set<std::pair<uint32_t, uint32_t>> getSOByP(const uint32_t &pid) {
//        return predicate_indexed_storage_[pid];
//    }

private:
    void initialize_() {
        predicate_size_ = 0;
        entity_size_ = 0;
        triplet_size_ = 0;
        id2so_.emplace_back("");
        id2p_.emplace_back("");
        id2so_count_.emplace_back(0);
        id2p_count_.emplace_back(0);
    }

    /* store database basic information */
    bool store_basic_info(const fs::path &path) const {
        fs::ofstream out(path, fs::ofstream::out | fs::ofstream::binary);
        if (out.is_open()) {
            std::string content = std::to_string(triplet_size_) + "\n" +
                                  std::to_string(predicate_size_) + "\n" +
                                  std::to_string(entity_size_) + "\n";
//            for (const uint32_t &item : id2p_count_) {
//                content += std::to_string(item) + " ";
//            }
            out.write(content.c_str(), content.size());
            out.close();
        } else {
            spdlog::error("store_basic_info function occurs problem, "
                          "`info` file cannot be written.");
            return false;
        }
        return true;
    }

    /* load database basic information */
    bool load_basic_info(const fs::path &path) {
        fs::ifstream in(path, fs::ifstream::in | fs::ifstream::binary);
        fs::ifstream::sync_with_stdio(false);
        in.tie(nullptr);
        if (in.is_open()) {
            in >> triplet_size_
               >> predicate_size_
               >> entity_size_;
//            id2p_count_.assign(predicate_size_ + 1, 0);
//            for (uint32_t  &item : id2p_count_) {
//                in >> item;
//            }
            in.close();
        } else {
            spdlog::error("load_basic_info function occurs problem, "
                          "`info` file cannot be read.");
            return false;
        }
        return true;
    }

    /* store the mapping between pid and predicates */
    bool store_predicate_ids_(const fs::path &path) const {
        fs::ofstream out(path, fs::ofstream::out | fs::ofstream::binary);
        if (out.is_open()) {
            for (size_t i = 1; i <= predicate_size_; ++ i) {
                std::string item = std::to_string(i) + "\t" +
                                   std::to_string(id2p_count_[i]) + "\t" +
                                   id2p_[i] + "\n";
                out.write(item.c_str(), item.size());
            }
            out.close();
        } else {
            spdlog::error("store_predicate_ids_ function occurs problem, "
                          "`id_predicates` file cannot be written.");
            return false;
        }
        return true;
    }

    /* load the mapping between pid and predicates */
    bool load_predicate_ids_(const fs::path &path, const uint32_t &predicate_size) {
        p2id_.clear();
        p2id_.reserve(static_cast<size_t>(predicate_size * 0.75));
        id2p_.clear();
        id2p_.resize(predicate_size + 1);
        id2p_count_.clear();
        id2p_count_.resize(predicate_size + 1);

        fs::ifstream in(path, fs::ifstream::in | fs::ifstream::binary);
        fs::ifstream::sync_with_stdio(false);
        in.tie(nullptr);
        if (in.is_open()) {
            for (size_t i = 1; i <= predicate_size_; ++ i) {
                uint32_t pid, pid_count;
                std::string predicate;
                in >> pid >> pid_count;
                in.ignore();
                std::getline(in, predicate);
                id2p_[pid] = predicate;
                id2p_count_[pid] = pid_count;
                p2id_[predicate] = pid;
            }
            in.close();
        } else {
            spdlog::error("load_predicate_ids_ function occurs problem, "
                          "`id_predicates` file cannot be read.");
            return false;
        }
        return true;
    }


    /* store the mapping between soid and entities */
    bool store_entity_ids_(const fs::path &path) const {
        fs::ofstream out(path, fs::ofstream::out | fs::ofstream::binary);
        if (out.is_open()) {
            for (size_t i = 1; i <= entity_size_; ++ i) {
                std::string item = std::to_string(i) + "\t" +
                                   std::to_string(id2so_count_[i]) + "\t" +
                                   id2so_[i] + "\n";
                out.write(item.c_str(), item.size());
            }
            out.close();
        } else {
            spdlog::error("store_entity_ids_ function occurs problem, "
                          "`id_entities` file cannot be written.");
            return false;
        }
        return true;
    }

    /* load the mapping between soid and entities */
    bool load_entity_ids_(const fs::path &path, const uint32_t &entity_size) {
        so2id_.clear();
        so2id_.reserve(static_cast<size_t>(entity_size * 0.75));
        id2so_.clear();
        id2so_.resize(entity_size + 1);
        id2so_count_.clear();
        id2so_count_.resize(entity_size + 1);

        fs::ifstream in(path, fs::ifstream::in | fs::ifstream::binary);
        fs::ifstream::sync_with_stdio(false);
        in.tie(nullptr);
        if (in.is_open()) {
            for (size_t i = 1; i <= entity_size_; ++ i) {
                uint32_t soid, soid_count;
                std::string entity;
                in >> soid >> soid_count;
                in.ignore();
                std::getline(in, entity);
                id2so_[soid] = entity;
                id2so_count_[soid] = soid_count;
                so2id_[entity] = soid;
            }
            in.close();
        } else {
            spdlog::error("load_entity_ids_ function occurs problem, "
                          "`id_entities` file cannot be read.");
            return false;
        }
        return true;
    }

    /* store the predicate -> <subject, object> */
    bool store_triplet_(const fs::path &path) {
        if (!fs::exists(path)) {
            spdlog::error("store_triplet_ function occurs problem, "
                          "`triplet` directory cannot be created");
            return false;
        }

        std::vector<std::future<bool>> task_list;
        task_list.reserve(predicate_indexed_storage_.size());

        for (uint32_t pid = 1; pid <= predicate_size_; ++pid) {
            task_list.push_back(std::move(std::async(std::launch::async,
                                              &DatabaseBuilder::Impl::store_triplet_with_pid_,
                                              this,
                                              path, pid)));
        }

        for (std::future<bool> &task : task_list) {
            task.get();
        }

        return true;
    }

    bool store_triplet_with_pid_(const fs::path &path, const uint32_t &pid) {
        fs::path child_path = path/fs::path(std::to_string(pid));
        fs::ofstream out(child_path, fs::ofstream::out | fs::ofstream::binary);
        fs::ofstream::sync_with_stdio(false);
        out.tie(nullptr);

        if (out.is_open()) {
            for (const auto &so : predicate_indexed_storage_[pid]) {
                std::string item = std::to_string(so.first) + " " +
                                   std::to_string(so.second) + "\n";
                out.write(item.c_str(), item.size());
            }
            out.close();
            return true;
        } else {
            spdlog::error("store_triplet_ function occurs problem, "
                          "`{}` cannot be written.", path.string());
            return false;
        }
    }

    /* store the predicate -> <subject, object> */
    bool load_all_triplet_(const fs::path &path, const uint32_t &predicate_size) {
        if (!fs::exists(path)) {
            spdlog::error("load_all_triplet_ function occurs problem, "
                          "`triplet` directory cannot be created");
            return false;
        }

        std::vector<std::future<bool>> task_list;
        task_list.reserve(predicate_size);

        for (uint32_t pid = 1; pid <= predicate_size; ++pid) {
            fs::path child_path = path/fs::path(std::to_string(pid));
            fs::ifstream in(child_path, fs::ifstream::in | fs::ifstream::binary);
            fs::ifstream::sync_with_stdio(false);
            in.tie(nullptr);

            if (in.is_open()) {
                predicate_indexed_storage_[pid].reserve(id2p_count_[pid]);
                while (!in.eof()) {
                    uint32_t sid, oid;
                    in >> sid >> oid;
                    predicate_indexed_storage_[pid].insert({sid, oid});
                }
                in.close();
            } else {
                spdlog::error("load_all_triplet_ function occurs problem, "
                              "`{}` cannot be read.", child_path.string());
                return false;
            }
//            task_list.push_back(std::move(std::async(std::launch::async,
//                                              &DatabaseBuilder::Impl::load_triplet_with_pid_,
//                                              this,
//                                              path, pid)));
        }

//        predicate_indexed_storage_.clear();
//        predicate_indexed_storage_.reserve(static_cast<size_t>(predicate_size * 0.75));
//        for (std::future<bool> &task : task_list) {
//            task.get();
//        }

        return true;
    }

    /* store the predicate -> <subject, object> */
    bool load_partial_triplet_(const fs::path &path, const std::vector<uint32_t> &pid_list) {
        if (!fs::exists(path)) {
            spdlog::error("load_partial_triplet_ function occurs problem, "
                          "`triplet` directory cannot be created");
            return false;
        }

        std::vector<std::future<bool>> task_list;
        task_list.reserve(pid_list.size());

        for (const uint32_t &pid: pid_list) {
            fs::path child_path = path/fs::path(std::to_string(pid));
            fs::ifstream in(child_path, fs::ifstream::in | fs::ifstream::binary);
            fs::ifstream::sync_with_stdio(false);
            in.tie(nullptr);

            if (in.is_open()) {
                predicate_indexed_storage_[pid].reserve(id2p_count_[pid]);
                while (!in.eof()) {
                    uint32_t sid, oid;
                    in >> sid >> oid;
                    predicate_indexed_storage_[pid].insert({sid, oid});
                }
                in.close();
            } else {
                spdlog::error("load_partial_triplet_ function occurs problem, "
                              "`{}` cannot be read.", child_path.string());
                return false;
            }
        }

        return true;
    }

    entity_pair_set load_triplet_with_pid_(const fs::path &path, const uint32_t &pid) {
        fs::path child_path = path/fs::path(std::to_string(pid));
        fs::ifstream in(child_path, fs::ifstream::in | fs::ifstream::binary);
        entity_pair_set pair_set;
        if (in.is_open()) {
            while (!in.eof()) {
                uint32_t sid, oid;
                in >> sid >> oid;
                pair_set.emplace(sid, oid);
            }
            in.close();
//            predicate_indexed_storage_.emplace(pid, std::move(pair_set));
            return pair_set;
        } else {
            spdlog::error("load_triplet_with_pid_ function occurs problem, "
                          "`{}` cannot be read.", child_path.string());
//            return false;
            return {};
        }
    }

public:
    std::string db_name_;
    uint32_t predicate_size_;
    uint32_t entity_size_;
    size_t triplet_size_;
    fs::path info_path_;
    fs::path id_predicates_path_;
    fs::path id_entities_path_;
    fs::path triplet_path_;
    std::unordered_map<std::string, uint32_t> so2id_;
    std::unordered_map<std::string, uint32_t> p2id_;
//    phmap::flat_hash_map<std::string, uint32_t> so2id_;
//    phmap::flat_hash_map<std::string, uint32_t> p2id_;
    std::vector<std::string> id2so_;
    std::vector<std::string> id2p_;
    std::vector<uint32_t> id2so_count_;
    std::vector<uint32_t> id2p_count_;

    std::unordered_map<uint32_t, entity_pair_set> predicate_indexed_storage_;
//    phmap::flat_hash_map<uint32_t, entity_pair_set> predicate_indexed_storage_;
};

DatabaseBuilder::DatabaseBuilder() = default;

DatabaseBuilder::~DatabaseBuilder() = default;

std::shared_ptr<DatabaseBuilder::Option>
DatabaseBuilder::Create(const std::string &db_name, const std::string &data_file) {
    std::shared_ptr<Impl> impl(new Impl());
    impl->create(db_name, data_file);
    return std::make_shared<DatabaseBuilder::Option>(impl);
}

std::shared_ptr<DatabaseBuilder::Option> DatabaseBuilder::LoadBasic(const std::string &db_name) {
    std::shared_ptr<Impl> impl(new Impl());
    impl->loadBasic(db_name);
    return std::make_shared<DatabaseBuilder::Option>(impl);
}

std::shared_ptr<DatabaseBuilder::Option>
DatabaseBuilder::LoadAll(const std::string &db_name) {
    std::shared_ptr<Impl> impl(new Impl());
    impl->loadAll(db_name);
    return std::make_shared<DatabaseBuilder::Option>(impl);
}

std::shared_ptr<DatabaseBuilder::Option>
DatabaseBuilder::LoadPartial(const std::string &db_name, const std::vector<std::string> &predicate_indexed_list) {
    std::shared_ptr<Impl> impl(new Impl());
    impl->loadPartial(db_name, predicate_indexed_list);
    return std::make_shared<DatabaseBuilder::Option>(impl);
}

DatabaseBuilder::Option::Option(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

DatabaseBuilder::Option::~Option() = default;

bool DatabaseBuilder::Option::save() {
    return impl_->save();
}

bool DatabaseBuilder::Option::save(const std::string &db_name) {
    return impl_->save(db_name);
}

void DatabaseBuilder::Option::unload() {
    return impl_->unload();
}

bool DatabaseBuilder::Option::insert(const std::string &s, const std::string &p, const std::string &o) {
    return impl_->insert(s, p, o);
}

bool DatabaseBuilder::Option::insert(const std::vector<std::tuple<std::string, std::string, std::string>> &triplets) {
    return impl_->insertFromTriplets(triplets);
}

uint32_t DatabaseBuilder::Option::getPredicateId(const std::string &predicate) const {
    return impl_->p2id_.at(predicate);
}

uint32_t DatabaseBuilder::Option::getPredicateId(const std::string &predicate) {
    return impl_->p2id_.at(predicate);
//    return impl_->getPredicateId(predicate);
}

std::string DatabaseBuilder::Option::getPredicateById(const uint32_t &pid) {
        return impl_->id2p_[pid];
}

uint32_t DatabaseBuilder::Option::getPredicateCountBy(const std::string &predicate) const {
    return impl_->id2p_count_[impl_->p2id_.at(predicate)];
//    return impl_->getPredicateCount(predicate);
}

uint32_t DatabaseBuilder::Option::getPredicateCountBy(const std::string &predicate) {
    return impl_->id2p_count_[impl_->p2id_.at(predicate)];
}

uint32_t DatabaseBuilder::Option::getEntityCountBy(const std::string &entity) const {
    return impl_->id2so_count_[impl_->so2id_.at(entity)];
}

uint32_t DatabaseBuilder::Option::getEntityCountBy(const std::string &entity) {
    return impl_->id2so_count_[impl_->so2id_.at(entity)];
}

std::vector<uint32_t> DatabaseBuilder::Option::getPredicateStatistics() {
    return impl_->id2p_count_;
}

uint32_t DatabaseBuilder::Option::getEntityId(const std::string &entity) {
    return impl_->so2id_.at(entity);
//    return impl_->getEntityId(entity);
}

uint32_t DatabaseBuilder::Option::getEntityId(const std::string &entity) const {
    return impl_->so2id_.at(entity);
//    return impl_->getEntityId(entity);
}

std::string DatabaseBuilder::Option::getEntityById(const uint32_t entity_id) {
//    return impl_->getEntityById(entity_id);
        return impl_->id2so_.at(entity_id);
}

std::string DatabaseBuilder::Option::getEntityById(uint32_t entity_id) const {
//    return impl_->getEntityById(entity_id);
    return impl_->id2so_.at(entity_id);
}

std::unordered_set<uint32_t>
DatabaseBuilder::Option::getSByPO(const uint32_t &pid, const uint32_t &oid) {
//    return impl_->getSByPO(pid, oid);
    std::unordered_set<uint32_t> ret;
    ret.reserve(static_cast<size_t>(impl_->id2p_count_[pid] * 0.75));
    for (const auto &item : impl_->predicate_indexed_storage_[pid]) {
        if (item.second == oid) {
            ret.insert(item.first);
        }
    }
    return ret;
}

std::unordered_set<uint32_t>
DatabaseBuilder::Option::getOBySP(const uint32_t &sid, const uint32_t &pid) {
//    return impl_->getOBySP(sid, pid);
    std::unordered_set<uint32_t> ret;
    ret.reserve(static_cast<size_t>(impl_->id2p_count_[pid] * 0.75));
    for (const auto &item : impl_->predicate_indexed_storage_[pid]) {
        if (item.first == sid) {
            ret.insert(item.second);
        }
    }
    return ret;
}

const std::unordered_multimap<uint32_t, uint32_t> &
DatabaseBuilder::Option::getS2OByP(const uint32_t &pid) {
//    return impl_->getS2OByP(pid);
    return impl_->predicate_indexed_storage_[pid];
}

std::unordered_multimap<uint32_t, uint32_t>
DatabaseBuilder::Option::getO2SByP(const uint32_t &pid) {
//    return impl_->getO2SByP(pid);
    std::unordered_multimap<uint32_t, uint32_t> ret;
    ret.reserve(static_cast<size_t>(impl_->id2p_count_[pid] * 0.75));
    for (const auto &item : impl_->predicate_indexed_storage_[pid]) {
        ret.emplace(item.second, item.first);
    }
    return ret;
}

uint32_t DatabaseBuilder::Option::getPredicateSize() {
    return impl_->predicate_size_;
}

uint32_t DatabaseBuilder::Option::getPredicateSize() const {
    return impl_->predicate_size_;
}

uint32_t DatabaseBuilder::Option::getEntitySize() {
    return impl_->entity_size_;
}

uint32_t DatabaseBuilder::Option::getEntitySize() const {
    return impl_->entity_size_;
}

uint32_t DatabaseBuilder::Option::getTripletSize() {
    return impl_->triplet_size_;
}

uint32_t DatabaseBuilder::Option::getTripletSize() const {
    return impl_->triplet_size_;
}

//std::set<std::pair<uint32_t, uint32_t>> DatabaseBuilder::Option::getSOByP(const uint32_t &pid) {
////    return impl_->getSOByP(pid);
//    return {};
//}


} // namespace inno
