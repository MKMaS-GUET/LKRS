/*
 * @FileName   : database.hpp
 * @CreateAt   : 2021/6/19
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: Database build and operate class,
 *               class `DatabaseBuilder::Impl` contains the details of all implementation,
 *               class `DatabaseBuilder` can `create` or `load` database instance,
 *               class `DatabaseBuilder::Option` contains the common operations about Database,
 *               such as 'save', 'unload', 'insert', etc.
 *               Note that only use `create` or `load` to get the `DatabaseBuilder::Option` instance firstly
 *               can access to call the methods of `DatabaseBuilder::Options`, which is a little bit similar with
 *               Builder Pattern to create and access the instance, the purpose of this is to ensure
 *               that database instance existed and operational when the user operates the database.
 */

#ifndef RETRIEVE_SYSTEM_DATABASE_HPP
#define RETRIEVE_SYSTEM_DATABASE_HPP

#include <memory>

#include "common/type.hpp"

namespace inno {

class DatabaseBuilder {
private:
    class Impl;

public:
    class Option;

public:
    DatabaseBuilder();
    ~DatabaseBuilder();

    /* create RDF database called @db_name from @data_file */
    static std::shared_ptr<DatabaseBuilder::Option> Create(const std::string &db_name, const std::string &data_file);

    /* load a RDF database named @db_name */
    static std::shared_ptr<DatabaseBuilder::Option>
    LoadBasic(const std::string &db_name);
    static std::shared_ptr<DatabaseBuilder::Option>
    LoadAll(const std::string &db_name);
    static std::shared_ptr<DatabaseBuilder::Option>
    LoadPartial(const std::string &db_name, const std::vector<std::string> &predicate_indexed_list);

public:
    class Option {
    public:
        explicit Option(std::shared_ptr<Impl> impl);
        ~Option();

        /* save the database data */
        bool save();
        bool save(const std::string &db_name);

        /* unload the database */
        void unload();

        /* insert RDF raw triplet, which is expressed as <s, p, o> */
        bool insert(const std::string &subject, const std::string &predicate, const std::string &object);
        bool insert(const std::vector<std::tuple<std::string, std::string, std::string>> &triplets);

        /* get basic information of RDF db */
        uint32_t getPredicateSize();
        uint32_t getEntitySize();
        uint32_t getTripletSize();
        uint32_t getPredicateSize() const;
        uint32_t getEntitySize() const ;
        uint32_t getTripletSize() const ;

        /* get pid corresponding to predicate */
        uint32_t getPredicateId(const std::string &predicate);
        uint32_t getPredicateId(const std::string &predicate) const;
        std::string getPredicateById(const uint32_t &pid);

        /* get entity id corresponding to entity (subject and object) */
        uint32_t getEntityId(const std::string &entity);
        uint32_t getEntityId(const std::string &entity) const;

        std::string getEntityById(uint32_t entity_id);
        std::string getEntityById(uint32_t entity_id) const;

        /* get the statistics of pid corresponding to predicate */
        uint32_t getPredicateCountBy(const std::string &predicate) const;
        uint32_t getPredicateCountBy(const std::string &predicate);

        /* get the statistics of soid corresponding to entity */
        uint32_t getEntityCountBy(const std::string &entity) const;
        uint32_t getEntityCountBy(const std::string &entity);

        std::vector<uint32_t> getPredicateStatistics();

        /* For querying */
        std::unordered_set<uint32_t>
        getSByPO(const uint32_t &pid, const uint32_t &oid);

        std::unordered_set<uint32_t>
        getOBySP(const uint32_t &sid, const uint32_t &pid);

        const std::unordered_multimap<uint32_t, uint32_t> &
        getS2OByP(const uint32_t &pid);
        std::unordered_multimap<uint32_t, uint32_t>
        getO2SByP(const uint32_t &pid);

//        std::set<std::pair<uint32_t, uint32_t>> getSOByP(const uint32_t &pid);

    private:
        std::shared_ptr<Impl> impl_;
    };
};

} // namespace inno

#endif //RETRIEVE_SYSTEM_DATABASE_HPP
