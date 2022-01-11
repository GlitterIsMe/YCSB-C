//
// Created by zzyyyww on 2021/8/26.
//

#ifndef YCSB_ROART_DB_H
#define YCSB_ROART_DB_H

#include "core/db.h"
#include "kv/roart/ART/Tree.h"

namespace roart_db {
    class RoartDB: public ycsbc::DB {
    public:
        RoartDB(){}

        ~RoartDB(){}

        void Init();

        void Close();

        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int record_count, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Delete(const std::string &table, const std::string &key);

    private:
        PART_ns::Tree* db_;
        bool inited_{false};
    };
}

#endif //YCSB_ROART_DB_H
//
