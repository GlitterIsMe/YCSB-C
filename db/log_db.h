//
// Created by zzyyyww on 2021/8/20.
//

#ifndef YCSB_LOG_DB_H
#define YCSB_LOG_DB_H

#include "core/db.h"
#include "lib/pm_log_store.h"

using namespace pm;
namespace ycsbc {
    class LogDB: public DB {
    public:
        LogDB();
        ~LogDB();

        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);
        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);
        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);
        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);
        int Delete(const std::string &table, const std::string &key);

    protected:
        LogStore* log_;
    };
}

#endif //YCSB_LOG_DB_H
