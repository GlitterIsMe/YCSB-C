//
// Created by zzyyyww on 2021/9/4.
//

#ifndef YCSB_HIKVDB_H
#define YCSB_HIKVDB_H

#include "core/db.h"
#include "lib/hikv/open_hikv.h"

namespace hikvdb {
    class HiKVDB: public ycsbc::DB {
    public:
        HiKVDB(){}

        ~HiKVDB(){}

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
        std::unique_ptr<open_hikv::OpenHiKV> hikv_;
        bool inited_ {false};
    };
}

#endif //YCSB_HIKVDB_H
