//
// Created by zzyyyww on 2021/8/26.
//

#include "utree_db.h"

namespace utree_db{
    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(raw);
        return *size;
    }

    const std::string LOG_PATH("/mnt/pmem/log_pool");
    const uint64_t LOG_SIZE = 40 * 1024UL * 1024UL * 1024UL;
    const std::string UTREE_PATH("/mnt/pmem/utree_pool");
    const uint64_t UTREE_SIZE = 40 * 1024UL * 1024UL * 1024UL;

    size_t mapped_len;
    int is_pmem;

    void uTreeDB::Init() {
        if (!inited_) {
            db_ = new treedb::TreeDB(LOG_PATH, LOG_SIZE);
            inited_ = true;
        } else {
            return ;
        }
    }

    void uTreeDB::Close() {
        delete db_;
    }

    int uTreeDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key = table + key;
        std::string value;
        for (auto item : values) {
            value.append(item.first + item.second);
        }
        db_->Put(whole_key, value);
        return DB::kOK;
    }

    int uTreeDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        std::string whole_key = table + key;
        std::string value;
        bool res = db_->Get(whole_key, &value);
        if (res) {
            return DB::kOK;
        } else {
            return DB::kErrorNoData;
        }
    }

    int uTreeDB::Scan(const std::string &table, const std::string &key, int record_count,
                      const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        std::string whole_key = table + key;
        std::vector<KVPair> res;
        db_->Scan(whole_key, res);
        return DB::kOK;
    }

    int uTreeDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }

    int uTreeDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = table + key;
        db_->Delete(whole_key);
        return DB::kOK;
    }
}
