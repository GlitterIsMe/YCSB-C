//
// Created by 張藝文 on 2021/11/9.
//

#include "roart_db.h"

//
// Created by zzyyyww on 2021/8/26.
//

#include "roart_db.h"

namespace roart_db{
    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(raw);
        return *size;
    }

    const std::string LOG_PATH("/mnt/pmem/roart/log");
    const uint64_t LOG_SIZE = 40 * 1024UL * 1024UL * 1024UL;
    const std::string UTREE_PATH("/mnt/pmem/roart/log");
    const uint64_t UTREE_SIZE = 40 * 1024UL * 1024UL * 1024UL;

    size_t mapped_len;
    int is_pmem;

    void RoartDB::Init() {
        if (!inited_) {
            db_ = new PART_ns::Tree();
            inited_ = true;
        } else {
            return ;
        }
    }

    void RoartDB::Close() {
        delete db_;
    }

    int RoartDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key = table + key;
        std::string value;
        for (auto item : values) {
            value.append(item.first + item.second);
        }
        bool res = db_->Put(whole_key, value);
        if (res) {
            {
                //std::string check;
                //bool check_res = db_->Get(key.ToString(), &check);
                //assert(value == leveldb::Slice(check));
                //printf("check put: %d\n", check_res);
            }
            //return 0;
        } else {
            //return -1;
        }
        return DB::kOK;
    }

    int RoartDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        std::string whole_key = table + key;
        std::string value;
        bool res = db_->Get(whole_key, &value);
        if (res) {
            return 1;
        } else {
            return 0;
        }
    }

    int RoartDB::Scan(const std::string &table, const std::string &key, int record_count,
                      const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        std::string whole_key = table + key;
        std::vector<KVPair> res;
        db_->Scan(whole_key, res);
        return DB::kOK;
    }

    int RoartDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }

    int RoartDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = table + key;
        std::string result;
        bool res = db_->Get(whole_key, &result);
        if (res) {
            db_->Delete(whole_key);
        }
        return DB::kOK;
    }
}

