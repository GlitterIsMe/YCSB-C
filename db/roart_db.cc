//
// Created by zzyyyww on 2021/8/26.
//

#include <filesystem>
#include "roart_db.h"

namespace roart_db{
    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(raw);
        return *size;
    }

    const std::string ROART_PATH("/mnt/pmem1/roart");

    void RoartDB::Init() {
        if (!inited_) {
            if (std::filesystem::exists(ROART_PATH)) {
                std::filesystem::remove_all(ROART_PATH);
            }
            std::filesystem::create_directory(ROART_PATH);
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
        //std::string whole_key = table + key;
        std::string value;
        for (auto item : values) {
            value.append(item.second);
        }
        bool res = db_->Put(key, value);
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
        //std::string whole_key = table + key;
        std::string value;
        bool res = db_->Get(key, &value);
        if (res) {
            return DB::kOK;
        } else {
            return DB::kErrorNoData;
        }
    }

    int RoartDB::Scan(const std::string &table, const std::string &key, int record_count,
                      const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        //std::string whole_key = table + key;
        std::vector<KVPair> res;
        db_->Scan(key, res);
        return DB::kOK;
    }

    int RoartDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }

    int RoartDB::Delete(const std::string &table, const std::string &key) {
        //std::string whole_key = table + key;
        std::string result;
        bool res = db_->Get(key, &result);
        if (res) {
            db_->Delete(key);
        }
        return DB::kOK;
    }
}

