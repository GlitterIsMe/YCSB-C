//
// Created by zzyyyww on 2021/9/4.
//

#include "lib/new-metakv/src/Slice.h"
#include "hikvdb.h"
#include "pmem_impl/config.h"

namespace hikvdb {
    void HiKVDB::Init() {
        open_hikv::HiKVConfig config {
            .pm_path_ = "/mnt/pmem/hikv/",
            .store_size = 1024 * 1024 * 1024,
            .shard_size = 625000 * 16 * 4,
            .shard_num = 256,
            .message_queue_shard_num = 16,
            .log_path_ = "/mnt/pmem/hikv/",
            .log_size_ = 60UL * 1024 * 1024 * 1024,
            .cceh_path_ = "/mnt/pmem/hikv/",
            .cceh_size_ = 40UL * 1024 * 1024 * 1024,
        };
        /*open_hikv::HiKVConfig config {
                .pm_path_ = "/mnt/pmem/hikv/",
                .store_size = 10UL * 1024 * 1024 * 1024,
                .shard_size = 65535 * 16 * 4,
                .shard_num = 32,
                .message_queue_shard_num = 4,
        };*/
        open_hikv::OpenHiKV::OpenPlainVanillaOpenHiKV(&hikv_, config);
    }

    void HiKVDB::Close() {}

    int HiKVDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key = table + key;
        std::string value;
        for (auto item : values) {
            value.append(item.first + item.second);
        }
        hikv_->Set(whole_key, value);
        return DB::kOK;
    }

    int HiKVDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                     std::vector<KVPair> &result) {
        std::string whole_key = table + key;
        open_hikv::Slice value;
        open_hikv::ErrorCode e = hikv_->Get(whole_key, &value);
        if (e == open_hikv::ErrorCode::kNotFound) {
            printf("not found\n");
            return kErrorNoData;
        } else {
            return kOK;
        }
    }

    int HiKVDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = table + key;
        hikv_->Del(whole_key);
        return kOK;
    }

    int HiKVDB::Scan(const std::string &table, const std::string &key, int record_count,
                     const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        std::string whole_key = table + key;
        std::string prefix = whole_key.substr(0, whole_key.find('-') + 1);
        std::vector<KVPair> res;
        hikv_->Scan(prefix, [&](const open_hikv::Slice& k, const open_hikv::Slice& v){
            if (k.ToString().find(prefix) != std::string::npos){
                res.emplace_back(KVPair(k.ToString(), v.ToString()));
                return true;
            } else {
                return false;
            }
        });
        //printf("find prefix [%s] total [%d] entries\n", prefix.c_str(), res.size());
        /*for (const auto &e : res) {
            printf("find %s\n", e.first.c_str());
            fflush(stdout);
        }*/
        return kOK;
    }

    int HiKVDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }
}
