//
// Created by zzyyyww on 2021/9/4.
//
#include <filesystem>
#include "lib/new-metakv/src/Slice.h"
#include "hikvdb.h"
#include "pmem_impl/config.h"

namespace hikvdb {
    const std::string PATH("/mnt/pmem1/hikv/");
    void HiKVDB::Init() {
        if (!inited_) {
            if (std::filesystem::exists(PATH)) {
                std::filesystem::remove_all(PATH);
            }
            std::filesystem::create_directory(PATH);
            open_hikv::HiKVConfig config {
                    .pm_path_ = PATH,
                    .store_size = 1024 * 1024 * 1024,
                    .shard_size = 625000 * 16 * 4,
                    .shard_num = 256,
                    .message_queue_shard_num = 8,
                    .log_path_ = PATH,
                    .log_size_ = 50UL * 1024 * 1024 * 1024,
                    .cceh_path_ = PATH,
                    .cceh_size_ = 32UL * 1024 * 1024 * 1024,
            };
            open_hikv::OpenHiKV::OpenPlainVanillaOpenHiKV(&hikv_, config);
            inited_ = true;
        } else {
           hikv_->Flush();
        }

    }

    void HiKVDB::Close() {
        hikv_->PrintUsage();
    }

    int HiKVDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string value;
        for (auto item : values) {
            value.append(item.second);
        }
        hikv_->Set(key, value);
        return DB::kOK;
    }

    int HiKVDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                     std::vector<KVPair> &result) {
        open_hikv::Slice value;
        open_hikv::ErrorCode e = hikv_->Get(key, &value);
        if (e == open_hikv::ErrorCode::kNotFound) {
            //printf("not found\n");
            return kErrorNoData;
        } else {
            return kOK;
        }
    }

    int HiKVDB::Delete(const std::string &table, const std::string &key) {
        hikv_->Del(key);
        return kOK;
    }

    int HiKVDB::Scan(const std::string &table, const std::string &key, int record_count,
                     const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        //std::string whole_key = table + key;
        //std::string prefix = whole_key.substr(0, whole_key.find('-') + 1);
        std::string prefix = key.substr(0, 8);
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
