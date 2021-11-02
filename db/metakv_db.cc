//
// Created by 張藝文 on 2021/9/8.
//

#include "metakv_db.h"

namespace ycsb_metakv{
    void ycsbMetaKV::Init() {
        Options options;
        options.cceh_file_size = 32UL * 1024 * 1024 * 1024;
        options.data_file_size = 128UL * 1024 * 1024 * 1024;
        db = MetaDB{};
        db.Open(options, "/mnt/pmem1/metakv");

        cnt.store(0);
    }

    void ycsbMetaKV::Close() {
        printf("metakv delete false cnt:%lu\n",cnt.load());
    }

    int ycsbMetaKV::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key(table + key);
        std::string whole_value;
        for (auto item : values) {
            whole_value.append(item.first + item.second);
        }
        ycsbKey internal_key(whole_key.substr(0, whole_key.find('-')),
                    whole_key.substr(whole_key.find('-'), whole_key.size()));
        ycsbValue internal_value(whole_value);
        //printf("insert %s\n", (table + key).c_str());
        bool res = db.Put(internal_key, internal_value);
        if (res) return DB::kOK;
        return DB::kErrorNoData;
    }

    int ycsbMetaKV::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                         std::vector<KVPair> &result) {
        std::string whole_key(table + key);
        ycsbValue value;
        std::string prefix = whole_key.substr(0, whole_key.find('-'));
        std::string fname = whole_key.substr(whole_key.find('-'), whole_key.size());
        // printf("prefix:%s fname:%s\n",prefix.c_str(),fname.c_str());
        ycsbKey internal_key(prefix,fname);
        bool res = db.Get(internal_key, value);
        if (res) {
            return DB::kOK;
        }
        return DB::kErrorNoData;
    }

    int ycsbMetaKV::Delete(const std::string &table, const std::string &key) {
        std::string whole_key(table + key);
        ycsbKey internal_key(whole_key.substr(0, whole_key.find('-')),
                             whole_key.substr(whole_key.find('-'), whole_key.size()));
        bool res = db.Delete(internal_key);
        if (!res) {
        //     return DB::kOK;
            cnt++;
        }
        // return DB::kErrorNoData;
        return DB::kOK;
    }

    int ycsbMetaKV::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        // for workload a/b/c/d/e/f should use insert instead of delete
        return Delete(table,key);
    }

    int ycsbMetaKV::Scan(const std::string &table, const std::string &key, int record_count,
                         const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        std::string whole_key(table + key);
        std::string tmp = whole_key.substr(0, whole_key.find('-'));
        Slice prefix = Slice(tmp);
        // printf("whole_key:%s\n prefix:%s\n",whole_key.c_str(),whole_key.substr(0,whole_key.find('-')).c_str());
        std::vector<LogEntryRecord> records;
        // printf("%s %d prefix:%s get #%lu items\n",__func__,__LINE__,prefix.ToString().c_str(),records.size());
        db.Scan(prefix,records);
        // printf("%s %d prefix:%s get #%lu items\n",__func__,__LINE__,prefix.ToString().c_str(),records.size());
        for (const auto record : records) {
           record.key.ToString();
           record.value.ToString();
        }
        return DB::kOK;
    }
}
