//
// Created by zzyyyww on 2021/8/23.
//

#include "pmem_rocksdb_db.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/iterator.h"

using namespace ycsbc;

namespace ycsb_pmem_rocksdb{

    const std::string PMEM_PATH("/mnt/pmem/");
    const std::string DB_NAME("/mnt/pmem/rocksdb/");
    const uint64_t PMEM_SIZE = 60 * 1024UL * 1024UL * 1024UL;

    void PmemRocksDB::Init() {
        rocksdb::Options options;

        options.max_background_jobs = 16;

        options.create_if_missing = true;
        //options.dcpmm_kvs_enable = false;
        //options.dcpmm_kvs_mmapped_file_fullpath = PMEM_PATH;
        //options.dcpmm_kvs_mmapped_file_size = PMEM_SIZE;
        //options.recycle_dcpmm_sst = true;
        //options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());

        rocksdb::Status s = rocksdb::DB::Open(options, DB_NAME, &db_);
        if (!s.ok()) {
            fprintf(stderr, "Open rocksdb failed [%s]\n", s.ToString().c_str());
            exit(-1);
        }
    }

    void PmemRocksDB::Close() {
        db_->Close();
        delete db_;
        db_ = nullptr;
    }

    int PmemRocksDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key = table + key;
        std::string whole_value;
        for (const auto& item : values) {
            whole_value.append(item.first + item.second);
        }
        rocksdb::WriteOptions options;
        printf("key size : %lu, value size: %lu\n", whole_key.size(), whole_value.size());
        rocksdb::Status s = db_->Put(options, rocksdb::Slice(whole_key), rocksdb::Slice(whole_value));
        if (s.ok()) {
            return DB::kOK;
        } else {
            fprintf(stderr, "put failed[%s]\n", s.ToString().c_str());
            return DB::kOK;
        }
    }

    int PmemRocksDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                          std::vector<KVPair> &result) {
        std::string whole_key = table + key;
        std::string raw_value;
        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(whole_key), &raw_value);
        if (s.IsNotFound()) {
            return DB::kErrorNoData;
        }
        return DB::kOK;
    }

    int PmemRocksDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }

    int PmemRocksDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = table + key;
        rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), rocksdb::Slice(whole_key));
        return DB::kOK;
    }

    bool isInDirectory(std::string dir_key, std::string key) {
        return dir_key.substr(0, 8) == key.substr(0, 8);
    }

    int PmemRocksDB::Scan(const std::string &table, const std::string &key, int record_count,
                          const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        rocksdb::Iterator *iter = db_->NewIterator(rocksdb::ReadOptions());
        std::string whole_key = table + key;
        std::vector<std::string> raw_values;
        iter->Seek(rocksdb::Slice(table + key));
        for(int i = 0; isInDirectory(whole_key, iter->key().ToString()) && iter->Valid(); i++, iter->Next()) {
            raw_values.push_back(iter->value().ToString());
        }
        return DB::kOK;
    }
}
