//
// Created by YiwenZhang on 2023/2/15.
//

#include "faster_db.h"

int FasterDB::live_sessions = 0;
FASTER::core::FasterKv<ycsbc_key_t, ycsbc_value_t,
    FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler,
    1073741824L>>* db = new FASTER::core::FasterKv<ycsbc_key_t, ycsbc_value_t,
    FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler, 1073741824L>>(
        16 * 1024 * 1024UL,
        10 * 1024 * 1024 * 1024UL,
        "/home/zyw/faster_test/log"
);
void FasterDB::Init() {
    session = db->StartSession();
    live_sessions++;
}

void FasterDB::Close() {
    db->StopSession();
    live_sessions--;
    //db->PrintHashUsage();
}

int FasterDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
    //printf("put key %s\n", key.c_str());
    std::string whole_value;
    for (auto segment : values) {
        whole_value.append(segment.second);
    }
    UpsertContext ctx{Slice(key), Slice(whole_value)};
    auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
        if (result != FASTER::core::Status::Ok) {
            fprintf(stderr, "Failed upsert requet\n");
        }
    };
    db->Upsert(ctx, callback, 1);

    /*ReadContext rctx {Slice(key)};
    auto rcallback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
        if (result != FASTER::core::Status::Ok) {
            fprintf(stderr, "Failed read requet\n");
        }
        if (result == FASTER::core::Status::NotFound) {
            fprintf(stderr, "Not found key\n");
        }
    };
    db->Read(rctx, rcallback, 1);*/

    db->Refresh();
    db->CompletePending(true);

    return 0;
}

int FasterDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) {
    //printf("get key %s\n", key.c_str());
    ReadContext ctx {Slice(key)};
    auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
        assert(result == FASTER::core::Status::Ok);
        if (result != FASTER::core::Status::Ok) {
            fprintf(stderr, "Failed read requet\n");
        }
        if (result == FASTER::core::Status::NotFound) {
            fprintf(stderr, "Not found key\n");
        }
    };
    db->Read(ctx, callback, 1);
    return 0;
}

int FasterDB::Delete(const std::string &table, const std::string &key) {
    return 0;
}

int FasterDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
    return 0;
}

int FasterDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) {
    return 0;
}
