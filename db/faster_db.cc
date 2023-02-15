//
// Created by YiwenZhang on 2023/2/15.
//

#include "faster_db.h"

int FasterDB::live_sessions = 0;

void FasterDB::Init() {
    db = new FASTER::core::FasterKv<ycsbc_key_t, ycsbc_value_t, FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler, 1073741824L>>(
            1024 * 1024 * 1024UL,
            10 * 1024 * 1024 * 1024UL,
            "/mnt/ssd"
            );
    session = db->StartSession();
    live_sessions++;
}

void FasterDB::Close() {
    db->StopSession();
}

int FasterDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
    
}

int FasterDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) {

}

int FasterDB::Delete(const std::string &table, const std::string &key) {


}

int FasterDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {

}

int FasterDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) {

}
