//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/lock_stl_db.h"
//#include "db/redis_db.h"
#include "db/tbb_rand_db.h"
#include "db/tbb_scan_db.h"
//#include "db/log_db.h.back"
#ifdef USING_PMEM_ROCKSDB
#include "db/pmem_rocksdb_db.h"
#endif

#ifdef USING_CCEH
#include "db/cceh_db.h"
#endif

#ifdef USING_UTREE
#include "db/utree_db.h"
#endif

#ifdef USING_VIPER
#include "db/viper_db.h"
#endif

#ifdef USING_HIKV
#include "db/hikvdb.h"
#endif

#ifdef USING_METAKV
#include "db/metakv_db.h"
#endif

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "lock_stl") {
    return new LockStlDB;
  } else if (props["dbname"] == "redis") {
    int port = stoi(props["port"]);
    int slaves = stoi(props["slaves"]);
    //return new RedisDB(props["host"].c_str(), port, slaves);
  } else if (props["dbname"] == "tbb_rand") {
    return new TbbRandDB;
  } else if (props["dbname"] == "tbb_scan") {
    return new TbbScanDB;
  //} else if (props["dbname"] == "logdb"){
  //    return new LogDB;
#ifdef USING_PMEM_ROCKSDB
  } else if (props["dbname"] == "pmem-rocksdb"){
      return new ycsb_pmem_rocksdb::PmemRocksDB;
#endif

#ifdef USING_CCEH
  } else if (props["dbname"] == "cceh"){
      return new cceh_db::CCEHDB;
#endif

#ifdef USING_UTREE
  } else if (props["dbname"] == "utree"){
      return new utree_db::uTreeDB;
#endif

#ifdef USING_VIPER
  } else if (props["dbname"] == "viper"){
      return new viper_db::ViperDB;
#endif

#ifdef USING_HIKV
  } else if (props["dbname"] == "hikv"){
      return new hikvdb::HiKVDB;
#endif

#ifdef USING_METAKV
      } else if (props["dbname"] == "metakv"){
      return new ycsb_metakv::ycsbMetaKV;
#endif

#ifdef USING_HybridHash
      } else if (props["dbname"] == "hybridhash"){
      return new ycsb_hybridhash::ycsbHybridHash;
#endif

  }
  else return NULL;
}

