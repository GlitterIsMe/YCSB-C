//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
#include "core/db.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

std::atomic<uint64_t> total_finished;

using OP = std::tuple<int, std::string, std::string>;
std::vector<OP> trace_ops;
std::vector<OP> trace_load1;
std::vector<OP> trace_load2;

const std::string rsync_trace_file("trace-rsync.out");
const std::string tar_trace_file("trace-tar.out");
const std::string find_trace_file("trace_find.out");

bool with_read = true;

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  //db->Init();
#ifdef USING_HybridHash
  ((ycsb_hybridhash::ycsbHybridHash*)db)->clht_init();
#endif
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  int count = 0;
  // load
  auto run_trace = [&](std::vector<OP>& trace){
      int count = 0;
      for(auto op : trace) {
          switch (std::get<0>(op)) {
              case 1: {
                  std::vector<ycsbc::DB::KVPair> value;
                  value.emplace_back("", std::get<2>(op));
                  db->Insert("", std::get<1>(op), value);
                  break;
              }
              case 2: {
                  std::vector<std::string> fields;
                  std::vector<ycsbc::DB::KVPair> result;
                  db->Read("", std::get<1>(op), &fields, result);
                  break;
              }
              case 3: {
                  db->Delete("", std::get<1>(op));
                  break;
              }
              case 4: {
                  std::vector<std::string> fields;
                  std::vector<std::vector<ycsbc::DB::KVPair>> results;
                  db->Scan("", std::get<1>(op), 0, &fields, results);
                  break;
              }
              default:
                  break;
          }
          count++;
          if (count % 10000 == 0) {
              fprintf(stderr, "...finished: %lu\r", total_finished.fetch_add(count, std::memory_order_acquire) + count);
              fflush(stderr);
              count = 0;
          }
      }
  };

  // run rsync
  auto start = std::chrono::high_resolution_clock::now();
  run_trace(trace_ops);
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "run rsync: " << (end - start).count() << "\n";
  // run tar
    auto start2 = std::chrono::high_resolution_clock::now();
    run_trace(trace_load1);
    auto end2 = std::chrono::high_resolution_clock::now();
    std::cout << "run tar: " <<  (end2 - start2).count() << "\n";

  // run find
    auto start3 = std::chrono::high_resolution_clock::now();
    run_trace(trace_load2);
    auto end3 = std::chrono::high_resolution_clock::now();
    std::cout << "run find: " << (end3 - start3).count() << "\n";
  /*for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client.DoInsert();
      count++;
    } else {
      oks += client.DoTransaction();
      count++;
    }
      if (oks % 100000 == 0) {
          fprintf(stderr, "...finished: %lu\r", total_finished.fetch_add(count, std::memory_order_acquire) + count);
          fflush(stderr);
          count = 0;
      }
  }*/

  //db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
  total_finished.store(0);
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  db->Init();
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  auto process_file = [&](std::string fname, std::vector<OP>& trace){
      std::ifstream stream;
      stream.open(fname, std::ios::in);
      std::string line;
      while (getline(stream, line, '\n')) {
          if (line[0] < '0' || line[0] > '9') continue;
          int op = std::stoi(line.substr(0, line.find(',')));
          if (op == 1) {
              int first_comma = line.find(",");
              int second_comma = line.rfind(",");
              std::string key = line.substr(first_comma + 1, second_comma - first_comma - 1);
              std::string value = line.substr(second_comma + 1, line.size() - second_comma - 1);
              if (key.size() <= 8) continue;
              trace.emplace_back(op, key, value);
          } else {
              int first_comma = line.find(',');
              std::string key = line.substr(first_comma + 1, line.size() - first_comma - 1);
              if (key.size() <= 8) continue;
              trace.emplace_back(op, key, "");
          }
      }
      printf("successful read %d entries\n", trace.size());
  };

  process_file(rsync_trace_file, trace_ops);
  process_file(tar_trace_file, trace_load1);
  process_file(find_trace_file, trace_load2);

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  // Loads data
  utils::Timer<double> timer1;
  timer1.Start();
  vector<future<int>> actual_ops;
  int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  for (int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, total_ops / num_threads, true));
  }
  assert((int)actual_ops.size() == num_threads);

  int sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  double duration1 = timer1.End();
  cout << "# Loading records:\t" << sum << " takes " << duration1 << " s"<< endl;
  cout << "# Load throughput (KTPS)" << endl;
  cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
  cout << total_ops / duration1 / 1000 << endl;

    db->Close();
    return 0;

  // Peforms transactions
  total_finished.store(0);
  actual_ops.clear();
  total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
  utils::Timer<double> timer;
  timer.Start();
  for (int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, total_ops / num_threads, false));
  }
  assert((int)actual_ops.size() == num_threads);

  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  double duration = timer.End();
  cout << "# Transaction throughput (KTPS)" << endl;
  cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
  cout << total_ops / duration / 1000 << endl;

  // perform transctions scan
  // init prop
  utils::Properties props2;
  file_name.assign("../workloads/workload-scan");
  std::ifstream input_scan("../workloads/workload-scan.spec");
  props2.Load(input_scan);
  props2.SetProperty("threadcount", props.GetProperty("thread_count"));
  props2.SetProperty("dbname", props.GetProperty("dbname"));
  props2.SetProperty("file_ratio", props.GetProperty("file_ratio"));
  // init workload
  ycsbc::CoreWorkload wl2;
  wl2.Init(props2);
  // perform transction
    total_finished.store(0);
    actual_ops.clear();
    total_ops = stoi(props2[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    utils::Timer<double> timer2;
    timer2.Start();
    for (int i = 0; i < num_threads; ++i) {
        actual_ops.emplace_back(async(launch::async,
                                      DelegateClient, db, &wl2, total_ops / num_threads, false));
    }
    assert((int)actual_ops.size() == num_threads);

    sum = 0;
    for (auto &n : actual_ops) {
        assert(n.valid());
        sum += n.get();
    }
    double duration2 = timer2.End();
    cout << "# Transaction throughput (KTPS)" << endl;
    cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cout << total_ops / duration2 / 1000 << endl;


  // perform transctions delete
// init prop
    utils::Properties props3;
    file_name.assign("../workloads/workload-delete");
    std::ifstream input_scan2("../workloads/workload-delete.spec");
    props3.Load(input_scan2);
    props3.SetProperty("threadcount", props.GetProperty("thread_count"));
    props3.SetProperty("dbname", props.GetProperty("dbname"));
    props3.SetProperty("file_ratio", props.GetProperty("file_ratio"));
    // init workload
    ycsbc::CoreWorkload wl3;
    wl3.Init(props3);
    // perform transction
    total_finished.store(0);
    actual_ops.clear();
    total_ops = stoi(props3[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    utils::Timer<double> timer3;
    timer3.Start();
    for (int i = 0; i < num_threads; ++i) {
        actual_ops.emplace_back(async(launch::async,
                                      DelegateClient, db, &wl3, total_ops / num_threads, false));
    }
    assert((int)actual_ops.size() == num_threads);

    sum = 0;
    for (auto &n : actual_ops) {
        assert(n.valid());
        sum += n.get();
    }
    double duration3 = timer3.End();
    cout << "# Transaction throughput (KTPS)" << endl;
    cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cout << total_ops / duration3 / 1000 << endl;

  db->Close();
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-file_ratio") == 0) {
      argindex++;
      if (argindex >= argc) {
          UsageMessage(argv[0]);
          exit(0);
      }
      props.SetProperty("file_ratio", argv[argindex]);
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

