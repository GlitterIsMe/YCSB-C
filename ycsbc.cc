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
#include "lib/latency_counter.h"

#ifdef USING_ROART
#include "kv/roart/nvm_mgr/threadinfo.h"
#endif

using namespace std;

void UsageMessage(const char *command);

bool StrStartWith(const char *str, const char *pre);

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

std::atomic<uint64_t> total_finished;

std::string db_name;

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
                   bool is_loading) {
#ifdef USING_ROART
    if (db_name == "roart") {
        NVMMgr_ns::register_threadinfo();
    }
#endif
    ycsbc::Client client(*db, *wl);
    int oks = 0;
    int count = 0;

    for (int i = 0; i < num_ops; ++i) {
        if (is_loading) {
            oks += client.DoInsert();
            count++;
        } else {
            oks += client.DoTransaction();
            count++;
        }
        total_finished++;
        if (oks % 100000 == 0) {
            //fprintf(stderr, "...finished: %lu\r", total_finished.fetch_add(count, std::memory_order_acquire) + count);
            fprintf(stderr, "...finished: %lu\r", total_finished.load( std::memory_order_acquire));
            fflush(stderr);
            count = 0;
        }
    }
#ifdef USING_ROART
    if (db_name == "roart") {
        NVMMgr_ns::unregister_threadinfo();
    }
#endif
    return oks;
}

int main(const int argc, const char *argv[]) {
    total_finished.store(0);
    utils::Properties props;
    string file_name = ParseCommandLine(argc, argv, props);

    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
//#ifndef USING_ROART
    db->Init();
//#endif
    if (!db) {
        cout << "Unknown database name " << props["dbname"] << endl;
        exit(0);
    }

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
    assert((int) actual_ops.size() == num_threads);

    int sum = 0;
    for (auto &n: actual_ops) {
        assert(n.valid());
        sum += n.get();
    }
    double duration1 = timer1.End();
    cout << "# Loading records:\t" << sum << " takes " << duration1 << " s" << endl;
    cout << "# Load throughput (KTPS)" << endl;
    cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cout << total_ops / duration1 / 1000 << endl;
    cout << "Load index latency: " << counter.index << " us,\tavg lat: " << (double) counter.index / total_ops
         << "us \n";
    cout << "Load log latency: " << counter.log << " us,\tavg lat: " << (double) counter.log / total_ops << "us \n";
    cout << "Load CCEH latency: " << counter.cceh << " us,\tavg lat: " << (double) counter.cceh / total_ops << "us \n";
    cout << "Load CLHT latency: " << counter.clht << " us,\tavg lat: " << (double) counter.clht / total_ops << "us \n";
    cout << "Load Peta Log latency: " << counter.petalog << " us,\tavg lat: " << (double) counter.petalog / total_ops << "us \n";
    counter.Clear();

    //db->Close();
    //return 0;

    {
        if (db_name == "hikv") {
            printf("Waiting HiKV Background Jobs Finish...\n");
            db->Init();
            printf("HiKV Background Jobs Finished\n");
        }
    }

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
    assert((int) actual_ops.size() == num_threads);

    sum = 0;
    {
        /*uint64_t last_op = total_finished.load(std::memory_order_acquire), cur_op = 0;
        int elapse = 1;
        while(cur_op < total_ops) {
            std::this_thread::sleep_for(std::chrono::seconds(elapse));
            cur_op = total_finished.load(std::memory_order_acquire);
            printf("cur throughput, %lu\n", (cur_op - last_op) / elapse);
            last_op = cur_op;
        }*/

        /*auto last_time = std::chrono::high_resolution_clock::now(), cur_time = std::chrono::high_resolution_clock::now();
        uint64_t cur_op = 0, last_op = 0;
        while(cur_op < total_ops) {
            if ((cur_op = total_finished.load(std::memory_order_acquire)) % 100000 == 0 && cur_op != last_op) {
                cur_time = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double, std::micro> elapse = cur_time - last_time;
                printf("cur throughput, %lf\n", (cur_op - last_op) / elapse.count());
                last_time = cur_time;
                last_op = cur_op;
            }
        }*/
    }
    for (auto &n: actual_ops) {
        assert(n.valid());
        sum += n.get();
    }
    double duration = timer.End();
    cout << "# Running operations:\t" << sum << " takes " << duration << " s" << endl;
    cout << "# Transaction throughput (KTPS)" << endl;
    cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cout << total_ops / duration / 1000 << endl;
    cout << "Transaction index latency: " << counter.index << " us,\tavg lat: " << (double) counter.index / total_ops
         << "us \n";
    cout << "Transaction log latency: " << counter.log << " us,\tavg lat: " << (double) counter.log / total_ops
         << "us \n";

    db->Close();
    return 0;

    // repeat scan
    file_name = "../workloads/workload-scan.spec";
    ifstream input_scan(file_name);
    props.Load(input_scan);
    wl.Init(props);

    total_finished.store(0);
    actual_ops.clear();
    total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    utils::Timer<double> timer2;
    timer2.Start();
    for (int i = 0; i < num_threads; ++i) {
        actual_ops.emplace_back(async(launch::async,
                                      DelegateClient, db, &wl, total_ops / num_threads, false));
    }
    assert((int) actual_ops.size() == num_threads);

    sum = 0;
    for (auto &n: actual_ops) {
        assert(n.valid());
        sum += n.get();
    }
    double duration2 = timer2.End();
    cout << "# Running operations:\t" << sum << " takes " << duration2 << " s" << endl;
    cout << "# Transaction throughput (KTPS)" << endl;
    cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cout << total_ops / duration2 / 1000 << endl;
    cout << "Transaction index latency: " << counter.index << " us,\tavg lat: " << (double) counter.index / total_ops
         << "us \n";
    cout << "Transaction log latency: " << counter.log << " us,\tavg lat: " << (double) counter.log / total_ops
         << "us \n";

    db->Close();
    return 0;
    // repeat delete
    file_name = "../workloads/workload-delete.spec";
    ifstream input_del(file_name);
    props.Load(input_del);
    wl.Init(props);

    total_finished.store(0);
    actual_ops.clear();
    total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    utils::Timer<double> timer3;
    timer3.Start();
    for (int i = 0; i < num_threads; ++i) {
        actual_ops.emplace_back(async(launch::async,
                                      DelegateClient, db, &wl, total_ops / num_threads, false));
    }
    assert((int) actual_ops.size() == num_threads);

    sum = 0;
    for (auto &n: actual_ops) {
        assert(n.valid());
        sum += n.get();
    }
    double duration3 = timer3.End();
    cout << "# Running operations:\t" << sum << " takes " << duration3 << " s" << endl;
    cout << "# Transaction throughput (KTPS)" << endl;
    cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cout << total_ops / duration3 / 1000 << endl;
    cout << "Transaction index latency: " << counter.index << " us,\tavg lat: " << (double) counter.index / total_ops
         << "us \n";
    cout << "Transaction log latency: " << counter.log << " us,\tavg lat: " << (double) counter.log / total_ops
         << "us \n";

    db->Close();
    return 0;
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
            db_name = std::string(argv[argindex]);
            //printf("%s cur db name %s\n", __FUNCTION__ , db_name.c_str());
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

