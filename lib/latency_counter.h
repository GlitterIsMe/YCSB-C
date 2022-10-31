//
// Created by YiwenZhang on 2022/7/4.
//

#ifndef YCSB_LATENCY_COUNTER_H
#define YCSB_LATENCY_COUNTER_H

#include <stdint.h>
#include <chrono>
#include <cstdio>

struct  LatencyCounter {
    double index {0};
    double log {0};

    double cceh {0};
    double clht {0};
    double petalog {0};

    std::chrono::time_point<std::chrono::high_resolution_clock> t1;
    std::chrono::time_point<std::chrono::high_resolution_clock> t2;

    void Clear() {
        index = 0;
        log = 0;
    }

    void Start() {
        t1 = std::chrono::high_resolution_clock::now();
    }

    void End() {
        t2 = std::chrono::high_resolution_clock::now();
    }

    void AddIndexLat() {
        End();
        std::chrono::duration<double, std::micro> elapse = t2 - t1;
        index += elapse.count();
    }

    void AddLogLat() {
        End();
        std::chrono::duration<double, std::micro> elapse = t2 - t1;
        log += elapse.count();
    }

    void AddCCEHLat() {
        End();
        std::chrono::duration<double, std::micro> elapse = t2 - t1;
        cceh += elapse.count();
    }

    void AddCLHTLat() {
        End();
        std::chrono::duration<double, std::micro> elapse = t2 - t1;
        clht += elapse.count();
    }

    void AddPetaLogLat() {
        End();
        std::chrono::duration<double, std::micro> elapse = t2 - t1;
        petalog += elapse.count();
    }

    void PrintCurLate() {
        std::chrono::duration<double, std::micro> elapse = t2 - t1;
        //printf("currenct latency: %lu\n", std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
        printf("currenct latency: %lf\n", elapse.count());
    }
};

extern LatencyCounter counter;

/*#define START counter.Start();
#define END counter.End();
#define INDEX_END counter.AddIndexLat();
#define LOG_END counter.AddLogLat();
#define CCEH_END counter.AddCCEHLat();
#define CLHT_END counter.AddCLHTLat();
#define PETALOG_END counter.AddPetaLogLat();*/

#define START
#define END
#define INDEX_END
#define LOG_END
#define CCEH_END
#define CLHT_END
#define PETALOG_END

#endif //YCSB_LATENCY_COUNTER_H
