#!/bin/bash

dbs=("pmem-rocksdb"
    #"utree"
    #"hikv"
    #"roart"
    )

workloads=(
    "read"
    #"delete"
    "scan"
)

threads=(
  #"1"
  "2"
  "4"
  "8"
  "16"
  "32"
)

frs=(
"80"
"160"
#"320"
)

rm -f ycsbc.output
for tn in ${threads[@]};
  do
  for db in ${dbs[@]};
  do
    rm -rf /mnt/pmem1/*

    for workload in ${workloads[@]};
    do
    #for fr in ${frs[@]};
    #do
      echo "Run $db workload-$workload with threads $tn ratio-20" >> "ycsbc-concurrency.output"
      cmd="numactl -N 1 ./cmake-build-release-2373-physic/ycsb -db $db -threads $tn -P workloads/workload-$workload.spec -file_ratio 20 >> "ycsbc-concurrency-$db.output""
      echo $cmd
      #eval $cmd
    #done
    done
  done
done
