#!/bin/bash

dbs=("pmem-rocksdb"
    "utree"
    "hikv"
    "roart")

workloads=(
    "read"
    "delete"
    "scan"
)

rm -f ycsbc.output

for db in ${dbs[@]} ;
do
  rm -rf /mnt/pmem1/*

  for workload in ${workloads[@]};
  do
    echo "Run $db workload-$workload with threads 1 ratio 20" >> "ycsbc.output"
    cmd="numactl -N 1 ./cmake-build-release-2373-physic/ycsb -db $db -threads 1 -P workloads/workload-$workload.spec -file_ratio 20 >> "ycsbc.output""
    #echo $cmd
    eval $cmd
  done
done
