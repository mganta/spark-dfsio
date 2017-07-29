#!/bin/bash

#number of files in parallel
fileCounts="1 10 25 50 100 150 200"

#size of each file
fileSizeMB="128 256 512 1024 2048 4096 8192"

#location to read and write
location="adl://dfsio.azuredatalakestore.net/dfsio"

#io buffer size
bufferSize=4096

#io operation read/write/both
operation=both

#test timestamp
testTime=`date "+%Y%m%d%H%M%S"`

echo DFSIO Tests start | tee -a benchmarks_$testTime.log

#run the tests for each file size and number of parallel file ops
for fileCount in $fileCounts
do
    for fileSize in $fileSizeMB
        do
             fileSizeBytes=$(($fileSize * 1000000))
             spark-submit --class com.msft.dfsio.DFSIOSpark  --master yarn --deploy-mode client --num-executors $fileCount --executor-memory 3G --executor-cores 1 --driver-memory 3G --driver-cores 4  spark-dfsio-1.0-SNAPSHOT.jar $fileCount $fileSizeBytes $location $bufferSize $operation | tee -a benchmarks_$testTime.log
        done
done

echo DFSIO Tests finished | tee -a benchmarks_$testTime.log