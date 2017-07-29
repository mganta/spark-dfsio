DFSIO using Spark

Currently writes & reads. First write and then read the files.

compile:

	 mvn clean package

Run:

 The program writes a text file of specified size per partition to a location and takes 5 parameters.
 
     1. number_of_files 
     
     2. file_size_in_bytes 
     
     3. file_write_location 
     
     4. buffer_size
     
     5. read or write or both
     

Sample Runs on HDInsight:

	  In order to control cores, enable cpu scheduling on HDI.

	  Write/Read 10 files in parallel of 1000000000 bytes each

Write

ADLS

	spark-submit --class com.msft.dfsio.DFSIOSpark  --master yarn --deploy-mode client --num-executors 100 --executor-memory 3G --executor-cores 1 --driver-memory 3G --driver-cores 4  spark-dfsio-1.0-SNAPSHOT.jar 100 1000000000 adl://sprkdfsio.azuredatalakestore.net/dfsio 4096 write

WASB

	spark-submit --class com.msft.dfsio.DFSIOSpark  --master yarn --deploy-mode client --num-executors 100 --executor-memory 4G --executor-cores 1 --driver-memory 3G --driver-cores 4 spark-dfsio-1.0-SNAPSHOT.jar 100 1000000000 wasb://sprkdfsio@sprkdfsio.blob.core.windows.net/user/foo/testdfsio 4096 write

Read

ADLS

	spark-submit --class com.msft.dfsio.DFSIOSpark  --master yarn --deploy-mode client --num-executors 100 --executor-memory 3G --executor-cores 1 --driver-memory 3G --driver-cores 4  spark-dfsio-1.0-SNAPSHOT.jar 100 1000000000 adl://sprkdfsio.azuredatalakestore.net/dfsio 4096 read

WASB

	spark-submit --class com.msft.dfsio.DFSIOSpark  --master yarn --deploy-mode client --num-executors 100 --executor-memory 4G --executor-cores 1 --driver-memory 3G --driver-cores 4 spark-dfsio-1.0-SNAPSHOT.jar 100 1000000000 wasb://sprkdfsio@sprkdfsio.blob.core.windows.net/user/foo/testdfsio 4096 read


Sample Output

	================================IO write benchmarks====================================
	Benchmark: Number of files : 10
	Benchmark: Size of each file : 10000000 Bytes
	Benchmark: File write location : adl://sprkdfsio.azuredatalakestore.net/dfsio
	Benchmark: Write buffer size : 4096
	Benchmark: Total write size : 100000000 Bytes
	Benchmark: Number of file write times collected : 10
	Benchmark: Fastest file write time : 0.231 s
	Benchmark: Slowest file write time : 0.316 s
	Benchmark: File write time range (Fastest - Slowest) : 85 milliseconds
	Benchmark: Total write time (Sum time across all files) : 2.627 s
	Benchmark: Average write time : 0.2627 s
	Benchmark: Median write time  : 0.255 s
	Benchmark: Variance on write time  : 911.2222222222222
	Benchmark: Standard Deviation on write time  : 0.03018645759644914 s
	Benchmark: Histogram on write time  : Map(231 -> 1, 236 -> 2, 237 -> 1, 249 -> 1, 261 -> 1, 273 -> 1, 292 -> 1, 296 -> 1, 316 -> 1)
	Benchmark: write start wall time epoch : 1500058180894
	Benchmark: write end Wall time epoch : 1500058183080
	Benchmark: Overall observed write wall time : 2.186 s
	Benchmark: Overall observed write throughput : 0.36596525 Gigabits per second
	================================IO write benchmarks====================================



