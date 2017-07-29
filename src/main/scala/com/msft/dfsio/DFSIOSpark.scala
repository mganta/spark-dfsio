package com.msft.dfsio

import java.lang.System.{currentTimeMillis => _time}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConversions._
import scala.util.Random

object DFSIOSpark {

  def profile[R](code: => R, t: Long = _time) = (code, _time - t)

  def main(args: Array[String]) {

    if (args.length < 5) {
      System.err.println("Usage: DFSIOSpark number_of_files file_size_in_bytes file_io_location buffer_size read|write|both")
      System.exit(1)
    }

    val nFiles = args(0).toInt
    val fSize = args(1).toLong
    val ioPath = args(2)
    val bufferSize = args(3).toInt
    val operation = args(4)

    val sparkConf = new SparkConf().setAppName("Azure " + operation + " Benchmark")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    if (operation.equalsIgnoreCase("write"))
      doWriteIO(sc, nFiles, fSize, ioPath, bufferSize, operation)
    else if (operation.equalsIgnoreCase("read"))
      doReadIO(sc, nFiles, fSize, ioPath, bufferSize, operation)
    else  if (operation.equalsIgnoreCase("both")) {
      doWriteIO(sc, nFiles, fSize, ioPath, bufferSize, "write")
      doReadIO(sc, nFiles, fSize, ioPath, bufferSize, "read")
    }
    else
      println("Unkown operation....skipping IO activity")

    sc.stop()
  }

  def doWriteIO(sc: SparkContext, nFiles: Int, fSize: Long, ioPath: String, bufferSize: Int, operation: String) = {
    println("================================IO " + operation + " benchmarks====================================")
    println("Benchmark: Number of files : " + nFiles)
    println("Benchmark: Size of each file : " + fSize + " Bytes")
    println("Benchmark: File write location : " + ioPath)
    println("Benchmark: Write buffer size : " + bufferSize)

    val fileWriteTimeAccumulator: CollectionAccumulator[Long] = sc.collectionAccumulator("Write Duration")
    val files = sc.parallelize(1 until nFiles + 1, nFiles)
    val startTime = _time

    files.foreachPartition(iterator => {
      val fs = FileSystem.get(URI.create(ioPath), new Configuration())
      val content = new Random().alphanumeric.take(bufferSize).mkString.getBytes
      while (iterator.hasNext) {
        val fileIndex = iterator.next()
        fs.delete(new Path(ioPath, "file_" + fileIndex), true)
        val (dummy, timeTaken) = profile {
          val dataOutputStream = fs.create(new Path(ioPath, "file_" + fileIndex), true, bufferSize)
          var nrRemaining = fSize
          while (nrRemaining > 0) {
            val curSize = if (bufferSize < nrRemaining) bufferSize else nrRemaining
            dataOutputStream.write(content, 0, curSize.toInt)
            nrRemaining -= bufferSize
          }
          dataOutputStream.close()
        }
        fileWriteTimeAccumulator.add(timeTaken)
      }
    })
    val endTime = _time
    extractBenchmarkDetails(fileWriteTimeAccumulator.value, startTime, endTime, nFiles, fSize, operation)
  }


  def doReadIO(sc: SparkContext, nFiles: Int, fSize: Long, ioPath: String, bufferSize: Int, operation: String) = {
    println("================================IO " + operation + " benchmarks====================================")
    println("Benchmark: Number of files : " + nFiles)
    println("Benchmark: Size of each file : " + fSize + " Bytes")
    println("Benchmark: File read location : " + ioPath)
    println("Benchmark: Read buffer size : " + bufferSize)

    val fileReadTimeAccumulator: CollectionAccumulator[Long] = sc.collectionAccumulator("Read Duration")
    val files = sc.parallelize(1 until nFiles + 1, nFiles)
    val startTime = _time
    files.foreachPartition(iterator => {
      val fs = FileSystem.get(URI.create(ioPath), new Configuration())
      val content = new Random().alphanumeric.take(bufferSize).mkString.getBytes
      while (iterator.hasNext) {
        val fileIndex = iterator.next()
        val (dummy, timeTaken) = profile {
          val dataInputStream = fs.open(new Path(ioPath, "file_" + fileIndex), bufferSize)
          var nrRemaining = fSize
          while (nrRemaining > 0) {
            val curSize = if (bufferSize < nrRemaining) bufferSize else nrRemaining
            dataInputStream.read(content, 0, curSize.toInt)
            nrRemaining -= bufferSize
          }
          dataInputStream.close()
        }
        fileReadTimeAccumulator.add(timeTaken)
      }
    })
    val endTime = _time
    extractBenchmarkDetails(fileReadTimeAccumulator.value, startTime, endTime, nFiles, fSize, operation)
  }

  def extractBenchmarkDetails(times: java.util.List[Long], startTime: Long, endTime: Long, nFiles: Int, fSize: Long, operation: String): Unit = {
    val ioTimes = times.sorted
    val len = ioTimes.length
    val mean = ioTimes.sum / len
    val median = if (len % 2 == 0) ((ioTimes((len / 2) - 1) + ioTimes((len / 2))) / 2) else ioTimes(((len + 1) / 2) - 1)

    val variance = {
      @scala.annotation.tailrec
      def vrec(list: List[Long], tot: Double): Double = list match {
        case head :: tail => vrec(tail, tot + math.pow(head - mean, 2))
        case _ => tot / (len - 1)
      }
      vrec(ioTimes.toList, 0)
    }

    val standardDeviation = math.sqrt(variance)
    val histogram = {
      scala.collection.immutable.TreeMap[Long, Long]() ++
        ioTimes.groupBy(x => x).mapValues(_.length)
    }

    println("Benchmark: Total read size : " + (nFiles.toLong * fSize) + " Bytes")
    println("Benchmark: Number of file " + operation + " times collected : " + ioTimes.length)
    println("Benchmark: Fastest file " + operation + " time : " + (ioTimes.min / 1000.toFloat) + " s")
    println("Benchmark: Slowest file " + operation + " time : " + (ioTimes.max / 1000.toFloat) + " s")
    println("Benchmark: File " + operation + " time range (Fastest - Slowest) : " + (ioTimes.max - ioTimes.min) + " milliseconds")
    println("Benchmark: Total " + operation + " time (Sum time across all files) : " + (ioTimes.sum / 1000.toFloat) + " s")
    println("Benchmark: Average " + operation + " time : " + (ioTimes.sum / (1000 * ioTimes.length).toFloat) + " s")
    println("Benchmark: Median " + operation + " time  : " + (median / 1000.toFloat) + " s")
    println("Benchmark: Variance on " + operation + " time  : " + variance)
    println("Benchmark: Standard Deviation on " + operation + " time  : " + (standardDeviation / 1000.toFloat) + " s")
    println("Benchmark: Histogram on " + operation + " time  : " + histogram)

    println("Benchmark: " + operation + " start wall time epoch : " + startTime)
    println("Benchmark: " + operation + " end Wall time epoch : " + endTime)
    println("Benchmark: Overall observed " + operation + " wall time : " + (endTime - startTime) / 1000.toFloat + " s")
    println("Benchmark: Overall observed " + operation + " throughput : " + ((nFiles.toLong * fSize * 8) / ((endTime - startTime) * 1000 * 1000).toFloat) + " Gigabits per second")
    println("================================IO " + operation + " benchmarks====================================")
  }
}
