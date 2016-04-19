/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bitshuffle

import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.spark.util.Benchmark
import org.xerial.snappy.{BitShuffle, Snappy}
import org.apache.parquet.column.values.delta._

object SnappyBitShuffleBench {

  private[this] val NUM_TEST_DATA = 1000000 // 40MB
  private[this] val NUM_TEST_COUNT = 64

  private[this] def runBitShuffleBenchmark[T](
      name: String,
      iters: Int,
      count: Int,
      bitShuffleFunc: Option[(String, Array[T] => Unit)],
      input: Array[T],
      inputSize: Int): Unit = {
    val benchmark = new Benchmark(name, iters * count)

    bitShuffleFunc.map { case (label, func) =>
      benchmark.addCase(s"${label}")({ i: Int =>
        for (n <- 0L until iters) {
          func(input)
        }
      })
    }

    benchmark.run()
  }

  private[this] def computeRatio(compressed: Array[Byte], origSize: Int) = {
    s"${((compressed.length + 0.0) / origSize).formatted("%.3f")}"
  }

  private[this] def runCompressBenchmark[T](
      name: String,
      iters: Int,
      count: Int,
      compressFuncs: Seq[(String, Array[T] => Array[Byte])],
      input: Array[T],
      inputSize: Int): Unit = {
    val benchmark = new Benchmark(name, iters * count)

    compressFuncs.foreach { case (label, func) =>
      val compressed = func(input)
      benchmark.addCase(s"${label}(${computeRatio(compressed, inputSize)})")({ i: Int =>
        for (n <- 0L until iters) {
          func(input)
        }
      })
    }

    benchmark.run()
  }

  private[this] def runUncompressBenchmark[T](
      name: String,
      iters: Int,
      count: Int,
      compressFuncs: Seq[(String, Array[T] => Array[Byte], Array[Byte] => Unit)],
      input: Array[T]): Unit = {
    val benchmark = new Benchmark(name, iters * count)

    compressFuncs.foreach { case (label, compressFunc, uncompressFunc) =>
      val compressed = compressFunc(input)
      benchmark.addCase(s"${label}")({ i: Int =>
        for (n <- 0L until iters) {
          uncompressFunc(compressed)
        }
      })
    }

    benchmark.run()
  }

  def main(args: Array[String]) = {
    // bitshuffle-benchmark
    runBitShuffleBenchmark[Int](
      "BitShuffle",
      NUM_TEST_COUNT,
      NUM_TEST_DATA,
      Some(("bitshuffle", (in: Array[Int]) =>
        BitShuffle.bitUnShuffleIntArray(BitShuffle.bitShuffle(in)))),
      Array.fill(NUM_TEST_DATA)(0),
      4 * NUM_TEST_DATA)

    // snappy-benchmark (4-byte integers)
    val lowerSkewTestData = {
      val rng = new LogNormalDistribution(0.0, 0.01)
      Array.fill(NUM_TEST_DATA)(rng.sample().toInt)
    }

    val higherSkewTestData = {
      val rng = new LogNormalDistribution(0.0, 1.0)
      Array.fill(NUM_TEST_DATA)(rng.sample().toInt)
    }

    val compressFuncs1 = Seq[(String, Array[Int] => Array[Byte])](
      ("vanilla snappy", (in: Array[Int]) => Snappy.compress(in)),
      ("snappy + bitshuffle", (in: Array[Int]) => Snappy.compress(BitShuffle.bitShuffle(in))),
      ("parquet encoder", (in: Array[Int]) => {
        val writer = new DeltaBinaryPackingValuesWriter(100, 1000)
        in.foreach { value =>
          writer.writeInteger(value)
        }
        writer.getBytes().toByteArray()
      })
    )

    /**
     * Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
     * Compress(Lower Skew):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     * -------------------------------------------------------------------------------------------
     * vanilla snappy(0.281)                     212 /  258         75.3          13.3       1.0X
     * snappy + bitshuffle(0.077)                 62 /   78        258.3           3.9       3.4X
     * parquet encoder(0.072)                    297 /  375         53.9          18.6       0.7X
     */
    runCompressBenchmark[Int](
      "Compress(Lower Skew)", NUM_TEST_COUNT, NUM_TEST_DATA, compressFuncs1,
      lowerSkewTestData, 4 * NUM_TEST_DATA)


    /**
     * Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
     * Compress(Higher Skew):              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     * -------------------------------------------------------------------------------------------
     * vanilla snappy(0.331)                     246 /  298         65.0          15.4       1.0X
     * snappy + bitshuffle(0.143)                 95 /  114        168.7           5.9       2.6X
     * parquet encoder(0.160)                    322 /  374         49.7          20.1       0.8X
     */
    runCompressBenchmark(
      "Compress(Higher Skew)", NUM_TEST_COUNT, NUM_TEST_DATA, compressFuncs1,
      higherSkewTestData, 4 * NUM_TEST_DATA)

    val compressFuncs2 = Seq[(String, Array[Int] => Array[Byte], Array[Byte] => Unit)](
      (
        "vanilla snappy",
        (in: Array[Int]) => Snappy.compress(in),
        (in: Array[Byte]) => Snappy.uncompressIntArray(in)
      ),
      (
        "snappy + bitshuffle",
        (in: Array[Int]) => Snappy.compress(BitShuffle.bitShuffle(in)),
        (in: Array[Byte]) => BitShuffle.bitUnShuffleIntArray(Snappy.uncompress(in))
      ),
      (
        "parquet encoder",
        (in: Array[Int]) => {
          val writer = new DeltaBinaryPackingValuesWriter(100, 1000)
          in.foreach { value =>
            writer.writeInteger(value)
          }
          writer.getBytes().toByteArray()
        },
        (in: Array[Byte]) => {
          val reader = new DeltaBinaryPackingValuesReader()
          reader.initFromPage(NUM_TEST_DATA, in, 0)
          for (i <- 0 until NUM_TEST_DATA) {
            reader.readInteger()
          }
        }
      )
    )

    /**
     * Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
     * Uncompress(Lower Skew):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     * -------------------------------------------------------------------------------------------
     * vanilla snappy                             93 /  105        171.8           5.8       1.0X
     * snappy + bitshuffle                        55 /   59        290.7           3.4       1.7X
     * parquet encoder                            77 /   78        207.2           4.8       1.2X
     */
    runUncompressBenchmark[Int](
      "Uncompress(Lower Skew)", NUM_TEST_COUNT, NUM_TEST_DATA, compressFuncs2,
      lowerSkewTestData)

    /**
     * Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
     * Uncompress(Higher Skew):            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     * -------------------------------------------------------------------------------------------
     * vanilla snappy                            105 /  115        152.5           6.6       1.0X
     * snappy + bitshuffle                        71 /  132        224.9           4.4       1.5X
     * parquet encoder                            84 /  106        189.4           5.3       1.2X
     */
    runUncompressBenchmark[Int](
      "Uncompress(Higher Skew)", NUM_TEST_COUNT, NUM_TEST_DATA, compressFuncs2,
      higherSkewTestData)
  }
}
