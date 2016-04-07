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

package test

import scala.util.Random

import org.apache.commons.math3.distribution.LogNormalDistribution
import org.scalatest.{FunSuite, PrivateMethodTester}
import org.xerial.snappy.BitShuffle

class SnappyBitShuffleSuite extends FunSuite with PrivateMethodTester {

  def testCompressBitShuffleData(input: Array[Int]) {
    val result = BitShuffle.bitUnShuffleIntArray(BitShuffle.bitShuffle(input))
    input.zipWithIndex.foreach { case (value, index) =>
      assertResult(value, s"Wrong ${index}-th decoded value") {
        result(index)
      }
    }
  }

  test("empty column") {
    testCompressBitShuffleData(Array.empty)
  }

  test("lower skew") {
    val rng = new LogNormalDistribution(0.0, 0.01)
    testCompressBitShuffleData(Array.fill(100000)(rng.sample().toInt))
  }

  test("higher skew") {
    val rng = new LogNormalDistribution(0.0, 1.0)
    testCompressBitShuffleData(Array.fill(100000)(rng.sample().toInt))
  }

  test("random") {
    val rng = new Random(0)
    testCompressBitShuffleData(Array.fill(100000)(rng.nextInt))
  }
}
