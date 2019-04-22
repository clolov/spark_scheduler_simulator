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

package uk.ic.ac.imperial.simulator

import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkContext}

object JobGenerator {

  /**
    * [A] <--(s_A)-- [B] <--(s_B)-- [C] <--(s_C)-- [D] <--(s_D)-- [E]
    *             \                /
    *               <-------------
    */
  private[simulator] def generate_sequential_job(sc: SparkContext): MyRDD = {
    val rddA = new MyRDD(sc, 1, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(rddA.context.conf, 1))

    val rddB = new MyRDD(sc, 1, List(shuffleDepA))
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(rddB.context.conf, 1))

    val rddC = new MyRDD(sc, 1, List(shuffleDepA, shuffleDepB))
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(rddC.context.conf, 1))

    val rddD = new MyRDD(sc, 1, List(shuffleDepC))
    val shuffleDepD = new ShuffleDependency(rddD, new HashPartitioner(rddD.context.conf, 1))

    new MyRDD(sc, 1, List(shuffleDepD))
  }

  /**
    * [A] <--(s_A)---
    *                \
    * [B] <--(s_B) <-- [D]
    *                /
    * [C] <--(s_C)---
    */
  private[simulator] def generate_parallel_job(sc: SparkContext): MyRDD = {
    val rddA = new MyRDD(sc, 1, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(rddA.context.conf, 1))

    val rddB = new MyRDD(sc, 1, Nil)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(rddB.context.conf, 1))

    val rddC = new MyRDD(sc, 1, Nil)
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(rddC.context.conf, 1))

    new MyRDD(sc, 1, List(shuffleDepA, shuffleDepB, shuffleDepC))
  }

  /**
    * [A] <--(s_A)---
    *                \
    * [B] <--(s_B) <-- [C]
    */
  private[simulator] def generate_simpler_parallel_job(sc: SparkContext): MyRDD = {
    val rddA = new MyRDD(sc, 1, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(rddA.context.conf, 1))

    val rddB = new MyRDD(sc, 1, Nil)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(rddB.context.conf, 1))

    new MyRDD(sc, 1, List(shuffleDepA, shuffleDepB))
  }
}
