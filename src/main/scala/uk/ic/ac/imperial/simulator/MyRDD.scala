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

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.annotation.meta.param

class MyRDD(
             sc: SparkContext,
             numPartitions: Int,
             dependencies: List[Dependency[_]],
             locations: Seq[Seq[String]] = Nil,
             @(transient @param) tracker: MapOutputTrackerMaster = null)
  extends RDD[(Int, Int)](sc, dependencies) with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")

  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    if (locations.isDefinedAt(partition.index)) {
      locations(partition.index)
    } else if (tracker != null && dependencies.size == 1 &&
      dependencies.head.isInstanceOf[ShuffleDependency[_, _, _]]) {
      // If we have only one shuffle dependency, use the same code path as ShuffledRDD for locality
      val dep = dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
      tracker.getPreferredLocationsForShuffle(dep, partition.index)
    } else {
      Nil
    }
  }

  override def toString: String = "DAGSchedulerSuiteRDD " + id
}
