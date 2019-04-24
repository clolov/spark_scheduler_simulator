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

import java.util.Properties

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.util.CallSite
import uk.ic.ac.imperial.simulator.JsonExtractor.Job

import scala.collection.mutable

class JobSubmitter(dagScheduler: DAGScheduler) {

  var dagEventProcessLoopTester: DAGSchedulerEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(dagScheduler)
  val results = new mutable.HashMap[Int, Any]()
  var failure: Exception = _

  private def submit(
                      rdd: RDD[_],
                      partitions: Array[Int],
                      func: (TaskContext, Iterator[_]) => _ = (context: TaskContext, it: Iterator[(_)]) => it.next.asInstanceOf[Tuple2[_, _]]._1,
                      listener: JobListener = new JobListener() {
                        override def taskSucceeded(index: Int, result: Any): Unit = results.put(index, result)
                        override def jobFailed(exception: Exception): Unit = { failure = exception }
                      },
                      properties: Properties = null): Int = {
    val jobId = dagScheduler.nextJobId.getAndIncrement()
    // TODO: There is a CoroutineJobSubmitted class as well. Is it needed?
    runEvent(JobSubmitted(jobId, rdd, func, partitions, CallSite("", ""), listener, properties))
    jobId
  }

  private def runEvent(event: DAGSchedulerEvent) {
    dagEventProcessLoopTester.post(event)
  }

  private[simulator] def submit(sc: SparkContext, job: Job): Int = {
    submit(JobGenerator.generate_job_from_representation(sc, job.representation), Array(0), properties = job.properties)
  }

}
