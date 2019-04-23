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

import org.apache.spark._
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import uk.ic.ac.imperial.simulator.JsonExtractor.{Job, MetaProperties}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

// scalastyle:off println
object Simulator {

  var finishTimes = new mutable.HashMap[Int, ArrayBuffer[Long]]()
  var taskPrinter: mutable.ArrayBuffer[((String, Long), mutable.ArrayBuffer[(Int, Int, String)])] = mutable.ArrayBuffer.empty[((String, Long), mutable.ArrayBuffer[(Int, Int, String)])]

  var dagScheduler: DAGScheduler = _
  var taskScheduler: TaskSchedulerImpl = _
  var sc: SparkContext = _

  var jobSubmitter: JobSubmitter = _

  val jobsToSubmit: mutable.Map[Int, ArrayBuffer[Job]] = new mutable.HashMap[Int, ArrayBuffer[Job]]()

  def setupTaskScheduler(executors: Int, coresPerExec: Int, stopwatch: Stopwatch,
                         finishTimes: mutable.HashMap[Int, ArrayBuffer[Long]],
                         taskPrinter: mutable.ArrayBuffer[((String, Long), mutable.ArrayBuffer[(Int, Int, String)])],
                         confs: ArrayBuffer[(String, String)]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Simulator")
    confs.foreach { case (k, v) => conf.set(k, v) }
    sc = new SparkContext(conf)
    taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new SimulatorSchedulerBackend(stopwatch, taskScheduler, finishTimes,
      taskPrinter, executors, coresPerExec))
  }

  def setupDAGScheduler(): Unit = {
    dagScheduler = new DAGScheduler(sc, taskScheduler)
    jobSubmitter = new JobSubmitter(dagScheduler)
  }

  def main(args: Array[String]): Unit = {
    val stopwatch = new Stopwatch

    val configuration = JsonExtractor.fromJsonFileToObject("conf/simulator_configuration.json")
    val simulatorConfiguration = configuration.simulatorConfiguration
    val jobProperties = configuration.jobs.map(job => job.metaproperties)

    jobProperties.zipWithIndex.foreach(entry => {
      val metaproperties = entry._1
      val index = entry._2
      for (i <- Range(metaproperties.arrivesAt, metaproperties.repeatsTimes, metaproperties.repeatsEvery)) {
        val jobsAtTime = jobsToSubmit.getOrElse(i, ArrayBuffer.empty[Job])
        jobsAtTime.+=(configuration.jobs(index))
        jobsToSubmit.put(i, jobsAtTime)
      }
    })

    setupTaskScheduler(simulatorConfiguration.executors,
      simulatorConfiguration.coresPerExec,
      stopwatch,
      finishTimes,
      taskPrinter,
      Configuration.getConfiguration(simulatorConfiguration.sparkSchedulerMode))

    taskScheduler.start()
    setupDAGScheduler()

    val inner = new Breaks

    do {
      inner.breakable {
        for (tick <- stopwatch.time to Integer.MAX_VALUE) {
          stopwatch.time = tick

          if (finishTimes.contains(stopwatch.time)) {

            System.out.println(s"Tasks to finish at ${stopwatch.time}: ${finishTimes(stopwatch.time).mkString(", ")}")

            finishTimes.remove(stopwatch.time).get.foreach { taskId =>
              val stageIdOfCurrentTask = taskScheduler.taskIdToTaskSetManager(taskId).taskSet.stageId
              val isResultStage = taskScheduler.dagScheduler.stageIdToStage(stageIdOfCurrentTask).isInstanceOf[ResultStage]

              val executorId = taskScheduler.taskIdToExecutorId(taskId)
              val executorDataMap = taskScheduler.backend.asInstanceOf[SimulatorSchedulerBackend].getExecutorDataMap()
              val executorData = executorDataMap(executorId)
              executorData.freeCores += taskScheduler.CPUS_PER_TASK
              executorDataMap.put(executorId, executorData)

              if (isResultStage) {
                taskScheduler.statusUpdate(taskId, TaskState.FINISHED, taskScheduler.sc.env.closureSerializer.newInstance().serialize(new DirectTaskResult[String](taskScheduler.sc.env.closureSerializer.newInstance().serialize(taskScheduler.taskIdToTaskSetManager(taskId).name), Seq())))
              } else {
                taskScheduler.statusUpdate(taskId, TaskState.FINISHED, taskScheduler.sc.env.closureSerializer.newInstance().serialize(new DirectTaskResult[Int](taskScheduler.sc.env.closureSerializer.newInstance().serialize(MapStatus(BlockManagerId("executor0", "blockManager0", 10), Array(0))), Seq())))
              }
            }

            taskScheduler.backend.asInstanceOf[SimulatorSchedulerBackend].getExecutorDataMap().values.foreach {
              executorData =>
                System.out.println(s"${executorData.executorId} ${executorData.executorHost} ${executorData.freeCores}")
            }


            // I don't know what usually revives the offers, but I have to revive them manually.
            // otherwise the system does not know how to start tasks which are already in th running set.
            /*
             Sleep for a bit before reviving so that messages can be propagated.
             This is due to the fact that if the messages have not propagated, we may start running a parent
             dependency right before we check whether its child's dependencies are fulfilled. If that is the
             case and we have the resources for the child we might start running the child.
             TODO: [How is dealt with in practice?]
             */
            Thread.sleep(500)
            taskScheduler.backend.reviveOffers()

            inner.break
          }

          jobsToSubmit.getOrElse(tick, ArrayBuffer.empty[Job]).map(job => jobSubmitter.submit(sc, job))
        }
      }

      // Sleep for a short period of time to avoid aggressive looping so that messages can propagate.
      Thread.sleep(500)
    } while (dagScheduler.waitingStages.nonEmpty || dagScheduler.runningStages.nonEmpty)

    for (i <- taskPrinter.indices) {
      val entry = taskPrinter(i)
      val stageAndTask = entry._1.toString()
      val runtimes = entry._2
      val timeline = runtimes.foldLeft("")( (acc, entry) => {
        acc + entry._3*(entry._2 - entry._1)
      })
      System.out.println(timeline + " "*10 + stageAndTask + " "*10 + runtimes)
    }
  }
}
