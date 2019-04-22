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

import org.apache.spark.scheduler.cluster.ExecutorData
import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl, WorkerOffer}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SimulatorSchedulerBackend(val stopwatch: Stopwatch, val taskScheduler: TaskSchedulerImpl,
                                val finishTimes: mutable.HashMap[Int, ArrayBuffer[Long]],
                                taskPrinter: mutable.ArrayBuffer[((String, Long), mutable.ArrayBuffer[(Int, Int, String)])],
                                val executors: Int = 1, val cores: Int = 1) extends SchedulerBackend {

  var taskToFinishTime = new mutable.HashMap[Long, Int]()
  var taskToPauseTime = new mutable.HashMap[Long, Int]()
  val taskToIndexInTaskPrinter = new mutable.HashMap[Long, Int]()

  override def start(): Unit = {
    0.until(executors).foreach { execID =>
      val data = new ExecutorData(null, null, s"executor$execID", s"host$execID", cores, cores, Map.empty)
      SimulatorSchedulerBackend.executorDataMap.put(s"executor$execID", data)
    }
  }

  override def stop(): Unit = {}

  override def pauseTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit = {
    val executorId = taskScheduler.taskIdToExecutorId(taskId)
    val executorDataMap = taskScheduler.backend.asInstanceOf[SimulatorSchedulerBackend].getExecutorDataMap()
    val executorData = executorDataMap(executorId)
    executorData.freeCores += taskScheduler.CPUS_PER_TASK
    executorDataMap.put(executorId, executorData)

    // Remove task from finishTimes
    val tasksToFinish: ArrayBuffer[Long] = finishTimes(taskToFinishTime(taskId))
    tasksToFinish.-=(taskId)

    val pauseTime = stopwatch.time

    // Record the last time the task was paused
    taskToPauseTime.put(taskId, pauseTime)

    // Update taskPrinter values
    val taskPrinterIndex = taskToIndexInTaskPrinter(taskId)
    val entry = taskPrinter(taskPrinterIndex)
    val runtimes = entry._2
    val lastEntryInRuntimes = runtimes.remove(runtimes.length - 1)
    val alteredLastEntry = (lastEntryInRuntimes._1, pauseTime, lastEntryInRuntimes._3)
    runtimes.+=(alteredLastEntry)

    reviveOffers()
  }

  override def resumeTask(taskId: Long, executorId: String): Unit = {
    val executorId = taskScheduler.taskIdToExecutorId(taskId)
    val executorDataMap = taskScheduler.backend.asInstanceOf[SimulatorSchedulerBackend].getExecutorDataMap()
    val executorData = executorDataMap(executorId)
    executorData.freeCores -= taskScheduler.CPUS_PER_TASK
    executorDataMap.put(executorId, executorData)

    val pauseTime = taskToPauseTime(taskId)
    val newFinishTime = (stopwatch.time - pauseTime) + taskToFinishTime(taskId)
    taskToFinishTime.put(taskId, newFinishTime)

    val tasksToFinish = finishTimes.getOrElse(newFinishTime, ArrayBuffer.empty)
    val updatedTasksToFinish = tasksToFinish.+=(taskId)
    finishTimes.put(newFinishTime, updatedTasksToFinish)

    val resumeTime = stopwatch.time
    val taskPrinterIndex = taskToIndexInTaskPrinter(taskId)
    val entry = taskPrinter(taskPrinterIndex)
    entry._2.+=((pauseTime, resumeTime, " "))
    entry._2.+=((resumeTime, newFinishTime, "|"))
  }

  def getOffers: IndexedSeq[WorkerOffer] = {
    getExecutorDataMap().values.filter {
      executorData => executorData.freeCores > 0
    }.map {
      executorData => WorkerOffer(executorData.executorId, executorData.executorHost, executorData.freeCores)
    }.toIndexedSeq
  }

  def reviveOffers() {
    val offers = getOffers
    for (task <- taskScheduler.resourceOffers(offers).flatten) {
      getExecutorDataMap().get(task.executorId) match {
        case Some(executorInfo) =>
          executorInfo.freeCores -= taskScheduler.CPUS_PER_TASK
        case None =>
          System.err.println(s"Attempted to update unknown executor ${task.executorId}")
      }

      // Schedule completion
      val duration = task.properties.getProperty("duration").toInt
      val finishTime = stopwatch.time + duration
      val tasksToFinish = finishTimes.getOrElse(finishTime, ArrayBuffer.empty)
      val updatedTasksToFinish = tasksToFinish.+=(task.taskId)
      finishTimes.put(finishTime, updatedTasksToFinish)
      taskToFinishTime.put(task.taskId, finishTime)
      val stageAndTaskTuple = (taskScheduler.taskIdToTaskSetManager(task.taskId).name, task.taskId)
      val beforeStart = (0, stopwatch.time, " ")
      val firstRuntime = (stopwatch.time, finishTime, "|")
      taskPrinter.+=((stageAndTaskTuple, mutable.ArrayBuffer(beforeStart, firstRuntime)))
      taskToIndexInTaskPrinter.put(task.taskId, taskPrinter.length - 1)
    }
  }

  override def defaultParallelism(): Int = executors * cores

  override def getExecutorDataMap(): mutable.HashMap[String, ExecutorData] = SimulatorSchedulerBackend.executorDataMap
}

object SimulatorSchedulerBackend {
  @transient val executorDataMap = new mutable.HashMap[String, ExecutorData]
}
