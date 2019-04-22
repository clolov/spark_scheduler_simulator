package uk.ic.ac.imperial.simulator

import org.apache.spark.scheduler.SchedulingMode

import scala.collection.mutable.ArrayBuffer

object Configuration {

  private val FIFO = "FIFO"
  private val FAIR = "FAIR"
  private val NEPTUNE = "NEPTUNE"

  def getConfiguration(sparkSchedulerMode: String): ArrayBuffer[(String, String)] = {
    sparkSchedulerMode match {
      case FIFO => ArrayBuffer("spark.scheduler.mode" -> FIFO)
      case FAIR => ArrayBuffer("spark.scheduler.mode" -> FAIR,
        "spark.scheduler.allocation.file" -> "conf/fairscheduler_simulator.xml")
      case NEPTUNE => ArrayBuffer("spark.scheduler.mode" -> NEPTUNE,
        "spark.neptune.task.coroutines" -> "true")
    }
  }

}
