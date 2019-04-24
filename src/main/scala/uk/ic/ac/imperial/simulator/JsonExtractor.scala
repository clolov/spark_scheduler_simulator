package uk.ic.ac.imperial.simulator

import java.util.Properties

import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object JsonExtractor {

  implicit val formats: DefaultFormats.type = DefaultFormats

  private val FIFO = "FIFO"
  private val FAIR = "FAIR"
  private val NEPTUNE = "NEPTUNE"

  def getSparkSchedulerConfiguration(sparkSchedulerMode: String): ArrayBuffer[(String, String)] = {
    sparkSchedulerMode match {
      case FIFO => ArrayBuffer("spark.scheduler.mode" -> FIFO)
      case FAIR => ArrayBuffer("spark.scheduler.mode" -> FAIR,
        "spark.scheduler.allocation.file" -> "conf/fairscheduler_simulator.xml")
      case NEPTUNE => ArrayBuffer("spark.scheduler.mode" -> NEPTUNE,
        "spark.neptune.task.coroutines" -> "true")
    }
  }

  def fromJsonToSimulatorConfiguration(json: JValue): SimulatorConfiguration = {
    val executors = (json \ "executors").extract[Int]
    val coresPerExec = (json \ "coresPerExec").extract[Int]
    val sparkSchedulerMode = getSparkSchedulerConfiguration((json \ "sparkSchedulerMode").extract[String])
    SimulatorConfiguration(executors, coresPerExec, sparkSchedulerMode)
  }

  def fromJsonToJobSubmissionTimeMap(json: JValue): mutable.Map[Int, ArrayBuffer[Job]] = {
    val jobsToSubmit = new mutable.HashMap[Int, ArrayBuffer[Job]]()

    json.extract[List[JValue]].foreach(jsonValue => {
      val range = fromJsonMetapropertiesToRange(jsonValue \ "metaproperties")

      val job = Job(((jsonValue \ "representation") \ "stages").extract[List[Stage]],
        fromJsonPropertiesToProperties(jsonValue \ "properties"))

      for (i <- range) {
        val jobsAtTime = jobsToSubmit.getOrElse(i, ArrayBuffer.empty[Job])
        jobsAtTime.+=(job)
        jobsToSubmit.put(i, jobsAtTime)
      }
    })

    jobsToSubmit
  }

  def fromJsonPropertiesToProperties(json: JValue): Properties = {
    val properties = new Properties()
    properties.setProperty("duration", (json \ "duration").extract[String])
    properties.setProperty("spark.scheduler.pool", (json \ "sparkSchedulerPool").extract[String])
    properties.setProperty("neptune_pri", (json \ "neptunePri").extract[String])
    properties
  }

  def fromJsonMetapropertiesToRange(json: JValue): Range = {
    val from = (json \ "arrivesAt").extract[Int]
    val step = (json \ "repeatsEvery").extract[Int]
    val numberOfRepetitions = (json \ "repeatsTimes").extract[Int]

    Range(from, from + step * numberOfRepetitions, step)
  }

  case class SimulatorConfiguration(executors: Int, coresPerExec: Int, confs: ArrayBuffer[(String, String)])

  case class Job(representation: List[Stage], properties: Properties)

  case class Stage(id: Int, dependsOn: List[Int])

  def fromJsonFileToObject(filename: String): (SimulatorConfiguration, mutable.Map[Int, ArrayBuffer[Job]]) = {
    val bufferedSource = Source.fromFile(filename).getLines.mkString

    val json = parse(bufferedSource)
    val simulatorConfiguration = fromJsonToSimulatorConfiguration(json \ "simulatorConfiguration")
    val jobs = fromJsonToJobSubmissionTimeMap(json \ "jobs")

    (simulatorConfiguration, jobs)
  }
}
