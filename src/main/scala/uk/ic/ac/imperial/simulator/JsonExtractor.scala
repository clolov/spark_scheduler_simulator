package uk.ic.ac.imperial.simulator

import java.util.Properties

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

import scala.io.Source

object JsonExtractor {
  case class SimulatorConfiguration(executors: Int, coresPerExec: Int, sparkSchedulerMode: String)

  case class MetaProperties(arrivesAt: Int, repeatsEvery: Int, repeatsTimes: Int)

//  case class Job(rdd: RDD[_], properties: Properties)

  implicit val formats: DefaultFormats.type = DefaultFormats

//  def fromJsonToSimulatorConfiguration(json: JValue): SimulatorConfiguration = {
//    val executors = (json \ "executors").extract[Int]
//    val coresPerExec = (json \ "cores_per_executor").extract[Int]
//    val sparkSchedulerMode = (json \ "spark_scheduler_mode").extract[String]
//    SimulatorConfiguration(executors, coresPerExec, sparkSchedulerMode)
//  }

  case class Configuration(simulatorConfiguration: SimulatorConfiguration, jobs: Seq[Job])

  case class Job(representation: Representation, properties: JobProperties, metaproperties: MetaProperties)

  case class JobProperties(duration: String, sparkSchedulerPool: String, neptunePri: String) {
    val properties = new Properties()
    properties.setProperty("duration", duration)
    properties.setProperty("spark.scheduler.pool", sparkSchedulerPool)
    properties.setProperty("neptune_pri", neptunePri)
  }

  case class Representation(stages: List[Stage])

  case class Stage(id: Int, dependsOn: List[Int])

  def fromJsonFileToObject(filename: String): Configuration = {
    val bufferedSource = Source.fromFile(filename).getLines.mkString

//    val json = parse(bufferedSource)
//    fromJsonToSimulatorConfiguration(json \ "simulator_configuration")
//    fromJsonToJobs(json \ "jobs")

    read[Configuration](bufferedSource)
  }
}
