package uk.ic.ac.imperial.simulator

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

import scala.io.Source

object JsonExtractor {
  case class SimulatorConfiguration(executors: Int, coresPerExec: Int, sparkSchedulerMode: String)

  implicit val formats: DefaultFormats.type = DefaultFormats

  def fromJsonFileToObject(filename: String): SimulatorConfiguration = {
    val bufferedSource = Source.fromFile(filename).getLines.mkString

    read[SimulatorConfiguration](bufferedSource)
  }
}
