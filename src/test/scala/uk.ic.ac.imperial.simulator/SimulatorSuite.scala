package uk.ic.ac.imperial.simulator

import java.io.{ByteArrayOutputStream, File, PrintWriter}

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.io.Source

class SimulatorSuite extends FunSuite with BeforeAndAfterEach {

  private val configurationsPrefix = "src/test/resources/configurations/"
  private val outputsPrefix = "src/test/resources/outputs/"

  test("FIFO_with_4_execs_1_core_2_jobs") {
    val outCapture = new ByteArrayOutputStream()
    val simulator = new Simulator
    Console.withOut(outCapture) {
      simulator.main(Array(configurationsPrefix + "FIFO_with_4_execs_1_core_2_jobs.json"))
    }
    val expectedToPrint = Source.fromFile(outputsPrefix + "FIFO_with_4_execs_1_core_2_jobs.txt").getLines.mkString("\n") + "\n"
    assert(expectedToPrint == outCapture.toString)
    simulator.sc.stop()
  }

  test("FAIR_with_4_execs_1_core_2_jobs") {
    val outCapture = new ByteArrayOutputStream()
    val simulator = new Simulator
    Console.withOut(outCapture) {
      simulator.main(Array(configurationsPrefix + "FAIR_with_4_execs_1_core_2_jobs.json"))
    }
    val expectedToPrint = Source.fromFile(outputsPrefix + "FAIR_with_4_execs_1_core_2_jobs.txt").getLines.mkString("\n") + "\n"
    assert(expectedToPrint == outCapture.toString)
    simulator.sc.stop()
  }

  test("NEPTUNE_with_4_execs_1_core_2_jobs") {
    val outCapture = new ByteArrayOutputStream()
    val simulator = new Simulator
    Console.withOut(outCapture) {
      simulator.main(Array(configurationsPrefix + "NEPTUNE_with_4_execs_1_core_2_jobs.json"))
    }
    val expectedToPrint = Source.fromFile(outputsPrefix + "NEPTUNE_with_4_execs_1_core_2_jobs.txt").getLines.mkString("\n") + "\n"
    assert(expectedToPrint == outCapture.toString)
    simulator.sc.stop()
  }

  def writeOutputToFile(filename: String, contents: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    pw.write(contents)
    pw.close()
  }

}
