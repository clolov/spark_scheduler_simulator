package uk.ic.ac.imperial.simulator

import java.io.{ByteArrayOutputStream, File, PrintWriter}

import org.scalatest.FunSuite

import scala.io.Source

class SimulatorSuite extends FunSuite {

  private val configurationsPrefix = "src/test/resources/configurations/"
  private val outputsPrefix = "src/test/resources/outputs/"

  test("FIFO_with_4_execs_1_core_2_jobs") {
    val outCapture = new ByteArrayOutputStream()
    Console.withOut(outCapture) {
      Simulator.main(Array(configurationsPrefix + "FIFO_with_4_execs_1_core_2_jobs.json"))
    }
    val expectedToPrint = Source.fromFile(outputsPrefix + "FIFO_with_4_execs_1_core_2_jobs.txt").getLines.mkString("\n") + "\n"
    assert(expectedToPrint == outCapture.toString)
//    val pw = new PrintWriter(new File("FIFO_with_4_execs_1_core_2_jobs.txt" ))
//    pw.write(expectedToPrint)
//    pw.close()
  }

}
