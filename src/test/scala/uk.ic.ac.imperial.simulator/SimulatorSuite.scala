package uk.ic.ac.imperial.simulator

import org.scalatest.FunSuite

class SimulatorSuite extends FunSuite {

  test("simple") {
    Simulator.main(Array("conf/simulator_configuration.json"))
    val expectedToPrint = "||          (TaskSet_0.0,0)          ArrayBuffer((0,0, ), (0,2,|))\n||          (TaskSet_1.0,1)          ArrayBuffer((0,0, ), (0,2,|))\n||          (TaskSet_2.0,2)          ArrayBuffer((0,0, ), (0,2,|))\n|          (TaskSet_4.0,3)          ArrayBuffer((0,0, ), (0,1,|))\n |          (TaskSet_5.0,4)          ArrayBuffer((0,1, ), (1,2,|))\n  ||          (TaskSet_3.0,5)          ArrayBuffer((0,2, ), (2,4,|))\n  |          (TaskSet_6.0,6)          ArrayBuffer((0,2, ), (2,3,|))\n   |          (TaskSet_7.0,7)          ArrayBuffer((0,3, ), (3,4,|))\n"
    assert(expectedToPrint == Simulator.toPrint)
  }

}
