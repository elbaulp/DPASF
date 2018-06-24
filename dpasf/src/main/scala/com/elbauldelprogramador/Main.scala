package com.elbauldelprogramador

import com.elbauldelprogramador.discretizers.IDADiscretizer
import org.apache.flink.api.scala._

class ElecNormNew(
  var date: Double,
  var day: Int,
  var period: Double,
  var nswprice: Double,
  var nswdemand: Double,
  var vicprice: Double,
  var vicdemand: Double,
  var transfer: Double,
  var label: String) extends Serializable {

  def this() = {
    this(0, 0, 0, 0, 0, 0, 0, 0, "")
  }
}

object Main {
  def main(args: Array[String]) {

    println("Hello")
    println("Hello")
    println("Hello")
    println("Hello")
    println("Hello")

    val env = ExecutionEnvironment.getExecutionEnvironment

    //val dataSet = env.readCsvFile[Iris](getClass.getResource("/iris.dat").getPath)
    //val ataSet = new ArffFileStream(getClass.getResource("/elecNormNew.arff").getPath, -1)
    //val dataSet = env.fromElements(1 to 10 by 1)
    val dataSet = env.readCsvFile[ElecNormNew](
      getClass.getResource("/elecNormNew.arff").getPath,
      pojoFields = Array("date", "day", "period", "nswprice", "nswdemand", "vicprice", "vicdemand", "transfer", "label"))

    dataSet.print

    //val a = new IDADiscretizer[ElecNormNew](dataSet)
    //a.discretize

  }
}
