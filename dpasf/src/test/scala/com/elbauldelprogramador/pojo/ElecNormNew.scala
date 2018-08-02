package com.elbauldelprogramador.pojo

// elecNormNew POJO
case class ElecNormNew(
  var date: Double,
  var day: Double,
  var period: Double,
  var nswprice: Double,
  var nswdemand: Double,
  var vicprice: Double,
  var vicdemand: Double,
  var transfer: Double,
  var label: Double) extends Serializable {

  //  def this() = {
  //    this(0, 0, 0, 0, 0, 0, 0, 0, "")
  //  }
}
