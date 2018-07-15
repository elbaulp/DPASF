package com.elbauldelprogramador.pojo

// elecNormNew POJO
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
