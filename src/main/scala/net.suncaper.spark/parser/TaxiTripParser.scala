package net.suncaper.spark.parser

import java.text.SimpleDateFormat

import org.apache.spark.sql.Row

case class Trip(
                 license: String, //出租车司机编号
                 pickupTime: Long, //乘客上车时间
                 dropoffTime: Long, //乘客下车时间
                 pickupLon: Double, //乘客上车地点的经度
                 pickupLat: Double, //乘客上车地点的纬度
                 dropoffLon: Double, //乘客下车地点的经度
                 dropoffLat: Double //乘客下车地点的纬度
               )

class RichRow(row: Row) {
  def getAs[T](field:String):Option[T] = if(row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs(field))
}

object TaxiTripParser {
  def parse = (row: Row) =>{
    val richRow = new RichRow(row)

    val license = row.getAs[String]("hack_license")
    val pickupTime = parseDateTime(richRow,"pickup_datetime")
    val dropoffTime = parseDateTime(richRow,"dropoff_datetime")
    val pickupLon = parseLocation(richRow,"pickup_longitude")
    val pickupLat = parseLocation(richRow,"pickup_latitude")
    val dropoffLon = parseLocation(richRow,"dropoff_longitude")
    val dropoffLat = parseLocation(richRow,"dropoff_latitude")



    Trip(license,pickupTime,dropoffTime,pickupLon,pickupLat,dropoffLon,dropoffLat)
  }

  def parseDateTime(richRow: RichRow, fieldName: String): Long ={
    val formatter = new SimpleDateFormat("yyyy/mm/dd HH:mm")


    richRow.getAs[String](fieldName).map(formatter.parse(_).getTime).getOrElse(0L)
  }

  def parseLocation(richRow: RichRow, fieldName: String): Double = {
    richRow.getAs[String](fieldName).map(_.toDouble).getOrElse(0.0)

  }

}
