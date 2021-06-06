package net.suncaper.spark

import java.util.concurrent.TimeUnit

import net.suncaper.spark.geo.GeoJson.FeatureCollection
import net.suncaper.spark.parser.TaxiTripParser
import org.apache.commons.math3.geometry.Point
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object TaxiAnalysisDriver {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    System.setProperty("hadoop.home.dir","D:/hadoop")
    // 1. 初始化SparkSession

    val spark = SparkSession.builder()
      .appName("TaxiAnalysis")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val rawTrip = spark.read
      .option("header", true)
      .csv("datasets/trip_data.csv")

    val parseDTrip = rawTrip.rdd
        .map(TaxiTripParser.parse)
        .toDS()

    spark.udf.register("HOURS", (pickup: Long, dropoff: Long) => TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MICROSECONDS))
    val cleanedTrip = parseDTrip
        .where("pickupLon != 0 AND pickupLat != 0 AND dropoffLon != 0 AND dropoffLat != 0")
        .where("HOURS(pickupTime, dropoffTime) BETWEEN 0 AND 3")


    cleanedTrip.show()
    cleanedTrip.printSchema()


    spark.stop()
  }
}
