package com.github.mrpowers.my.cool.project

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql.SparkSession

object JsonReader {
  implicit val formats: Formats = DefaultFormats

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").appName("json_reader").getOrCreate()
    val sc = spark.sparkContext
    val filename = args(0)

    sc.textFile(filename)
      .map(json => parse(json).extract[Wine])
      .foreach(wine => println(wine))
  }

  case class Wine(id: Option[Long],
                  country: Option[String],
                  points: Option[Long],
                  title: Option[String],
                  variety: Option[String],
                  winery: Option[String])

}
