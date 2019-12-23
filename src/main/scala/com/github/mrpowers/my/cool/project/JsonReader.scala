package com.github.mrpowers.my.cool.project

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql.SparkSession

object JsonReader extends App {
  implicit val formats: Formats = DefaultFormats

  val spark = SparkSession.builder().master("local").appName("json_reader").getOrCreate()
  val sc = spark.sparkContext
  val filename = "D:/users/Aleksandr/winemag-data-130k-v2.json"//args(0)

  sc.textFile(filename)
    .map(json => parse(json).extract[Wine])
    .foreach(wine => println(wine))


  case class Wine(id: Long, country: String, points: Long, title: String, variety: String, winery: String)
}
