package ModeloDimensionalPullRequest.TimeDimension

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object TimeDimensionDF extends App {

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path1 = "src/datasets/github/TimeDimension/hour/00"
  val path2 = "src/datasets/github/TimeDimension/hour/01"
  val path3 = "src/datasets/github/TimeDimension/hour/02"
  val path4 = "src/datasets/github/TimeDimension/hour/03"
  val path5 = "src/datasets/github/TimeDimension/hour/04"
  val path6 = "src/datasets/github/TimeDimension/hour/05"
  val path7 = "src/datasets/github/TimeDimension/hour/06"
  val path8 = "src/datasets/github/TimeDimension/hour/07"
  val path9 = "src/datasets/github/TimeDimension/hour/08"
  val path10 = "src/datasets/github/TimeDimension/hour/09"
  val path11 = "src/datasets/github/TimeDimension/hour/10"
  val path12 = "src/datasets/github/TimeDimension/hour/11"
  val path13 = "src/datasets/github/TimeDimension/hour/12"
  val path14 = "src/datasets/github/TimeDimension/hour/13"
  val path15 = "src/datasets/github/TimeDimension/hour/14"
  val path16 = "src/datasets/github/TimeDimension/hour/15"
  val path17 = "src/datasets/github/TimeDimension/hour/16"
  val path18 = "src/datasets/github/TimeDimension/hour/17"
  val path19 = "src/datasets/github/TimeDimension/hour/18"
  val path20 = "src/datasets/github/TimeDimension/hour/19"
  val path21 = "src/datasets/github/TimeDimension/hour/20"
  val path22 = "src/datasets/github/TimeDimension/hour/21"
  val path23 = "src/datasets/github/TimeDimension/hour/22"
  val path24 = "src/datasets/github/TimeDimension/hour/23"

  def return12Hour(hour24: Int): Int = {
    val hour12 = hour24 match {
      case 13 => 1
      case 14 => 2
      case 15 => 3
      case 16 => 4
      case 17 => 5
      case 18 => 6
      case 19 => 7
      case 20 => 8
      case 21 => 9
      case 22 => 10
      case 23 => 11
      case 0  => 12
      case _ => hour24 // the default, catch-all
    }
    hour12
  }

  def returnMeridianIndicator(hour24: Int): String = {
    val indicador = hour24 match {
      case 12 => "M"
      case 13 => "PM"
      case 14 => "PM"
      case 15 => "PM"
      case 16 => "PM"
      case 17 => "PM"
      case 18 => "PM"
      case 19 => "PM"
      case 20 => "PM"
      case 21 => "PM"
      case 22 => "PM"
      case 23 => "PM"
      case _ => "AM" // the default, catch-all
    }
    indicador
  }

  def returnTime12(time24: String, hour12: Int,meridian:String): String = {
    var indicador=""
    if(meridian == "AM"){
      indicador="a.m."
    } else if (meridian == "PM") {
      indicador="p.m."
    }else {
      indicador="m"
    }
    val newHour = hour12.toString + time24.substring(2, 8) + " "+indicador
    newHour
  }

  def returnPeriodIndicador(hour24:Int):String={
    val indicador = hour24 match {
      case 12 => "Midday"
      case 13 => "Afternoon"
      case 14 => "Afternoon"
      case 15 => "Afternoon"
      case 16 => "Afternoon"
      case 17 => "Afternoon"
      case 18 => "Afternoon"
      case 19 => "Night"
      case 20 => "Night"
      case 21 => "Night"
      case 22 => "Night"
      case 23 => "Night"
      case 0 => "Midnight"
      case _ => "Morning" // the default, catch-all
    }
    indicador
  }

  val return12HourUDF = udf(return12Hour _)

  val returnMeridianIndicatorUDF = udf(returnMeridianIndicator _)

  val returnTime12UDF = udf(returnTime12 _)

  val returnPeriodIndicadorUDF = udf(returnPeriodIndicador _)

  val df = spark.read.parquet(path1, path2, path3, path4, path5, path6, path7, path8, path9, path10, path11, path12, path13,
    path14, path15, path16, path17, path18, path19, path20, path21, path22, path23, path24)

  val timeDF = df.withColumn("time_key", monotonically_increasing_id())
    .withColumnRenamed("time", "time24h")
    .withColumn("hour", hour(col("time24h"))).withColumn("minute", minute(col("time24h")))
    .withColumn("second", second(col("time24h"))).withColumn("hour12", return12HourUDF(col("hour")))
    .withColumn("meridian_indicator", returnMeridianIndicatorUDF(col("hour")))
    .withColumn("time_ampm", returnTime12UDF(col("time24h"), col("hour12"),col("meridian_indicator")))
    .withColumn("period", returnPeriodIndicadorUDF(col("hour"))).sort(col("time24h"))

  val path = "src/datasets/github/TimeDimension/TimeDimensionDF"
  //timeDF.filter(col("meridian_indicator")==="AM").show(120,false)
  timeDF.write.mode(SaveMode.Overwrite).parquet(path)
  println("-- PARQUET TIME CREADO CON EXITO")

}
