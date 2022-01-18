package ModeloDimensionalPullRequest.TimeDimension

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TimeDimensionCreate extends App {

  val hour = "23"
  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path1 = "src/datasets/github/TimeDimension/00"
  val path2 = "src/datasets/github/TimeDimension/01"
  val path3 = "src/datasets/github/TimeDimension/02"
  val path4 = "src/datasets/github/TimeDimension/03"
  val path5 = "src/datasets/github/TimeDimension/04"
  val path6 = "src/datasets/github/TimeDimension/05"
  val path7 = "src/datasets/github/TimeDimension/06"
  val path8 = "src/datasets/github/TimeDimension/07"
  val path9 = "src/datasets/github/TimeDimension/08"
  val path10 = "src/datasets/github/TimeDimension/09"
  val path11 = "src/datasets/github/TimeDimension/10"
  val path12 = "src/datasets/github/TimeDimension/11"
  val path13 = "src/datasets/github/TimeDimension/12"
  val path14 = "src/datasets/github/TimeDimension/13"
  val path15 = "src/datasets/github/TimeDimension/14"
  val path16 = "src/datasets/github/TimeDimension/15"
  val path17 = "src/datasets/github/TimeDimension/16"
  val path18 = "src/datasets/github/TimeDimension/17"
  val path19 = "src/datasets/github/TimeDimension/18"
  val path20 = "src/datasets/github/TimeDimension/19"
  val path21 = "src/datasets/github/TimeDimension/20"
  val path22 = "src/datasets/github/TimeDimension/21"
  val path23 = "src/datasets/github/TimeDimension/22"
  val path24 = "src/datasets/github/TimeDimension/23"
  val path25 = "src/datasets/github/TimeDimension/24"
  val path26 = "src/datasets/github/TimeDimension/25"
  val path27 = "src/datasets/github/TimeDimension/26"
  val path28 = "src/datasets/github/TimeDimension/27"
  val path29 = "src/datasets/github/TimeDimension/28"
  val path30 = "src/datasets/github/TimeDimension/29"

  val path31 = "src/datasets/github/TimeDimension/30"
  val path32 = "src/datasets/github/TimeDimension/31"
  val path33 = "src/datasets/github/TimeDimension/32"
  val path34 = "src/datasets/github/TimeDimension/33"
  val path35 = "src/datasets/github/TimeDimension/34"
  val path36 = "src/datasets/github/TimeDimension/35"
  val path37 = "src/datasets/github/TimeDimension/36"
  val path38 = "src/datasets/github/TimeDimension/37"
  val path39 = "src/datasets/github/TimeDimension/38"
  val path40 = "src/datasets/github/TimeDimension/39"
  val path41 = "src/datasets/github/TimeDimension/40"
  val path42 = "src/datasets/github/TimeDimension/41"
  val path43 = "src/datasets/github/TimeDimension/42"
  val path44 = "src/datasets/github/TimeDimension/43"
  val path45 = "src/datasets/github/TimeDimension/44"
  val path46 = "src/datasets/github/TimeDimension/45"
  val path47 = "src/datasets/github/TimeDimension/46"
  val path48 = "src/datasets/github/TimeDimension/47"
  val path49 = "src/datasets/github/TimeDimension/48"
  val path50 = "src/datasets/github/TimeDimension/49"
  val path51 = "src/datasets/github/TimeDimension/50"
  val path52 = "src/datasets/github/TimeDimension/51"
  val path53 = "src/datasets/github/TimeDimension/52"
  val path54 = "src/datasets/github/TimeDimension/53"
  val path55 = "src/datasets/github/TimeDimension/54"
  val path56 = "src/datasets/github/TimeDimension/55"
  val path57 = "src/datasets/github/TimeDimension/56"
  val path58 = "src/datasets/github/TimeDimension/57"
  val path59 = "src/datasets/github/TimeDimension/58"
  val path60 = "src/datasets/github/TimeDimension/59"

  val path = "src/datasets/github/TimeDimension/hour/" + hour

  def returnTime(time24: String): String = {
    val newHour = hour + time24.substring(2, 8)
    newHour
  }

  val returnTimeUDF = udf(returnTime _)

  val df = spark.read.parquet(path1, path2, path3, path4, path5, path6, path7, path8, path9, path10, path11, path12, path13,
    path14, path15, path16, path17, path18, path19, path20, path21, path22, path23, path24, path25, path26, path27, path28, path29,
    path30, path31, path32, path33, path34, path35, path36, path37, path38, path39, path40, path41, path42, path43, path44, path45,
    path46, path47, path48, path49, path50, path51, path52, path53, path54, path55, path56, path57, path58, path59, path60)

  val hourDF = df.select(returnTimeUDF(col("time")).as("time")).sort(col("time")).drop("seq")
  //hourDF.show(120,false)
  //print(df.count())
  hourDF.write.mode(SaveMode.Overwrite).parquet(path)

  /*
 val timeDF= df.withColumn("time_key",monotonically_increasing_id()).withColumn("")
   .sort(col("time")).drop("seq")*/
  /*
  val seqInitial = Seq(
    ("00"),("01"),("02"),("03"),("04"),("05"),("06"),("07"),("08"),("09"),("10"),("11"),("12"),("13"),("14"),("15"),
    ("16"),("17"),("18"),("19"),("20"),("21"),("22"),("23"),("24"),("25"),("26"),("27"),("28"),("29"),("30"),("31"),
    ("32"),("33"),("34"),("35"),("36"),("37"),("38"),("39"),("40"),("41"),("42"),("43"),("44"),("45"),("46"),("47"),
    ("48"),("49"),("50"),("51"),("52"),("53"),("54"),("55"),("56"),("57"),("58"),("59")
  )

  val rdd = spark.sparkContext.parallelize(seqInitial)

  val dfInitial = rdd.toDF("seq")

  val path = "src/datasets/github/TimeDimension/"+minuto

  def returnSeconds(seconds:String):String={
    var dato = "00:"+minuto+":"+seconds
    dato
  }

  val returnSecondsUDF = udf(returnSeconds _)

  val dfTimeInitial = dfInitial.withColumn("time",returnSecondsUDF(col("seq")))

  dfTimeInitial.withColumn("second",second(col("time"))).show(60,false)
  dfTimeInitial.show(60,false)
  dfTimeInitial.write.mode(SaveMode.Overwrite).parquet(path)
  val dato = spark.read.parquet(path)

 */
  //dato.show(120)

  /*val pathcsv = "src/datasets/github/githubjson/segundos2.csv"
  val seqcsv = spark.read.option("header","true").csv(pathcsv)
  val secondsHoursDF = seqcsv.withColumn("hour",lit("0000-00-00 00:00:00.000"))
  val secondsHoursTimestamp = secondsHoursDF.withColumn("timestamp2",to_timestamp(col("hour"),"HH:mm:ss"))
  secondsHoursTimestamp.createOrReplaceTempView("secondsHours")
  val secondsHoursDF2 = spark.sql("select timestamp2,seq,cast(seq as int) as seq2, cast(timestamp2 as TIMESTAMP) + INTERVAL (seq2) SECOND as addsecond from secondsHours")
   secondsHoursDF2.show(100,false)*/
  //seqcsv.show(100)

}
