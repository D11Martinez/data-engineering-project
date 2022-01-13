package ModeloDimensionalPullRequest.DateDimension

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DateDimensionDF extends App {

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path2018 = "src/datasets/github/DateDimension/2018"
  val path2019 = "src/datasets/github/DateDimension/2019"
  val path2020 = "src/datasets/github/DateDimension/2020"
  val path = "src/datasets/github/DateDimension/DateDimensionDF"

  val dateDF2018 = spark.read.parquet(path2018)
  val dateDF2019 = spark.read.parquet(path2019)
  val dateDf2020 = spark.read.parquet(path2020)

  val dateDimensionDF = dateDF2018.union(dateDF2019).union(dateDf2020)

  def returnDia(diaNum: Int): String = {
    var dia = diaNum match {
      case 1 => "Sunday"
      case 2 => "Monday"
      case 3 => "Tuesday"
      case 4 => "Wednesday"
      case 5 => "Thursday"
      case 6 => "Friday"
      case 7 => "Saturday"
      case _ => "Invalid Day"
    }
    dia
  }

  val returnDiaUDF = udf(returnDia _)

  def returnMonth(monthNum: Int): String = {
    val month = monthNum match {
      case 1  => "January"
      case 2  => "February"
      case 3  => "March"
      case 4  => "April"
      case 5  => "May"
      case 6  => "June"
      case 7  => "July"
      case 8  => "August"
      case 9  => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
      case _  => "Invalid month" // the default, catch-all
    }
    month
  }

  val returnMonthUDF = udf(returnMonth _)

  val dateDimensionTotalDF = dateDimensionDF
    .withColumn("date_key", monotonically_increasing_id())
    .withColumn("full_date_description", date_format(col("date"), "MMMM dd yyyy"))
    .withColumn("day_of_week", returnDiaUDF(dayofweek(col("date"))))
    .withColumn("calendar_month", returnMonthUDF(month(col("date"))))
    .withColumn("calendar_quarter", quarter(col("date")))
    .withColumn("calendar_year", year(col("date")))
    .withColumn(
      "holiday_indicator",
      when(
        (month(col("date")) === 10 && dayofmonth(col("date")) === 31) || (month(
          col("date")
        ) === 12 && dayofmonth(col("date")) === 31) ||
          (month(col("date")) === 1 && dayofmonth(col("date")) === 1) || (month(
            col("date")
          ) === 12 && dayofmonth(col("date")) === 24) ||
          (month(col("date")) === 12 && dayofmonth(col("date")) === 25),
        "Holiday"
      ).otherwise("Non-holiday")
    )

  dateDimensionTotalDF.show(5, false)
  dateDimensionTotalDF.write.mode(SaveMode.Overwrite).parquet(path)
}
