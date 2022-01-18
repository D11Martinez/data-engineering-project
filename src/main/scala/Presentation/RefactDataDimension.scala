package presentation

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, dayofweek, udf, weekofyear}

object RefactDataDimension extends App {
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

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val dateDimensionPath = "src/dataset/presentation/date-dimension"
  val updatedDateDimensionPath = "src/dataset/presentation/updated-date-dimension"

  val dateDim = spark.read.parquet(dateDimensionPath)
  dateDim.printSchema(2)

  val updatedDateDim = dateDim
    .filter(col("date_key") =!= "-1")
    .withColumn("day_of_week", returnDiaUDF(dayofweek(col("date"))))
    .withColumn("week_of_year", weekofyear(col("date")))
    .distinct()
    .select("*")

  updatedDateDim.printSchema(3)

  // Date Dimension
  val dateColumnNull = Seq(
    "date",
    "full_date_description",
    "day_of_week",
    "calendar_month",
    "calendar_quarter",
    "calendar_year",
    "holiday_indicator",
    "date_key",
    "week_of_year"
  )

  val dateDataNull = Seq(
    (
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      0,
      0,
      "Undefined",
      -1,
      0
    )
  )

  val dateNull = spark
    .createDataFrame(dateDataNull)
    .toDF(dateColumnNull: _*)

  updatedDateDim
    .unionByName(dateNull)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(updatedDateDimensionPath)
}
