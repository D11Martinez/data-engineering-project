package ModeloDimensionalPullRequest.DateDimension

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

object DateDimensionCreate extends App {

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val dfDateInitial = Seq(
    (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15),
    (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31)
  ).toDF("seq")

  val anio = "2020"

  val path = "src/datasets/github/DateDimension/" + anio

  val dfDateInitialEnero = dfDateInitial.withColumn("dateIni", lit(anio + "-01-01"))

  val dateEneroDF = dfDateInitialEnero.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateEneroDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialFebrero = dfDateInitial.withColumn("dateIni", lit(anio + "-02-01"))

  val dateFebreroDF = dfDateInitialFebrero.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateFebreroDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialMarzo = dfDateInitial.withColumn("dateIni", lit(anio + "-03-01"))

  val dateMarzoDF = dfDateInitialMarzo.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateMarzoDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialAbril = dfDateInitial.withColumn("dateIni", lit(anio + "-04-01"))

  val dateAbrilDF = dfDateInitialAbril.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateAbrilDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialMayo = dfDateInitial.withColumn("dateIni", lit(anio + "-05-01"))

  val dateMayoDF = dfDateInitialMayo.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateMayoDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialJunio = dfDateInitial.withColumn("dateIni", lit(anio + "-06-01"))

  val dateJunioDF = dfDateInitialJunio.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  // dateJunioDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialJulio = dfDateInitial.withColumn("dateIni", lit(anio + "-07-01"))

  val dateJulioDF = dfDateInitialJulio.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateJulioDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialAgosto = dfDateInitial.withColumn("dateIni", lit(anio + "-08-01"))

  val dateAgostoDF = dfDateInitialAgosto.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateAgostoDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialSeptiembre = dfDateInitial.withColumn("dateIni", lit(anio + "-09-01"))

  val dateSeptiembreDF = dfDateInitialSeptiembre.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  // dateSeptiembreDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialOctubre = dfDateInitial.withColumn("dateIni", lit(anio + "-10-01"))

  val dateOctubreDF = dfDateInitialOctubre.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateOctubreDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialNoviembre = dfDateInitial.withColumn("dateIni", lit(anio + "-11-01"))

  val dateNoviembreDF = dfDateInitialNoviembre.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  // dateNoviembreDF.show(32)

  //-----------------------------------------------------------------------
  val dfDateInitialDiciembre = dfDateInitial.withColumn("dateIni", lit(anio + "-12-01"))

  val dateDiciembreDF = dfDateInitialDiciembre.selectExpr("dateIni", "seq",
    "date_add(to_date(dateIni,'yyyy-MM-dd'),cast(seq as int)) as date").drop("dateIni", "seq")

  //dateDiciembreDF.show(32)

  val dateDF = dateEneroDF.union(dateFebreroDF).union(dateMarzoDF).union(dateAbrilDF).union(dateMayoDF)
    .union(dateJunioDF).union(dateJulioDF).union(dateAgostoDF).union(dateSeptiembreDF).union(dateOctubreDF)
    .union(dateNoviembreDF).union(dateDiciembreDF)

  val dateFinalDF = dateDF.distinct().sort("date")

  dateFinalDF.write.mode(SaveMode.Overwrite).parquet(path)

  //val dateDimensionDF =

}
