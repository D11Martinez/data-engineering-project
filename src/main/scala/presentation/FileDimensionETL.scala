package presentation

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{
  col,
  explode,
  lit,
  size,
  split,
  udf,
  when
}
import staging.UserStagingETL.userStagingSchema

import scala.util.Try

object FileDimensionETL extends App {
  // user defined functions
  val uuid = udf(() => java.util.UUID.randomUUID().toString)

  val getLastUDF = udf((xs: Seq[String]) => xs.last)

  val getFilePathUDF =
    udf((elements: Seq[String]) => {
      elements.size match {
        case 0 => ""
        case 1 => ""
        case _ => elements.slice(0, elements.size - 1).mkString("/")
      }
    })

  // configuration paths
  val commitStagingSource = "src/dataset/staging/commits"
  val fileDimensionOutput = "src/dataset/presentation/file-dimension"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val commitStagingDF =
    spark.read.parquet(commitStagingSource)

  val fileDimensionDF =
    commitStagingDF
      .withColumn("file_name", getLastUDF(split(col("file_filename"), "/")))
      .withColumn("file_path", getFilePathUDF(split(col("file_filename"), "/")))
      .withColumn(
        "file_extension",
        getLastUDF(split(col("file_name"), "."))
      ) // TODO: this not works
      .select(
        col("file_sha"),
        col("file_name"),
        col("file_extension"),
        col("file_path"),
        col("file_filename"),
        col("file_blob_url"),
        col("file_raw_url"),
        col("file_contents_url")
      )
      .distinct()
      .withColumn("id", uuid())
      .select(
        col("id"),
        col("file_sha"),
        col("file_name"),
        col("file_extension"),
        col("file_path"),
        col("file_filename"),
        col("file_blob_url"),
        col("file_raw_url"),
        col("file_contents_url")
      )

  fileDimensionDF.printSchema(3)
  fileDimensionDF.show(10)

  fileDimensionDF.write
    .mode(SaveMode.Overwrite)
    .parquet(fileDimensionOutput)
}
