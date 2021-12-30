package presentation

import Presentation.NullDimension
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object FileDimensionETL extends App {
  // user defined functions
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
  val eventPayloadStagingSource = "src/dataset/staging/events-payloads"
  val fileDimensionOutput = "src/dataset/presentation/file-dimension"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val eventPayloadDF = spark.read.parquet(eventPayloadStagingSource)

  val fileDimensionDF =
    eventPayloadDF
      .select(
        "pull_request_commit_file_sha",
        "pull_request_commit_file_filename"
      )
      .withColumn(
        "file_name",
        getLastUDF(split(col("pull_request_commit_file_filename"), "/"))
      )
      .withColumn(
        "file_path",
        getFilePathUDF(split(col("pull_request_commit_file_filename"), "/"))
      )
      .withColumn(
        "file_extension",
        getLastUDF(split(col("file_name"), "\\."))
      )
      .select(
        when(
          col("pull_request_commit_file_sha").isNull,
          "File sha not available"
        )
          .otherwise(
            when(
              length(trim(col("pull_request_commit_file_sha"))) === 0,
              "Empty value"
            )
              .otherwise(col("pull_request_commit_file_sha"))
          )
          .as("sha"),
        when(col("file_name").isNull, "Filename not available")
          .otherwise(
            when(length(trim(col("file_name"))) === 0, "Empty value")
              .otherwise(col("file_name"))
          )
          .as("name"),
        when(col("file_extension").isNull, "File extension not available")
          .otherwise(
            when(length(trim(col("file_extension"))) === 0, "Empty value")
              .otherwise(col("file_extension"))
          )
          .as("extension"),
        when(col("file_path").isNull, "File path not available")
          .otherwise(
            when(length(trim(col("file_path"))) === 0, "Root path")
              .otherwise(col("file_path"))
          )
          .as("path"),
        when(
          col("pull_request_commit_file_filename").isNull,
          "Full filename not available"
        )
          .otherwise(
            when(
              length(trim(col("pull_request_commit_file_filename"))) === 0,
              "Empty value"
            )
              .otherwise(col("pull_request_commit_file_filename"))
          )
          .as("full_file_name")
      )
      .distinct()
      .withColumn("pk_id", monotonically_increasing_id())
      .select("*")

  val fileUndefinedRowDF = spark
    .createDataFrame(NullDimension.fileDataNull)
    .toDF(NullDimension.fileColumnNull: _*)

  val fileDimensionWithUndefinedRowDF =
    fileDimensionDF.unionByName(fileUndefinedRowDF)

  fileDimensionWithUndefinedRowDF.printSchema(3)
  fileDimensionWithUndefinedRowDF.show(10)

  fileDimensionWithUndefinedRowDF.write
    .mode(SaveMode.Overwrite)
    .parquet(fileDimensionOutput)
}
