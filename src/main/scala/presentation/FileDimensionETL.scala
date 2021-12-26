package presentation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

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
        getLastUDF(split(col("file_name"), "\\."))
      ) // TODO: this not works
      .select(
        when(col("file_sha").isNull, "File sha not available")
          .otherwise(
            when(length(trim(col("file_sha"))) === 0, "Empty value")
              .otherwise(col("file_sha"))
          )
          .as("file_sha"),
        when(col("file_name").isNull, "Filename not available")
          .otherwise(
            when(length(trim(col("file_name"))) === 0, "Empty value")
              .otherwise(col("file_name"))
          )
          .as("file_name"),
        when(col("file_extension").isNull, "File extension not available")
          .otherwise(
            when(length(trim(col("file_extension"))) === 0, "Empty value")
              .otherwise(col("file_extension"))
          )
          .as("file_extension"),
        when(col("file_path").isNull, "File path not available")
          .otherwise(
            when(length(trim(col("file_path"))) === 0, "Root path")
              .otherwise(col("file_path"))
          )
          .as("file_path"),
        when(col("file_filename").isNull, "Full filename not available")
          .otherwise(
            when(length(trim(col("file_filename"))) === 0, "Empty value")
              .otherwise(col("file_filename"))
          )
          .as("full_file_name"),
        when(col("file_blob_url").isNull, "File blob URL not available")
          .otherwise(
            when(length(trim(col("file_blob_url"))) === 0, "Empty value")
              .otherwise(col("file_blob_url"))
          )
          .as("file_blob_url"),
        when(col("file_raw_url").isNull, "File raw URL not available")
          .otherwise(
            when(length(trim(col("file_raw_url"))) === 0, "Empty value")
              .otherwise(col("file_raw_url"))
          )
          .as("file_raw_url"),
        when(col("file_contents_url").isNull, "File contents URL not available")
          .otherwise(
            when(length(trim(col("file_contents_url"))) === 0, "Empty value")
              .otherwise(col("file_contents_url"))
          )
          .as("file_contents_url")
      )
      .distinct()
      .withColumn("id", uuid())
      .select("*")

  fileDimensionDF.printSchema(3)
  fileDimensionDF.show(10)

  fileDimensionDF.write
    .mode(SaveMode.Overwrite)
    .parquet(fileDimensionOutput)
}
