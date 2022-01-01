package presentation

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileDimensionETL {

  val getLastUDF: UserDefinedFunction = udf((xs: Seq[String]) => xs.last)

  val getFilePathUDF: UserDefinedFunction =
    udf((elements: Seq[String]) => {
      elements.size match {
        case 0 => ""
        case 1 => ""
        case _ => elements.slice(0, elements.size - 1).mkString("/")
      }
    })

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {

    val fileDimensionDF =
      eventPayloadStagingDF
        .select(
          "pull_request_commit_file_sha",
          "pull_request_commit_file_filename"
        )
        .withColumn(
          "sha",
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
        )
        .withColumn(
          "file_name",
          when(col("pull_request_commit_file_filename").isNull, lit(null)).otherwise(
            getLastUDF(split(col("pull_request_commit_file_filename"), "/"))
          )
        )
        .withColumn(
          "name",
          when(col("file_name").isNull, "Filename not available")
            .otherwise(
              when(length(trim(col("file_name"))) === 0, "Empty value")
                .otherwise(col("file_name"))
            )
        )
        .withColumn(
          "file_path",
          when(col("pull_request_commit_file_filename").isNull, lit(null)).otherwise(
            getFilePathUDF(split(col("pull_request_commit_file_filename"), "/"))
          )
        )
        .withColumn(
          "path",
          when(col("file_path").isNull, "File path not available")
            .otherwise(
              when(length(trim(col("file_path"))) === 0, "Root path")
                .otherwise(col("file_path"))
            )
        )
        .withColumn(
          "file_extension",
          when(col("file_name").isNull, lit(null)).otherwise(
            getLastUDF(split(col("file_name"), "\\."))
          )
        )
        .withColumn(
          "extension",
          when(col("file_extension").isNull, "File extension not available")
            .otherwise(
              when(length(trim(col("file_extension"))) === 0, "Empty value")
                .otherwise(col("file_extension"))
            )
        )
        .withColumn(
          "full_file_name",
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
        )
        .drop(
          "pull_request_commit_file_sha",
          "pull_request_commit_file_filename",
          "file_name",
          "file_path",
          "file_extension"
        )
        .distinct()
        .withColumn("pk_id", monotonically_increasing_id())
        .select("*")

    val fileUndefinedRowDF = sparkSession
      .createDataFrame(NullDimension.fileDataNull)
      .toDF(NullDimension.fileColumnNull: _*)

    val fileDimensionWithUndefinedRowDF =
      fileDimensionDF.unionByName(fileUndefinedRowDF)

    fileDimensionWithUndefinedRowDF.printSchema(3)
    fileDimensionWithUndefinedRowDF.show(10)

    fileDimensionWithUndefinedRowDF
  }

}
