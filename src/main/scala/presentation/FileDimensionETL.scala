package presentation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import presentation.CustomUDF.{getFilePathUDF, getLanguageFromExtension, getLastUDF}

object FileDimensionETL {
  val FILE_DIMENSION_PATH = "src/dataset/presentation/file-dimension"
  val FILE_DIMENSION_TEMP_PATH =
    "src/dataset/presentation/temp/file-dimension" // This route must exists
  val fileDimensionSchema: StructType = StructType(
    Array(
      StructField("pk_id", LongType, nullable = false),
      StructField("sha", StringType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("path", StringType, nullable = false),
      StructField("extension", StringType, nullable = false),
      StructField("full_file_name", StringType, nullable = false),
      StructField("language", StringType, nullable = false)
    )
  )

  // Transform the staging data to dimension format without pk_id
  def transform(eventPayloadStagingDF: DataFrame): DataFrame = {
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
          when(col("file_name").isNull, "Not available")
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
          when(col("file_path").isNull, "Not available")
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
          when(col("file_extension").isNull, "Not available")
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
        .withColumn(
          "language",
          when(col("extension") === "Empty value", "Empty Value")
            .otherwise(
              when(col("extension") === "Not available", "Not available")
                .otherwise(getLanguageFromExtension(col("extension")))
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
        .select("*")

    fileDimensionDF
  }

  // load the incoming data from staging layer applying SCD conditions
  def loadApplyingSCD(
      dataFromStagingDF: DataFrame,
      currentDimensionDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentFileDimensionDF = currentDimensionDF
    val filesFromStagingDF = dataFromStagingDF

    val tempFileDimDF = if (currentFileDimensionDF.isEmpty) {
      // reading the null row
      val fileUndefinedRowDF = sparkSession
        .createDataFrame(NullDimension.fileDataNull)
        .toDF(NullDimension.fileColumnNull: _*)

      val fileDimWithUndefinedRowDF = filesFromStagingDF
        .withColumn("pk_id", monotonically_increasing_id())
        .unionByName(fileUndefinedRowDF)

      val fileDimDF = fileDimWithUndefinedRowDF

      fileDimDF
    } else {
      val newFilesDF = filesFromStagingDF
        .join(
          currentFileDimensionDF,
          filesFromStagingDF("sha") === currentFileDimensionDF("sha"), // Joining by natural Key
          "left_anti"
        )
        .withColumn("pk_id", monotonically_increasing_id())

      val editedFilesDF = currentFileDimensionDF
        .as("current")
        .join(
          filesFromStagingDF.as("staging"),
          filesFromStagingDF("sha") === currentFileDimensionDF("sha"), // Joining by natural Key
          "inner"
        )
        .select(
          // applying SCD 0
          col("current.pk_id"),
          col("current.sha"),
          col("current.name"),
          col("current.path"),
          col("current.extension"),
          col("current.full_file_name"),
          col("current.language")
        )

      val notUpdatedFilesDF = currentFileDimensionDF.join(
        filesFromStagingDF,
        filesFromStagingDF("sha") === currentFileDimensionDF("sha"), // Joining by natural Key
        "left_anti"
      )

      // Union of all dimension pieces
      val updatedFileDimDF = newFilesDF
        .unionByName(editedFilesDF)
        .unionByName(notUpdatedFilesDF)

      updatedFileDimDF
    }

    // Returning the final dimension as temporal
    tempFileDimDF
  }

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentFileDimensionDF = sparkSession.read
      .schema(fileDimensionSchema)
      .parquet(FILE_DIMENSION_PATH)

    val filesFromStagingDF = transform(eventPayloadStagingDF)
    val tempFileDimDF = loadApplyingSCD(filesFromStagingDF, currentFileDimensionDF, sparkSession)

    // optional
    tempFileDimDF.printSchema(3)
    tempFileDimDF.show(10)

    // Write temporal dimension
    tempFileDimDF.write
      .mode(SaveMode.Overwrite)
      .parquet(FILE_DIMENSION_TEMP_PATH)

    // Move temporal dimension to the final dimension
    sparkSession.read
      .parquet(FILE_DIMENSION_TEMP_PATH)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(FILE_DIMENSION_PATH)

    tempFileDimDF
  }

}
