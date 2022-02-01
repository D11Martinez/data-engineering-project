package presentation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PullRequestDimensionETL {
  val PULL_REQUEST_DIMENSION_PATH = "src/dataset/presentation/pullrequest-dimension"
  val PULL_REQUEST_TEMP_PATH =
    "src/dataset/presentation/temp/pullrequest-dimension" // This route must exists

  val pullRequestDimensionSchema:StructType = StructType(
    Array(
      StructField("pk_id", LongType, nullable = false),
      StructField("pull_request_id", StringType, nullable = false),
      StructField("number", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("body", StringType, nullable = false),
      StructField("locked", StringType, nullable = false),
    )
  )

  // Transform the staging data to dimension format without pk_id
  def transform(eventPayloadStagingDF: DataFrame): DataFrame = {
      val pullRequestDimensionDF =
        eventPayloadStagingDF
          .dropDuplicates("pull_request_id")
          .select(
            col("pull_request_id"),
            col("pull_request_number").as("number"),
            col("pull_request_title").as("title"),
            when(col("pull_request_body").isNull, "No Pull Request body available")
              .otherwise(col("pull_request_body"))
              .as("body"),
            when(col("pull_request_locked") === true, "Is locked")
              .otherwise("Is not locked")
              .as("locked")
          )

    pullRequestDimensionDF
  }

  // load the incoming data from staging layer applying SCD conditions
  def loadApplyingSCD(
                       dataFromStagingDF: DataFrame,
                       currentDimensionDF: DataFrame,
                       sparkSession: SparkSession
                     ): DataFrame = {

    val currentPullRequestDimensionDF = currentDimensionDF
    val pullRequestFromStagingDF = dataFromStagingDF

    val tempPullRequestDimDF = if(currentPullRequestDimensionDF.isEmpty){
      // reading the null row
      val pullUnderFinedRowDF = sparkSession
        .createDataFrame(NullDimension.PullRequestDataNull)
        .toDF(NullDimension.PullRequestColumnNull: _*)

      val pullRequestDimWithUnderFinedRowDF = pullRequestFromStagingDF
        .withColumn("pk_id",monotonically_increasing_id())
        .unionByName(pullUnderFinedRowDF)

      val pullRequestDimDF = pullRequestDimWithUnderFinedRowDF

      pullRequestDimDF
    }else {
      val newPullRequestDF = pullRequestFromStagingDF
        .join(
          currentPullRequestDimensionDF,
          Seq("pull_request_id"),
          "left_anti"
        )
        .withColumn("pk_id",monotonically_increasing_id())

      val editedPullRequestDF = currentPullRequestDimensionDF
        .as("current")
        .join(
          pullRequestFromStagingDF.as("staging"),
          Seq("pull_request_id"),
          "inner"
        )
        .select(
          col("current.pk_id"),
          col("current.pull_request_id"),
          col("current.number"),
          col("current.title"),
          col("current.body"),
          col("current.locked")
        )

      val noUpdatedPullRequestDF = currentPullRequestDimensionDF.join(
        pullRequestFromStagingDF,
        Seq("pull_request_id"),
        "left_anti"
      )

      //Union of all dimension pieces
      val updatedPullRequestDimDF = newPullRequestDF
        .unionByName(editedPullRequestDF)
        .unionByName(noUpdatedPullRequestDF)

      updatedPullRequestDimDF
    }
    tempPullRequestDimDF
  }

  def getDataFrame(
      stagingPullRequestDF: DataFrame,
      sparkSession: SparkSession
  ): Unit = {
 val currentPullRequestDimensionDF = sparkSession.read
   .schema(pullRequestDimensionSchema)
   .parquet(PULL_REQUEST_DIMENSION_PATH)

    val pullRequestFromStagingDF = transform(stagingPullRequestDF)
    val tempPullRequestDimDF = loadApplyingSCD(pullRequestFromStagingDF,currentPullRequestDimensionDF,sparkSession)

    //write temporal dimension
     tempPullRequestDimDF.write
       .mode(SaveMode.Overwrite)
       .parquet(PULL_REQUEST_TEMP_PATH)

    //Move temporal dimension to the final dimension
    sparkSession.read
      .parquet(PULL_REQUEST_TEMP_PATH)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(PULL_REQUEST_DIMENSION_PATH)


   /* val pullRequestDimensionDF = stagingPullRequestDF
      .dropDuplicates("pull_request_id")
      .withColumn("pk_id", monotonically_increasing_id())
      .select(
        col("pk_id"),
        col("pull_request_id"),
        col("pull_request_number").as("number"),
        col("pull_request_title").as("title"),
        when(col("pull_request_body").isNull, "No Pull Request body available")
          .otherwise(col("pull_request_body"))
          .as("body"),
        when(col("pull_request_locked") === true, "Is locked")
          .otherwise("Is not locked")
          .as("locked")
      )

    val pullRequestNullDF = sparkSession
      .createDataFrame(NullDimension.PullRequestDataNull)
      .toDF(NullDimension.PullRequestColumnNull: _*)

    val pullRequestUnionDF =
      pullRequestDimensionDF.unionByName(pullRequestNullDF)

    pullRequestUnionDF.printSchema(3)
    pullRequestUnionDF.show(10)

    pullRequestUnionDF*/
  }
}
