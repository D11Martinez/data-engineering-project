package presentation

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import presentation.CustomUDF.validateMessageUDF

object CommitDimensionETL {

  val commitDimensionPath = "src/dataset/presentation/commit-dimension"
  val commitDimensionTempPath = "src/dataset/presentation/temp/commit-dimension"

  val validateMessageUDF: UserDefinedFunction =
    udf((message: String) => {
      if (message.nonEmpty) {
        val isGoodLength = message.length <= 80

        isGoodLength
      } else false
    })

    val commitDimensionSchema: StructType = StructType(
 Array(
   StructField("pk_id", LongType, nullable=false),
   StructField("sha", StringType, nullable=false),
   StructField("message", StringType, nullable=false),
   StructField("message_with_good_practices", StringType, nullable=false),
   StructField("changes", StringType, nullable=false),
   StructField("additions", StringType, nullable=false),
   StructField("deletions", StringType, nullable=false),
   StructField("comment_count", StringType, nullable=false),
   StructField("pull_request_id", StringType, nullable=false),
   StructField("pull_request_number", StringType, nullable=false),
   StructField("pull_request_title", StringType, nullable=false),
   StructField("pull_request_body", StringType, nullable=false),
   StructField("pull_request_state", StringType, nullable=false),
   StructField("pull_request_locked", StringType, nullable=false),
   StructField("pull_request_merged", StringType, nullable=false),
   StructField("pull_request_merge_commit_sha", StringType, nullable=false),
   StructField("pull_request_author_association", StringType, nullable=false),
 ))

 def transform(eventPayloadStagingDF: DataFrame): DataFrame = {
  val commitDimensionDF = eventPayloadStagingDF
        .select(
          col("pull_request_commit_sha"),
          col("pull_request_commit_message"),
          col("pull_request_commit_total_changes"),
          col("pull_request_commit_total_additions"),
          col("pull_request_commit_total_deletions"),
          col("pull_request_commit_comment_count"),
          col("pull_request_id"),
          col("pull_request_number"),
          col("pull_request_title"),
          col("pull_request_body"),
          col("pull_request_state"),
          col("pull_request_locked"),
          col("pull_request_merged"),
          col("pull_request_merge_commit_sha"),
          col("pull_request_author_association")
        )
        .distinct()
        .withColumn(
          "sha",
          when(col("pull_request_commit_sha").isNull, "Not available").otherwise(
            col("pull_request_commit_sha")
          )
        )
        .withColumn(
          "message_with_good_practices",
          when(col("pull_request_commit_message").isNull, lit(null)).otherwise(
            validateMessageUDF(col("pull_request_commit_message"))
          )
        )
        .withColumn(
          "message",
          when(col("pull_request_commit_message").isNull, "Message not available")
            .otherwise(
              when(
                length(trim(col("pull_request_commit_message"))) === 0,
                "Empty value"
              )
                .otherwise(col("pull_request_commit_message"))
            )
        )
        .withColumn(
          "changes",
          when(col("pull_request_commit_total_changes").isNull, 0)
            .otherwise(col("pull_request_commit_total_changes"))
        )
        .withColumn(
          "additions",
          when(col("pull_request_commit_total_additions").isNull, 0)
            .otherwise(col("pull_request_commit_total_additions"))
        )
        .withColumn(
          "deletions",
          when(col("pull_request_commit_total_deletions").isNull, 0)
            .otherwise(col("pull_request_commit_total_deletions"))
        )
        .withColumn(
          "comment_count",
          when(col("pull_request_commit_comment_count").isNull, 0)
            .otherwise(col("pull_request_commit_comment_count"))
        )
        .select(
          col("sha"),
          col("message"),
          when(
            col("message_with_good_practices") === true,
            "Good message"
          )
            .otherwise("It could be better")
            .as("message_with_good_practices"),
          col("changes"),
          col("additions"),
          col("deletions"),
          col("comment_count"),
          when(col("pull_request_id").isNull, "Not available")
            .otherwise(col("pull_request_id"))
            .as("pull_request_id"),
          when(
            col("pull_request_number").isNull,
            "Pull request number not available"
          )
            .otherwise(col("pull_request_number"))
            .as("pull_request_number"),
          when(col("pull_request_title").isNull, "Not available")
            .otherwise(
              when(length(trim(col("pull_request_title"))) === 0, "Empty value")
                .otherwise(col("pull_request_title"))
            )
            .as("pull_request_title"),
          when(col("pull_request_body").isNull, "Not available")
            .otherwise(
              when(length(trim(col("pull_request_body"))) === 0, "Empty value")
                .otherwise(col("pull_request_body"))
            )
            .as("pull_request_body"),
          when(col("pull_request_state").isNull, "Not available")
            .otherwise(col("pull_request_state"))
            .as("pull_request_state"),
          when(col("pull_request_locked") === true, "Locked")
            .otherwise(
              when(col("pull_request_locked") === false, "Unlocked")
                .otherwise("Undefined")
            )
            .as("pull_request_locked"),
          when(col("pull_request_merged") === true, "Merged")
            .otherwise(
              when(col("pull_request_merged") === false, "Unmerged")
                .otherwise("Undefined")
            )
            .as("pull_request_merged"),
          when(col("pull_request_merge_commit_sha").isNull, "Not available")
            .otherwise(col("pull_request_merge_commit_sha"))
            .as("pull_request_merge_commit_sha"),
          when(col("pull_request_author_association").isNull, "Not available")
            .otherwise(col("pull_request_author_association"))
            .as("pull_request_author_association")
        )
        .distinct()
        .select("*")
  
  commitDimensionDF
}

  def loadApplyingSCD(
      dataFromStagingDF: DataFrame,
      currentDimensionDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentCommitDimensionDF = currentDimensionDF
    val commitsFromStagingDF = dataFromStagingDF

    val tempCommitDimDF = if (currentCommitDimensionDF.isEmpty) {
      // reading the null row
      val commitUndefinedRowDF = sparkSession
        .createDataFrame(NullDimension.commitDataNull)
        .toDF(NullDimension.commitColumnNull: _*)

      val commitDimWithUndefinedRowDF = commitsFromStagingDF
        .withColumn("pk_id", monotonically_increasing_id())
        .unionByName(commitUndefinedRowDF)

      val commitDimDF = commitDimWithUndefinedRowDF

      commitDimDF
    } else {
      val newCommitsDF = commitsFromStagingDF
        .join(
          currentCommitDimensionDF,
          commitsFromStagingDF("sha") === commitsFromStagingDF("sha"), // Joining by natural Key
          "left_anti"
        )
        .withColumn("pk_id", monotonically_increasing_id())

      val editedCommitsDF = currentCommitDimensionDF
        .as("current")
        .join(
          commitsFromStagingDF.as("staging"),
          commitsFromStagingDF("sha") === currentCommitDimensionDF("sha"), // Joining by natural Key
          "inner"
        )
        .select(
          // applying SCD 0
          col("current.pk_id"),
          col("current.sha"),
          col("staging.message"),
          col("staging.message_with_good_practices"),
          col("staging.changes"),
          col("staging.additions"),
          col("staging.deletions"),
          col("staging.comment_count"),
          col("current.pull_request_id"),
          col("current.pull_request_number"),
          col("staging.pull_request_title"),
          col("staging.pull_request_body"),
          col("staging.pull_request_state"),
          col("staging.pull_request_locked"),
          col("staging.pull_request_merged"),
          col("staging.pull_request_merge_commit_sha"),
          col("staging.pull_request_author_association"),
          
        )

      val notUpdatedCommitsDF = currentCommitDimensionDF.join(
        commitsFromStagingDF,
        commitsFromStagingDF("sha") === currentCommitDimensionDF("sha"), // Joining by natural Key
        "left_anti"
      )

      // Union of all dimension pieces
      val updatedCommitDimDF = newCommitsDF
        .unionByName(editedCommitsDF)
        .unionByName(notUpdatedCommitsDF)

      updatedCommitDimDF
    }

    // Returning the final dimension as temporal
    tempCommitDimDF
  }

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): Unit = {
    
    val currentCommitDimensionDF = sparkSession.read
          .schema(commitDimensionSchema)
          .parquet(commitDimensionPath)

    //val stagingCommitDF = spark.read.parquet(eventsPayloadStagingPath)

    val commitsFromStagingDF = transform(eventPayloadStagingDF)
    val tempCommitDimDF = loadApplyingSCD(commitsFromStagingDF, currentCommitDimensionDF, sparkSession)

    // Write temporal dimension
    tempCommitDimDF.write
      .mode(SaveMode.Overwrite)
      .parquet(commitDimensionTempPath)

    // Move temporal dimension to the final dimension
    sparkSession.read
      .parquet(commitDimensionTempPath)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(commitDimensionPath)
  }
}
