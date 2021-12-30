package Presentation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CommitDimensionETL {
  // user defined functions
  val validateMessageUDF =
    udf((message: String) => {
      if (message.nonEmpty) {
        val isGoodLength = message.length <= 80

        isGoodLength
      } else false
    })

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
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
        "message_with_good_practices",
        validateMessageUDF(col("pull_request_commit_message"))
      )
      .select(
        col("pull_request_commit_sha").as("sha"),
        when(col("pull_request_commit_message").isNull, "Message not available")
          .otherwise(
            when(
              length(trim(col("pull_request_commit_message"))) === 0,
              "Empty value"
            )
              .otherwise(col("pull_request_commit_message"))
          )
          .as("message"),
        when(
          col("message_with_good_practices") === true,
          "Good message"
        )
          .otherwise("It could be better")
          .as("message_with_good_practices"),
        when(col("pull_request_commit_total_changes").isNull, 0)
          .otherwise(col("pull_request_commit_total_changes"))
          .as("changes"),
        when(col("pull_request_commit_total_additions").isNull, 0)
          .otherwise(col("pull_request_commit_total_additions"))
          .as("additions"),
        when(col("pull_request_commit_total_deletions").isNull, 0)
          .otherwise(col("pull_request_commit_total_deletions"))
          .as("deletions"),
        when(col("pull_request_commit_comment_count").isNull, 0)
          .otherwise(col("pull_request_commit_comment_count"))
          .as("comment_count"),
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
      .withColumn("pk_id", monotonically_increasing_id())
      .select("*")

    val commitUndefinedRowDF = sparkSession
      .createDataFrame(NullDimension.commitDataNull)
      .toDF(NullDimension.commitColumnNull: _*)

    val commitDimensionWithUndefinedRowDF =
      commitDimensionDF.unionByName(commitUndefinedRowDF)

    commitDimensionWithUndefinedRowDF.printSchema(3)
    commitDimensionWithUndefinedRowDF.show(10)

    commitDimensionWithUndefinedRowDF
  }
}
