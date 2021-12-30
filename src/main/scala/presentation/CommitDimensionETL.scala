package Presentation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object CommitDimensionETL extends App {
  // user defined functions
  val validateMessageUDF =
    udf((message: String) => {
      if (message.nonEmpty) {
        val isInitialCapitalLetter = message.charAt(0).isUpper
        val isGoodLength = message.length <= 80

        isInitialCapitalLetter && isGoodLength
      } else false
    })

  // configuration paths
  val eventPayloadStagingSource = "src/dataset/staging/events-payloads"
  val commitDimensionOutput = "src/dataset/presentation/commit-dimension"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val eventPayloadDF = spark.read.parquet(eventPayloadStagingSource)

  val commitDimensionDF = eventPayloadDF
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
    .withColumn("id", monotonically_increasing_id())
    .select("*")

  commitDimensionDF.printSchema(3)
  commitDimensionDF.show(10)

  commitDimensionDF.write
    .mode(SaveMode.Overwrite)
    .parquet(commitDimensionOutput)
}
