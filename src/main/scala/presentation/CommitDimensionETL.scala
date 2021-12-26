package presentation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object CommitDimensionETL extends App {
  // user defined functions
  val uuid = udf(() => java.util.UUID.randomUUID().toString)

  val validateMessageUDF =
    udf((message: String) => {
      if (message.nonEmpty) {
        val isInitialCapitalLetter = message.charAt(0).isUpper
        val isGoodLength = message.length <= 80

        isInitialCapitalLetter && isGoodLength
      } else false
    })

  // configuration paths
  val commitStagingSource = "src/dataset/staging/commits"
  val eventPayloadStagingSource = "src/dataset/staging/events-payloads"
  val commitDimensionOutput = "src/dataset/presentation/commit-dimension"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val commitStagingDF =
    spark.read.parquet(commitStagingSource)
  val eventPayloadDF = spark.read.parquet(eventPayloadStagingSource)

  val commitFieldsDF = commitStagingDF
    .select(
      col("sha"),
      col("message"),
      col("total_changes"),
      col("total_additions"),
      col("total_deletions"),
      col("comment_count")
    )
    .distinct()
    .withColumn(
      "message_with_good_practices",
      validateMessageUDF(col("message"))
    )
    .select(
      col("sha"),
      when(col("message").isNull, "Message not available")
        .otherwise(
          when(length(trim(col("message"))) === 0, "Empty value")
            .otherwise(col("message"))
        )
        .as("message"),
      when(
        col("message_with_good_practices") === true,
        "Good message"
      )
        .otherwise("It could be better")
        .as("message_with_good_practices"),
      when(col("total_changes").isNull, 0)
        .otherwise(col("total_changes"))
        .as("changes"),
      when(col("total_additions").isNull, 0)
        .otherwise(col("total_additions"))
        .as("additions"),
      when(col("total_deletions").isNull, 0)
        .otherwise(col("total_deletions"))
        .as("deletions"),
      when(col("comment_count").isNull, 0)
        .otherwise(col("comment_count"))
        .as("comment_count")
    )

  val commitDimensionDF = commitFieldsDF
    .join(
      eventPayloadDF,
      commitFieldsDF("sha") === eventPayloadDF("pull_request_commit_sha"),
      "inner"
    )
    .select(
      col("sha"),
      col("message"),
      col("message_with_good_practices"),
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
    .withColumn("id", uuid())
    .select("*")

  commitDimensionDF.printSchema(3)
  commitDimensionDF.show(10)

  commitDimensionDF.write
    .mode(SaveMode.Overwrite)
    .parquet(commitDimensionOutput)
}
