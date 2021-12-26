package Presentation

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, monotonically_increasing_id, when}

object PullRequestPresentationETL {

  def getDataFrame(stagingPullRequestDF: DataFrame):DataFrame={

    val pullRquestDimension = stagingPullRequestDF
      .dropDuplicates("pull_request_id")
      .withColumn("pk_id", monotonically_increasing_id())
      .select(
        col("pk_id"),
        col("pull_request_id"),
        col("pull_request_number").as("number"),
        col("pull_request_title").as("title"),
        when(col("pull_request_body").isNull, "No Pull Request body available")
          .otherwise(col("pull_request_body")).as("body"),
        when(col("pull_request_locked") === true, "Is locked").otherwise("Is not locked").as("locked")
      )

    pullRquestDimension
  }
}
