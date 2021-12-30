package Presentation

import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PullRequestDimensionETL {

  def getDataFrame(
      stagingPullRequestDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {

    val pullRequestDimensionDF = stagingPullRequestDF
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

    pullRequestUnionDF
  }
}
