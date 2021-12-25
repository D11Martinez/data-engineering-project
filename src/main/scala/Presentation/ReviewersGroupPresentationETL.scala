package Presentation

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, monotonically_increasing_id, when}

object ReviewersGroupPresentationETL {

  def getDataFrame(stagingPullRequestDF: DataFrame,sparkSession: SparkSession):DataFrame={

    val reviewersDF = stagingPullRequestDF
      .filter(col("pull_request_assignees_id") =!= -1)
      .select(
        col("pull_request_id").as("reviewers_group_id"),
        col("pull_request_requested_reviewer_id").as("user_dim_id"))

    val ColumnNull = Seq("asignees_group_id","user_dim_id")
    val DataNull = Seq(("-1","Not available"))
    val reviewersNull = sparkSession.createDataFrame(DataNull).toDF(ColumnNull:_*)

    val reviewersUnion = reviewersDF.unionByName(reviewersNull)

    reviewersUnion

  }

}
