package Presentation

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, monotonically_increasing_id, when}

object ReviewersGroupBridgeETL {

  val userPresentationOutput = "src/dataset/presentation/users-dimension"

  def getDataFrame(stagingPullRequestDF: DataFrame,sparkSession: SparkSession):DataFrame={

    val userDim = sparkSession.read.parquet(userPresentationOutput)

    val reviewersDF = stagingPullRequestDF
      .filter(col("pull_request_requested_reviewer").isNotNull)
      .select(
        col("pull_request_id").as("reviewers_group_id"),
        col("pull_request_requested_reviewer.id").as("user_dim_id"))

    val reviewersDF2 = reviewersDF.as("reviewers")
      .join(userDim.as("user"),reviewersDF("user_dim_id")===userDim("user_id"),"inner")
      .select(col("reviewers.reviewers_group_id"),col("user.pk_id").as("user_dim_id"))

    val ColumnNull = Seq("reviewers_group_id","user_dim_id")
    val DataNull = Seq((-1,-1))
    val reviewersNull = sparkSession.createDataFrame(DataNull).toDF(ColumnNull:_*)

    val reviewersUnion = reviewersDF2.unionByName(reviewersNull)

    reviewersUnion

  }

}
