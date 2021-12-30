package Presentation

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object AssigneesGroupBridgeETL {
  val userPresentationOutput = "src/dataset/presentation/users-dimension"

  def getDataFrame(
      stagingPullRequestDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {

    val userDim = sparkSession.read.parquet(userPresentationOutput)

    val asigneesDF = stagingPullRequestDF
      .filter(col("pull_request_assignees_item").isNotNull)
      .select(
        col("pull_request_id").as("asignees_group_id"),
        col("pull_request_assignees_item.id").as("user_dim_id")
      )

    val asigneesDF2 = asigneesDF
      .as("asignees")
      .join(
        userDim.as("user"),
        asigneesDF("user_dim_id") === userDim("user_id"),
        "inner"
      )
      .select(
        col("asignees.asignees_group_id"),
        col("user.pk_id").as("user_dim_id")
      )

    val ColumnNull = Seq("asignees_group_id", "user_dim_id")
    val DataNull = Seq((-1, -1))
    val asigneesNull =
      sparkSession.createDataFrame(DataNull).toDF(ColumnNull: _*)

    val asigneesUnion = asigneesDF2.unionByName(asigneesNull)

    asigneesUnion

  }

}
