package presentation

import org.apache.spark.sql.functions.{
  col,
  lit,
  monotonically_increasing_id,
  when
}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BranchDimensionETL {

  def getDataFrameBranchHead(
      stagingPullRequestDF: DataFrame
  ): DataFrame = {
    val branchDimension = stagingPullRequestDF
      .withColumn("protected_branch", lit("Not available"))
      .withColumn("disabled_repo", lit("Not available"))
      .select(
        col("pull_request_head_ref").as("branch_name"),
        col("pull_request_head_sha").as("branch_sha"),
        col("protected_branch"),
        col("pull_request_head_repo_full_name").as("full_name_repo"),
        when(col("pull_request_head_repo_description").isNull, "Description not available")
          .otherwise(col("pull_request_head_repo_description")).as("description_repo"),
        col("pull_request_head_repo_default_branch").as("default_branch_repo"),
        when(col("pull_request_head_repo_language").isNull, "Languge not available")
          .otherwise(col("pull_request_head_repo_language"))
          .as("language_repo"),
        when(col("pull_request_head_repo_license").isNull, "License not available")
          .otherwise(col("pull_request_head_repo_license.name"))
          .as("license_repo"),
        when(col("pull_request_head_repo_fork") === true, "Has been forked")
          .otherwise("Has not been forked")
          .as("is_forked_repo"),
        when(col("pull_request_head_repo_archived") === true, "Is archived")
          .otherwise("Is not archived")
          .as("archived_repo"),
        when(col("pull_request_head_repo_private") === true, "Private repository")
          .otherwise("Public repository")
          .as("private_repo"),
        when(col("pull_request_head_repo_size").isNull, -1)
          .otherwise(col("pull_request_head_repo_size"))
          .as("size_repo"),
        col("disabled_repo"),
        col("pull_request_head_repo_open_issues").as("open_issues_repo"),
        col("pull_request_head_repo_forks").as("forks_repo"),
        col("pull_request_head_repo_id").as("repo_id"),
        col("pull_request_head_repo_stargazers_count").as("stargazer_count_repo"),
        col("pull_request_head_repo_watchers_count").as("watchers_count_repo"),
        col("pull_request_head_repo_pushed_at").as("pushed_at")
      )

    val branchDimension2 = branchDimension.dropDuplicates("repo_id")
    branchDimension2
  }

  def getDataFrameBranchBase(
                              stagingPullRequestDF: DataFrame
                            ): DataFrame = {

    val branchDimension = stagingPullRequestDF
      .withColumn("protected_branch", lit("Not available"))
      .withColumn("disabled_repo", lit("Not available"))
      .select(
        col("pull_request_base_ref").as("branch_name"),
        col("pull_request_base_sha").as("branch_sha"),
        col("protected_branch"),
        col("pull_request_base_repo_full_name").as("full_name_repo"),
        when(col("pull_request_base_repo_description").isNull, "Description not available")
          .otherwise(col("pull_request_base_repo_description")).as("description_repo"),
        col("pull_request_base_repo_default_branch").as("default_branch_repo"),
        when(col("pull_request_base_repo_language").isNull, "Languge not available")
          .otherwise(col("pull_request_base_repo_language"))
          .as("language_repo"),
        when(col("pull_request_base_repo_license").isNull, "License not available")
          .otherwise(col("pull_request_base_repo_license.name"))
          .as("license_repo"),
        when(col("pull_request_base_repo_fork") === true, "Has been forked")
          .otherwise("Has not been forked")
          .as("is_forked_repo"),
        when(col("pull_request_base_repo_archived") === true, "Is archived")
          .otherwise("Is not archived")
          .as("archived_repo"),
        when(col("pull_request_base_repo_private") === true, "Private repository")
          .otherwise("Public repository")
          .as("private_repo"),
        when(col("pull_request_base_repo_size").isNull, -1)
          .otherwise(col("pull_request_base_repo_size"))
          .as("size_repo"),
        col("disabled_repo"),
        col("pull_request_base_repo_open_issues").as("open_issues_repo"),
        col("pull_request_base_repo_forks").as("forks_repo"),
        col("pull_request_base_repo_id").as("repo_id"),
        col("pull_request_base_repo_stargazers_count").as("stargazer_count_repo"),
        col("pull_request_base_repo_watchers_count").as("watchers_count_repo"),
        col("pull_request_base_repo_pushed_at").as("pushed_at")
      )

    val branchDimension2 = branchDimension.dropDuplicates("repo_id")
    branchDimension2

  }

  def getDataFrame(
      stagingPullRequestDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {

    val BranchHead =
      getDataFrameBranchHead(stagingPullRequestDF)

    val BranchBase =
      getDataFrameBranchBase(stagingPullRequestDF)

    val BranchUnion = BranchHead.unionByName(BranchBase)

    /*val BranchDF = BranchUnion
      .as("branch1")
      .join(BranchUnion.as("branch2"))
      .filter(
        (col("branch1.repo_id") === col("branch2.repo_id"))
          && (col("branch1.pushed_at").gt(col("branch2.pushed_at")) ||
            col("branch1.pushed_at") === col("branch2.pushed_at"))
      )
      .select("branch1.*")*/

    val branchUnique = BranchUnion
      .dropDuplicates("repo_id")
      .withColumn("pk_id", monotonically_increasing_id())

    val branchNUll = sparkSession
      .createDataFrame(NullDimension.BranchDataNull)
      .toDF(NullDimension.BranchColumnNull: _*)

    val branchUnionDF = branchUnique.unionByName(branchNUll)

   // branchUnionDF.printSchema(3)
   // branchUnionDF.show(10)

    branchUnionDF

  }

}
