package Presentation

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, monotonically_increasing_id, when}

object BranchPresentationETL {


  def getDataFrameBranch(stagingPullRequestDF: DataFrame,columna:String):DataFrame={
    val branchDimension = stagingPullRequestDF
     // .dropDuplicates("pull_request_id")
      .withColumn("branch",col(columna))
      .withColumn("protected_branch",lit("Not available"))
      .withColumn("description_repo",col("branch.description"))
      .withColumn("language_repo",col("branch.language"))
      .withColumn("license_repo",col("branch.license"))
      .withColumn("is_forked_repo",col("branch.fork"))
      .withColumn("archived_repo",col("branch.archived"))
      .withColumn("private_repo",col("branch.private"))
      .withColumn("size_repo",col("branch.size"))
      .withColumn("disabled_repo",lit("Not available"))

      .select(
        col("branch.name").as("branch_name"),
        col("protected_branch"),
        col("branch.full_name").as("full_name_repo"),
        when(col("description_repo").isNull,"Description not available").otherwise(col("description_repo")).as("description_repo"),
        col("branch.default_branch").as("default_branch_repo"),
        when(col("language_repo").isNull,"Languge not available").otherwise(col("language_repo")).as("language_repo"),
        when(col("license_repo").isNull,"License not available").otherwise(col("license_repo.name")).as("license_repo"),
        when(col("is_forked_repo")=== true,"Has been forked").otherwise("Has not been forked").as("is_forked_repo"),
        when(col("archived_repo")===true,"Is archived").otherwise("Is not archived").as("archived_repo"),
        when(col("private_repo")===true,"Private repository").otherwise("Public repository").as("private_repo"),
        when(col("size_repo").isNull,-1).otherwise(col("size_repo")).as("size_repo"),
        col("disabled_repo"),
        col("branch.open_issues").as("open_issues_repo"),
        col("branch.forks").as("forks_repo"),
        col("branch.id").as("repo_id"),
        col("branch.stargazers_count").as("stargazer_count_repo"),
        col("branch.watchers_count").as("watchers_count_repo"),
        col("branch.pushed_at").as("pushed_at"),
      )

    branchDimension
  }

  def getDataFrame(stagingPullRequestDF: DataFrame):DataFrame={

    val BranchHead = getDataFrameBranch(stagingPullRequestDF,"pull_request_head.repo")

    val BranchBase = getDataFrameBranch(stagingPullRequestDF,"pull_request_base.repo")

    val BrancUnion = BranchHead.unionByName(BranchBase)

    val BranchDF = BrancUnion.as("branch1").join(BrancUnion.as("branch2"))
      .filter((col("branch1.repo_id")===col("branch2.repo_id"))
        &&(col("branch1.pushed_at").gt(col("branch2.pushed_at")) ||
        col("branch1.pushed_at") === col("branch2.pushed_at")))
      .select("branch1.*")

    val branchUnique = BranchDF.dropDuplicates("repo_id").withColumn("pk_id", monotonically_increasing_id())

    branchUnique

  }

}
