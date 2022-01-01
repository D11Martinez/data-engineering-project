package presentation

import org.apache.spark.sql.functions.{
  col,
  date_format,
  monotonically_increasing_id,
  when
}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PullRequestFactETL {

  val userPresentationOutput = "src/dataset/presentation/users-dimension"
  val orgPresentationOutput = "src/dataset/presentation/organizations-dimension"
  val pullRequestPresentationOutput =
    "src/dataset/presentation/pullrequest-dimension"
  val branchPresentationOutput = "src/dataset/presentation/branch-dimension"

  def getDataFrameBranch(
      stagingPullRequestDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {

    val userDim = sparkSession.read.parquet(userPresentationOutput)
    val orgDim = sparkSession.read.parquet(orgPresentationOutput)
    val branchDim = sparkSession.read.parquet(branchPresentationOutput)
    val pullRequestDim =
      sparkSession.read.parquet(pullRequestPresentationOutput)

    val pullRequestStaging = stagingPullRequestDF
      .dropDuplicates("id")
      .withColumn(
        "assignee_group_id",
        when(col("pull_request_assignees_item").isNull, -1)
          .otherwise(col("pull_request_id"))
      )
      .withColumn(
        "reviewers_group_id",
        when(col("pull_request_requested_reviewer").isNull, -1)
          .otherwise(col("pull_request_id"))
      )
      .select(
        col("pull_request_id"),
        col("pull_request_head.repo.id").as("base_branch_id"),
        col("pull_request_base.repo.id").as("head_branch_id"),
        col("actor_id"),
        col("pull_request_merged_by_id").as("merged_id"),
        col("pull_request_user_id").as("user_id"),
        col("pull_request_head.repo.owner.id").as("owner_repo_id"),
        col("org_id").as("organization_id"),
        date_format(col("create_at_date_temporal"), "yyyyMMdd")
          .as("created_at_date"),
        date_format(col("create_at_time_temporal"), "HHmmss")
          .as("created_at_time"),
        col("pull_request_state").as("state"),
        col("pull_request_additions").as("additions"),
        col("pull_request_deletions").as("deletions"),
        col("pull_request_changed_files").as("changed_files"),
        col("pull_request_commits").as("commits"),
        col("pull_request_comments").as("comments"),
        col("pull_request_review_comments").as("review_comments"),
        when(col("pull_request_updated_at").isNull, "Not available")
          .otherwise(date_format(col("pull_request_updated_at"), "yyyy-MM-dd"))
          .as("updated_at_date"),
        when(col("pull_request_closed_at").isNull, "Not available")
          .otherwise(date_format(col("pull_request_closed_at"), "yyyy-MM-dd"))
          .as("closed_at_date"),
        when(col("pull_request_merged_at").isNull, "Not available")
          .otherwise(date_format(col("pull_request_merged_at"), "yyyy-MM-dd"))
          .as("merged_at_date"),
        when(col("pull_request_updated_at").isNull, "Not available")
          .otherwise(date_format(col("pull_request_updated_at"), "HH:mm:ss"))
          .as("updated_at_time"),
        when(col("pull_request_closed_at").isNull, "Not available")
          .otherwise(date_format(col("pull_request_closed_at"), "HH:mm:ss"))
          .as("closed_at_time"),
        when(col("pull_request_merged_at").isNull, "Not available")
          .otherwise(date_format(col("pull_request_merged_at"), "HH:mm:ss"))
          .as("merged_at_time"),
        when(col("pull_request_merged") === true, "Is merged")
          .otherwise("Is not merged")
          .as("merged"),
        when(col("pull_request_mergeable") === true, "Is mergeable")
          .otherwise(
            when(col("pull_request_mergeable") === false, "Is not mergeable")
              .otherwise("Not available")
          )
          .as("mergeable"),
        when(col("pull_request_merge_commit_sha").isNull, "Not available")
          .otherwise(col("pull_request_merge_commit_sha"))
          .as("merge_commit_sha"),
        col("pull_request_author_association").as("author_association"),
        col("assignee_group_id"),
        col("reviewers_group_id")
      )

    println("cantidad rows staging:" + pullRequestStaging.count())

    val pullRequestFact1 = pullRequestStaging
      .as("stagingPullFact")
      .join(
        pullRequestDim.as("pullRequestDim"),
        pullRequestStaging("pull_request_id") === pullRequestDim(
          "pull_request_id"
        ),
        "left"
      )
      .withColumn("pk_id_pull", col("pullRequestDim.pk_id"))
      .select("stagingPullFact.*", "pk_id_pull")

    println("cantidad rows pullRequestFact1:" + pullRequestFact1.count())

    val pullRequestFact2 = pullRequestFact1
      .as("pullRequestFact1")
      .join(
        branchDim.as("branchHeadDim"),
        pullRequestFact1("head_branch_id") === branchDim("repo_id"),
        "left"
      )
      .withColumn("pk_id_branch_head", col("branchHeadDim.pk_id"))
      .select("pullRequestFact1.*", "pk_id_branch_head")

    println("cantidad rows pullRequestFact2:" + pullRequestFact2.count())

    val pullRequestFact3 = pullRequestFact2
      .as("pullRequestFact2")
      .join(
        branchDim.as("branchBaseDim"),
        pullRequestFact2("base_branch_id") === branchDim("repo_id"),
        "left"
      )
      .withColumn("pk_id_branch_base", col("branchBaseDim.pk_id"))
      .select("pullRequestFact2.*", "pk_id_branch_base")

    println("cantidad rows pullRequestFact3:" + pullRequestFact3.count())

    val pullRequestFact4 = pullRequestFact3
      .as("pullRequestFact3")
      .join(
        userDim.as("userActor"),
        pullRequestFact3("actor_id") === userDim("user_id"),
        "left"
      )
      .withColumn("pk_id_actor", col("userActor.pk_id"))
      .select("pullRequestFact3.*", "pk_id_actor")

    println("cantidad rows pullRequestFact4:" + pullRequestFact4.count())

    val pullRequestFact5 = pullRequestFact4
      .as("pullRequestFact4")
      .join(
        userDim.as("userMerged"),
        pullRequestFact4("merged_id") === userDim("user_id"),
        "left"
      )
      .withColumn("pk_id_merged", col("userMerged.pk_id"))
      .select("pullRequestFact4.*", "pk_id_merged")

    println("cantidad rows pullRequestFact5:" + pullRequestFact5.count())

    val pullRequestFact6 = pullRequestFact5
      .as("pullRequestFact5")
      .join(
        userDim.as("user"),
        pullRequestFact5("user_id") === userDim("user_id"),
        "left"
      )
      .withColumn("pk_id_user", col("user.pk_id"))
      .select("pullRequestFact5.*", "pk_id_user")

    println("cantidad rows pullRequestFact6:" + pullRequestFact6.count())

    val pullRequestFact7 = pullRequestFact6
      .as("pullRequestFact6")
      .join(
        userDim.as("userOwner"),
        pullRequestFact6("user_id") === userDim("user_id"),
        "left"
      )
      .withColumn("pk_id_owner", col("userOwner.pk_id"))
      .select("pullRequestFact6.*", "pk_id_owner")

    println("cantidad rows pullRequestFact7:" + pullRequestFact7.count())

    val pullRequestFact8 = pullRequestFact7
      .as("pullRequestFact7")
      .join(
        orgDim.as("org"),
        pullRequestFact7("organization_id") === orgDim("organization_id"),
        "left"
      )
      .withColumn("pk_id_org", col("org.pk_id"))
      .select("pullRequestFact7.*", "pk_id_org")

    println("cantidad rows pullRequestFact8:" + pullRequestFact8.count())

    val finalPullRequestFactDF = pullRequestFact8
      .drop(
        "pull_request_id",
        "base_branch_id",
        "head_branch_id",
        "actor_id",
        "merged_id",
        "user_id",
        "owner_repo_id",
        "organization_id"
      )
      .withColumnRenamed("pk_id_pull", "pull_request_id")
      .withColumnRenamed("pk_id_branch_head", "head_branch")
      .withColumnRenamed("pk_id_branch_base", "base_branch")
      .withColumnRenamed("pk_id_actor", "actor_id")
      .withColumnRenamed("pk_id_merged", "merged_by_id")
      .withColumnRenamed("pk_id_user", "user_id")
      .withColumnRenamed("pk_id_owner", "owner_repo")
      .withColumnRenamed("pk_id_org", "organization_id")
      .withColumn("pk_id", monotonically_increasing_id())
      .na
      .fill(-1)

    finalPullRequestFactDF.printSchema(3)
    finalPullRequestFactDF.show(10)

    finalPullRequestFactDF
  }

}
