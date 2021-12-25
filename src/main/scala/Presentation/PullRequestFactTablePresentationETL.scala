package Presentation

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, explode, lit, monotonically_increasing_id, when}

object PullRequestFactTablePresentationETL {

  val userPresentationOutput = "src/dataset/presentation/usersDimension"
  val orgPresentationOutput = "src/dataset/presentation/organizationsDimension"
  val pullRequestPresentationOutput = "src/dataset/presentation/PullRequestDimension"
  val branchPresentationOutput = "src/dataset/presentation/BranchDimension"
  val reviewersPresentationOutput = "src/dataset/presentation/ReviewersGroupDimension"
  val asigneesPresentationOutput = "src/dataset/presentation/AsigneesGroupDimension"
  val timePresentationOutput = "src/dataset/presentation/TimeDimension"
  val datePresentationOutput = "src/dataset/presentation/DateDimension"

  def getDataFrameBranch(stagingPullRequestDF: DataFrame,sparkSession:SparkSession):DataFrame={

    val userDim = sparkSession.read.parquet(userPresentationOutput)
    val orgDim = sparkSession.read.parquet(orgPresentationOutput)
    val branchDim = sparkSession.read.parquet(branchPresentationOutput)
    val timeDim = sparkSession.read.parquet(timePresentationOutput)
    val dateDim =  sparkSession.read.parquet(datePresentationOutput)
    val pullRequestDim =  sparkSession.read.parquet(pullRequestPresentationOutput)

    val pullRequesStaging = stagingPullRequestDF
      .dropDuplicates("id")

      .select(
        col("pull_request_id"),
        col("pull_request_head.repo.id").as("base_branch_id"),
        col("pull_request_base.repo.id").as("head_branch_id"),
        col("actor_id"),
        col("pull_request_merged_by_id").as("merged_id"),
        col("pull_request_user_id").as("user_id"),
        col("pull_request_head.repo.owner.id").as("owner_repo_id"),
        col("org_id").as("organization_id"),
        col("create_at_date_temporal").as("created_at_date"),
        col("create_at_time_temporal").as("created_at_time"),
        col("pull_request_state").as("state"),
        col("pull_request_additions").as("additions"),
        col("pull_request_deletions").as("deletions"),
        col("pull_request_changed_files").as("changed_files"),
        col("pull_request_commits").as("commits"),
        col("pull_request_comments").as("comments"),
        col("pull_request_review_comments").as("review_comments"),
        date_format(col("pull_request_updated_at"),"yyyy-MM-dd").as("updated_at_date"),
        date_format(col("pull_request_closed_at"),"yyyy-MM-dd").as("closed_at_date"),
        date_format(col("pull_request_merged_at"),"yyyy-MM-dd").as("merged_at_date"),
        date_format(col("pull_request_updated_at"),"HH:mm:ss").as("updated_at_time"),
        date_format(col("pull_request_closed_at"),"HH:mm:ss").as("closed_at_time"),
        date_format(col("pull_request_merged_at"),"HH:mm:ss").as("merged_at_time"),
        when(col("pull_request_merged") === true, "Is merged").otherwise("Is not merged").as("merged"),
        when(col("pull_request_mergeable") === true, "Is mergeable")
          .otherwise(when(col("pull_request_mergeable") === false, "Is not mergeable").otherwise("Not available"))
          .as("mergeable"),
        col("pull_request_merge_commit_sha").as("merge_commit_sha"),
        col("pull_request_author_association").as("author_association"),
        col("pull_request_id").as("assignee_group_id"),
        col("pull_request_id").as("reviewers_group_id")
      )


    val pullRequestFact1 = pullRequesStaging.as("stagingPullFact")
      .join(pullRequestDim.as("pullRequestDim"),pullRequesStaging("pull_request_id")===pullRequestDim("pull_request_id"),"inner")
      .withColumn("pk_id_pull",col("pullRequestDim.pk_id"))
      .select("stagingPullFact.*","pk_id_pull")

    val pullRequestFact2 = pullRequestFact1.as("pullRequestFact1")
      .join(branchDim.as("branchHeadDim"),pullRequestFact1("head_branch_id")===branchDim("repo_id"),"inner")
      .withColumn("pk_id_branch_head",col("branchHeadDim.pk_id"))
      .select("pullRequestFact1.*","pk_id_branch_head")

    val pullRequestFact3 = pullRequestFact2.as("pullRequestFact2")
      .join(branchDim.as("branchBaseDim"),pullRequestFact2("base_branch_id")===branchDim("repo_id"),"inner")
      .withColumn("pk_id_branch_base",col("branchBaseDim.pk_id"))
      .select("pullRequestFact2.*","pk_id_branch_base")

    val pullRequestFact4 = pullRequestFact3.as("pullRequestFact3")
      .join(userDim.as("userActor"),pullRequestFact3("actor_id")===userDim("user_id"),"inner")
      .withColumn("pk_id_actor",col("userActor.pk_id"))
      .select("pullRequestFact3.*","pk_id_actor")

    val pullRequestFact5 = pullRequestFact4.as("pullRequestFact4")
      .join(userDim.as("userMerged"),pullRequestFact4("merged_id")===userDim("user_id"),"inner")
      .withColumn("pk_id_merged",col("userMerged.pk_id"))
      .select("pullRequestFact4.*","pk_id_merged")

    val pullRequestFact6 = pullRequestFact5.as("pullRequestFact5")
      .join(userDim.as("user"),pullRequestFact5("user_id")===userDim("user_id"),"inner")
      .withColumn("pk_id_user",col("user.pk_id"))
      .select("pullRequestFact5.*","pk_id_user")

    val pullRequestFact7 = pullRequestFact6.as("pullRequestFact6")
      .join(userDim.as("userOwner"),pullRequestFact6("user_id")===userDim("user_id"),"inner")
      .withColumn("pk_id_owner",col("userOwner.pk_id"))
      .select("pullRequestFact6.*","pk_id_owner")

    val pullRequestFact8 = pullRequestFact7.as("pullRequestFact7")
      .join(orgDim.as("org"),pullRequestFact7("organization_id")===orgDim("organization_id"),"inner")
      .withColumn("pk_id_org",col("org.pk_id"))
      .select("pullRequestFact7.*","pk_id_org")

    val pullRequestFact9 = pullRequestFact8.as("pullRequestFact8")
      .join(timeDim.as("time"),pullRequestFact8("created_at_time")===timeDim("time24h"),"inner")
      .withColumn("pk_id_time",col("time.time_key"))
      .select("pullRequestFact8.*","pk_id_time")

    val pullRequestFact10 = pullRequestFact9.as("pullRequestFact9")
      .join(dateDim.as("date"),pullRequestFact9("created_at_date")===dateDim("date"),"inner")
      .withColumn("pk_id_date",col("date.date_key"))
      .select("pullRequestFact9.*","pk_id_date")

    pullRequestFact10.drop(
      "pull_request_id",
      "base_branch_id",
      "head_branch_id",
      "actor_id",
      "merged_id",
      "user_id",
      "owner_repo_id",
      "organization_id",
      "created_at_date",
      "created_at_time"
    ).withColumnRenamed("pk_id_pull","pull_request_id")
      .withColumnRenamed("pk_id_branch_head","head_branch")
      .withColumnRenamed("pk_id_branch_base","base_branch")
      .withColumnRenamed("pk_id_actor","actor_id")
      .withColumnRenamed("pk_id_merged","merged_by_id")
      .withColumnRenamed("pk_id_user","user_id")
      .withColumnRenamed("pk_id_owner","owner_repo")
      .withColumnRenamed("pk_id_org","organization_id")
      .withColumnRenamed("pk_id_time","created_at_time")
      .withColumnRenamed("pk_id_date","created_at_date")

  }


}
