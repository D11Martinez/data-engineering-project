package staging

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, when}
import org.apache.spark.sql.types.{
  BooleanType,
  LongType,
  StringType,
  StructField,
  StructType
}

object UserStagingETL {
  val temporalActorsOutput = "src/dataset/staging/temp/actors"
  val temporalOrganizationsOutput = "src/dataset/staging/temp/organizations"
  val temporalOwnersOutput = "src/dataset/staging/temp/owners"
  val temporalPRUsersOutput = "src/dataset/staging/temp/pr-users"
  val temporalMergedByOutput = "src/dataset/staging/temp/merged_by"
  val temporalPRAssigneeOutput = "src/dataset/staging/temp/pr-assignee"
  val temporalPRAssigneesOutput = "src/dataset/staging/temp/pr-assignees"
  val temporalPRReviewersOutput = "src/dataset/staging/temp/reviewers"
  val temporalCommitAuthorOutput = "src/dataset/staging/temp/commit-authors"
  val temporalCommitCommitterOutput =
    "src/dataset/staging/temp/commit-committers"

  val userStagingSchema: StructType = StructType(
    Array(
      StructField("id", LongType, true),
      StructField("login", StringType, true),
      StructField("node_id", StringType, true),
      StructField("avatar_url", StringType, true),
      StructField("gravatar_id", StringType, true),
      StructField("url", StringType, true),
      StructField("html_url", StringType, true),
      StructField("followers_url", StringType, true),
      StructField("following_url", StringType, true),
      StructField("gists_url", StringType, true),
      StructField("starred_url", StringType, true),
      StructField("subscriptions_url", StringType, true),
      StructField("organizations_url", StringType, true),
      StructField("repos_url", StringType, true),
      StructField("events_url", StringType, true),
      StructField("received_events_url", StringType, true),
      StructField("type", StringType, true),
      StructField("site_admin", BooleanType, true),
      StructField("name", StringType, true),
      StructField("company", StringType, true),
      StructField("blog", StringType, true),
      StructField("location", StringType, true),
      StructField("email", StringType, true),
      StructField("hireable", BooleanType, true),
      StructField("bio", StringType, true),
      StructField("twitter_username", StringType, true),
      StructField("public_repos", LongType, true),
      StructField("public_gists", LongType, true),
      StructField("followers", LongType, true),
      StructField("following", LongType, true),
      StructField("created_at", StringType, true),
      StructField("updated_at", StringType, true)
    )
  )
  def parseToUser(
      dataFrame: DataFrame,
      sparkSession: SparkSession,
      temporalDataOutput: String
  ): DataFrame = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .parquet(temporalDataOutput)

    val parsedUserDF =
      sparkSession.read.schema(userStagingSchema).parquet(temporalDataOutput)

    parsedUserDF
  }

  def getDataFrame(
      rawPullRequestsDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    // Extracting users
    val actorDF =
      rawPullRequestsDF.select(col("actor")).distinct().select("actor.*")

    val organizationsDF =
      rawPullRequestsDF.select(col("org")).distinct().select("org.*")

    val ownerDF = rawPullRequestsDF
      .select(col("payload.pull_request.head.repo.owner"))
      .distinct()
      .select("owner.*")

    val pullRequestUserDF =
      rawPullRequestsDF
        .select("payload.pull_request.user")
        .distinct()
        .select("user.*")

    val mergedByDF = rawPullRequestsDF
      .select("payload.pull_request.merged_by")
      .distinct()
      .select("merged_by.*")

    val pullRequestAssigneeDF =
      rawPullRequestsDF
        .select("payload.pull_request.assignee")
        .distinct()
        .select("assignee.*")

    val pullRequestAssigneesDF =
      rawPullRequestsDF
        .select(explode(col("payload.pull_request.assignees")))
        .distinct()
        .select("col.*")

    val reviewersDF =
      rawPullRequestsDF.select(
        explode(col("payload.pull_request.requested_reviewers"))
      )

    val commitAuthorDF = rawPullRequestsDF
      .select(
        explode(col("payload.pull_request.commits_list")).as("commit")
      )
      .distinct()
      .select("commit.author.*")

    val commitCommitterDF = rawPullRequestsDF
      .select(
        explode(col("payload.pull_request.commits_list")).as("commit")
      )
      .distinct()
      .select("commit.committer.*")

    val userStagingETL = parseToUser(
      actorDF,
      sparkSession,
      temporalActorsOutput
    )
      .unionByName(
        parseToUser(organizationsDF, sparkSession, temporalOrganizationsOutput)
      )
      .unionByName(
        parseToUser(ownerDF, sparkSession, temporalOwnersOutput)
      )
      .unionByName(
        parseToUser(pullRequestUserDF, sparkSession, temporalPRUsersOutput)
      )
      .unionByName(
        parseToUser(mergedByDF, sparkSession, temporalMergedByOutput)
      )
      .unionByName(
        parseToUser(
          pullRequestAssigneeDF,
          sparkSession,
          temporalPRAssigneeOutput
        )
      )
      .unionByName(
        parseToUser(
          pullRequestAssigneesDF,
          sparkSession,
          temporalPRAssigneesOutput
        )
      )
      .unionByName(
        parseToUser(reviewersDF, sparkSession, temporalPRReviewersOutput)
      )
      .unionByName(
        parseToUser(commitAuthorDF, sparkSession, temporalCommitAuthorOutput)
      )
      .unionByName(
        parseToUser(
          commitCommitterDF,
          sparkSession,
          temporalCommitCommitterOutput
        )
      )
      .distinct()
      .select("*")

    userStagingETL
  }
}
