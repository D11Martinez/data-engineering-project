package staging

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

  val OrganizationsOutput = "src/dataset/staging/organizations"

  val userStagingSchema: StructType = StructType(
    Array(
      StructField("id", LongType, nullable = true),
      StructField("login", StringType, nullable = true),
      StructField("node_id", StringType, nullable = true),
      StructField("avatar_url", StringType, nullable = true),
      StructField("gravatar_id", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("html_url", StringType, nullable = true),
      StructField("followers_url", StringType, nullable = true),
      StructField("following_url", StringType, nullable = true),
      StructField("gists_url", StringType, nullable = true),
      StructField("starred_url", StringType, nullable = true),
      StructField("subscriptions_url", StringType, nullable = true),
      StructField("organizations_url", StringType, nullable = true),
      StructField("repos_url", StringType, nullable = true),
      StructField("events_url", StringType, nullable = true),
      StructField("received_events_url", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("site_admin", BooleanType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("blog", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("hireable", BooleanType, nullable = true),
      StructField("bio", StringType, nullable = true),
      StructField("twitter_username", StringType, nullable = true),
      StructField("public_repos", LongType, nullable = true),
      StructField("public_gists", LongType, nullable = true),
      StructField("followers", LongType, nullable = true),
      StructField("following", LongType, nullable = true),
      StructField("created_at", StringType, nullable = true),
      StructField("updated_at", StringType, nullable = true)
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
    organizationsDF.write.mode(SaveMode.Overwrite).parquet(OrganizationsOutput)

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
        .select(explode(col("payload.pull_request.assignees")).as("assignees"))
        .distinct()
        .select("assignees.*")

    val reviewersDF =
      rawPullRequestsDF
        .select(
          explode(col("payload.pull_request.requested_reviewers"))
            .as("requested_reviewers")
        )
        .distinct()
        .select("requested_reviewers.*")

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
      .unionByName(parseToUser(ownerDF, sparkSession, temporalOwnersOutput))
      .unionByName(parseToUser(pullRequestUserDF, sparkSession, temporalPRUsersOutput))
      .unionByName(parseToUser(mergedByDF, sparkSession, temporalMergedByOutput))
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
      .unionByName(parseToUser(reviewersDF, sparkSession, temporalPRReviewersOutput))
      .unionByName(parseToUser(commitAuthorDF, sparkSession, temporalCommitAuthorOutput))
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
