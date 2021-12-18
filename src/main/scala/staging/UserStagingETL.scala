package staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object UserStagingETL {
  def getDataFrame(rawPullRequestsDF: DataFrame): DataFrame = {
    // Extracting users
    val actorDF = rawPullRequestsDF.select(col("actor")).select("actor.*")

    val organizationsDF = rawPullRequestsDF.select(col("org")).select("org.*")

    val ownerDF = rawPullRequestsDF
      .select(col("payload.pull_request.head.repo.owner"))
      .select("owner.*")

    val pullRequestUserDF =
      rawPullRequestsDF.select("payload.pull_request.user").select("user.*")

    val mergedByDF = rawPullRequestsDF
      .select("payload.pull_request.merged_by")
      .select("merged_by.*")

    val pullRequestAssigneeDF =
      rawPullRequestsDF
        .select("payload.pull_request.assignee")
        .select("assignee.*")

    val pullRequestAssigneesDF =
      rawPullRequestsDF
        .select(explode(col("payload.pull_request.assignees")))
        .select("col.*")

    val reviewersDF =
      rawPullRequestsDF.select(
        explode(col("payload.pull_request.requested_reviewers"))
      )

    val commitAuthorDF = rawPullRequestsDF
      .select(
        explode(col("payload.pull_request.commits_list")).as("commit")
      )
      .select("commit.author")

    val commitCommitterDF = rawPullRequestsDF
      .select(
        explode(col("payload.pull_request.commits_list")).as("commit")
      )
      .select("commit.committer")

    val userStagingETL = actorDF
      .unionByName(organizationsDF)
      .unionByName(ownerDF)
      .unionByName(pullRequestUserDF)
      .unionByName(mergedByDF)
      .unionByName(pullRequestAssigneeDF)
      .unionByName(pullRequestAssigneesDF)
      .unionByName(reviewersDF)
      .unionByName(commitAuthorDF)
      .unionByName(commitCommitterDF)
      .distinct()
      .select("*")

    userStagingETL
  }
}
