package staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, date_format, explode, when}

object EventPayloadStagingETL {
  def getDataFrame(rawPullRequestsDF: DataFrame): DataFrame = {
    // Do transformations here
    val eventPayloadDF =
      rawPullRequestsDF
        .select(
          col("id"),
          col("type"),
          col("actor.id").as("actor_id"),
          col("org.id").as("org_id"),
          col("repo.id").as("repo_id"),
          col("repo.name").as("repo_name"),
          col("repo.url").as("repo_url"),
          col("public"),
          col("created_at"),
          col("payload.action").as("payload_action"),
          col("payload.number").as("payload_number"),
          col("payload.pull_request.url").as("pull_request_url"),
          col("payload.pull_request.id").as("pull_request_id"),
          col("payload.pull_request.html_url").as("pull_request_html_url"),
          col("payload.pull_request.diff_url").as("pull_request_diff_url"),
          col("payload.pull_request.patch_url").as("pull_request_patch_url"),
          col("payload.pull_request.issue_url").as("pull_request_issue_url"),
          col("payload.pull_request.number").as("pull_request_number"),
          col("payload.pull_request.state").as("pull_request_state"),
          col("payload.pull_request.locked").as("pull_request_locked"),
          col("payload.pull_request.title").as("pull_request_title"),
          col("payload.pull_request.user.id").as("pull_request_user_id"),
          col("payload.pull_request.body").as("pull_request_body"),
          col("payload.pull_request.created_at").as("pull_request_created_at"),
          col("payload.pull_request.updated_at").as("pull_request_updated_at"),
          col("payload.pull_request.closed_at").as("pull_request_closed_at"),
          col("payload.pull_request.merged_at").as("pull_request_merged_at"),
          col("payload.pull_request.merge_commit_sha")
            .as("pull_request_merge_commit_sha"),
          col("payload.pull_request.assignee.id")
            .as("pull_request_assignee_id"),
          col("payload.pull_request.assignees").as(
            "pull_request_assignees_list"
          )
          ,
          col("payload.pull_request.requested_reviewers")
            .as("pull_request_requested_reviewers_list"),
          col("payload.pull_request.milestone").as("pull_request_milestone"),
          col("payload.pull_request.commits_url")
            .as("pull_request_commits_url"),
          col("payload.pull_request.review_comments_url")
            .as("pull_request_review_comments_url"),
          col("payload.pull_request.review_comment_url")
            .as("pull_request_review_comment_url"),
          col("payload.pull_request.comments_url").as(
            "pull_request_comments_url"
          ),
          col("payload.pull_request.statuses_url").as(
            "pull_request_statuses_url"
          ),
          col("payload.pull_request.head.sha").as("pull_request_head_sha"),
          col("payload.pull_request.base.sha").as("pull_request_base_sha"),
          col("payload.pull_request.head").as("pull_request_head"),
          col("payload.pull_request.base").as("pull_request_base"),
          col("payload.pull_request.author_association").as(
            "pull_request_author_association"
          ),
          col("payload.pull_request.merged").as("pull_request_merged"),
          col("payload.pull_request.mergeable").as("pull_request_mergeable"),
          col("payload.pull_request.rebaseable").as("pull_request_rebaseable"),
          col("payload.pull_request.mergeable_state").as(
            "pull_request_mergeable_state"
          ),
          col("payload.pull_request.merged_by.id").as(
            "pull_request_merged_by_id"
          ),
          col("payload.pull_request.comments").as("pull_request_comments"),
          col("payload.pull_request.review_comments").as(
            "pull_request_review_comments"
          ),
          col("payload.pull_request.maintainer_can_modify").as(
            "pull_request_maintainer_can_modify"
          ),
          col("payload.pull_request.commits").as("pull_request_commits"),
          col("payload.pull_request.additions").as("pull_request_additions"),
          col("payload.pull_request.deletions").as("pull_request_deletions"),
          col("payload.pull_request.changed_files").as(
            "pull_request_changed_files"
          ),
          col("payload.pull_request.commits_list").as(
            "pull_request_commits_list"
          )
        )
        .withColumn(
          "pull_request_commit",
          explode(col("pull_request_commits_list"))
        )
        .withColumn("pull_request_commit_sha", col("pull_request_commit.sha"))
        .withColumn(
          "pull_request_requested_reviewer",when(col("pull_request_requested_reviewers_list")==="[]",-1)
            .otherwise(explode(col("pull_request_requested_reviewers_list")))
        )
        .withColumn(
          "pull_request_requested_reviewer_id",when(col("pull_request_requested_reviewer")=!= -1,
            col("pull_request_requested_reviewer.id")).otherwise(-1)
        )
        .withColumn(
          "pull_request_assignees_item",when(col("pull_request_assignees_list")==="[]",-1)
            .otherwise(explode(col("pull_request_assignees_list")))
        )
        .withColumn(
          "pull_request_assignees_id",when(col("pull_request_assignees_item") =!= -1,
            col("pull_request_assignees_item.id")).otherwise(-1)
        )
        .withColumn(
        "create_at_time_temporal",
        date_format(col("pull_request_created_at"), "HH:mm:ss")
      ).withColumn(
        "create_at_date_temporal",
        date_format(col("pull_request_created_at"), "yyyy-MM-dd")
      )
        .drop(
          "pull_request_commits_list",
          "pull_request_commit",
          "pull_request_requested_reviewers_list",
          "pull_request_requested_reviewer",
          "pull_request_assignees_list",
          "pull_request_assignees_item"
        )

    eventPayloadDF
  }
}
