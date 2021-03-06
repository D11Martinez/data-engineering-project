package staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, explode_outer}

object EventPayloadStagingETL {
  def getDataFrame(rawPullRequestsDF: DataFrame): DataFrame = {
    // Do transformations here
    val eventPayloadWithoutCommitDF =
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
          col("payload.pull_request.merge_commit_sha").as("pull_request_merge_commit_sha"),
          col("payload.pull_request.assignee.id").as("pull_request_assignee_id"),
          col("payload.pull_request.assignees").as("pull_request_assignees_list"),
          col("payload.pull_request.requested_reviewers")
            .as("pull_request_requested_reviewers_list"),
          col("payload.pull_request.milestone").as("pull_request_milestone"),
          col("payload.pull_request.commits_url").as("pull_request_commits_url"),
          col("payload.pull_request.review_comments_url").as("pull_request_review_comments_url"),
          col("payload.pull_request.review_comment_url").as("pull_request_review_comment_url"),
          col("payload.pull_request.comments_url").as("pull_request_comments_url"),
          col("payload.pull_request.statuses_url").as("pull_request_statuses_url"),
          col("payload.pull_request.head.sha").as("pull_request_head_sha"),
          col("payload.pull_request.base.sha").as("pull_request_base_sha"),
          col("payload.pull_request.head").as("pull_request_head"),
          col("payload.pull_request.base").as("pull_request_base"),
          col("payload.pull_request.head.repo").as("pull_request_head_repo"),
          col("payload.pull_request.base.repo").as("pull_request_base_repo"),
          col("payload.pull_request.author_association").as("pull_request_author_association"),
          col("payload.pull_request.merged").as("pull_request_merged"),
          col("payload.pull_request.mergeable").as("pull_request_mergeable"),
          col("payload.pull_request.rebaseable").as("pull_request_rebaseable"),
          col("payload.pull_request.mergeable_state").as("pull_request_mergeable_state"),
          col("payload.pull_request.merged_by.id").as("pull_request_merged_by_id"),
          col("payload.pull_request.comments").as("pull_request_comments"),
          col("payload.pull_request.review_comments").as("pull_request_review_comments"),
          col("payload.pull_request.maintainer_can_modify")
            .as("pull_request_maintainer_can_modify"),
          col("payload.pull_request.commits").as("pull_request_commits"),
          col("payload.pull_request.additions").as("pull_request_additions"),
          col("payload.pull_request.deletions").as("pull_request_deletions"),
          col("payload.pull_request.changed_files")
            .as("pull_request_changed_files"),
          col("payload.pull_request.commits_list")
            .as("pull_request_commits_list")
        )
        .withColumn("pull_request_head_repo_owner_id", col("pull_request_head_repo.owner.id"))
        .withColumn("pull_request_head_ref", col("pull_request_head.ref"))
        .withColumn("pull_request_head_repo_description", col("pull_request_head_repo.description"))
        .withColumn("pull_request_head_repo_language", col("pull_request_head_repo.language"))
        .withColumn("pull_request_head_repo_license", col("pull_request_head_repo.license"))
        .withColumn("pull_request_head_repo_fork", col("pull_request_head_repo.fork"))
        .withColumn("pull_request_head_repo_archived", col("pull_request_head_repo.archived"))
        .withColumn("pull_request_head_repo_private", col("pull_request_head_repo.private"))
        .withColumn("pull_request_head_repo_size", col("pull_request_head_repo.size"))
        .withColumn("pull_request_head_repo_full_name", col("pull_request_head_repo.full_name"))
        .withColumn(
          "pull_request_head_repo_default_branch",
          col("pull_request_head_repo.default_branch")
        )
        .withColumn(
          "pull_request_head_repo_open_issues",
          col("pull_request_head_repo.open_issues")
        )
        .withColumn("pull_request_head_repo_forks", col("pull_request_head_repo.forks"))
        .withColumn("pull_request_head_repo_id", col("pull_request_head_repo.id"))
        .withColumn(
          "pull_request_head_repo_stargazers_count",
          col("pull_request_head_repo.stargazers_count")
        )
        .withColumn(
          "pull_request_head_repo_watchers_count",
          col("pull_request_head_repo.watchers_count")
        )
        .withColumn("pull_request_head_repo_pushed_at", col("pull_request_head_repo.pushed_at"))
        .withColumn("pull_request_base_ref", col("pull_request_base.ref"))
        .withColumn("pull_request_base_repo_description", col("pull_request_base_repo.description"))
        .withColumn("pull_request_base_repo_language", col("pull_request_base_repo.language"))
        .withColumn("pull_request_base_repo_license", col("pull_request_base_repo.license"))
        .withColumn("pull_request_base_repo_fork", col("pull_request_base_repo.fork"))
        .withColumn("pull_request_base_repo_archived", col("pull_request_base_repo.archived"))
        .withColumn("pull_request_base_repo_private", col("pull_request_base_repo.private"))
        .withColumn("pull_request_base_repo_size", col("pull_request_base_repo.size"))
        .withColumn("pull_request_base_repo_full_name", col("pull_request_base_repo.full_name"))
        .withColumn(
          "pull_request_base_repo_default_branch",
          col("pull_request_base_repo.default_branch")
        )
        .withColumn(
          "pull_request_base_repo_open_issues",
          col("pull_request_base_repo.open_issues")
        )
        .withColumn("pull_request_base_repo_forks", col("pull_request_base_repo.forks"))
        .withColumn("pull_request_base_repo_id", col("pull_request_base_repo.id"))
        .withColumn(
          "pull_request_base_repo_stargazers_count",
          col("pull_request_base_repo.stargazers_count")
        )
        .withColumn(
          "pull_request_base_repo_watchers_count",
          col("pull_request_base_repo.watchers_count")
        )
        .withColumn("pull_request_base_repo_pushed_at", col("pull_request_base_repo.pushed_at"))
        .withColumn("pull_request_commit", explode_outer(col("pull_request_commits_list")))
        .withColumn(
          "pull_request_requested_reviewer",
          explode_outer(col("pull_request_requested_reviewers_list"))
        )
        .withColumn(
          "pull_request_assignees_item",
          explode_outer(col("pull_request_assignees_list"))
        )
        .withColumn(
          "create_at_time_temporal",
          date_format(col("pull_request_created_at"), "HH:mm:ss")
        )
        .withColumn(
          "create_at_date_temporal",
          date_format(col("pull_request_created_at"), "yyyy-MM-dd")
        )
        .drop(
          "pull_request_commits_list",
          "pull_request_requested_reviewers_list",
          //"pull_request_requested_reviewer",
          "pull_request_assignees_list",
          //"pull_request_base_repo",
          //"pull_request_head_repo",
          "pull_request_base",
          "pull_request_head"
          //"pull_request_assignees_item"
        )

    val eventPayloadDF = eventPayloadWithoutCommitDF
      .withColumn("pull_request_commit_sha", col("pull_request_commit.sha"))
      .withColumn("pull_request_commit_node_id", col("pull_request_commit.node_id"))
      .withColumn("pull_request_commit_author_id", col("pull_request_commit.author.id"))
      .withColumn("pull_request_commit_author_name", col("pull_request_commit.commit.author.name"))
      .withColumn(
        "pull_request_commit_author_email",
        col("pull_request_commit.commit.author.email")
      )
      .withColumn("pull_request_commit_author_date", col("pull_request_commit.commit.author.date"))
      .withColumn("pull_request_commit_committer_id", col("pull_request_commit.committer.id"))
      .withColumn(
        "pull_request_commit_committer_name",
        col("pull_request_commit.commit.committer.name")
      )
      .withColumn(
        "pull_request_commit_committer_email",
        col("pull_request_commit.commit.committer.email")
      )
      .withColumn(
        "pull_request_commit_committer_date",
        col("pull_request_commit.commit.committer.date")
      )
      .withColumn("pull_request_commit_message", col("pull_request_commit.commit.message"))
      .withColumn("pull_request_commit_tree_sha", col("pull_request_commit.commit.tree.sha"))
      .withColumn("pull_request_commit_tree_url", col("pull_request_commit.commit.tree.url"))
      .withColumn("pull_request_commit_url", col("pull_request_commit.commit.url"))
      .withColumn(
        "pull_request_commit_comment_count",
        col("pull_request_commit.commit.comment_count")
      )
      .withColumn(
        "pull_request_commit_verification_verified",
        col("pull_request_commit.commit.verification.verified")
      )
      .withColumn(
        "pull_request_commit_verification_reason",
        col("pull_request_commit.commit.verification.reason")
      )
      .withColumn(
        "pull_request_commit_verification_signature",
        col("pull_request_commit.commit.verification.signature")
      )
      .withColumn(
        "pull_request_commit_verification_payload",
        col("pull_request_commit.commit.verification.payload")
      )
      .withColumn("pull_request_commit_html_url", col("pull_request_commit.html_url"))
      .withColumn("pull_request_commit_comments_url", col("pull_request_commit.comments_url"))
      .withColumn("pull_request_commit_parents", col("pull_request_commit.parents"))
      .withColumn("pull_request_commit_total_changes", col("pull_request_commit.stats.total"))
      .withColumn("pull_request_commit_total_additions", col("pull_request_commit.stats.additions"))
      .withColumn("pull_request_commit_total_deletions", col("pull_request_commit.stats.deletions"))
      .withColumn("pull_request_commit_file", explode_outer(col("pull_request_commit.files")))
      .withColumn("pull_request_commit_file_sha", col("pull_request_commit_file.sha"))
      .withColumn("pull_request_commit_file_filename", col("pull_request_commit_file.filename"))
      .withColumn("pull_request_commit_file_status", col("pull_request_commit_file.status"))
      .withColumn("pull_request_commit_file_additions", col("pull_request_commit_file.additions"))
      .withColumn("pull_request_commit_file_deletions", col("pull_request_commit_file.deletions"))
      .withColumn("pull_request_commit_file_changes", col("pull_request_commit_file.changes"))
      .withColumn("pull_request_commit_file_blob_url", col("pull_request_commit_file.blob_url"))
      .withColumn("pull_request_commit_file_raw_url", col("pull_request_commit_file.raw_url"))
      .withColumn(
        "pull_request_commit_file_contents_url",
        col("pull_request_commit_file.contents_url")
      )
      .withColumn("pull_request_commit_file_patch", col("pull_request_commit_file.patch"))
      .withColumn("pull_request_commit_parent", explode_outer(col("pull_request_commit_parents")))
      .withColumn("pull_request_commit_parent_sha", col("pull_request_commit_parent.sha"))
      .drop(
        "pull_request_commit_file",
        "pull_request_commit",
        "pull_request_commit_parents",
        "pull_request_commit_parent"
      )

    eventPayloadDF.printSchema(3)
    eventPayloadDF
  }
}
