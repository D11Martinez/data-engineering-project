package presentation
object NullDimension {

  // PULL REQUEST DIMENSION
  val PullRequestColumnNull =
    Seq("pk_id", "pull_request_id", "number", "title", "body", "locked")
  val PullRequestDataNull = Seq(
    (-1, "Undefined", "Undefined", "Undefined", "Undefined", "Undefined")
  )

  // ORG DIMENSION
  val OrgColumnNull = Seq(
    "pk_id",
    "organization_id",
    "login",
    "email",
    "type",
    "name",
    "description",
    "company",
    "location",
    "is_verified",
    "has_organization_projects",
    "has_repository_projects",
    "blog",
    "twitter_username",
    "created_at",
    "updated_at",
    "public_repos",
    "followers",
    "following",
    "public_repos_category",
    "followers_category",
    "following_category"
  )
  val OrgDataNull = Seq(
    (
      -1,
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined"
    )
  )

  // USER DIMENSION
  val UserColumnNull = Seq(
    "pk_id",
    "user_id",
    "login",
    "type",
    "site_admin",
    "email",
    "name",
    "location",
    "hireable",
    "bio",
    "company",
    "blog",
    "twitter_username",
    "created_at",
    "updated_at",
    "followers",
    "following",
    "public_repos",
    "followers_category",
    "following_category",
    "public_repos_category"
  )
  val UserDataNull = Seq(
    (
      -1,
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined"
    )
  )

  // BRANCH DIMENSION

  val BranchColumnNull = Seq(
    "pk_id",
    "branch_sha",
    "branch_name",
    "protected_branch",
    "full_name_repo",
    "description_repo",
    "default_branch_repo",
    "language_repo",
    "license_repo",
    "is_forked_repo",
    "archived_repo",
    "private_repo",
    "size_repo",
    "disabled_repo",
    "open_issues_repo",
    "forks_repo",
    "repo_id",
    "stargazer_count_repo",
    "watchers_count_repo",
    "pushed_at"
  )

  val BranchDataNull = Seq(
    (
      -1,
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined"
    )
  )

  // File Dimension
  val fileColumnNull = Seq(
    "pk_id",
    "sha",
    "name",
    "extension",
    "path",
    "full_file_name",
    "language"
  )

  val fileDataNull = Seq(
    (
      -1,
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "undefined"
    )
  )

  // Commit Dimension
  val commitColumnNull = Seq(
    "pk_id",
    "sha",
    "message",
    "message_with_good_practices",
    "changes",
    "additions",
    "deletions",
    "comment_count",
    "pull_request_id",
    "pull_request_number",
    "pull_request_title",
    "pull_request_body",
    "pull_request_state",
    "pull_request_locked",
    "pull_request_merged",
    "pull_request_merge_commit_sha",
    "pull_request_author_association"
  )

  val commitDataNull = Seq(
    (
      -1,
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined",
      "Undefined"
    )
  )

}
