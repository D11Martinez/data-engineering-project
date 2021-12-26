package staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object CommitStagingETL {
  def getDataFrame(rawPullRequestsDF: DataFrame): DataFrame = {
    // Do transformations here
    val commitsDF = rawPullRequestsDF
      .select(explode(col("payload.pull_request.commits_list")))
      .select("col.*")
      .select(
        col("sha"),
        col("node_id"),
        col("commit.author.name").as("author_name"),
        col("commit.author.email").as("author_email"),
        col("commit.author.date").as("author_date"),
        col("commit.committer.name").as("committer_name"),
        col("commit.committer.email").as("committer_email"),
        col("commit.committer.date").as("committer_date"),
        col("commit.message").as("message"),
        col("commit.tree.sha").as("tree_sha"),
        col("commit.tree.url").as("tree_url"),
        col("commit.url").as("url"),
        col("commit.comment_count").as("comment_count"),
        col("commit.verification.verified").as("verification_verified"),
        col("commit.verification.reason").as("verification_reason"),
        col("commit.verification.signature").as("verification_signature"),
        col("commit.verification.payload").as("verification_payload"),
        col("html_url"),
        col("comments_url"),
        col("author.id").as("author_id"),
        col("committer.id").as("committer_id"),
        col("parents"),
        col("stats.total").as("total_changes"),
        col("stats.additions").as("total_additions"),
        col("stats.deletions").as("total_deletions"),
        explode(col("files")).as("file")
      )
      .withColumn("file_sha", col("file.sha"))
      .withColumn("file_filename", col("file.filename"))
      .withColumn("file_status", col("file.status"))
      .withColumn("file_additions", col("file.additions"))
      .withColumn("file_deletions", col("file.deletions"))
      .withColumn("file_changes", col("file.changes"))
      .withColumn("file_blob_url", col("file.blob_url"))
      .withColumn("file_raw_url", col("file.raw_url"))
      .withColumn("file_contents_url", col("file.contents_url"))
      .withColumn("file_patch", col("file.patch"))
      .withColumn("parent", explode(col("parents")))
      .drop("file", "parents")

    commitsDF.printSchema(3)
    commitsDF.show(10)

    commitsDF
  }
}
