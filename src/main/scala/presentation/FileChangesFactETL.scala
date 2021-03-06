package presentation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileChangesFactETL {
  val commitDimensionSource = "src/dataset/presentation/commit-dimension"
  val fileDimensionSource = "src/dataset/presentation/file-dimension"
  val orgDimensionSource = "src/dataset/presentation/organizations-dimension"
  val branchDimensionSource = "src/dataset/presentation/branch-dimension"
  val userDimensionSource = "src/dataset/presentation/users-dimension"

  def applyLeftJoin(
      leftDF: DataFrame,
      rightDF: DataFrame,
      leftField: String,
      rightField: String,
      rightId: String,
      renamedRightId: String
  ): DataFrame = {
    leftDF
      .as("leftDF")
      .join(rightDF, leftDF(leftField) === rightDF(rightField), "left")
      .select(
        col("leftDF.*"),
        col(rightId).as(renamedRightId)
      )
  }

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val commitDimDF = sparkSession.read.parquet(commitDimensionSource)
    val fileDimDF = sparkSession.read.parquet(fileDimensionSource)
    val organizationDimDF = sparkSession.read.parquet(orgDimensionSource)
    val userDimDF = sparkSession.read.parquet(userDimensionSource)
    val branchDimDF = sparkSession.read.parquet(branchDimensionSource)

    val fileChangesFactRawFieldsDF =
      eventPayloadStagingDF
        .select(
          col("pull_request_commit_sha"),
          col("pull_request_commit_parent_sha"),
          col("pull_request_head_sha"),
          col("pull_request_commit_author_id"),
          col("pull_request_commit_committer_id"),
          col("pull_request_head_repo_owner_id"),
          col("pull_request_commit_file_sha"),
          col("org_id"),
          col("pull_request_commit_file_additions").as("file_additions"),
          col("pull_request_commit_file_deletions").as("file_deletions"),
          col("pull_request_commit_file_changes").as("file_changes"),
          col("pull_request_commit_file_status").as("file_status"),
          col("pull_request_commit_file_patch").as("file_patch"),
          col("pull_request_commit_committer_date")
        )
        .distinct()
        .withColumn(
          "committed_at_time",
          when(col("pull_request_commit_committer_date").isNull, -1)
            .otherwise(date_format(col("pull_request_commit_committer_date"), "HHmmss"))
        )
        .withColumn(
          "committed_at_date",
          when(col("pull_request_commit_committer_date").isNull, -1)
            .otherwise(date_format(col("pull_request_commit_committer_date"), "yyyyMMdd"))
        )
        .withColumn(
          "commiter_date",
          regexp_replace(col("pull_request_commit_committer_date"), "T", " ")
        )
        .select("*")

    val fileChangesFactWithCommitsDF = applyLeftJoin(
      fileChangesFactRawFieldsDF,
      commitDimDF,
      "pull_request_commit_sha",
      "sha",
      "pk_id",
      "commit_id"
    ) //.drop("pull_request_commit_sha")

    val fileChangesFactWithFilesDF = applyLeftJoin(
      fileChangesFactWithCommitsDF,
      fileDimDF,
      "pull_request_commit_file_sha",
      "sha",
      "pk_id",
      "file_id"
    ).drop("pull_request_commit_file_sha")

    val fileChangesFactWithOrgDF = applyLeftJoin(
      fileChangesFactWithFilesDF,
      organizationDimDF,
      "org_id",
      "organization_id",
      "pk_id",
      "organization_id"
    ).drop("org_id")

    val fileChangesFactWithAuthorDF = applyLeftJoin(
      fileChangesFactWithOrgDF,
      userDimDF,
      "pull_request_commit_author_id",
      "user_id",
      "pk_id",
      "author_id"
    ).drop("pull_request_commit_author_id")

    val fileChangesFactWithCommitterDF = applyLeftJoin(
      fileChangesFactWithAuthorDF,
      userDimDF,
      "pull_request_commit_committer_id",
      "user_id",
      "pk_id",
      "committer_id"
    ).drop("pull_request_commit_committer_id")

    val fileChangesFactWithRepoOwnerDF = applyLeftJoin(
      fileChangesFactWithCommitterDF,
      userDimDF,
      "pull_request_head_repo_owner_id",
      "user_id",
      "pk_id",
      "repo_owner_id"
    ).drop("pull_request_head_repo_owner_id")

    val fileChangesFactWithBranchDF = applyLeftJoin(
      fileChangesFactWithRepoOwnerDF,
      branchDimDF,
      "pull_request_head_sha",
      "branch_sha",
      "pk_id",
      "branch_id"
    )

    val fileChangesFactDF = fileChangesFactWithBranchDF
      .withColumn(
        "bytes_changes",
        when(col("file_patch").isNull, lit(0)).otherwise(
          length(col("file_patch"))
        )
      )
      .select(
        when(col("commit_id").isNull, lit(-1))
          .otherwise(col("commit_id"))
          .as("commit_id"),
        when(col("file_id").isNull, lit(-1))
          .otherwise(col("file_id"))
          .as("file_id"),
        when(col("organization_id").isNull, lit(-1))
          .otherwise(col("organization_id"))
          .as("organization_id"),
        when(col("author_id").isNull, lit(-1))
          .otherwise(col("author_id"))
          .as("author_id"),
        when(col("committer_id").isNull, lit(-1))
          .otherwise(col("committer_id"))
          .as("committer_id"),
        when(col("repo_owner_id").isNull, lit(-1))
          .otherwise(col("repo_owner_id"))
          .as("repo_owner_id"),
        when(col("branch_id").isNull, lit(-1))
          .otherwise(col("branch_id"))
          .as("branch_id"),
        when(col("file_additions").isNull, lit(0))
          .otherwise(col("file_additions"))
          .as("additions"),
        when(col("file_deletions").isNull, lit(0))
          .otherwise(col("file_deletions"))
          .as("deletions"),
        when(col("file_changes").isNull, lit(0))
          .otherwise(col("file_changes"))
          .as("changes"),
        col("file_status"),
        col("bytes_changes"),
        col("committed_at_time"),
        col("committed_at_date"),
        regexp_replace(col("commiter_date"), "Z", "").as("commiter_date"),
        col("pull_request_commit_sha"),
        col("pull_request_commit_parent_sha")
      )
      .distinct()
      .select("*")
      .withColumn("pk_id", monotonically_increasing_id())

    val fileChangesFactDF2 = fileChangesFactDF
      .dropDuplicates("pull_request_commit_sha")
      .withColumnRenamed("pull_request_commit_sha", "pull_request_commit_sha2")

    val fileChangesFactDF3 = fileChangesFactDF
      .as("fileChangesFactDF")
      .join(
        fileChangesFactDF2.as("fileChangesFactDF2"),
        fileChangesFactDF("pull_request_commit_parent_sha") === fileChangesFactDF2(
          "pull_request_commit_sha2"
        ),
        "left"
      )
      .withColumn("commiter_parent_date", col("fileChangesFactDF2.commiter_date"))
      .select(col("fileChangesFactDF.*"), col("commiter_parent_date"))
      .na
      .fill("Not available")

    val fileChangesFactDF4 = fileChangesFactDF3
      .withColumn(
        "commiter_date_second",
        when(col("commiter_date") =!= "Not available", unix_timestamp(col("commiter_date")))
          .otherwise(col("commiter_date"))
      )
      .withColumn(
        "commiter_parent_date_second",
        when(
          col("commiter_parent_date") =!= "Not available",
          unix_timestamp(col("commiter_parent_date"))
        )
          .otherwise(col("commiter_parent_date"))
      )
      .withColumn(
        "diferent_time_commit",
        when(
          col("commiter_parent_date_second") =!= "Not available",
          round((col("commiter_date_second") - col("commiter_parent_date_second")) / 3600, 2)
        )
          .otherwise(0)
      )
      .withColumnRenamed(
        "commiter_date",
        "committed_at_date_full"
      )
      .withColumnRenamed(
        "commiter_parent_date",
        "committed_at_parent_date_full"
      )
      .drop(
        // "commiter_date",
        // "commiter_parent_date",
        "commiter_date_second",
        "commiter_parent_date_second",
        "pull_request_commit_sha",
        "pull_request_commit_parent_sha"
      )

    //fileChangesFactDF3.printSchema(3)
    fileChangesFactDF4.show(10, false)
    // println("cantidad de registros:"+fileChangesFactDF3.count())
    fileChangesFactDF4
  }
}
