package presentation

import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BranchDimensionETL {

    val branchDimensionPath = "src/dataset/presentation/branch-dimension"
    val branchDimensionTempPath = "src/dataset/presentation/temp/branch-dimension"

  val branchDimensionSchema: StructType = StructType(
 Array(
   StructField("pk_id", LongType, nullable = false),
   StructField("branch_name", StringType, nullable = false),
   StructField("branch_sha", StringType, nullable = false),
   StructField("protected_branch", StringType, nullable = false),
   StructField("full_name_repo", StringType, nullable = false),
   StructField("description_repo", StringType, nullable = false),
   StructField("default_branch_repo", StringType, nullable = false),
   StructField("language_repo", StringType, nullable = false),
   StructField("license_repo", StringType, nullable=false),
   StructField("is_forked_repo", StringType, nullable=false),
   StructField("archived_repo", StringType, nullable=false),
   StructField("private_repo", StringType, nullable=false),
   StructField("size_repo", StringType, nullable=false),
   StructField("disabled_repo", StringType, nullable=false),
   StructField("open_issues_repo", StringType, nullable=false),
   StructField("forks_repo", StringType, nullable=false),
   StructField("repo_id", StringType, nullable=false),
   StructField("stargazer_count_repo", StringType, nullable=false),
   StructField("watchers_count_repo", StringType, nullable=false),
   StructField("pushed_at", StringType, nullable=false),
 ))

   def transform(eventPayloadStagingDF: DataFrame): DataFrame = {
    val branchDimensionHead = eventPayloadStagingDF
      .withColumn("protected_branch", lit("Not available"))
      .withColumn("disabled_repo", lit("Not available"))
      .select(
        col("pull_request_head_ref").as("branch_name"),
        col("pull_request_head_sha").as("branch_sha"),
        col("protected_branch"),
        col("pull_request_head_repo_full_name").as("full_name_repo"),
        when(col("pull_request_head_repo_description").isNull, "Description not available")
          .otherwise(col("pull_request_head_repo_description")).as("description_repo"),
        col("pull_request_head_repo_default_branch").as("default_branch_repo"),
        when(col("pull_request_head_repo_language").isNull, "Languge not available")
          .otherwise(col("pull_request_head_repo_language"))
          .as("language_repo"),
        when(col("pull_request_head_repo_license").isNull, "License not available")
          .otherwise(col("pull_request_head_repo_license.name"))
          .as("license_repo"),
        when(col("pull_request_head_repo_fork") === true, "Has been forked")
          .otherwise("Has not been forked")
          .as("is_forked_repo"),
        when(col("pull_request_head_repo_archived") === true, "Is archived")
          .otherwise("Is not archived")
          .as("archived_repo"),
        when(col("pull_request_head_repo_private") === true, "Private repository")
          .otherwise("Public repository")
          .as("private_repo"),
        when(col("pull_request_head_repo_size").isNull, -1)
          .otherwise(col("pull_request_head_repo_size"))
          .as("size_repo"),
        col("disabled_repo"),
        col("pull_request_head_repo_open_issues").as("open_issues_repo"),
        col("pull_request_head_repo_forks").as("forks_repo"),
        col("pull_request_head_repo_id").as("repo_id"),
        col("pull_request_head_repo_stargazers_count").as("stargazer_count_repo"),
        col("pull_request_head_repo_watchers_count").as("watchers_count_repo"),
        col("pull_request_head_repo_pushed_at").as("pushed_at")
      )

    val branchDimensionHead2 = branchDimensionHead.dropDuplicates("repo_id")
   
   val branchDimensionBase = eventPayloadStagingDF
      .withColumn("protected_branch", lit("Not available"))
      .withColumn("disabled_repo", lit("Not available"))
      .select(
        col("pull_request_base_ref").as("branch_name"),
        col("pull_request_base_sha").as("branch_sha"),
        col("protected_branch"),
        col("pull_request_base_repo_full_name").as("full_name_repo"),
        when(col("pull_request_base_repo_description").isNull, "Description not available")
          .otherwise(col("pull_request_base_repo_description")).as("description_repo"),
        col("pull_request_base_repo_default_branch").as("default_branch_repo"),
        when(col("pull_request_base_repo_language").isNull, "Languge not available")
          .otherwise(col("pull_request_base_repo_language"))
          .as("language_repo"),
        when(col("pull_request_base_repo_license").isNull, "License not available")
          .otherwise(col("pull_request_base_repo_license.name"))
          .as("license_repo"),
        when(col("pull_request_base_repo_fork") === true, "Has been forked")
          .otherwise("Has not been forked")
          .as("is_forked_repo"),
        when(col("pull_request_base_repo_archived") === true, "Is archived")
          .otherwise("Is not archived")
          .as("archived_repo"),
        when(col("pull_request_base_repo_private") === true, "Private repository")
          .otherwise("Public repository")
          .as("private_repo"),
        when(col("pull_request_base_repo_size").isNull, -1)
          .otherwise(col("pull_request_base_repo_size"))
          .as("size_repo"),
        col("disabled_repo"),
        col("pull_request_base_repo_open_issues").as("open_issues_repo"),
        col("pull_request_base_repo_forks").as("forks_repo"),
        col("pull_request_base_repo_id").as("repo_id"),
        col("pull_request_base_repo_stargazers_count").as("stargazer_count_repo"),
        col("pull_request_base_repo_watchers_count").as("watchers_count_repo"),
        col("pull_request_base_repo_pushed_at").as("pushed_at")
      )

    val branchDimensionBase2 = branchDimensionBase.dropDuplicates("repo_id")
   
   val BranchUnion = branchDimensionHead2.unionByName(branchDimensionBase2)

    val branchUnique = BranchUnion
          .dropDuplicates("repo_id")
   
   branchUnique
 }

  def loadApplyingSCD(
      dataFromStagingDF: DataFrame,
      currentDimensionDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentBranchDimensionDF = currentDimensionDF
    val branchesFromStagingDF = dataFromStagingDF

    val tempBranchDimDF = if (currentBranchDimensionDF.isEmpty) {
      // reading the null row
      val branchUndefinedRowDF = sparkSession
        .createDataFrame(NullDimension.BranchDataNull)
        .toDF(NullDimension.BranchColumnNull: _*)

      val branchDimWithUndefinedRowDF = branchesFromStagingDF
        .withColumn("pk_id", monotonically_increasing_id())
        .unionByName(branchUndefinedRowDF)

      val branchDimDF = branchDimWithUndefinedRowDF

      branchDimDF
    } else {
      val newBranchesDF = branchesFromStagingDF
        .join(
          currentBranchDimensionDF,
          branchesFromStagingDF("branch_sha") === branchesFromStagingDF("branch_sha"), // Joining by natural Key
          "left_anti"
        )
        .withColumn("pk_id", monotonically_increasing_id())

      val editedBranchesDF = currentBranchDimensionDF
        .as("current")
        .join(
          branchesFromStagingDF.as("staging"),
          branchesFromStagingDF("branch_sha") === currentBranchDimensionDF("branch_sha"), // Joining by natural Key
          "inner"
        )
        .select(
          col("current.branch_name"),
          col("current.branch_sha"),
          col("staging.protected_branch"),
          col("staging.full_name_repo"),
          col("staging.description_repo"),
          col("staging.default_branch_repo"),
          col("staging.lenguage_repo"),
          col("staging.license_repo"),
          col("staging.is_fork_repo"),
          col("staging.archived_repo"),
          col("staging.private_repo"),
          col("staging.size_repo"),
          col("staging.disabled_repo"),
          col("staging.open_issues_repo"),
          col("staging.forks_repo"),
          col("current.repo_id"),
          col("staging.stargazer_count_repo"),
          col("staging.watchers_count_repo"),
          col("staging.pushed_at"),
          col("current.pk_id"),
        )

      val notUpdatedBranchesDF = currentBranchDimensionDF.join(
        branchesFromStagingDF,
        branchesFromStagingDF("branch_sha") === currentBranchDimensionDF("branch_sha"), // Joining by natural Key
        "left_anti"
      )

      // Union of all dimension pieces
      val updatedBranchDimDF = newBranchesDF
        .unionByName(editedBranchesDF)
        .unionByName(notUpdatedBranchesDF)

      updatedBranchDimDF
    }

    // Returning the final dimension as temporal
    tempBranchDimDF
  }
    
  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentBranchDimensionDF = sparkSession.read
      .schema(branchDimensionSchema)
      .parquet(branchDimensionPath)

    val eventPayloadStagingDF = spark.read.parquet(eventsPayloadStagingPath)

    val branchesFromStagingDF = transform(eventPayloadStagingDF)
    val tempBranchDimDF = loadApplyingSCD(branchesFromStagingDF, currentBranchDimensionDF, sparkSession)

    // Write temporal dimension
    tempBranchDimDF.write
      .mode(SaveMode.Overwrite)
      .parquet(branchDimensionTempPath)

    // Move temporal dimension to the final dimension
    sparkSession.read
      .parquet(branchDimensionTempPath)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(branchDimensionPath)

    tempFileDimDF
  }

}
