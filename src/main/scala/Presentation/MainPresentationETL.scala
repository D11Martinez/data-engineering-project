package presentation

import org.apache.spark.sql.{SaveMode, SparkSession}
object MainPresentationETL extends App {

  // these are the default paths staging
  val userStagingOutput = "src/dataset/staging/users"
  val commitStagingOutPut = "src/dataset/staging/commits"
  val eventPayloadStagingOutput = "src/dataset/staging/events-payloads"
  val OrganizationsOutput = "src/dataset/staging/organizations"

  // paths presentation
  val USER_DIMENSION_PATH = "src/dataset/presentation/users-dimension"
  val ORG_DIMENSION_PATH = "src/dataset/presentation/organizations-dimension"
  val PULL_REQUEST_DIMENSION_PATH =
    "src/dataset/presentation/pullrequest-dimension"
  val BRANCH_DIMENSION_PATH = "src/dataset/presentation/branch-dimension"
  val FILE_DIMENSION_PATH = "src/dataset/presentation/file-dimension"
  val COMMIT_DIMENSION_PATH = "src/dataset/presentation/commit-dimension"
  val REVIEWERS_GROUP_BRIDGE_PATH =
    "src/dataset/presentation/reviewersgroup-dimension"
  val ASSIGNEES_GROUP_BRIDGE_PATH =
    "src/dataset/presentation/asigneesgroup-dimension"
  val PULL_REQUEST_FACT_TABLE_PATH = "src/dataset/presentation/pullrequest-factTable"
  val FILE_CHANGES_FACT_TABLE_PATH = "src/dataset/presentation/file-changes-fact"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .config("dfs.client.read.shortcircuit.skip.checksum", "true")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  spark.sparkContext.hadoopConfiguration
    .set("parquet.enable.summary-metadata", "false")

  val stagingOrg = spark.read.parquet(OrganizationsOutput)
  val stagingUser = spark.read.parquet(userStagingOutput)
  val stagingPullRequest =
    spark.read.option("inferSchema", value = true).parquet(eventPayloadStagingOutput)

  OrgDimensionETL
    .getDataFrame(stagingOrg, spark)
  println("-- ORG DIMENSION COMPLETED --")

  UserDimensionETL
    .getDataFrame(stagingUser, spark)
  println("-- USER DIMENSION COMPLETED --")

  PullRequestDimensionETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(PULL_REQUEST_DIMENSION_PATH)
  println("-- PULL REQUEST DIMENSION COMPLETED --")

  BranchDimensionETL
    .getDataFrame(stagingPullRequest, spark)
  println("-- BRANCH DIMENSION COMPLETED --")

  FileDimensionETL.getDataFrame(stagingPullRequest, spark)
  println("-- FILE DIMENSION COMPLETED --")

  CommitDimensionETL
    .getDataFrame(stagingPullRequest, spark)
  println("-- COMMIT DIMENSION COMPLETED --")

  AssigneesGroupBridgeETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(ASSIGNEES_GROUP_BRIDGE_PATH)
  println("-- ASSIGNEES DIMENSION COMPLETED --")

  ReviewersGroupBridgeETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(REVIEWERS_GROUP_BRIDGE_PATH)
  println("-- REVIEWERS DIMENSION COMPLETED --")

  PullRequestFactETL
    .getDataFrameBranch(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(PULL_REQUEST_FACT_TABLE_PATH)
  println("-- PULL REQUEST FACT TABLE  COMPLETED --")

  FileChangesFactETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(FILE_CHANGES_FACT_TABLE_PATH)
  println("-- FILE CHANGES FACT TABLE  COMPLETED --")

}
