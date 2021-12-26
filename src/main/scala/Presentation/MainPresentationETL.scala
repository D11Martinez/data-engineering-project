package Presentation

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col}
object MainPresentationETL extends App {

  // these are the default paths staging
  val userStagingOutput = "src/dataset/staging/users"
  val commitStagingOutPut = "src/dataset/staging/commits"
  val eventPayloadStagingOutput = "src/dataset/staging/events-payloads"
  val OrganizationsOutput = "src/dataset/staging/organizations"

  // paths presentation
  val userPresentationOutput = "src/dataset/presentation/usersDimension"
  val orgPresentationOutput = "src/dataset/presentation/organizationsDimension"
  val pullRequestPresentationOutput = "src/dataset/presentation/PullRequestDimension"
  val branchPresentationOutput = "src/dataset/presentation/BranchDimension"
  val reviewersPresentationOutput = "src/dataset/presentation/ReviewersGroupDimension"
  val asigneesPresentationOutput = "src/dataset/presentation/AsigneesGroupDimension"
  val pullRequestFactTable = "src/dataset/presentation/PullRequestFactTable"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val stagingOrg = spark.read.parquet(OrganizationsOutput)
  val stagingUser = spark.read.parquet(userStagingOutput)
  val stagingPullRequest = spark.read.option("inferSchema", true).parquet(eventPayloadStagingOutput)


  //stagingPullRequest.select("pull_request_id").groupBy("pull_request_id").count().show(10,false)
  //stagingPullRequest.select("*").show(10,false)
  //spark.read.parquet(pullRequestFactTable).select("*").show(100,false)

  OrgPresentationETL.getDataFrame(stagingOrg).write.mode(SaveMode.Overwrite).parquet(orgPresentationOutput)
  println("-- ORG DIMENSION EXITO --")

  UserPreserntationETL.getDataFrame(stagingUser).write.mode(SaveMode.Overwrite).parquet(userPresentationOutput)
  println("-- USER DIMENSION EXITO --")

  PullRequestPresentationETL.getDataFrame(stagingPullRequest).write.mode(SaveMode.Overwrite).parquet(pullRequestPresentationOutput)
  println("-- PULLREQUEST DIMENSION EXITO --")


  BranchPresentationETL.getDataFrame(stagingPullRequest).write.mode(SaveMode.Overwrite).parquet(branchPresentationOutput)
  println("-- BRANCH DIMENSION EXITO --")


  AssigneesGroupPresentationETL.getDataFrame(stagingPullRequest,spark).write.mode(SaveMode.Overwrite).parquet(asigneesPresentationOutput)
  println("-- ASIGNEES DIMENSION EXITO --")

  ReviewersGroupPresentationETL.getDataFrame(stagingPullRequest,spark).write.mode(SaveMode.Overwrite).parquet(reviewersPresentationOutput)
  println("-- REVIEWERS DIMENSION EXITO --")

  PullRequestFactTablePresentationETL.getDataFrameBranch(stagingPullRequest,spark).write.mode(SaveMode.Overwrite).parquet(pullRequestFactTable)
  println("-- PULLREQUEST FACT TABLE  EXITO --")

 // PullRequestFactTablePresentationETL.getDataFrameBranch(stagingPullRequest,spark).show()





}
