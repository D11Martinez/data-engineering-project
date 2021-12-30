package Presentation

import org.apache.spark.sql.{SaveMode, SparkSession}
import presentation.{FileChangesFactETL, FileDimensionETL}
object MainPresentationETL extends App {

  // these are the default paths staging
  val userStagingOutput = "src/dataset/staging/users"
  val commitStagingOutPut = "src/dataset/staging/commits"
  val eventPayloadStagingOutput = "src/dataset/staging/events-payloads"
  val OrganizationsOutput = "src/dataset/staging/organizations"

  // paths presentation
  val userDimensionOutput = "src/dataset/presentation/users-dimension"
  val orgDimensionOutput = "src/dataset/presentation/organizations-dimension"
  val pullRequestDimensionOutput =
    "src/dataset/presentation/pullrequest-dimension"
  val branchDimensionOutput = "src/dataset/presentation/branch-dimension"
  val fileDimensionOutput = "src/dataset/presentation/file-dimension"
  val commitDimensionOutput = "src/dataset/presentation/commit-dimension"
  val reviewersGroupBridgeOutput =
    "src/dataset/presentation/reviewersgroup-dimension"
  val assigneesGroupBridgeOutput =
    "src/dataset/presentation/asigneesgroup-dimension"
  val pullRequestFactTable = "src/dataset/presentation/pullrequest-factTable"
  val fileChangesFactOutput = "src/dataset/presentation/file-changes-fact"
  //val prueba = "src/dataset/presentation/pruebaAsignees"
  //val pruebauser = "src/dataset/presentation/pruebaUser"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .config("dfs.client.read.shortcircuit.skip.checksum", "true")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  spark.sparkContext.hadoopConfiguration
    .set("parquet.enable.summary-metadata", "false")
  //val fs = org.apache.hadoop.fs.FileSystem.get(conf)
  // fs.setVerifyChecksum(false)

  val stagingOrg = spark.read.parquet(OrganizationsOutput)
  val stagingUser = spark.read.parquet(userStagingOutput)
  val stagingPullRequest =
    spark.read.option("inferSchema", true).parquet(eventPayloadStagingOutput)

  //stagingPullRequest.select("pull_request_id").groupBy("pull_request_id").count().show(10,false)
  //stagingPullRequest.select("*").show(10,false)
  //spark.read.parquet(pullRequestFactTable).filter(col("closed_at_date")==="Not available").select("*").show(100,false)

  OrgDimensionETL
    .getDataFrame(stagingOrg, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(orgDimensionOutput)
  println("-- ORG DIMENSION COMPLETED --")

  UserDimensionETL
    .getDataFrame(stagingUser, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(userDimensionOutput)
  println("-- USER DIMENSION COMPLETED --")

  PullRequestDimensionETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(pullRequestDimensionOutput)
  println("-- PULL REQUEST DIMENSION COMPLETED --")

  BranchDimensionETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(branchDimensionOutput)
  println("-- BRANCH DIMENSION COMPLETED --")

  FileDimensionETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(fileDimensionOutput)
  println("-- FILE DIMENSION COMPLETED --")

  CommitDimensionETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(commitDimensionOutput)
  println("-- COMMIT DIMENSION COMPLETED --")

  AssigneesGroupBridgeETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(assigneesGroupBridgeOutput)
  println("-- ASSIGNEES DIMENSION COMPLETED --")

  ReviewersGroupBridgeETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(reviewersGroupBridgeOutput)
  println("-- REVIEWERS DIMENSION COMPLETED --")

  PullRequestFactETL
    .getDataFrameBranch(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(pullRequestFactTable)
  println("-- PULL REQUEST FACT TABLE  COMPLETED --")

  FileChangesFactETL
    .getDataFrame(stagingPullRequest, spark)
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(fileChangesFactOutput)
  println("-- FILE CHANGES FACT TABLE  COMPLETED --")
  // PullRequestFactTablePresentationETL.getDataFrameBranch(stagingPullRequest,spark).show()

  //AssigneesGroupBridgeETL.getDataFrame(stagingPullRequest,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(prueba)
  //println("-- ASIGNEES DIMENSION EXITO --")

  //AssigneesGroupBridgeETL.getDataFrame(stagingPullRequest,spark).write.mode(SaveMode.Overwrite).parquet(asigneesPresentationOutput)
  // println("-- ASIGNEES DIMENSION EXITO --")

  // UserDimensionETL.getDataFrame(stagingUser,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(pruebauser)
  //println("-- USER DIMENSION EXITO --")

  //println("user 1:"+spark.read.parquet(pruebauser).count())
  //println("user normal:"+spark.read.parquet(userPresentationOutput).count())
  // spark.read.parquet(pruebauser).show(100,false)
  // println("asignees normal:"+spark.read.parquet(asigneesPresentationOutput).count())

}
