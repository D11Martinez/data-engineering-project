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
  val userPresentationOutput = "src/dataset/presentation/users-dimension"
  val orgPresentationOutput = "src/dataset/presentation/organizations-dimension"
  val pullRequestPresentationOutput = "src/dataset/presentation/pullrequest-dimension"
  val branchPresentationOutput = "src/dataset/presentation/branch-dimension"
  val reviewersPresentationOutput = "src/dataset/presentation/reviewersgroup-dimension"
  val asigneesPresentationOutput = "src/dataset/presentation/asigneesgroup-dimension"
  val pullRequestFactTable = "src/dataset/presentation/pullrequest-factTable"
  //val prueba = "src/dataset/presentation/pruebaAsignees"
  //val pruebauser = "src/dataset/presentation/pruebaUser"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .config("dfs.client.read.shortcircuit.skip.checksum", "true")
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
  spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
  //val fs = org.apache.hadoop.fs.FileSystem.get(conf)
 // fs.setVerifyChecksum(false)

  val stagingOrg = spark.read.parquet(OrganizationsOutput)
  val stagingUser = spark.read.parquet(userStagingOutput)
  val stagingPullRequest = spark.read.option("inferSchema", true).parquet(eventPayloadStagingOutput)


  //stagingPullRequest.select("pull_request_id").groupBy("pull_request_id").count().show(10,false)
  //stagingPullRequest.select("*").show(10,false)
  //spark.read.parquet(pullRequestFactTable).filter(col("closed_at_date")==="Not available").select("*").show(100,false)


  OrgDimensionETL.getDataFrame(stagingOrg,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(orgPresentationOutput)
  println("-- ORG DIMENSION EXITO --")

  UserDimensionETL.getDataFrame(stagingUser,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(userPresentationOutput)
  println("-- USER DIMENSION EXITO --")


  PullRequestDimensionETL.getDataFrame(stagingPullRequest,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(pullRequestPresentationOutput)
  println("-- PULLREQUEST DIMENSION EXITO --")


  BranchDimensionETL.getDataFrame(stagingPullRequest,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(branchPresentationOutput)
  println("-- BRANCH DIMENSION EXITO --")


  AssigneesGroupBridgeETL.getDataFrame(stagingPullRequest,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(asigneesPresentationOutput)
  println("-- ASIGNEES DIMENSION EXITO --")

  ReviewersGroupBridgeETL.getDataFrame(stagingPullRequest,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(reviewersPresentationOutput)
  println("-- REVIEWERS DIMENSION EXITO --")

  PullRequestFactETL.getDataFrameBranch(stagingPullRequest,spark).coalesce(1).write.mode(SaveMode.Overwrite).parquet(pullRequestFactTable)
  println("-- PULLREQUEST FACT TABLE  EXITO --")

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
