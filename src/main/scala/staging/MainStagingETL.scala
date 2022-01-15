package staging

import org.apache.spark.sql.{SaveMode, SparkSession}

object MainStagingETL extends App {

  // Add your own config paths here
  val rawPullRequestPath =
    "D:/Documents/UES/TBS115_2021/Proyecto/Datasets/github-dataset"

  // these are the default paths
  val userStagingOutput = "src/dataset/staging/users"
  val commitStagingOutPut = "src/dataset/staging/commits"
  val eventPayloadStagingOutput = "src/dataset/staging/events-payloads"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()
  val rawPullRequestsDF = spark.read
    .option("inferSchema", "true")
    .json(rawPullRequestPath)

  UserStagingETL
    .getDataFrame(rawPullRequestsDF, spark)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(userStagingOutput)

//  Deprecated
//  CommitStagingETL
//    .getDataFrame(rawPullRequestsDF)
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet(commitStagingOutPut)

  EventPayloadStagingETL
    .getDataFrame(rawPullRequestsDF)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(eventPayloadStagingOutput)

}
