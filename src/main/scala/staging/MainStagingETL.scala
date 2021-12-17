package staging

import org.apache.spark.sql.{SaveMode, SparkSession}

object MainStagingETL extends App {
  // Add your config paths here
  val rawPullRequestPath = ""

  // Add your config outputs paths here
  val userStagingOutput = ""
  val eventPayloadStagingOutput = ""

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val rawPullRequestsDF = spark.read
    .json(rawPullRequestPath)

  UserStagingETL
    .getDataFrame(rawPullRequestsDF)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(userStagingOutput)

  EventPayloadStagingETL
    .getDataFrame(rawPullRequestsDF)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(eventPayloadStagingOutput)

}
