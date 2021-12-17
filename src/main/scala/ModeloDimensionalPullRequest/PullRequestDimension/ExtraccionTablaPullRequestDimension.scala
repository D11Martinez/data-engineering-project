package ModeloDimensionalPullRequest.PullRequestDimension

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExtraccionTablaPullRequestDimension extends App {

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()


  val path2 = "src/datasets/github/pullrequest"
  val path = "src/datasets/github/githubjson"
  val path3 = "src/datasets/github/nuevojson/nuevo2.json"
  val githubDF = spark.read.option("inferSchema", "true").json(path3)

  //githubDF.printSchema()
  val payLoadDF = githubDF.withColumn("pullrequest", col("payload.pull_request"))

  val pullRequestDimensionDF = payLoadDF.withColumn("id", monotonically_increasing_id())
    .withColumn("pull_request_id", col("pullrequest.id"))
    .withColumn("locked", when(col("pullrequest.locked") === true, "Is locked").otherwise("Is not locked"))
    .withColumn("merged", when(col("pullrequest.merged") === true, "Is merged").otherwise("Is not merged"))
    .withColumn("mergeable", when(col("pullrequest.mergeable") === true, "Is mergeable")
      .otherwise(when(col("pullrequest.mergeable") === false, "Is not mergeable").otherwise("Not available")))
    .withColumn("body", when(col("pullrequest.body").isNull, "No Pull Request body available")
      .otherwise(substring(col("pullrequest.body"), 1, 50)))
    .select(col("id"), col("pull_request_id"), col("pullrequest.number"), col("pullrequest.title"),
      col("body"), col("locked"), col("merged")
      , col("mergeable"), col("pullrequest.merge_commit_sha"), col("pullrequest.author_association"))

  //pullRequestDimensionDF.limit(20).show()
  println("--- PARQUET PULLREQUESTDIMENSION CREADO CON EXITO ---")
  pullRequestDimensionDF.write.mode(SaveMode.Overwrite).parquet(path2)
}
