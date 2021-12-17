package ModeloDimensionalPullRequest.UserDimension

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, when}

object ExtraccionTablaUserReviewersGroup extends App {
  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path = "src/datasets/github/reviewers/reviewersCreate"
  val path2 = "src/datasets/github/userDimension"
  val path3 = "src/datasets/github/reviewers/reviewersDF"

  val reviewersDF = spark.read.parquet(path)
  val userDF = spark.read.parquet(path2)


  val reviewersExplodeDF = reviewersDF.withColumn("reviewer",explode(col("reviewers_group_bridge")))
    .select(col("reviewer.id").as("user_id"),col("reviewers_group_id"))

  val reviewersFinalDF =  reviewersExplodeDF.as("reviewer").join(userDF.as("user"),
    reviewersExplodeDF("user_id")===userDF("user_id"),"inner")
    .select(col("reviewer.reviewers_group_id"),col("user.id").as("user_dim_id"))

reviewersFinalDF.write.mode(SaveMode.Overwrite).parquet(path3)
println("-- PARQUET REVIEWERS CREADO CON EXITO--")



}
