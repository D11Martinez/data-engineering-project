package ModeloDimensionalPullRequest.UserDimension

import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExtraccionTablaOwnerDimension extends App {

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path2 = "src/datasets/github/owner"
  val path = "src/datasets/github/githubjson"
  val path3 = "src/datasets/github/nuevojson/nuevo2.json"
  val githubDF = spark.read.option("inferSchema", "true").json(path3)

  val ownerColDF = githubDF.withColumn("user", col("payload.pull_request.head.repo.owner"))
    .withColumn("site_admin", when(col("user.site_admin") === true, "User is site admin")
      .otherwise("User is not site admin"))
    .withColumn("email", lit("Email not available"))
    .withColumn("name", lit("Name not available"))
    .withColumn("location", lit("Location not available"))
    .withColumn("hireable", lit("Not available"))
    .withColumn("bio", lit("Biography not available"))
    .withColumn("company", lit("Company not available"))
    .withColumn("blog", lit("Blog not available"))
    .withColumn("twitter_username", lit("Twitter username not available"))
    .withColumn("created_at", lit("Created at not available"))
    .withColumn("updated_at", lit("Updated at not available"))
    .withColumn("followers", lit("Followers not available"))
    .withColumn("following", lit("Following not available"))
    .withColumn("public_repos", lit("Public repos not available"))
    .select(col("user.login"), col("user.id").cast(StringType).as("user_id")
      , col("user.type"), col("site_admin"), col("email"), col("name"), col("location"),
      col("hireable"), col("bio"), col("company"), col("blog"), col("twitter_username"),
      col("created_at"), col("updated_at"), col("followers"), col("following"),col("public_repos"))


  //Eliminando duplicados
  val userDistintcDF = ownerColDF.dropDuplicates("user_id", "login")

  userDistintcDF.write.mode(SaveMode.Overwrite).parquet(path2)
  println("--PARQUET OWNER CREADO CON ÉXITO--")


}
