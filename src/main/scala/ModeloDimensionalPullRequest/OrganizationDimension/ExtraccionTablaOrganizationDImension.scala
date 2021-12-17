package ModeloDimensionalPullRequest.OrganizationDimension

import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExtraccionTablaOrganizationDImension extends App {

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path2 = "src/datasets/github/organization"
  val path = "src/datasets/github/githubjson"
  val githubDF = spark.read.option("inferSchema", "true").json(path)

  val orgDF = githubDF
    .withColumn("organization_id", col("org.id"))
    .withColumn("email", lit("---"))
    .withColumn("name", lit("---"))
    .withColumn("location", lit("---"))
    .withColumn("site_admin", lit("---"))
    .withColumn("type", lit("---"))
    .withColumn("hireable", lit("---"))
    .withColumn("bio", lit("---"))
    .withColumn("company", lit("---"))
    .withColumn("blog", lit("---"))
    .withColumn("twitter_username", lit("---"))
    .withColumn("created_at", lit("00-00-00"))
    .withColumn("updated_at", lit("00-00-00"))
    .withColumn("followers", lit(0))
    .withColumn("following", lit(0))
    .withColumn("is_verified", lit("---"))
    .withColumn("has_organization_projects", lit("---"))
    .withColumn("has_repository_projects", lit("---"))
    .withColumn("public_repos", lit(0))
    .withColumn("id", lit(monotonically_increasing_id()))
    .withColumn("description", lit("---"))
    .select(col("id"), col("organization_id"), col("org.login"), col("email"), col("type"),
      col("name"), col("description"), col("company"), col("location"), col("is_verified"),
      col("has_organization_projects"), col("has_repository_projects"), col("blog"), col("twitter_username"),
      col("created_at"), col("updated_at"), col("public_repos"), col("followers"), col("following"))

  orgDF.write.mode(SaveMode.Overwrite).parquet(path2)
}
