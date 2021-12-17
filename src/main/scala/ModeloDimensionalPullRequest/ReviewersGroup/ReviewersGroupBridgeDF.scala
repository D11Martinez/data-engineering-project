package ModeloDimensionalPullRequest.ReviewersGroup
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReviewersGroupBridgeDF extends App{
  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path = "src/datasets/github/reviewers/reviewersCreate"
  val path2="src/datasets/github/userDimension"
  val path3 = "src/datasets/github/reviewers/reviewersUser"

  val reviewersDF = spark.read.parquet(path)
  val userDimensionDF = spark.read.parquet(path2)

  val reviewersExplodeDF = reviewersDF.withColumn("reviewer",explode(col("reviewers_group_bridge")))
    .withColumn("site_admin", when(col("reviewer.site_admin") === true, "User is site admin")
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
    .select(col("reviewer.id").as("user_id")
      ,col("reviewer.type")
      ,col("reviewer.login")
      ,col("site_admin"),col("email"), col("name"), col("location")
      ,col("hireable"), col("bio"), col("company"), col("blog")
      ,col("twitter_username"),
      col("created_at"), col("updated_at"), col("followers"), col("following"),
      col("public_repos"))

  val reviewersUniqueDF = reviewersExplodeDF.dropDuplicates("user_id", "login")
  reviewersUniqueDF.write.mode(SaveMode.Overwrite).parquet(path3)
  println("--PARQUET REVIEWERS CREADO CON ÉXITO--")

}
