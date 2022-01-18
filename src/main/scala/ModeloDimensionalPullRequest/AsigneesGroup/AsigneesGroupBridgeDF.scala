package ModeloDimensionalPullRequest.AsigneesGroup
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object AsigneesGroupBridgeDF extends App{
  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path = "src/datasets/github/asignees/asigneesCreate"
  val path2="src/datasets/github/userDimension"
  val path3 = "src/datasets/github/asignees/asigneesUser"

  val asigneesDF = spark.read.parquet(path)
  val userDimensionDF = spark.read.parquet(path2)

  val asgneesExplodeDF = asigneesDF.withColumn("asignee",explode(col("assignes_group_bridge")))
    .withColumn("site_admin", when(col("asignee.site_admin") === true, "User is site admin")
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
    .select(col("asignee.id").as("user_id")
      ,col("asignee.type")
      ,col("asignee.login")
      ,col("site_admin"),col("email"), col("name"), col("location")
      ,col("hireable"), col("bio"), col("company"), col("blog")
      ,col("twitter_username"),
      col("created_at"), col("updated_at"), col("followers"), col("following"),
      col("public_repos"))

  val asigneesUniqueDF = asgneesExplodeDF.dropDuplicates("user_id", "login")

  asigneesUniqueDF.write.mode(SaveMode.Overwrite).parquet(path3)
  println("--PARQUET ASIGNEES CREADO CON ÉXITO--")

}
