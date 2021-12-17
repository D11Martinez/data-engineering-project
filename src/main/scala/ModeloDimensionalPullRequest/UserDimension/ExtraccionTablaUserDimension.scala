package ModeloDimensionalPullRequest.UserDimension

import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExtraccionTablaUserDimension extends App {

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path2 = "src/datasets/github/user"
  val path = "src/datasets/github/githubjson"
  val path3 = "src/datasets/github/nuevojson/nuevo2.json"
  val githubDF = spark.read.option("inferSchema", "true").json(path3)

  val userColDF = githubDF.withColumn("user", col("payload.pull_request.user"))
    .withColumn("email", when(col("user.email").isNull,"Email not available")
      .otherwise(col("user.email")))
    .withColumn("name", when(col("user.name").isNull,"Name not available")
      .otherwise(col("user.name")))
    .withColumn("location", when(col("user.location").isNull,"Location not available")
      .otherwise(col("user.location")))
    .withColumn("site_admin", when(col("user.site_admin").isNull,"Name not available")
      .when(col("user.site_admin")===true,"User is site admin")
      .otherwise("User is not site admin"))
    .withColumn("type", when(col("user.type").isNull,"Type not available")
      .otherwise(col("user.type")))
    .withColumn("hireable", when(col("user.hireable").isNull,"Not available")
      .when(col("user.hireable")===true,"Is hireable")
      .otherwise("Is not hireable"))
    .withColumn("bio", when(col("user.bio").isNull,"Biography not available")
      .otherwise(col("user.bio")))
    .withColumn("company", when(col("user.company").isNull,"Company not available")
      .otherwise(col("user.company")))
    .withColumn("blog", when(col("user.blog").isNull,"Blog not available")
      .otherwise(col("user.blog")))
    .withColumn("twitter_username", when(col("user.twitter_username").isNull,"Twitter username not available")
      .otherwise(col("user.twitter_username")))
    .withColumn("created_at",  when(col("user.created_at").isNull,"Created at not available")
      .otherwise(col("user.created_at")))
    .withColumn("updated_at", when(col("user.updated_at").isNull,"Updated at not available")
      .otherwise(col("user.updated_at")))
    .withColumn("followers", when(col("user.followers").isNull,"Followers not available")
      .otherwise(col("user.followers")))
    .withColumn("following", when(col("user.following").isNull,"Following not available")
      .otherwise(col("user.following")))
    .withColumn("public_repos", when(col("user.public_repos").isNull,"Public repos not available")
      .otherwise(col("user.public_repos")))
    .select(col("user.login"), col("user.id").cast(StringType).as("user_id")
      , col("type"), col("site_admin"), col("email"), col("name"), col("location"),
      col("hireable"), col("bio"), col("company"), col("blog"), col("twitter_username"),
      col("created_at"), col("updated_at"), col("followers"), col("following"),col("public_repos"))

  val userUniqueDF = userColDF.as("user1").join(userColDF.as("user2"))
    .filter((col("user1.user_id") === col("user2.user_id"))&&
      (col("user1.updated_at").gt(col("user2.updated_at")) ||
        col("user1.updated_at") === col("user2.updated_at")))
    .select(col("user1.login"), col("user1.user_id")
      , col("user1.type"), col("user1.site_admin"),
      col("user1.email"), col("user1.name"), col("user1.location"),
      col("user1.hireable"), col("user1.bio"), col("user1.company"),
      col("user1.blog"), col("user1.twitter_username"),
      col("user1.created_at"), col("user1.updated_at"),
      col("user1.followers"), col("user1.following"),col("user1.public_repos"))

  //Eliminando duplicados
  val userDistintcDF = userUniqueDF.dropDuplicates("user_id", "login")

  userDistintcDF.write.mode(SaveMode.Overwrite).parquet(path2)
  println("--PARQUET USER CREADO CON ÉXITO--")

}
