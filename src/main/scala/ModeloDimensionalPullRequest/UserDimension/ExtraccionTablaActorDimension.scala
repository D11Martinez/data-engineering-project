package ModeloDimensionalPullRequest.UserDimension

import org.apache.spark.sql.functions.{array, array_contains, col, lit, monotonically_increasing_id, udf, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.apache.spark.sql.functions.vector_to_array

import scala.util.Try

object ExtraccionTablaActorDimension extends App {

  /*implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def hasColumn(colName: String):Any = {
      if(df.columns.contains(colName)){
        df.col(colName)
      }else{
        "data no available"
      }
    }
  }*/

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()


  val path2 = "src/datasets/github/actor"
  val path = "src/datasets/github/githubjson"
  val path3 = "src/datasets/github/nuevojson/nuevo2.json"
  val githubDF = spark.read.option("inferSchema", "true").json(path3)
  //githubDF.printSchema()

  val actorDF = githubDF
    .withColumn("user", col("actor"))
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
    .select(col("user.login").as("login"), col("user.id").cast(StringType).as("user_id")
      , col("type"), col("site_admin"), col("email"), col("name"), col("location"),
      col("hireable"), col("bio"), col("company"), col("blog"), col("twitter_username"),
      col("created_at"), col("updated_at"), col("followers"), col("following"),col("public_repos"))


  val actorUniqueDF = actorDF.as("actor1").join(actorDF.as("actor2"))
    .filter((col("actor1.user_id") === col("actor2.user_id"))&&
      (col("actor1.updated_at").gt(col("actor2.updated_at")) ||
        col("actor1.updated_at") === col("actor2.updated_at")))
    .select(col("actor1.login"), col("actor1.user_id")
      , col("actor1.type"), col("actor1.site_admin"),
      col("actor1.email"), col("actor1.name"), col("actor1.location"),
      col("actor1.hireable"), col("actor1.bio"), col("actor1.company"),
      col("actor1.blog"), col("actor1.twitter_username"),
      col("actor1.created_at"), col("actor1.updated_at"),
      col("actor1.followers"), col("actor1.following"),col("actor1.public_repos"))


  //Eliminando duplicados
  val userDistintcDF = actorUniqueDF.dropDuplicates("user_id", "login")

  userDistintcDF.write.mode(SaveMode.Overwrite).parquet(path2)

  println("--PARQUET ACTOR CREADO CON ÉXITO--")
}
