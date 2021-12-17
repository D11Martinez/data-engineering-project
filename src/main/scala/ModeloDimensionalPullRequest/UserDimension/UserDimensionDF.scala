package ModeloDimensionalPullRequest.UserDimension
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserDimensionDF extends App{

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path1 = "src/datasets/github/user"
  val path2 = "src/datasets/github/owner"
  val path3 = "src/datasets/github/merged"
  val path4 = "src/datasets/github/actor"
  val path5="src/datasets/github/userDimension"
  val path6 = "src/datasets/github/asignees/asigneesUser"
  val path7 = "src/datasets/github/reviewers/reviewersUser"

  val userDF = spark.read.parquet(path1)
  val ownerDF = spark.read.parquet(path2)
  val mergedDF = spark.read.parquet(path3)
  val actorDF = spark.read.parquet(path4)
  val assigneesDF = spark.read.parquet(path6)
  val reviewersDF = spark.read.parquet(path7)

  val userDimensionDF = userDF.unionByName(ownerDF).unionByName(mergedDF).unionByName(actorDF)
    .unionByName(assigneesDF).unionByName(reviewersDF)

  val userDimensionFinalDF = userDimensionDF.dropDuplicates("login","user_id")
    .withColumn("id",lit(monotonically_increasing_id()))

  userDimensionFinalDF.write.mode(SaveMode.Overwrite).parquet(path5)
  println("--PARQUET USER DIMENSION CREADO CON ÉXITO--")

}
