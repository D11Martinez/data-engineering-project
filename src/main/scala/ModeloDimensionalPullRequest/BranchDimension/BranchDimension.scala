package ModeloDimensionalPullRequest.BranchDimension
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
object BranchDimension extends App{
  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path1="src/datasets/github/branch/head"
  val path2="src/datasets/github/branch/base"

  val branchDF = spark.read.parquet(path1,path2)

  val branchDistintcDF = branchDF.dropDuplicates("full_name_repo","repo_id")
    .withColumn("id",monotonically_increasing_id())

  val path3="src/datasets/github/branch/total"
  branchDistintcDF.write.mode(SaveMode.Overwrite).parquet(path3)

  println("--PARQUET DIMENSION TOTAL CREADO CON ÉXITO--"+java.time.LocalTime.now().toString)

}
