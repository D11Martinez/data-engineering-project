package ModeloDimensionalPullRequest.UserDimension

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, when}

object ExtraccionTablaUserAssigneesGroup extends App{
  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path = "src/datasets/github/asignees/asigneesCreate"
  val path2 = "src/datasets/github/userDimension"
  val path3 = "src/datasets/github/asignees/asigneesDF"

  val asigneesDF = spark.read.parquet(path)
  val userDF = spark.read.parquet(path2)

  val asgneesExplodeDF = asigneesDF.withColumn("asignee",explode(col("assignes_group_bridge")))
    .select(col("asignee.id").as("user_id"),col("assignee_group_id"))

val asigneesFinalDF =  asgneesExplodeDF.as("asignees").join(userDF.as("user"),
  asgneesExplodeDF("user_id")===userDF("user_id"),"inner")
  .select(col("asignees.assignee_group_id"),col("user.id").as("user_dim_id"))

 // asigneesFinalDF.show(10,false)
  asigneesFinalDF.write.mode(SaveMode.Overwrite).parquet(path3)

}
