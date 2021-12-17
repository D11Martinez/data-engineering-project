package ModeloDimensionalPullRequest
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.UUID.randomUUID
object ExtraccionFactTablePullRequest extends App{

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path3 = "src/datasets/github/DateDimension/DateDimensionDF"
  val path2 = "src/datasets/github/pullrequestFactTable"
  val path = "src/datasets/github/githubjson"
  val path4 = "src/datasets/github/asignees/asigneesCreate"
  val path5 = "src/datasets/github/reviewers/reviewersCreate"

  val githubDF = spark.read.option("inferSchema", "true").json(path)
  val dateGlobalDF = spark.read.parquet(path3).select("date_key","date")

  val pullRequestFTDF = githubDF.withColumn("id",monotonically_increasing_id())
    .withColumn("pullrequest",col("payload.pull_request"))
    .withColumn("head_id_temporal",col("pullrequest.head.repo.id"))
    .withColumn("base_id_temporal",col("pullrequest.base.repo.id"))
    .withColumn("pullrequest_id_temporal",col("pullrequest.id"))
    .withColumn("closed_at_date",when(col("pullrequest.closed_at").isNull,"Has not been closed")
      .otherwise(date_format(col("pullrequest.closed_at"),"yyyy-MM-dd")))
    .withColumn("merged_at_date",when(col("pullrequest.merged_at").isNull,"Has not been merged")
      .otherwise(date_format(col("pullrequest.merged_at"),"yyyy-MM-dd")))
    .withColumn("closed_at_time",when(col("pullrequest.closed_at").isNull,"Has not been closed")
      .otherwise(date_format(col("pullrequest.closed_at"),"HH:mm:ss")))
    .withColumn("merged_at_time",when(col("pullrequest.merged_at").isNull,"Has not been merged")
      .otherwise(date_format(col("pullrequest.merged_at"),"HH:mm:ss")))
    .withColumn("create_at_date_temporal",date_format(col("pullrequest.created_at"),"yyyy-MM-dd"))
    .withColumn("create_at_time_temporal",date_format(col("pullrequest.created_at"),"HH:mm:ss"))
    .withColumn("user_id_temporal",col("pullrequest.user.id"))
    .withColumn("actor_id_temporal",col("actor.id"))
    .withColumn("org_id_temporal",col("org.id"))
    .withColumn("owner_id_temporal",col("pullrequest.head.repo.owner.id"))
    .withColumn("merged_id_temporal",col("pullrequest.merged_by.id"))
    .withColumn("assignee_group_id",when(col("pullrequest.assignees").getItem(0).isNull,lit(-1)).otherwise(monotonically_increasing_id()))
    .withColumn("reviewers_group_id",when(col("pullrequest.requested_reviewers").getItem(0).isNull,lit(-1)).otherwise(monotonically_increasing_id()))
    .withColumn("assignes_group_bridge",col("pullrequest.assignees"))
    .withColumn("reviewers_group_bridge",col("pullrequest.requested_reviewers"))
    //.withColumn("create_at_time",lit("--"))
    .select(col("id"),col("pullrequest.state"),col("pullrequest.additions"),col("pullrequest.deletions"),
    col("pullrequest.changed_files"),col("pullrequest.commits"),col("pullrequest.comments"),
    col("pullrequest.review_comments"),date_format(col("pullrequest.updated_at"),"yyyy-MM-dd").as("updated_at_date"),
      date_format(col("pullrequest.updated_at"),"HH:mm:ss").as("updated_at_time"),
    col("closed_at_date"), col("merged_at_date"),col("closed_at_time"),col("merged_at_time")
      ,col("create_at_date_temporal"),col("create_at_time_temporal"),col("pullrequest_id_temporal"),col("head_id_temporal"),
      col("base_id_temporal"),col("user_id_temporal"),col("actor_id_temporal"),col("org_id_temporal"),
      col("owner_id_temporal"),col("merged_id_temporal"),col("assignee_group_id"),col("reviewers_group_id"),
      col("assignes_group_bridge"),col("reviewers_group_bridge"))

  //extrayendo datos de asignees group
  val asigneesgroupDF = pullRequestFTDF.filter(col("assignee_group_id") =!=  1)
    .select("assignee_group_id","assignes_group_bridge")
  asigneesgroupDF.write.mode(SaveMode.Overwrite).parquet(path4)
  println("--PARQUET ASIGNEES GROUP CREADO CON ÉXITO--")

  //extrayendo datos de reviewers group
  val reviewersgroupDF = pullRequestFTDF.filter(col("reviewers_group_id") =!=  1)
    .select("reviewers_group_id","reviewers_group_bridge")
  reviewersgroupDF.write.mode(SaveMode.Overwrite).parquet(path5)
  println("--PARQUET REVIEWERS GROUP CREADO CON ÉXITO--")

  //pullrequestfatc
  val pullRequestFTDF2 = pullRequestFTDF.select(col("id"),col("state"),col("additions"),col("deletions"),
    col("changed_files"),col("commits"),col("comments"),
    col("review_comments"),col("updated_at_date"),
    col("updated_at_time"),
    col("closed_at_date"), col("merged_at_date"),col("closed_at_time"),col("merged_at_time")
    ,col("create_at_date_temporal"),col("create_at_time_temporal"),col("pullrequest_id_temporal"),col("head_id_temporal"),
    col("base_id_temporal"),col("user_id_temporal"),col("actor_id_temporal"),col("org_id_temporal"),
    col("owner_id_temporal"),col("merged_id_temporal"),col("assignee_group_id"),col("reviewers_group_id"),
    )

  pullRequestFTDF2.write.mode(SaveMode.Overwrite).parquet(path2)
  println("--PARQUET PULL REQUEST FACT TABLE CREADO CON ÉXITO--")
}
