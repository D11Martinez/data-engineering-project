package ModeloDimensionalPullRequest.BranchDimension
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExtraccionTablaBranchDImensionHead extends App{

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path = "src/datasets/github/githubjson"
  val githubDF = spark.read.option("inferSchema", "true").json(path)

  val headDF = githubDF.withColumn("head",col("payload.pull_request.head.repo")).select("head")

  val branchIniDF = headDF
    .withColumn("branch_name",lit("---"))
    .withColumn("protected_branch",lit("---"))
    .withColumn("full_name_repo",col("head.full_name"))
    .withColumn("description_repo",when(col("head.description").isNull,"Description not available").otherwise(col("head.description")))
    .withColumn("default_branch_repo",col("head.default_branch"))
    .withColumn("language_repo",when(col("head.language").isNull,"Languge not available").otherwise(col("head.language")))
    .withColumn("license_repo",when(col("head.license").isNull,"License not available")
      .otherwise(when(col("head.license.name").isNull,"License not available").otherwise(col("head.license.name"))))
    .withColumn("is_forked_repo",when(col("head.fork")=== true,"Has been forked").otherwise("Has not been forked"))
    .withColumn("disabled_repo",lit("---"))
    .withColumn("archived_repo",when(col("head.archived")===true,"Is archived").otherwise("Is not archived"))
    .withColumn("private_repo",when(col("head.private")===true,"Private repository").otherwise("Public repository"))
    .withColumn("size_repo",when(col("head.size").isNull,-1).otherwise(col("head.size")))
    .withColumn("open_issues_repo",col("head.open_issues_count"))
    .withColumn("forks_repo",col("head.forks"))
    .withColumn("repo_id",col("head.id"))
    .withColumn("stargazer_count_repo",col("head.stargazers_count"))
    .withColumn("watchers_count_repo",col("head.watchers_count"))
    .withColumn("pushed_at",date_format(col("head.pushed_at"),"yyyy-MM-dd HH:mm:ss"))

  val branchSelectDF= branchIniDF.select("branch_name","protected_branch","full_name_repo","description_repo",
  "default_branch_repo","language_repo","license_repo","is_forked_repo","disabled_repo","archived_repo","private_repo",
  "size_repo","open_issues_repo","forks_repo","repo_id","stargazer_count_repo","watchers_count_repo","pushed_at")


  val branchUniqueDF = branchSelectDF.as("branch1").join(branchSelectDF.as("branch2"))
    .filter((col("branch1.repo_id")===col("branch2.repo_id"))
      &&(col("branch1.pushed_at").gt(col("branch2.pushed_at"))))
    .select("branch1.branch_name","branch1.protected_branch","branch1.full_name_repo","branch1.description_repo",
      "branch1.default_branch_repo","branch1.language_repo","branch1.license_repo","branch1.is_forked_repo",
      "branch1.disabled_repo","branch1.archived_repo","branch1.private_repo",
      "branch1.size_repo","branch1.open_issues_repo","branch1.forks_repo","branch1.repo_id","branch1.stargazer_count_repo"
      ,"branch1.watchers_count_repo")
    .orderBy(col("branch1.repo_id"))

  val branchDistintcDF = branchUniqueDF.dropDuplicates("full_name_repo","repo_id")


  val path2="src/datasets/github/branch/head"
  branchDistintcDF.write.mode(SaveMode.Overwrite).parquet(path2)
  println("--PARQUET DIMENSION HEAD CREADO CON ÉXITO--"+java.time.LocalTime.now().toString)
}
