package ModeloDimensionalPullRequest.BranchDimension
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExtraccionTablaBranchDimensionBase extends App{

  //Creacion de SparkSession
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val path = "src/datasets/github/githubjson"
  val githubDF = spark.read.option("inferSchema", "true").json(path)

  val baseDF = githubDF.withColumn("base",col("payload.pull_request.base.repo")).select("base")

  val branchIniDF = baseDF
    .withColumn("branch_name",lit("---"))
    .withColumn("protected_branch",lit("---"))
    .withColumn("full_name_repo",col("base.full_name"))
    .withColumn("description_repo",when(col("base.description").isNull,"Description not available").otherwise(col("base.description")))
    .withColumn("default_branch_repo",col("base.default_branch"))
    .withColumn("language_repo",when(col("base.language").isNull,"Languge not available").otherwise(col("base.language")))
    .withColumn("license_repo",when(col("base.license").isNull,"License not available")
      .otherwise(when(col("base.license.name").isNull,"License not available").otherwise(col("base.license.name"))))
    .withColumn("is_forked_repo",when(col("base.fork")=== true,"Has been forked").otherwise("Has not been forked"))
    .withColumn("disabled_repo",lit("---"))
    .withColumn("archived_repo",when(col("base.archived")===true,"Is archived").otherwise("Is not archived"))
    .withColumn("private_repo",when(col("base.private")===true,"Private repository").otherwise("Public repository"))
    .withColumn("size_repo",when(col("base.size").isNull,-1).otherwise(col("base.size")))
    .withColumn("open_issues_repo",col("base.open_issues_count"))
    .withColumn("forks_repo",col("base.forks"))
    .withColumn("repo_id",col("base.id"))
    .withColumn("stargazer_count_repo",col("base.stargazers_count"))
    .withColumn("watchers_count_repo",col("base.watchers_count"))
    .withColumn("pushed_at",date_format(col("base.pushed_at"),"yyyy-MM-dd HH:mm:ss"))

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


  val path2="src/datasets/github/branch/base"
  branchDistintcDF.write.mode(SaveMode.Overwrite).parquet(path2)
  println("--PARQUET DIMENSION BASE CREADO CON ÉXITO--"+java.time.LocalTime.now().toString)
}
