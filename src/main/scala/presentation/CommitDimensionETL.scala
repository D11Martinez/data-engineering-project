package presentation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import presentation.FileDimensionETL.uuid

object CommitDimensionETL extends App {
  // user defined functions
  val uuid = udf(() => java.util.UUID.randomUUID().toString)

  val getLastUDF = udf((xs: Seq[String]) => xs.last)

  val getFilePathUDF =
    udf((elements: Seq[String]) => {
      elements.size match {
        case 0 => ""
        case 1 => ""
        case _ => elements.slice(0, elements.size - 1).mkString("/")
      }
    })

  // configuration paths
  val commitStagingSource = "src/dataset/staging/commits"
  val commitDimensionOutput = "src/dataset/presentation/commit-dimension"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val commitStagingDF =
    spark.read.parquet(commitStagingSource)

  commitStagingDF.printSchema(3)

  val fileDimensionDF = commitStagingDF
    .select(
      col("sha"),
      col("message"),
      col("message_good_practice"),
      col("author_id"), // TODO: Refer to a user in UserDimension
      col("author_name"),
      col("author_email"),
      col("author_date"),
      col("committer_id"), // TODO: Refer to a user in UserDimension
      col("committer_name"),
      col("committer_email"),
      col("committer_date"),
      col("total_changes"),
      col("total_additions"),
      col("total_deletions"),
      col("commit.comment_count") // TODO: Change field name
    )
    .distinct()
    .withColumn("id", uuid())
    .select(
      col("id"),
      col("sha"),
      col("message"),
      col("message_good_practice"),
      col("author_id"),
      col("author_name"),
      col("author_email"),
      col("author_date"),
      col("committer_id"),
      col("committer_name"),
      col("committer_email"),
      col("committer_date"),
      col("total_changes"),
      col("total_additions"),
      col("total_deletions"),
      col("commit.comment_count")
    )

}
