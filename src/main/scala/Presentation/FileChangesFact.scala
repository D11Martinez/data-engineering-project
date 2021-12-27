package presentation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, lit}

object FileChangesFact extends App {
  val commitStagingSource = "src/dataset/staging/commits"

  val commitDimensionSource = "src/dataset/presentation/commitDimension"
  val fileDimensionSource = "src/dataset/presentation/fileDimension"
  val dateDimensionSource = "src/dataset/presentation/DateDimension"
  val timeDimensionSource = "src/dataset/presentation/TimeDimension"
  val branchDimensionSource = "src/dataset/presentation/BranchDimension"
  val userDimensionSource = "src/dataset/presentation/usersDimension"
  val orgDimensionSource = "src/dataset/presentation/organizationsDimension"

  val fileChangesFactOutput = "src/dataset/presentation/fileChangesFact"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  val commitStagingDF = spark.read.parquet(commitStagingSource)
  /*val commitDimDF = spark.read.parquet(commitDimensionSource)
  val fileDimDF = spark.read.parquet(fileDimensionSource)
  val dateDimDF = spark.read.parquet(dateDimensionSource)
  val timeDimDF = spark.read.parquet(timeDimensionSource)
  val branchDimDF = spark.read.parquet(branchDimensionSource)
  val userDimDF = spark.read.parquet(userDimensionSource)
  val organizationDimDF = spark.read.parquet(orgDimensionSource)*/

  commitStagingDF.printSchema(3)
  commitStagingDF.show(10)

  val fileChangesFactDF = commitStagingDF //TODO: Link data correctly
    .withColumn("commit_id", lit(0))
    .withColumn("branch_id", lit(0))
    .withColumn("author_id", lit(0))
    .withColumn("committer_id", lit(0))
    .withColumn("owner_repo", lit(0))
    .withColumn("file_id", lit(0))
    .withColumn("byte_changes", lit(0))
    .withColumn("size", lit(0))
    .withColumn(
      "committed_at_time",
      date_format(col("committer_date"), "HH:mm:ss")
    )
    .withColumn(
      "committed_at_date",
      date_format(col("committer_date"), "yyyy-MM-dd")
    )
    .withColumn("organization_id", lit(0))
    .select(
      col("commit_id"),
      col("branch_id"),
      col("author_id"),
      col("committer_id"),
      col("owner_repo"),
      col("file_id"),
      col("file_additions"),
      col("file_deletions"),
      col("file_changes"),
      col("byte_changes"),
      col("size"),
      col("file_status"),
      col("committed_at_date"),
      col("committed_at_time"),
      col("organization_id")
    )
}
