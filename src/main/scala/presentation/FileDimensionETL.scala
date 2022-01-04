package presentation

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileDimensionETL {

  val getLastUDF: UserDefinedFunction = udf((xs: Seq[String]) => xs.last)

  val getFilePathUDF: UserDefinedFunction =
    udf((elements: Seq[String]) => {
      elements.size match {
        case 0 => ""
        case 1 => ""
        case _ => elements.slice(0, elements.size - 1).mkString("/")
      }
    })

  val getLanguageFromExtension: UserDefinedFunction = udf((extension: String) => {
    val javaExtensions = Array("java", "class", "jar", "jmod", "jsp")
    val javaScriptExtensions = Array("js", "cjs", "mjs")
    val typeScriptExtensions = Array("ts", "tsx")
    val phpExtensions = Array("php", "phar", "phtml", "pht", "phps")
    val pythonExtensions = Array("py", "pyi", "pyc", "pyd", "pyo", "pyw", "pyz")
    val scalaExtensions = Array("scala", "sc")
    val cExtensions = Array("c", "cc", "cpp", "cxx", "c++", "h", "H", "hh", "hpp", "hxx", "h++")
    val rExtensions = Array("r", "rdata", "rds", "rda")
    val goExtensions = Array("go")
    val rubyExtensions = Array("rb")
    val dartExtensions = Array("dart")
    val cCharExtensions = Array("cs", "csx")
    val kotlinExtensions = Array("kt", "kts", "ktm")
    val swiftExtensions = Array("swift", "SWIFT")
    val rustExtensions = Array("rs", "rlib")
    val luaExtensions = Array("lua")
    val sqlExtensions = Array("sql")

    extension match {
      case x if typeScriptExtensions.contains(x) => "TypeScript"
      case x if javaScriptExtensions.contains(x) => "JavaScript"
      case x if pythonExtensions.contains(x)     => "Python"
      case x if javaExtensions.contains(x)       => "Java"
      case x if scalaExtensions.contains(x)      => "Scala"
      case x if cExtensions.contains(x)          => "C/C++"
      case x if rExtensions.contains(x)          => "R"
      case x if phpExtensions.contains(x)        => "PHP"
      case x if cCharExtensions.contains(x)      => "C#"
      case x if kotlinExtensions.contains(x)     => "Kotlin"
      case x if swiftExtensions.contains(x)      => "Swift"
      case x if goExtensions.contains(x)         => "Go"
      case x if dartExtensions.contains(x)       => "Dart"
      case x if rubyExtensions.contains(x)       => "Ruby"
      case x if rustExtensions.contains(x)       => "Rust"
      case x if luaExtensions.contains(x)        => "Lua"
      case x if sqlExtensions.contains(x)        => "SQL"
      case _                                     => "Other"
    }
  })

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {

    val fileDimensionDF =
      eventPayloadStagingDF
        .select(
          "pull_request_commit_file_sha",
          "pull_request_commit_file_filename"
        )
        .withColumn(
          "sha",
          when(
            col("pull_request_commit_file_sha").isNull,
            "File sha not available"
          )
            .otherwise(
              when(
                length(trim(col("pull_request_commit_file_sha"))) === 0,
                "Empty value"
              )
                .otherwise(col("pull_request_commit_file_sha"))
            )
        )
        .withColumn(
          "file_name",
          when(col("pull_request_commit_file_filename").isNull, lit(null)).otherwise(
            getLastUDF(split(col("pull_request_commit_file_filename"), "/"))
          )
        )
        .withColumn(
          "name",
          when(col("file_name").isNull, "Not available")
            .otherwise(
              when(length(trim(col("file_name"))) === 0, "Empty value")
                .otherwise(col("file_name"))
            )
        )
        .withColumn(
          "file_path",
          when(col("pull_request_commit_file_filename").isNull, lit(null)).otherwise(
            getFilePathUDF(split(col("pull_request_commit_file_filename"), "/"))
          )
        )
        .withColumn(
          "path",
          when(col("file_path").isNull, "Not available")
            .otherwise(
              when(length(trim(col("file_path"))) === 0, "Root path")
                .otherwise(col("file_path"))
            )
        )
        .withColumn(
          "file_extension",
          when(col("file_name").isNull, lit(null)).otherwise(
            getLastUDF(split(col("file_name"), "\\."))
          )
        )
        .withColumn(
          "extension",
          when(col("file_extension").isNull, "Not available")
            .otherwise(
              when(length(trim(col("file_extension"))) === 0, "Empty value")
                .otherwise(col("file_extension"))
            )
        )
        .withColumn(
          "full_file_name",
          when(
            col("pull_request_commit_file_filename").isNull,
            "Full filename not available"
          )
            .otherwise(
              when(
                length(trim(col("pull_request_commit_file_filename"))) === 0,
                "Empty value"
              )
                .otherwise(col("pull_request_commit_file_filename"))
            )
        )
        .withColumn(
          "language",
          when(col("extension") === "Empty value", "Empty Value")
            .otherwise(
              when(col("extension") === "Not available", "Not available")
                .otherwise(getLanguageFromExtension(col("extension")))
            )
        )
        .drop(
          "pull_request_commit_file_sha",
          "pull_request_commit_file_filename",
          "file_name",
          "file_path",
          "file_extension"
        )
        .distinct()
        .withColumn("pk_id", monotonically_increasing_id())
        .select("*")

    val fileUndefinedRowDF = sparkSession
      .createDataFrame(NullDimension.fileDataNull)
      .toDF(NullDimension.fileColumnNull: _*)

    val fileDimensionWithUndefinedRowDF =
      fileDimensionDF.unionByName(fileUndefinedRowDF)

    fileDimensionWithUndefinedRowDF.printSchema(3)
    fileDimensionWithUndefinedRowDF.show(10)

    fileDimensionWithUndefinedRowDF
  }

}
