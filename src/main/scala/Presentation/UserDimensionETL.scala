package presentation

import presentation.CustomUDF.getIntervalCategory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object UserDimensionETL {

  val userDimensionPath = "src/dataset/presentation/user-dimension"
  val userDimensionTempPath = val FILE_DIMENSION_TEMP_PATH =
    "src/dataset/presentation/temp/user-dimension"

      val getIntervalCategory: UserDefinedFunction = udf((quantityStr: String) => {
    quantityStr.toInt match {
      case x if 0 until 16 contains x       => "0 - 15"
      case x if 16 until 51 contains x      => "16 - 50"
      case x if 51 until 101 contains x     => "51 - 100"
      case x if 101 until 501 contains x    => "101 - 500"
      case x if 501 until 1001 contains x   => "501 - 1000"
      case x if 1001 until 2501 contains x  => "1001 - 2500"
      case x if 2501 until 5001 contains x  => "2501 - 5000"
      case x if 5001 until 10001 contains x => "5001 - 10000"
      case x if x > 10000                   => "10000+"
      case _                                => "Uncategorized"
    }
  })

    val userDimensionSchema: StructType = StructType(
    Array(
      StructField("pk_id", LongType, nullable = false),
      StructField("user_id", StringType, nullable = false),
      StructField("login", StringType, nullable = false),
      StructField("email", StringType, nullable = false),
      StructField("type", StringType, nullable = false),
      StructField("site_admin", StringType, nullable = false),      
      StructField("name", StringType, nullable = false),
      StructField("location", StringType, nullable = false),
      StructField("hireable", StringType, nullable = false),
      StructField("bio", StringType, nullable = false),
      StructField("company", StringType, nullable = false),
      StructField("blog", StringType, nullable = false),      
      StructField("twitter_username", StringType, true),
      StructField("created_at", StringType, nullable = false),
      StructField("updated_at", StringType, nullable = false),
      StructField("public_repos", StringType, nullable = false),
      StructField("followers", StringType, nullable = false),
      StructField("following", StringType, nullable = false),
      StructField("followers_category", StringType, nullable = false),
      StructField("following_category", StringType, nullable = false),
      StructField("public_repos_category", StringType, nullable = false)
    )
  )

  def transform(eventPayloadStagingDF: DataFrame): DataFrame = {
  val userDimension = eventPayloadStagingDF
      .dropDuplicates("id")
      .withColumn("user_id", col("id"))
      .withColumn(
        "email",
        when(col("email").isNull, "Email not available")
          .otherwise(col("email"))
      )
      .withColumn(
        "name",
        when(col("name").isNull, "Name not available")
          .otherwise(col("name"))
      )
      .withColumn(
        "location",
        when(col("location").isNull, "Location not available")
          .otherwise(col("location"))
      )
      .withColumn(
        "site_admin",
        when(col("site_admin").isNull, "Name not available")
          .when(col("site_admin") === true, "User is site admin")
          .otherwise("User is not site admin")
      )
      .withColumn(
        "type",
        when(col("type").isNull, "Type not available")
          .otherwise(col("type"))
      )
      .withColumn(
        "hireable",
        when(col("hireable").isNull, "Not available")
          .when(col("hireable") === true, "Is hireable")
          .otherwise("Is not hireable")
      )
      .withColumn(
        "bio",
        when(col("bio").isNull, "Biography not available")
          .otherwise(col("bio"))
      )
      .withColumn(
        "company",
        when(col("company").isNull, "Company not available")
          .otherwise(col("company"))
      )
      .withColumn(
        "blog",
        when(col("blog").isNull, "Blog not available")
          .otherwise(col("blog"))
      )
      .withColumn(
        "twitter_username",
        when(col("twitter_username").isNull, "Twitter username not available")
          .otherwise(col("twitter_username"))
      )
      .withColumn(
        "created_at",
        when(col("created_at").isNull, "Created at not available")
          .otherwise(col("created_at"))
      )
      .withColumn(
        "updated_at",
        when(col("updated_at").isNull, "Updated at not available")
          .otherwise(col("updated_at"))
      )
      .withColumn(
        "followers",
        when(col("followers").isNull, "Followers not available")
          .otherwise(col("followers"))
      )
      .withColumn(
        "following",
        when(col("following").isNull, "Following not available")
          .otherwise(col("following"))
      )
      .withColumn(
        "public_repos",
        when(col("public_repos").isNull, "Public repos not available")
          .otherwise(col("public_repos"))
      )
      .withColumn(
        "followers_category",
        when(col("followers") === "Followers not available", col("followers"))
          .otherwise(getIntervalCategory(col("followers")))
      )
      .withColumn(
        "following_category",
        when(col("following") === "Following not available", col("following"))
          .otherwise(getIntervalCategory(col("following")))
      )
      .withColumn(
        "public_repos_category",
        when(col("public_repos") === "Public repos not available", col("public_repos"))
          .otherwise(getIntervalCategory(col("public_repos")))
      )
      .select(
        col("user_id"),
        col("login"),
        col("type"),
        col("site_admin"),
        col("email"),
        col("name"),
        col("location"),
        col("hireable"),
        col("bio"),
        col("company"),
        col("blog"),
        col("twitter_username"),
        col("created_at"),
        col("updated_at"),
        col("followers"),
        col("following"),
        col("public_repos"),
        col("followers_category"),
        col("following_category"),
        col("public_repos_category")
      )
  userDimension
}

  def loadApplyingSCD(
      dataFromStagingDF: DataFrame,
      currentDimensionDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentUserDimensionDF = currentDimensionDF
    val usersFromStagingDF = dataFromStagingDF

    val tempUserDimDF = if (currentUserDimensionDF.isEmpty) {
      // reading the null row
      val userUndefinedRowDF = sparkSession
        .createDataFrame(NullDimension.UserDataNull)
        .toDF(NullDimension.UserColumnNull: _*)

      val userDimWithUndefinedRowDF = usersFromStagingDF
        .withColumn("pk_id", monotonically_increasing_id())
        .unionByName(userUndefinedRowDF)

      val userDimDF = userDimWithUndefinedRowDF

      userDimDF
    } else {
      val newUsersDF = usersFromStagingDF
        .join(
          currentUserDimensionDF,
          usersFromStagingDF("user_id") === currentUserDimensionDF("user_id"), // Joining by natural Key
          "left_anti"
        )
        .withColumn("pk_id", monotonically_increasing_id())

      val editedUsersDF = currentUserDimensionDF
        .as("current")
        .join(
          usersFromStagingDF.as("staging"),
          usersFromStagingDF("user_id") === currentUserDimensionDF("user_id"), // Joining by natural Key
          "inner"
        )
        .select(
          // applying SCD 0
          col("current.pk_id"),
          col("current.user_id"),
          col("staging.login"),
          col("staging.email"),
          col("current.type"),
          col("staging.site_admin"),
          col("staging.name"),
          col("staging.location"),
          col("staging.hireable"),
          col("staging.bio"),
          col("staging.company"),
          col("staging.blog"),
          col("staging.twitter_username"),
          col("current.created_at"),
          col("staging.updated_at"),
          col("staging.public_repos"),          
          col("staging.followers"),
          col("staging.following"),      
          col("staging.followers_category"),
          col("staging.following_category"),          
          col("staging.public_repos_category"),              
        )

      val notUpdatedUsersDF = currentUserDimensionDF.join(
        usersFromStagingDF,
        usersFromStagingDF("user_id") === currentUserDimensionDF("user_id"), // Joining by natural Key
        "left_anti"
      )

      // Union of all dimension pieces
      val updatedUserDimDF = newUsersDF
        .unionByName(editedUsersDF)
        .unionByName(notUpdatedUsersDF)

      updatedUserDimDF
    }

    // Returning the final dimension as temporal
    tempUserDimDF
  }

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentUserDimensionDF = sparkSession.read
      .schema(userDimensionSchema)
      .parquet(userDimensionPath)

val stagingUserDF = spark.read.parquet(userStagingPath)

  val usersFromStagingDF = transform(stagingUserDF)
  val tempUserDimDF = loadApplyingSCD(usersFromStagingDF, currentUserDimensionDF, sparkSession)

    // Write temporal dimension
    tempUserDimDF.write
      .mode(SaveMode.Overwrite)
      .parquet(userDimensionTempPath)

    // Move temporal dimension to the final dimension
    sparkSession.read
      .parquet(userDimensionTempPath)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(userDimensionPath)

    tempFileDimDF
  }

}
