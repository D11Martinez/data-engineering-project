package presentation

import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import presentation.CustomUDF.getIntervalCategory

object UserDimensionETL {
  def getDataFrame(
      stagingUserDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {

    val userDimension = stagingUserDF
      .dropDuplicates("id")
      .withColumn("pk_id", lit(monotonically_increasing_id()))
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
        col("pk_id"),
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

    val userNull = sparkSession
      .createDataFrame(NullDimension.UserDataNull)
      .toDF(NullDimension.UserColumnNull: _*)

    val userUnionDF = userDimension.unionByName(userNull)

    userUnionDF.printSchema(3)
    userUnionDF.show(10)

    userUnionDF
  }

}
