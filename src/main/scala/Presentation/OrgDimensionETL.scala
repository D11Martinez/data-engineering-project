package presentation

import presentation.CustomUDF.getIntervalCategory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object OrgDimensionETL {

    val organizationDimensionPath = "src/dataset/presentation/org-dimension"
    val organizationDimensionTempPath = "src/dataset/presentation/temp/org-dimension"

    val orgDimensionSchema: StructType = StructType(
 Array(
   StructField("pk_id", LongType, nullable=false),
   StructField("organization_id", StringType, nullable=false),
   StructField("login", StringType, nullable=false),
   StructField("email", StringType, nullable=false),
   StructField("type", StringType, nullable=false),
   StructField("name", StringType, nullable=false),
   StructField("description", StringType, nullable=false),
   StructField("company", StringType, nullable=false),
   StructField("location", StringType, nullable=false),
   StructField("is_verified", StringType, nullable=false),
   StructField("has_organization_projects", StringType, nullable=false),
   StructField("has_repository_projects", StringType, nullable=false),
   StructField("blog", StringType, nullable=false),
   StructField("twitter_username", StringType, nullable=false),
   StructField("created_at", StringType, nullable=false),
   StructField("updated_at", StringType, nullable=false),
   StructField("public_repos", StringType, nullable=false),
   StructField("followers", StringType, nullable=false),
   StructField("following", StringType, nullable=false),
   StructField("followers_category", StringType, nullable=false),
   StructField("following_category", StringType, nullable=false),
   StructField("public_repos_category", StringType, nullable=false),
 ))

  def transform(eventPayloadStagingDF: DataFrame): DataFrame = {  
  val orgDimension = eventPayloadStagingDF
        .filter(col("type") === "Organization")
        .dropDuplicates("id")
        .select(
          col("id").as("organization_id"),
          col("login"),
          when(col("email").isNull, "Email not available")
            .otherwise(col("email"))
            .as("email"),
          when(col("type").isNull, "Type not available")
            .otherwise(col("type"))
            .as("type"),
          when(col("name").isNull, "Name not available")
            .otherwise(col("name"))
            .as("name"),
          when(
            col("description").isNull || col("description") === "",
            "Description not available"
          ).otherwise(col("description")).as("description"),
          when(col("company").isNull, "Company not available")
            .otherwise(col("company"))
            .as("company"),
          when(col("location").isNull, "Location not available")
            .otherwise(col("location"))
            .as("location"),
          when(col("is_verified").isNull, "Is verified not available")
            .when(col("is_verified") === false, "Is not verified")
            .otherwise("Is verified")
            .as("is_verified"),
          when(
            col("has_organization_projects").isNull,
            "Organization projects not available"
          ).when(
            col("has_organization_projects") === false,
            "Is not organization projects"
          ).otherwise("Is organization projects")
            .as("has_organization_projects"),
          when(
            col("has_repository_projects").isNull,
            "Repository projects not available"
          ).when(
            col("has_repository_projects") === false,
            "Is not repository projects"
          ).otherwise("Is repository projects")
            .as("has_repository_projects"),
          when(col("blog").isNull, "Blog not available")
            .otherwise(col("blog"))
            .as("blog"),
          when(col("twitter_username").isNull, "Twitter not available")
            .otherwise(col("twitter_username"))
            .as("twitter_username"),
          when(col("created_at").isNull, "Created at not available")
            .otherwise(col("created_at"))
            .as("created_at"),
          when(col("updated_at").isNull, "Updated at not available")
            .otherwise(col("updated_at"))
            .as("updated_at"),
          when(col("public_repos").isNull, "Public repos at not available")
            .otherwise(col("public_repos"))
            .as("public_repos"),
          when(col("followers").isNull, "Followers repos at not available")
            .otherwise(col("followers"))
            .as("followers"),
          when(col("following").isNull, "Following repos at not available")
            .otherwise(col("following"))
            .as("following")
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
  orgDimension
}
  def loadApplyingSCD(
      dataFromStagingDF: DataFrame,
      currentDimensionDF: DataFrame,
      sparkSession: SparkSession
  ): DataFrame = {
    val currentOrgDimensionDF = currentDimensionDF
    val orgsFromStagingDF = dataFromStagingDF

    val tempOrgsDimDF = if (currentOrgDimensionDF.isEmpty) {
      // reading the null row
      val orgUndefinedRowDF = sparkSession
        .createDataFrame(NullDimension.OrgDataNull)
        .toDF(NullDimension.OrgColumnNull: _*)

      val orgDimWithUndefinedRowDF = orgsFromStagingDF
        .withColumn("pk_id", monotonically_increasing_id())
        .unionByName(orgUndefinedRowDF)

      val orgDimDF = orgDimWithUndefinedRowDF

      orgDimDF
    } else {
      val newOrgsDF = orgsFromStagingDF
        .join(
          currentOrgDimensionDF,
          orgsFromStagingDF("organization_id") === currentOrgDimensionDF("organization_id"), // Joining by natural Key
          "left_anti"
        )
        .withColumn("pk_id", monotonically_increasing_id())

      val editedOrgsDF = currentOrgDimensionDF
        .as("current")
        .join(
          orgsFromStagingDF.as("staging"),
          orgsFromStagingDF("organization_id") === currentOrgDimensionDF("organization_id"), // Joining by natural Key
          "inner"
        )
        .select(
          // applying SCD 0
          col("current.pk_id"),
          col("current.organization_id"),
          col("staging.login"),
          col("staging.email"),
          col("current.type"),
          col("staging.name"),
          col("staging.description"),
          col("staging.company"),
          col("staging.location"),
          col("staging.is_verified"),
          col("staging.has_organization_projects"),
          col("staging.has_repository_project"),
          col("staging.blog"),
          col("staging.twitter_username"),
          col("current.created_at"),
          col("staging.updated_at"),
          col("staging.public_repos"),
          col("staging.followers"),
          col("staging.following"),
          col("staging.public_repos_category"),
          col("staging.followers_category"),
          col("staging.following_category"),
        )

      val notUpdatedOrgsDF = currentOrgDimensionDF.join(
        orgsFromStagingDF,
        orgsFromStagingDF("organization_id") === currentOrgDimensionDF("organization_id"), // Joining by natural Key
        "left_anti"
      )

      // Union of all dimension pieces
      val updatedOrgDimDF = newOrgsDF
        .unionByName(editedOrgsDF)
        .unionByName(notUpdatedOrgsDF)

      updatedOrgDimDF
    }

    // Returning the final dimension as temporal
    tempOrgsDimDF
  }

  def getDataFrame(
      eventPayloadStagingDF: DataFrame,
      sparkSession: SparkSession
  ): Unit = {
    val currentOrgDimensionDF = sparkSession.read
      .schema(orgDimensionSchema)
      .parquet(organizationDimensionPath)

    //val eventPayloadStagingDF = spark.read.parquet(organizationStagingPath)

    val orgsFromStagingDF = transform(eventPayloadStagingDF)
    val tempOrgDimDF = loadApplyingSCD(orgsFromStagingDF, currentOrgDimensionDF, sparkSession)

    // Write temporal dimension
    tempOrgDimDF.write
      .mode(SaveMode.Overwrite)
      .parquet(organizationDimensionTempPath)

    // Move temporal dimension to the final dimension
    sparkSession.read
      .parquet(organizationDimensionTempPath)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(organizationDimensionPath)

  }

}
