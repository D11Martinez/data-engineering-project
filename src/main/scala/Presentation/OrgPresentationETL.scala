package Presentation

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, monotonically_increasing_id, when}

object OrgPresentationETL {

  def getDataFrame(stagingOrgDF: DataFrame):DataFrame={

    val orgDimension = stagingOrgDF.filter(col("type")==="Organization").dropDuplicates("id")
      .withColumn("pk_id",lit(monotonically_increasing_id()))
      .select(
        col("pk_id"),col("id").as("organization_id"), col("login"),
        when(col("email").isNull,"Email not available").otherwise(col("email")).as("email"),
        when(col("type").isNull,"Type not available").otherwise(col("type")).as("type"),
        when(col("name").isNull,"Name not available").otherwise(col("name")).as("name"),
        when(col("description").isNull || col("description")==="" ,"Description not available").otherwise(col("description")).as("description"),
        when(col("company").isNull,"Company not available").otherwise(col("company")).as("company"),
        when(col("location").isNull,"Location not available").otherwise(col("location")).as("location"),
        when(col("is_verified").isNull,"Is verified not available").when(col("is_verified")===false,"Is not verified").otherwise("Is verified").as("is_verified"),
        when(col("has_organization_projects").isNull,"Organization projects not available").when(col("has_organization_projects")===false,"Is not organization projects").otherwise("Is organization projects").as("has_organization_projects"),
        when(col("has_repository_projects").isNull,"Repository projects not available").when(col("has_repository_projects")===false,"Is not repository projects").otherwise("Is repository projects").as("has_repository_projects"),
        when(col("blog").isNull,"Blog not available").otherwise(col("blog")).as("blog"),
        when(col("twitter_username").isNull,"Twitter not available").otherwise(col("twitter_username")).as("twitter_username"),
        when(col("created_at").isNull,"Created at not available").otherwise(col("created_at")).as("created_at"),
        when(col("updated_at").isNull,"Updated at not available").otherwise(col("updated_at")).as("updated_at"),
        when(col("public_repos").isNull,"Public repos at not available").otherwise(col("public_repos")).as("public_repos"),
        when(col("followers").isNull,"Followers repos at not available").otherwise(col("followers")).as("followers"),
        when(col("following").isNull,"Following repos at not available").otherwise(col("following")).as("following")
      )

    orgDimension
  }
}
