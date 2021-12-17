package staging

import org.apache.spark.sql.DataFrame

object UserStagingETL {
  def getDataFrame(rawPullRequestsDF: DataFrame): DataFrame = {
    // Do transformations here
    rawPullRequestsDF
  }
}
