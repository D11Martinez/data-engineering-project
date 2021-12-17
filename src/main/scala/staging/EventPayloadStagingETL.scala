package staging

import org.apache.spark.sql.DataFrame

object EventPayloadStagingETL {
  def getDataFrame(rawPullRequestsDF: DataFrame): DataFrame = {
    // Do transformations here
    rawPullRequestsDF
  }
}
