package org.sunbird.userorg.job.report

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.util.{EmbeddedCassandra, SparkSpec}

class TestDeleteUsersAssetReportJob extends SparkSpec(null) with MockFactory {
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/user_data.cql") // Load test data in embedded cassandra server
  }

  override def afterAll(): Unit = {
    super.afterAll();
  }


  "DeletedUsersAssetsReportJob" should "generate reports" in {
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.userorg.job.report.DeletedUsersAssetsReportJob"}}"""
    DeletedUsersAssetsReportJob.main(strConfig)
  }

  "fetchDeletedUsers" should "return a DataFrame" in {
    val deletedUsersDF: DataFrame = DeletedUsersAssetsReportJob.fetchDeletedUsers(spark)
    assert(deletedUsersDF != null)
    assert(deletedUsersDF.columns.length > 0)
  }

  "getUserIdsFromDeletedUsers" should "return a list of user ids" in {
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]
    val deletedUsersDF: DataFrame = DeletedUsersAssetsReportJob.fetchDeletedUsers(spark)
    val userIds: List[String] = DeletedUsersAssetsReportJob.getUserIdsFromDeletedUsers(deletedUsersDF)
    assert(userIds != null)
    assert(userIds.nonEmpty)
  }

  "fetchContentAssets" should "return a DataFrame" in {
    val mockedSpark: SparkSession = spark
    val contentAssetsDF: DataFrame = DeletedUsersAssetsReportJob.fetchContentAssets()(mockedSpark)
    assert(contentAssetsDF != null)
    assert(contentAssetsDF.columns.length > 0)
  }

  "fetchCourseAssets" should "return a DataFrame" in {
    val mockedSpark: SparkSession = spark
    val courseAssetsDF: DataFrame = DeletedUsersAssetsReportJob.fetchCourseAssets()(mockedSpark)
    assert(courseAssetsDF != null)
    assert(courseAssetsDF.columns.length > 0)
  }

}