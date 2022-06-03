package org.sunbird.lern.job.report

import java.io.File
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.lern.audit.CollectionReconciliationJob
import org.sunbird.lern.job.report.{BaseReportSpec, BaseReportsJob}
import org.sunbird.lern.util.{EmbeddedCassandra, UserData}

import scala.collection.mutable


class TestCollectionReconciliationJob extends BaseReportSpec with MockFactory {


  var spark: SparkSession = _

  var courseBatchDF: DataFrame = _
  var userEnrolments: DataFrame = _
  var userDF: DataFrame = _
  var organisationDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"
  val esIndexName = "composite"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/collection-summary/course_batch_data.csv")
      .cache()

    userEnrolments = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/collection-summary/user_courses_data.csv")
      .cache()

    userDF = spark.read.json("src/test/resources/collection-summary/user_data.json")
      .cache()

    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server

  }

  override def afterAll(): Unit = {
    super.afterAll()
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, objectKey + "collection-summary-reports-v2/")
  }

  it should "generate the report for all the batches" in {

    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.CollectionReconciliationJob","modelParams":{"mode":"prodrun","brokerList":"11.2.1.15:9092","topic":"sunbirddev.issue.certificate.request","sparkCassandraConnectionHost":"11.2.3.83"},"parallelization":30,"appName":"CollectionReconciliationJob"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val reportData = CollectionReconciliationJob.execute()(spark, mockFc, jobConfig)
  }
}
