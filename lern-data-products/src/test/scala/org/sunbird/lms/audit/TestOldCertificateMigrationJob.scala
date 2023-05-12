package org.sunbird.lms.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.util.{BaseSpec, EmbeddedCassandra}

class TestOldCertificateMigrationJob extends BaseSpec with MockFactory {
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/old-cert-migration/data.cql")
  }

  override def afterEach(): Unit = {
    spark.close()
    EmbeddedCassandra.close()
  }

  it should "migrate the certificates data" in {
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.audit.OldCertificateMigrationJob","modelParams":{"mode":"dryrun","store":"azure","sparkCassandraConnectionHost":"localhost", "cert_base_path": "https://dev.sunbirded.org/", "cloud_storage_base_url": "https://sunbirddev.blob.core.windows.net", "cloud_store_base_path_placeholder": "CLOUD_BASE_PATH","content_cloud_storage_container": "sunbird-content-staging", "cloud_storage_cname_url": "https://obj.stage.sunbirded.org", "batchId": "0134278454483681283", "kafka_broker": "localhost:9092", "kafka_topic": "sunbirddevlern.rc.certificate.migrate","output_file_path":"./reports/"},"parallelization":8,"appName":"OldCertificateMigrationJob"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = OldCertificateMigrationJob.migrateData(jobConfig)
    val certEvents = spark.read.format("json").load(s"./reports/0134278454483681283.json")

    certEvents.count() should be (1)
  }
}