package org.sunbird.lms.audit

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkContext, sql}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{Dispatcher, FrameworkContext, IJob, JobConfig, OutputDispatcher}
import org.sunbird.lms.job.report.BaseReportsJob

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

case class ActorObject(id: String = "Certificate Migrator", `type`: String = "System")

case class EventContext(pdata: Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.learning.platform"))


case class EventObject(id: String, `type`: String = "MigrateCertificate")

case class BEJobRequestEvent(actor: ActorObject= ActorObject(),
                             eid: String = "BE_JOB_REQUEST",
                             edata: Map[String, AnyRef],
                             ets: Long = System.currentTimeMillis(),
                             context: EventContext = EventContext(),
                             mid: String = s"LMS.${UUID.randomUUID().toString}",
                             `object`: EventObject
                            )


object OldCertificateMigrationJob extends IJob with BaseReportsJob {
  val courseBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  val certRegistryDBSettings = Map("table" -> "cert_registry", "keyspace" -> AppConf.getConfig("sunbird.user.keyspace"), "cluster" -> "UserCluster")
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  implicit val className: String = "org.sunbird.lms.audit.OldCertificateMigrationJob"
  val jobName = "OldCertificateMigrationJob"

  val identifier: String = "identifier"
  val completedOn: String = "completedon"
  val issuer: String = "issuer"
  val signatoryList: String = "signatoryList"
  var certBasePath: String = AppConf.getConfig("cert_base_path") + "/certs"

  var cloudStoreBasePath = ""
  var cloudStoreBasePathPlaceholder = ""
  var contentCloudStorageContainer = ""
  var baseUrl = ""

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))

    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)

    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    try {
      val res = CommonUtil.time(migrateData(jobConfig))
      val total_records = res._2.count()
      res._2.unpersist()
      JobLogger.log(s"Updating the $total_records records in the cassandra table", None, INFO)
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRecordsUpdated" -> total_records)))
    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR);
        println(ex.getMessage)
        ex.printStackTrace()
      }
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$
  def migrateData(jobConfig: JobConfig)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    implicit val executeEnabled:Boolean = jobConfig.modelParams.get.getOrElse("mode", "dryrun").asInstanceOf[String].equalsIgnoreCase("execute")
    val batchIds: List[String] = jobConfig.modelParams.get.getOrElse("batchId", "").asInstanceOf[String].split(",").toList.filter(_.nonEmpty)
    val kafkaTopic: String = jobConfig.modelParams.get.getOrElse("kafka_topic", "rc.certificate.migrate").toString
    val brokerList: String = jobConfig.modelParams.get.getOrElse("kafka_broker", AppConf.getConfig("metric.kafka.broker")).toString

    certBasePath = jobConfig.modelParams.get("cert_base_path").toString + "/certs"

    cloudStoreBasePath = jobConfig.modelParams.get("cloud_storage_base_url").toString
    cloudStoreBasePathPlaceholder = jobConfig.modelParams.get("cloud_store_base_path_placeholder").toString
    contentCloudStorageContainer = jobConfig.modelParams.get("content_cloud_storage_container").toString
    baseUrl = jobConfig.modelParams.get.getOrElse("cloud_storage_cname_url", cloudStoreBasePath).toString

    val cbDf = fetchCourseBatchData(spark, batchIds)
    println("fetchCertRegistryData started")
    val crDf = fetchCertRegistryData(spark, cbDf)
    println("fetchCertRegistryData completed")
    val certIssueRDD = crDf.rdd.map(e => generateCertificateEvent(e))

    if(executeEnabled) {
      OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> brokerList, "topic" -> kafkaTopic)), certIssueRDD)(spark.sparkContext, fc)
    } else {
      val outputFilePath = jobConfig.modelParams.get.getOrElse("output_file_path", "/mount/data/analytics/reports/") + batchIds.mkString("_") + ".json"
      OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> outputFilePath)), certIssueRDD)(spark.sparkContext, fc)
    }
    crDf
  }

  def fetchCourseBatchData(session: SparkSession, batchIds: List[String]): DataFrame = {
    var courseBatchDF = fetchData(session, courseBatchDBSettings, cassandraUrl, new StructType()).select("courseid", "batchid", "cert_templates")
    if (batchIds.nonEmpty) {
      import session.sqlContext.implicits._
      val batchIdDF = session.sparkContext.parallelize(batchIds).toDF("batchid")
      courseBatchDF = courseBatchDF.join(batchIdDF, Seq("batchid"), "inner");
      courseBatchDF
    } else {
      throw new Exception("BatchID is mandatory parameter")
    }

  }

  def fetchCertRegistryData(session: SparkSession, cbDf: DataFrame): DataFrame = {
//    val schema = new StructType(MapType(StringType,StringType))
    var certRegistryDF = fetchData(session, certRegistryDBSettings, cassandraUrl, new StructType())
      .where(col("isrevoked").equalTo(false) && col("related").isNotNull && col("related").contains("batchId") && col("related").contains("{"))
      .select("id", "data", "related", "createdat", "recipient")
      .persist()
    println("data_json ")
    certRegistryDF = certRegistryDF.withColumn("data_json", from_json(col("data"), MapType(StringType,StringType)))
      .drop("data")
      .withColumnRenamed("data_json", "data")
    println("data_json completed")
    certRegistryDF = certRegistryDF.withColumn("related_json", from_json(col("related"), MapType(StringType,StringType)))
      .drop("related")
      .withColumnRenamed("related_json", "related")
    println("related_json completed")
    certRegistryDF = certRegistryDF.withColumn("recipient_json", from_json(col("recipient"), MapType(StringType,StringType)))
      .drop("recipient")
      .withColumnRenamed("recipient_json", "recipient")
    println("recipient_json completed")

    var resultDf = certRegistryDF.join(cbDf, certRegistryDF("related.batchId") === cbDf("batchid") && certRegistryDF("related.courseId") === cbDf("courseid"), "inner")
      .select("id", "data", "related", "cert_templates", "recipient", "createdat", "courseid", "batchid")
    resultDf.show(2000, false)
    resultDf = resultDf.na.fill("")
    resultDf
  }

  def generateCertificateEvent(row: Row): String = {
    val oldId = row.get(0).asInstanceOf[String]

    val recipientName = row.get(4).asInstanceOf[Map[String, AnyRef]].getOrElse("name", "")
    val recipientId = row.get(4).asInstanceOf[Map[String, AnyRef]].getOrElse("id", "").asInstanceOf[String]
    val badge = JSONUtils.deserialize[Map[String, AnyRef]](row.get(1).asInstanceOf[Map[String, AnyRef]].getOrElse("badge", "{}").asInstanceOf[String])
    val courseName = badge.getOrElse("name", "")
    val templateName = badge.getOrElse("criteria", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("narrative", "").asInstanceOf[String]

    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val related = row.get(2).asInstanceOf[Map[String, String]]

    val templateData = row.get(3).asInstanceOf[Map[String, Map[String, AnyRef]]].filter(rec => rec._2.getOrElse("name", "").toString.equalsIgnoreCase(templateName)).head._2
    val eData = Map[String, AnyRef] (
      "issuedDate" -> dateFormatter.format(row.get(5)),
      "data" -> List(Map[String, AnyRef]("recipientName" -> recipientName, "recipientId" -> recipientId)),
      "criteria" -> Map[String, String]("narrative" -> templateName),
      "svgTemplate" -> templateData.getOrElse("url", "").asInstanceOf[String].replace(cloudStoreBasePathPlaceholder, baseUrl+"/"+contentCloudStorageContainer),
      "oldId" -> oldId,
      "templateId" -> templateData.getOrElse("identifier", ""),
      "userId" -> recipientId,
      "orgId" -> "",
      "issuer" -> JSONUtils.deserialize[Map[String, AnyRef]](templateData.getOrElse("issuer", "{}").asInstanceOf[String]),
      "signatoryList" -> JSONUtils.deserialize[List[Map[String, AnyRef]]](templateData.getOrElse("signatoryList", "[]").asInstanceOf[String]),
      "courseName" -> courseName,
      "basePath" -> certBasePath,
      "related" ->  related,
      "name" -> templateData.getOrElse("name", ""),
      "tag" -> row.get(7).asInstanceOf[String],
      "identifier" -> oldId
    )

    JSONUtils.serialize(BEJobRequestEvent(edata = eData, `object` = EventObject(id = recipientId)))
  }

}
