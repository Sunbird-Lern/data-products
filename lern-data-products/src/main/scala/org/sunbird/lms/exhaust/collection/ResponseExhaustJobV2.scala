package org.sunbird.lms.exhaust.collection

import org.apache.spark.sql.functions.{col, explode_outer, from_unixtime, round}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.sunbird.core.exhaust.ExhaustUtil

object ResponseExhaustJobV2 extends BaseCollectionExhaustJob {

  override def getClassName = "org.sunbird.lms.exhaust.collection.ResponseExhaustJobV2"

  override def jobName() = "ResponseExhaustJobV2";

  override def jobId() = "response-exhaust";

  override def getReportPath() = "response-exhaust/";

  override def getReportKey() = "response";

  private val persistedDF: scala.collection.mutable.ListBuffer[DataFrame] = scala.collection.mutable.ListBuffer[DataFrame]();

  private val assessmentAggDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");

  case class AssessmentData(user_id: String, course_id: String, batch_id: String, content_id: String, attempt_id: String, created_on: Option[String], grand_total: Option[String], last_attempted_on: Option[String], question: List[Question], total_max_score: Option[Double], total_score: Option[Double], updated_on: Option[String]) extends scala.Product with scala.Serializable

  case class Question(id: String, assess_ts: String, max_score: Double, score: Double, `type`: String, title: String, resvalues: List[Map[String, String]], params: List[Map[String, String]], description: String, duration: Double) extends scala.Product with scala.Serializable

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val assessmentDF = getAssessmentDF(userEnrolmentDF, collectionBatch).persist();
    persistedDF.append(assessmentDF);
    val contentIds = assessmentDF.select("content_id").dropDuplicates().collect().map(f => f.get(0));
    val contentDF = searchContent(Map("request" -> Map("filters" -> Map("identifier" -> contentIds)))).withColumnRenamed("collectionName", "contentname").select("identifier", "contentname");
    val reportDF = assessmentDF.join(contentDF, assessmentDF("content_id") === contentDF("identifier"), "left_outer").drop("identifier").select(reportColumnList.head, reportColumnList.tail: _*);
    organizeDF(reportDF, reportColumnMapping, reportColumnMapping.values.toList);
  }

  override def unpersistDFs() {
    persistedDF.foreach(f => f.unpersist(true))
  }

  def getAssessmentDF(userEnrolmentDF: DataFrame, batch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    val userEnrolmentDataDF = userEnrolmentDF
      .select(
        col("userid"),
        col("courseid"),
        col("collectionName"),
        col("batchName"),
        col("batchid"))

    val assessAggregateData = loadData(assessmentAggDBSettings, cassandraFormat, new StructType()).where(col("batch_id").equalTo(batch.batchId))

    val preparedAssessAggregateData = prepareReportDf(assessAggregateData)

    val joinedDF = try {
      val assessBlobData = prepareReportDf(getAssessmentBlobDF(batch, config))
      preparedAssessAggregateData.unionByName(assessBlobData).dropDuplicates("course_id", "batch_id", "user_id", "attempt_id", "questionid")
    } catch {
      case e: Exception => {
        JobLogger.log("Blob does not contain any file for batchid: " + batch.batchId)
        preparedAssessAggregateData
      }
    }

    joinedDF.join(userEnrolmentDataDF, joinedDF.col("user_id") === userEnrolmentDataDF.col("userid") && joinedDF.col("course_id") === userEnrolmentDataDF.col("courseid") && joinedDF.col("batch_id") === userEnrolmentDataDF.col("batchid"), "inner")
  }

  def getAssessmentBlobDF(batch: CollectionBatch, config: JobConfig)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val archivalFetcherConfig = config.modelParams.get("assessmentFetcherConfig").asInstanceOf[Map[String, AnyRef]]

    val store = archivalFetcherConfig("store").asInstanceOf[String]
    val format: String = archivalFetcherConfig.getOrElse("format", "csv").asInstanceOf[String]
    val filePath = archivalFetcherConfig.getOrElse("filePath", "archival-data/").asInstanceOf[String]
    val container = archivalFetcherConfig.getOrElse("container", "reports").asInstanceOf[String]

    val assessAggData = ExhaustUtil.getArchivedData(store, filePath, container, Map("batchId" -> batch.batchId, "collectionId" -> batch.collectionId), Option(format))

    assessAggData.withColumn("question", UDFUtils.convertStringToList(col("question")))
      .withColumn("last_attempted_on", from_unixtime(col("last_attempted_on") / 1000, "yyyy-MM-dd HH:mm:ss"))
  }

  def prepareReportDf(df: DataFrame): DataFrame = {
    df.withColumn("questiondata", explode_outer(col("question")))
      .withColumn("questionid", col("questiondata.id"))
      .withColumn("questiontype", col("questiondata.type"))
      .withColumn("questiontitle", col("questiondata.title"))
      .withColumn("questiondescription", col("questiondata.description"))
      .withColumn("questionduration", round(col("questiondata.duration")))
      .withColumn("questionscore", col("questiondata.score"))
      .withColumn("questionmaxscore", col("questiondata.max_score"))
      .withColumn("questionresponse", UDFUtils.toJSON(col("questiondata.resvalues")))
      .withColumn("questionoption", UDFUtils.toJSON(col("questiondata.params")))
      .withColumn("last_attempted_on", col("last_attempted_on").cast(StringType))
      .drop("question", "questiondata", "question_data", "created_on", "updated_on")
  }
}
