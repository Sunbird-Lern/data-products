package org.sunbird.ml.exhaust

import org.apache.spark.sql.functions.{col, explode, expr, first, lit, when}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.sunbird.core.exhaust.JobRequest
import org.sunbird.core.exhaust.UserInfoUtil
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.sunbird.lms.exhaust.collection.{ProcessedRequest, UDFUtils}

import scala.collection.mutable.ListBuffer

case class RequestBody(`type`: String, `params`: Map[String, AnyRef])

object ProgramUserInfoExhaustJob extends BaseMLExhaustJob with Serializable {
  private val programEnrolmentDBSettings = Map("table" -> "program_enrollment", "keyspace" -> AppConf.getConfig("sunbird.program.report.keyspace"), "cluster" -> "ProgramCluster");

  override def getClassName = "org.sunbird.ml.exhaust.ProgramUserInfoExhaustJob"

  override def jobName() = "ProgramUserInfoExhaustJob";

  override def jobId() = "program-user-exhaust";

  override def getReportPath() = "program-user-exhaust";

  override def getReportKey() = "programuserinfo";

  override def validateRequest(request: JobRequest): Boolean = {
    if (super.validateRequest(request)) {
      if (request.encryption_key.isDefined) true else false;
    } else {
      false;
    }
  }

  override def processProgram(request: JobRequest, storageConfig: StorageConfig, requestsCompleted: ListBuffer[ProcessedRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    markRequestAsProcessing(request)

    println("request "+ request)
    println("storage config "+ storageConfig)

    val requestBody = JSONUtils.deserialize[RequestBody](request.request_data)
    val requestParamsBody = requestBody.`params`
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    try {
      val pgmEnrollFilters = requestParamsBody.getOrElse("filters", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]].find(f => f("table_name") == "program_enrollment").getOrElse(Map())
      var multiplePgmEnrollFilter: String = "";
      //get data from cassandra for program enrollment details
      pgmEnrollFilters.getOrElse("table_filters", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]].zipWithIndex.foreach { case (f1, cnt) =>
        if (cnt == pgmEnrollFilters.getOrElse("table_filters", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]].length - 1) {
          multiplePgmEnrollFilter = multiplePgmEnrollFilter + f1.getOrElse("name", "") + f1.getOrElse("operator", "") + "'" + f1.getOrElse("value", "") + "'"
        } else {
          multiplePgmEnrollFilter = multiplePgmEnrollFilter + f1.getOrElse("name", "") + f1.getOrElse("operator", "") + "'" + f1.getOrElse("value", "") + "' and "
        }
      }
      JobLogger.log("Program Enrollment Cassandra Table Filters", Some(multiplePgmEnrollFilter), INFO)

      val modelParamsPgmEnroll = modelParams.getOrElse("table", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]].find(f => f("name") == "program_enrollment").getOrElse(Map())
      val pgmEnrollCols: List[String] = modelParamsPgmEnroll.getOrElse("columns", List[String]()).asInstanceOf[List[String]]
      val entitiesPgmEnrollCols: List[String] = modelParamsPgmEnroll.getOrElse("user_locations_columns", List[String]()).asInstanceOf[List[String]]

      println("program enrollment cassandra filters "+ multiplePgmEnrollFilter)
      println("program enrollment cassandra columns "+ pgmEnrollCols)
      println("program enrollment cassandra entity columns "+ entitiesPgmEnrollCols)
      val resPgmEnrollDataDf = CommonUtil.time({
        val pgmEnrollDataDf = getProgramEnrolment(multiplePgmEnrollFilter, pgmEnrollCols, entitiesPgmEnrollCols, true)
        println("program enrollment cassandra query output")
        pgmEnrollDataDf.show()
        (pgmEnrollDataDf.count(), pgmEnrollDataDf)
      })
      JobLogger.log("Time to fetch program enrollment from cassandra", Some(Map("timeTaken" -> resPgmEnrollDataDf._1, "count" -> resPgmEnrollDataDf._2._1)), INFO)
      val programEnrollDataDf: DataFrame = resPgmEnrollDataDf._2._2;
      val programEnrollDataDfPII = programEnrollDataDf.filter(col("pii_consent_required") === true)
      var programEnrollDataDfNoPII = programEnrollDataDf.filter(col("pii_consent_required") === false).drop("pii_consent_required")

      val modelParamsUserCache = modelParams.getOrElse("table", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]].find(f => f("name") == "user").getOrElse(Map())
      val userCacheCols: Seq[String] = modelParamsUserCache.getOrElse("columns", Seq[String]()).asInstanceOf[Seq[String]]
      val userCacheEncryptCols: List[String] = modelParamsUserCache.getOrElse("encrypted_columns", List[String]()).asInstanceOf[List[String]]

      val pgmEnrolPIIDataDf: DataFrame = if (programEnrollDataDfPII.count() > 0) {
        JobLogger.log("Data Found for PII Consent Required = true", Some(Map("requestId" -> request.request_id)), INFO)
        // get data from user consent cassandra table
        val userConsentDataDF: DataFrame = getUserConsent(requestParamsBody.getOrElse("filters", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]])
        // get data from user redis cache
        val resUserCache = CommonUtil.time({
          val userDF = UserInfoUtil.getUserCacheDF(userCacheCols, persist = true)
          (userDF.count(), userDF)
        })
        JobLogger.log("Time to fetch user details from redis", Some(Map("timeTaken" -> resUserCache._1, "count" -> resUserCache._2._1)), INFO)
        val userCachedDF = resUserCache._2._2;
        userCachedDF.unpersist();
        val pgmEnrolUsrConsentDF = programEnrollDataDfPII.join(userConsentDataDF, programEnrollDataDfPII.col("user_id") === userConsentDataDF.col("userid"), "left")
          .drop("userid", "pii_consent_required")
        val pgmUserDF = pgmEnrolUsrConsentDF.join(userCachedDF, pgmEnrolUsrConsentDF.col("user_id") === userCachedDF.col("userid"), "left")
          .drop("userid", "firstname", "lastname")
        val pgmEnrollUsrDataDF = CommonUtil.time({
          UserInfoUtil.decryptUserInfo(pgmUserDF, userCacheEncryptCols)
        })
        JobLogger.log("Time to decrypt user info", Some(Map("timeTaken" -> pgmEnrollUsrDataDF._1)), INFO)
        pgmEnrollUsrDataDF._2
      } else {
        JobLogger.log("Data Not Found for PII Consent Required = true", Some(Map("requestId" -> request.request_id)), INFO)
        spark.emptyDataFrame
      }
      val pgmEnrolNoPIIDataDf: DataFrame = if (programEnrollDataDfNoPII.count() > 0 && programEnrollDataDfPII.count() > 0) {
        JobLogger.log("Data Found for both PII Consent Required = false and PII Consent Required = true", Some(Map("requestId" -> request.request_id)), INFO)
        val piiColNames = Seq("consentprovideddate") ++ modelParamsUserCache.getOrElse("final_columns", Seq[String]()).asInstanceOf[Seq[String]]
        programEnrollDataDfNoPII = programEnrollDataDfNoPII.withColumn("consentflag", lit("false"))
        programEnrollDataDfNoPII = piiColNames.foldLeft(programEnrollDataDfNoPII)((programEnrollDataDfNoPII, name) => programEnrollDataDfNoPII.withColumn(name, lit("")))
        programEnrollDataDfNoPII
      } else if (programEnrollDataDfNoPII.count() > 0 && programEnrollDataDfPII.count() == 0) {
        JobLogger.log("Data Found for only PII Consent Required = false", Some(Map("requestId" -> request.request_id)), INFO)
        programEnrollDataDfNoPII
      } else {
        JobLogger.log("Data Not Found for PII Consent Required = false", Some(Map("requestId" -> request.request_id)), INFO)
        spark.emptyDataFrame
      }
      val pgmEnrollDataDFFinal: DataFrame = if (programEnrollDataDfPII.count() > 0 && programEnrollDataDfNoPII.count() > 0) {
        JobLogger.log("Data Found for both PII Consent Required = true and PII Consent Required = false", Some(Map("requestId" -> request.request_id)), INFO)
        val pgmEnrollDataDFUnion = pgmEnrolPIIDataDf.union(pgmEnrolNoPIIDataDf)
        //label mapping
        val pgmEnrollLabelMapDF: DataFrame = organizeDF(pgmEnrollDataDFUnion, modelParams.getOrElse("label_mapping", Map[String, String]()).asInstanceOf[Map[String, String]], modelParams.getOrElse("order_of_csv_column", List[String]()).asInstanceOf[List[String]]).na.fill("")
        pgmEnrollLabelMapDF
      } else if (programEnrollDataDfPII.count() > 0 && programEnrollDataDfNoPII.count() == 0) {
        JobLogger.log("Data Found for PII Consent Required = true", Some(Map("requestId" -> request.request_id)), INFO)
        //label mapping
        val pgmEnrollLabelMapDF: DataFrame = organizeDF(pgmEnrolPIIDataDf, modelParams.getOrElse("label_mapping", Map[String, String]()).asInstanceOf[Map[String, String]], modelParams.getOrElse("order_of_csv_column", List[String]()).asInstanceOf[List[String]]).na.fill("")
        pgmEnrollLabelMapDF
      } else if (programEnrollDataDfPII.count() == 0 && programEnrollDataDfNoPII.count() > 0) {
        JobLogger.log("Data Found for PII Consent Required = false", Some(Map("requestId" -> request.request_id)), INFO)
        //label mapping
        val consentColNames = Set("consentprovideddate", "consentflag") ++ modelParamsUserCache.getOrElse("final_columns", Seq[String]()).asInstanceOf[Seq[String]]
        var noPIILabelMapping = modelParams.getOrElse("label_mapping", Map[String, String]()).asInstanceOf[Map[String, String]]
        val noPIIOrderColConfig = modelParams.getOrElse("order_of_csv_column", List[String]()).asInstanceOf[List[String]]
        val noPIIOrderCol = scala.collection.mutable.ListBuffer[String]()
        consentColNames.map(f => {
          noPIIOrderCol += noPIILabelMapping.getOrElse(f, "")
        })
        noPIILabelMapping = noPIILabelMapping -- consentColNames
        val noPIIOrderColConfig1 = noPIIOrderColConfig.diff(noPIIOrderCol)
        val pgmEnrollLabelMapDF: DataFrame = organizeDF(pgmEnrolNoPIIDataDf, noPIILabelMapping, noPIIOrderColConfig1).na.fill("")
        println("final dataframe")
        pgmEnrollLabelMapDF.show()
        pgmEnrollLabelMapDF
      } else {
        JobLogger.log("Data Not Found for both PII Consent Required = false", Some(Map("requestId" -> request.request_id)), INFO)
        spark.emptyDataFrame
      }
      pgmEnrollDataDFFinal
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        JobLogger.log(s"Failed to Process the Request ${ex.getMessage}", Some(Map("requestId" -> request.request_id)), ERROR)
        spark.emptyDataFrame
    }
  }

  def getProgramEnrolment(filters: String, cols: List[String], entityCol:List[String], persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("Program Enrollment Cassandra Table is being queried", None, INFO)
    import spark.implicits._
    println("programEnrolmentDBSettings "+ programEnrolmentDBSettings)
    println("cassandraFormat "+ cassandraFormat)
    var df = loadData(programEnrolmentDBSettings, cassandraFormat, new StructType())
      .where(s"""$filters""").select(cols.head, cols.tail: _*)
    println("getProgramEnrolment function cassandra query output")
    df.show()
    df = df.select($"*", explode($"user_locations")).drop("user_locations")
    val enrollCols: List[String] = cols.filter(_ != "user_locations")
    val columns = enrollCols ++ entityCol
    df = df.groupBy(enrollCols.map(col): _*).pivot("key").agg(first("value"))
      .drop("key", "value")
    entityCol.map(entCol=> {
      if (df.schema.fieldNames.contains(entCol) == false){
        df = df.withColumn(entCol,lit(null).cast("string"))
      }
    })
    df = df.select(columns.head, columns.tail: _*)
    if (persist) df.persist() else df
  }

  def getUserConsent(requestFilters: List[Map[String, AnyRef]])(implicit spark: SparkSession): DataFrame = {
    val usrConsentFilters = requestFilters.find(f => f("table_name") == "user_consent").getOrElse(Map())
    //    var userConsentDFFinal: List[DataFrame] = usrConsentFilters.filter(f => f("table_name") == "user_consent").map(f3 => {
    var multUsrConsentFilter: String = "";
    usrConsentFilters.getOrElse("table_filters", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]].zipWithIndex.foreach { case (f4, cnt1) =>
      if (cnt1 == usrConsentFilters.getOrElse("table_filters", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]].length - 1) {
        multUsrConsentFilter = multUsrConsentFilter + f4.getOrElse("name", "") + f4.getOrElse("operator", "") + "'" + f4.getOrElse("value", "") + "'"
      } else {
        multUsrConsentFilter = multUsrConsentFilter + f4.getOrElse("name", "") + f4.getOrElse("operator", "") + "'" + f4.getOrElse("value", "") + "' and "
      }
    }
    JobLogger.log("User Consent Cassandra Table Filters", Some(multUsrConsentFilter), INFO)
    println("User Consent Cassandra Table Filters "+ multUsrConsentFilter)
    val resUserConsent = CommonUtil.time({
      val userConsentDF = UserInfoUtil.getUserConsentDF(multUsrConsentFilter, persist = true)
      println("user consent cassandra query output")
      userConsentDF.show()
      (userConsentDF.count(), userConsentDF)
    })
    JobLogger.log("Time to fetch user consent details from cassandra table", Some(Map("timeTaken" -> resUserConsent._1, "count" -> resUserConsent._2._1)), INFO)
    val userConsentDF: DataFrame = resUserConsent._2._2;
    userConsentDF
  }
}
