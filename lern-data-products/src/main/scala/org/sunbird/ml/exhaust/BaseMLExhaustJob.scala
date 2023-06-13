package org.sunbird.ml.exhaust

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, StorageConfig}
import org.ekstep.analytics.util.Constants
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.core.util.{DecryptUtil, RedisConnect}
import org.sunbird.core.exhaust.{BaseReportsJob, JobRequest, OnDemandExhaustJob}
import org.sunbird.core.util.DataSecurityUtil.{getOrgId, getSecuredExhaustFile, getSecurityLevel}
import org.sunbird.lms.exhaust.collection.{ProcessedRequest}

import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer

case class Metrics(totalRequests: Option[Int], failedRequests: Option[Int], successRequests: Option[Int], duplicateRequests: Option[Int])

case class ProgramResponse(file: String, status: String, statusMsg: String, execTime: Long, fileSize: Long)

trait BaseMLExhaustJob extends BaseReportsJob with IJob with OnDemandExhaustJob with Serializable {
  val cassandraFormat = "org.apache.spark.sql.cassandra";
  val MAX_ERROR_MESSAGE_CHAR = 250

  // $COVERAGE-OFF$
  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    JobLogger.init(jobName())
    JobLogger.start(s"${jobName()} started executing", Option(Map("config" -> config, "model" -> jobName)))

    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    try {
      val res = CommonUtil.time(execute());
      // generate metric event and push it to kafka topic
      val metrics = List(Map("id" -> "total-requests", "value" -> res._2.totalRequests), Map("id" -> "success-requests", "value" -> res._2.successRequests), Map("id" -> "failed-requests", "value" -> res._2.failedRequests), Map("id" -> "duplicate-requests", "value" -> res._2.duplicateRequests), Map("id" -> "time-taken-secs", "value" -> Double.box(res._1 / 1000).asInstanceOf[AnyRef]))
      val metricEvent = getMetricJson(jobName, Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS", metrics)
      if (AppConf.getConfig("push.metrics.kafka").toBoolean)
        KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRequests" -> res._2.totalRequests, "successRequests" -> res._2.successRequests, "failedRequests" -> res._2.failedRequests, "duplicateRequests" -> res._2.duplicateRequests)))
    } catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, ERROR);
        JobLogger.end(jobName + " execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
        // generate metric event and push it to kafka topic in case of failure
        val metricEvent = getMetricJson(jobName(), Option(new DateTime().toString(CommonUtil.dateFormat)), "FAILED", List())
        // $COVERAGE-OFF$
        if (AppConf.getConfig("push.metrics.kafka").toBoolean)
          KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      // $COVERAGE-ON$
    } finally {
      frameworkContext.closeContext();
      spark.close()
      cleanUp()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("ProgramCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.program.report.host")))
  }
  // $COVERAGE-ON$
  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Metrics = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val maxErrorMessageLength: Int = modelParams.getOrElse("maxErrorMessageLength", MAX_ERROR_MESSAGE_CHAR).asInstanceOf[Int]

    val requests = getRequests(jobId(), None)
    val storageConfig = getStorageConfig(config, AppConf.getConfig("ml.exhaust.store.prefix"))
    val totalRequests = new AtomicInteger(requests.length)
    JobLogger.log("Total Requests are ", Some(Map("jobId" -> jobId(), "totalRequests" -> requests.length)), INFO)

    val dupRequests = getDuplicateRequests(requests)
    val dupRequestsList = dupRequests.values.flatten.map(f => f.request_id).toList
    val filteredRequests = requests.filter(f => !dupRequestsList.contains(f.request_id))
    JobLogger.log("The Request count details", Some(Map("Total Requests" -> requests.length, "filtered Requests" -> filteredRequests.length, "Duplicate Requests" -> dupRequestsList.length)), INFO)

    val requestsCompleted: ListBuffer[ProcessedRequest] = ListBuffer.empty
    var reqOrgAndLevelDtl : List[(String, String, String)] = List()

    val result = for (request <- filteredRequests) yield {
      JobLogger.log(s"executeOnDemand for channel= " + request.requested_channel, None, INFO)
      val orgId = request.requested_channel //getOrgId("", request.requested_channel)
      val level = getSecurityLevel(jobId(), orgId)
      println("security level "+ level)
      JobLogger.log(s"executeOnDemand for url = $orgId and level = $level and channel= $request.requested_channel", None, INFO)
      val reqOrgAndLevel = (request.request_id, orgId, level)
      reqOrgAndLevelDtl :+= reqOrgAndLevel
      val updRequest: (JobRequest, StorageConfig) = {
        try {
          val processedCount = if (requestsCompleted.isEmpty) 0 else requestsCompleted.count(f => f.channel.equals(request.requested_channel))
          val processedSize = if (requestsCompleted.isEmpty) 0 else requestsCompleted.filter(f => f.channel.equals(request.requested_channel)).map(f => f.fileSize).sum
          JobLogger.log("Channel details at execute", Some(Map("channel" -> request.requested_channel, "file size" -> processedSize, "completed programs" -> processedCount)), INFO)

          println("validate request " + validateRequest(request))
          if (validateRequest(request)) {
            println("validate request function")
            val res = CommonUtil.time(processProgram(request, storageConfig, requestsCompleted));
            val finalRes = transformData(res, request, storageConfig, requestsCompleted, totalRequests, orgId, level)
            finalRes
          } else {
            JobLogger.log("Not a Valid Request", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
            (markRequestAsFailed(request, "Not a Valid Request"), storageConfig)
          }
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            JobLogger.log(s"Failed to Process the Request ${ex.getMessage}", Some(Map("requestId" -> request.request_id)), ERROR)
            (markRequestAsFailed(request, s"Internal Server Error: ${ex.getMessage.take(maxErrorMessageLength)}"), storageConfig)
        }
      }
      val updRequest1: JobRequest = updRequest._1
      // check for duplicates and update with same urls
      if (dupRequests.contains(updRequest1.request_id)) {
        val dupReq = dupRequests(updRequest1.request_id)
        val res = for (req <- dupReq) yield {
          val dupUpdReq = markDuplicateRequest(req, updRequest1)
          dupUpdReq
        }
        saveRequests(storageConfig, res.toArray,reqOrgAndLevelDtl)(spark.sparkContext.hadoopConfiguration, fc)
      }
      saveRequestAsync(updRequest._2, updRequest1, reqOrgAndLevel)(spark.sparkContext.hadoopConfiguration, fc)
    }
    CompletableFuture.allOf(result: _*) // Wait for all the async tasks to complete
    val completedResult = result.map(f => f.join()); // Get the completed job requests
    Metrics(totalRequests = Some(requests.length), failedRequests = Some(completedResult.count(x => x.status.toUpperCase() == "FAILED")), successRequests = Some(completedResult.count(x => x.status.toUpperCase == "SUCCESS")), duplicateRequests = Some(dupRequestsList.length))
  }

  def validateRequest(request: JobRequest): Boolean = {
    val requestMap = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data);
    println("request map"+ requestMap)
    println("request map datatype "+ requestMap.getClass)
    println("request map isEmpty "+ requestMap.isEmpty)
    try {
      if (requestMap.isEmpty) return false else true
    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR);
        println(ex.getMessage)
        ex.printStackTrace()
        false
      }
    }
//          print("requestdata")
//        println(request.request_data)
//        Option(request.request_data) match {
//          case Some(s) => if (s.trim.isEmpty)
//          {
//            println("isempty block")
//          println(s)
//          println(s.trim)
//          println(s.trim.isEmpty)
//            false} else {
//            println("notempty block")
//            println(s)
//            println(s.trim)
//            println(s.trim.isEmpty)
//            true}
//          case None => false
//        }
    //    if (Option(request.request_data).isEmpty) false else true;
  }
  def getDuplicateRequests(requests: Array[JobRequest]): Map[String, List[JobRequest]] = {
    /*
    reqHashMap: contains hash(request_data, encryption_key, requested_by) as key and list of entire req as value
      sample reqHashMap data
      Map<"hash-1", List<JobRequest1, JobRequest3>, "hash-2", List<JobRequest2>>
    */
    val reqHashMap: scala.collection.mutable.Map[String, List[JobRequest]] = scala.collection.mutable.Map()
    requests.foreach { req =>
      // get hash
      val key = Array(req.request_data, req.encryption_key.getOrElse(""), req.requested_by).mkString("|")
      val hash = MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString
      if (!reqHashMap.contains(hash)) reqHashMap.put(hash, List(req))
      else {
        val newList = reqHashMap(hash) ++ List(req)
        reqHashMap.put(hash, newList)
      }
    }
    /*
    step-1: filter reqHashMap - with more than 1 entry in value list which indicates duplicates
      sample filtered map data
      Map<"hash-1", List<JobRequest1, JobRequest3>>
    step-2: transform map to have first request_id as key and remaining req list as value
      sample final map data
      Map<"request_id-1", List<JobRequest3>>
    */
    reqHashMap.toMap.filter(f => f._2.size > 1).map(f => (f._2.head.request_id -> f._2.tail))
  }

  def unpersistDFs() {};

  def jobId(): String;

  def jobName(): String;

  def getReportPath(): String;

  def getReportKey(): String;

  def processProgram(request: JobRequest, storageConfig: StorageConfig, requestsCompleted: ListBuffer[ProcessedRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame;

  def markRequestAsProcessing(request: JobRequest) = {
    request.status = "PROCESSING";
    updateStatus(request);
  }

  def transformData(resultData: (Long, DataFrame), request: JobRequest, storageConfig: StorageConfig, requestsCompleted: ListBuffer[ProcessedRequest], totalRequests: AtomicInteger, orgId: String, level: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): (JobRequest, StorageConfig) = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val reportDF1 = resultData._2
    val transformedDataDF: DataFrame = if (reportDF1.count() > 0) {
      //sort the dataframe
      JobLogger.log("Applying Sort to the Dataframe", None, INFO)
      val sortDfColNames = modelParams.get("sort").asInstanceOf[Option[List[String]]]
      var reportDF: DataFrame = reportDF1.sort(sortDfColNames.get.head, sortDfColNames.get.tail: _*)
      JobLogger.log("Applying Quote Column to the Dataframe", None, INFO)

      val quoteColumns = modelParams.getOrElse("quote_column", List[String]()).asInstanceOf[List[String]]
      val quoteCols = ListBuffer[String]()
      quoteColumns.map(f => {
        if (reportDF.columns.contains(f)){
          quoteCols += f
        }
      })
      if (quoteCols.nonEmpty) {
        val quoteStr = udf((column: String) => if (column.nonEmpty) "\'" + column + "\'" else  column)
        quoteCols.map(column => {
          reportDF = reportDF.withColumn(column, quoteStr(col(column)))
        })
      }
      reportDF
    } else {
      JobLogger.log("No Data Found", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
      markRequestAsFailed(request, "No Data Found")
      spark.emptyDataFrame
    }
    if (transformedDataDF.count() > 0) {
      JobLogger.log("Saving Dataframe to File", None, INFO)
      val result = CommonUtil.time(saveToFile(transformedDataDF, storageConfig, Some(request.request_id), Some(request.requested_channel), requestsCompleted.toList, resultData._1, level, orgId, request.encryption_key, request))
      val response = result._2;
      val failedPrograms = response.filter(p => p.status.equals("FAILED"))
      val processingPrograms = response.filter(p => p.status.equals("PROCESSING"))
      if (response.size == 0) {
        JobLogger.log("No Data Found", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
        (markRequestAsFailed(request, "No Data Found"), storageConfig)
      } else if (failedPrograms.size > 0) {
        (markRequestAsFailed(request, failedPrograms.map(f => f.statusMsg).mkString(","), Option("[]")), storageConfig)
      } else if (processingPrograms.size > 0) {
        (markRequestAsSubmitted(request, "[]"), storageConfig)
      } else {
        val storageConfig1 = getStorageConfig(config, response.head.file)
        request.status = "SUCCESS";
        request.download_urls = Option(response.map(f => f.file).toList);
        request.execution_time = Option(result._1);
        request.dt_job_completed = Option(System.currentTimeMillis)
        request.processed_batches = Option("[]")
        (request, storageConfig1)
      }
    } else {
      JobLogger.log("No Data Found", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
      request.status = "FAILED";
      request.dt_job_completed = Option(System.currentTimeMillis());
      request.iteration = Option(request.iteration.getOrElse(0) + 1);
      request.err_message = Option("No Data Found");
      (request, storageConfig)
    }
  }
  def saveToFile(reportDf: DataFrame, storageConfig: StorageConfig, requestId: Option[String], requestChannel: Option[String], processedRequests: List[ProcessedRequest], execTimeTaken: Long, level:String, orgId:String, encryptionKey:Option[String], jobRequest: JobRequest)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ProgramResponse] = {
    var processedCount = if (processedRequests.isEmpty) 0 else processedRequests.count(f => f.channel.equals(requestChannel.getOrElse("")))
    var processedSize = if (processedRequests.isEmpty) 0 else processedRequests.filter(f => f.channel.equals(requestChannel.getOrElse(""))).map(f => f.fileSize).sum
    var newFileSize: Long = 0
    JobLogger.log("Channel details at saveToFile", Some(Map("channel" -> requestChannel, "file size" -> processedSize, "completed batches" -> processedCount)), INFO)
    try {
      val fileFormat = "csv"
      val filePath = getFilePath(requestId.getOrElse(""))
      val files = reportDf.saveToBlobStore(storageConfig, fileFormat, filePath, Option(Map("header" -> "true")), None)

      JobLogger.log(s"processPrograms filePath: $filePath", Some("filePath" -> filePath), INFO)
      files.foreach(file => getSecuredExhaustFile(level, orgId, requestChannel.get, file, encryptionKey.getOrElse(""), storageConfig, jobRequest))

      newFileSize = fc.getHadoopFileUtil().size(files.head, spark.sparkContext.hadoopConfiguration)
      List(ProgramResponse(storageConfig.fileName + "/" + filePath + "." + fileFormat, "SUCCESS", "", execTimeTaken, newFileSize))

    } catch {
      case ex: Exception =>
        ex.printStackTrace();
        JobLogger.log(s"Failed to save the file ${ex.getMessage}", None, ERROR)
        List(ProgramResponse("", "FAILED", ex.getMessage, 0, 0));
    } finally {
      processedCount = processedCount + 1
      processedSize = processedSize + newFileSize
      unpersistDFs();
      reportDf.unpersist(true)
    }

  }

  def organizeDF(reportDF: DataFrame, finalColumnMapping: Map[String, String], finalColumnOrder: List[String]): DataFrame = {
    JobLogger.log("Label Mapping and Ordering of Columns in the dataframe", None, INFO)
    val fields = reportDF.schema.fieldNames
    val colNames = for (e <- fields) yield finalColumnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !finalColumnMapping.keySet.contains(e))
    val columnWithOrder = (finalColumnOrder ::: dynamicColumns).distinct
    reportDF.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*).na.fill("")
  }
  def markDuplicateRequest(request: JobRequest, referenceRequest: JobRequest): JobRequest = {
    request.status = referenceRequest.status
    request.download_urls = referenceRequest.download_urls
    request.execution_time = referenceRequest.execution_time
    request.dt_job_completed = referenceRequest.dt_job_completed
    request.processed_batches = referenceRequest.processed_batches
    request.iteration = referenceRequest.iteration
    request.err_message = referenceRequest.err_message
    request
  }
  def getFilePath(requestId: String)(implicit config: JobConfig): String = {
    val requestIdPath = if (requestId.nonEmpty) requestId.concat("_") else ""
    getReportPath() + "/" + requestIdPath + getDate()
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

}