package org.sunbird.userorg.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.ekstep.analytics.framework.JobDriver.className
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob}
import org.sunbird.core.util.Constants
import org.sunbird.lms.audit.CollectionSummaryJobV2.loadData
import org.sunbird.lms.exhaust.collection.{CollectionDetails, CourseBatch, DeleteCollectionInfo}

import java.text.SimpleDateFormat
import java.util.Date

object DeletedUsersAssetsReportJob extends IJob with Serializable {

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
//    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val spark = SparkSession.builder().appName(name())
      .master("local[*]").getOrCreate()
    val deletedUsersDF = fetchDeletedUsers(spark)
    System.out.println(deletedUsersDF.count())
    val contentAssetsDF = fetchContentAssets()(spark)
    //  val mlProgramAssetsDF = fetchMLProgramAssets(spark)
    val courseAssetsDF = fetchCourseAssets()(spark)


    val joinedUserDF = deletedUsersDF.select(
      deletedUsersDF("id").alias("userId"),
      deletedUsersDF("username"),
      deletedUsersDF("rootorgid").alias("organisationId"),
      concat_ws(",", deletedUsersDF("roles")).alias("roles"),
      lit(null).cast(StringType).alias("assetIdentifier"),
      lit(null).cast(StringType).alias("assetName"),
      lit(null).cast(StringType).alias("assetStatus"),
      lit(null).cast(StringType).alias("objectType")
    )

    val joinedContentDF = contentAssetsDF.select(
      contentAssetsDF("userId"),
      lit(null).cast(StringType).alias("username"),
      lit(null).cast(StringType).alias("organisationId"),
      lit(null).cast(StringType).alias("roles"),
      contentAssetsDF("identifier").alias("assetIdentifier"),
      contentAssetsDF("name").alias("assetName"),
      contentAssetsDF("status").alias("assetStatus"),
      contentAssetsDF("objectType")
    )

    val joinedCourseDF = courseAssetsDF.select(
      courseAssetsDF("userId"),
      lit(null).cast(StringType).alias("username"),
      lit(null).cast(StringType).alias("organisationId"),
      lit(null).cast(StringType).alias("roles"),
      courseAssetsDF("identifier").alias("assetIdentifier"),
      courseAssetsDF("name").alias("assetName"),
      when(courseAssetsDF("status") === "0", "Upcoming Batch")
        .when(courseAssetsDF("status") === "1", "Ongoing Batch")
        .when(courseAssetsDF("status") === "2", "Batch ended").alias("assetStatus") ,
      courseAssetsDF("objectType")
    )

    val reorderedColumnsDF = joinedCourseDF.select(joinedUserDF.columns.head, joinedCourseDF.columns.tail: _*)
    val mergedDF = joinedUserDF.union(joinedContentDF).union(reorderedColumnsDF)
    mergedDF.show()
    val coalescedDF: Dataset[Row] = mergedDF.coalesce(1)
    val formattedDate: String = {
      new SimpleDateFormat("yyyyMMdd").format(new Date())
    }
    val fileName: String = s"delete_user_$formattedDate.csv"

    coalescedDF
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"/home/bharathwajshankar/Ekstep/files/$fileName")

  }

  def name(): String = "DeletedUsersAssetsReportJob"

  //def fetchMLProgramAssets(spark: SparkSession): DataFrame = {
  //
  //  val mlProgramAssetsDF = spark.read.json("/path/to/ml/program/assets/data.json")
  //    .select("userId", "assetIdentifier", "assetName", "assetStatus", "objectType")
  //
  //  mlProgramAssetsDF
  //}

  def fetchContentAssets()(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside content assets")
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]

    val apiURL  = Constants.CONTENT_SEARCH_URL
    val limit = 10 // Set the desired limit for each request
    var offset = 0
    var totalRecords = 0

    var contentDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(
        StructField("identifier", StringType),
        StructField("userId", StringType),
        StructField("name", StringType),
        StructField("objectType", StringType),
        StructField("status", StringType)
      ))
    )

    do {
      val requestMap = Map(
        "request" -> Map(
          "filters" -> Map(
            "channel" -> "tn",
            "createdBy" -> getUserIdsFromDeletedUsers(fetchDeletedUsers(spark)),
            "status" -> Array("Live", "Draft", "Review", "Unlisted")
          ),
          "fields" -> Array("identifier", "createdBy", "name", "objectType", "status", "lastUpdatedOn"),
          "sortBy" -> Map("createdOn" -> "Desc"),
          "offset" -> offset,
          "limit" -> limit
        )
      )

      val request = JSONUtils.serialize(requestMap)
      val response = RestUtil.post[CollectionDetails](apiURL, request).result
      val count = response.getOrElse("count", 0).asInstanceOf[Int]
      val contentList = response.getOrElse("content", List.empty).asInstanceOf[List[Map[String, Any]]]

      if (contentList.nonEmpty) {
        val contents = contentList.map(entry => DeleteCollectionInfo(
          entry("identifier").toString,
          entry("createdBy").toString,
          entry("name").toString,
          entry("objectType").toString,
          entry("status").toString
        ))
        val contentDFBatch = spark.createDataFrame(contents)
          .withColumnRenamed("createdBy", "userId")
          .select("identifier", "userId", "name", "objectType", "status")
        contentDf = contentDf.union(contentDFBatch)
      }
      totalRecords = count
      offset += limit
    } while (offset < totalRecords)
    contentDf.show()
    System.out.println(contentDf.count())
    contentDf
  }

  def getUserIdsFromDeletedUsers(df: DataFrame)(implicit enc: Encoder[String]): List[String] = {
    val userIds: List[String] = df.select("id").as[String](enc).collect().toList
    userIds
  }

  def fetchCourseAssets()(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside course assets")
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]()

    val apiUrl = "https://staging.sunbirded.org/api/course/v1/batch/list"
    val headers = Map(
      "Authorization" -> "",
      "X-Authenticated-User-token" -> "",
      "Content-Type" -> "application/json"
    )

    val limit = 10 // Set the desired limit for each request
    var offset = 0
    var totalRecords = 0

    var courseDataDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("identifier", StringType), StructField("userId", StringType),
        StructField("name", StringType), StructField("status", StringType), StructField("objectType",StringType)))
    )

    do {
      val requestMap = Map(
        "request" -> Map(
          "filters" -> Map(
            "createdBy" -> getUserIdsFromDeletedUsers(fetchDeletedUsers(spark)),
            "status" -> 1),
          "fields" -> Array("identifier", "name", "createdBy", "status"),
          "sortBy" -> Map("createdOn" -> "Desc"),
          "offset" -> offset,
          "limit" -> limit
        )
      )

      val request = JSONUtils.serialize(requestMap)
      val response = RestUtil.post[CollectionDetails](apiUrl, request, Some(headers)).result
      val responseMap = response.getOrElse("response", Map.empty).asInstanceOf[Map[String, Any]]
      val count = responseMap.getOrElse("count", 0).asInstanceOf[Int]
      val content = responseMap.getOrElse("content", List.empty).asInstanceOf[List[Map[String, Any]]]

      if (content.nonEmpty) {
        val courses = content.map(entry => CourseBatch(
          entry("identifier").toString,
          entry("createdBy").toString,
          entry("name").toString,
          entry("status").toString
        ))
        val coursesDF = spark.createDataFrame(courses)
          .withColumnRenamed("createdBy", "userId")
          .select("identifier", "userId", "name", "status")
          .withColumn("objectType", lit("Course Batch"))
        courseDataDF = courseDataDF.union(coursesDF)
      }

      totalRecords = count
      offset += limit
    } while (offset < totalRecords)

    courseDataDF.show()
    System.out.println(courseDataDF.count())
    courseDataDF
  }

  def fetchDeletedUsers(implicit spark: SparkSession): DataFrame = {
    val sunbirdKeyspace = "sunbird"
    val userDf = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace), None).select(
      col("id"),
      col("username"),
      col("rootorgid"),
      col("roles"),
      col("status")).filter("status = 2").persist()
    userDf
  }

  def getReportingFrameworkContext()(implicit fc: Option[FrameworkContext]): FrameworkContext = {
    fc match {
      case Some(value) =>
        value
      case None =>
        new FrameworkContext();
    }
  }
}