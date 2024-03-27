package org.sunbird.userorg.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.ekstep.analytics.framework.JobDriver.className
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.sunbird.core.util.Constants
import org.sunbird.lms.exhaust.collection.{CollectionDetails, CourseBatch, DeleteCollectionInfo}
import org.sunbird.lms.job.report.BaseReportsJob

import java.text.SimpleDateFormat
import java.util.Date

object DeletedUsersAssetsReportJob extends IJob with BaseReportsJob with Serializable {

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val configuredUserId: List[String] = jobConfig.modelParams.get("configuredUserId").asInstanceOf[List[String]]
    val configuredChannel: List[String] = jobConfig.modelParams.get("configuredChannel").asInstanceOf[List[String]]
    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val spark = openSparkSession(jobConfig)
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]
    val userIds: List[String] = if (configuredUserId.nonEmpty) configuredUserId else getUserIdsFromDeletedUsers(fetchDeletedUsers(spark))
    val channels: List[String] = if (configuredChannel.nonEmpty) configuredChannel else List.empty[String]
    val deletedUsersDF = fetchDeletedUsers(spark)
    System.out.println(deletedUsersDF.count())
    deletedUsersDF.show()
    val contentAssetsDF = fetchContentAssets(userIds, channels)(spark)
    val courseAssetsDF = fetchCourseAssets(userIds, channels)(spark)
    val renamedDeletedUsersDF = deletedUsersDF
      .withColumnRenamed("id", "userIdAlias")
      .withColumnRenamed("username", "usernameAlias")
      .withColumnRenamed("rootorgid", "organisationIdAlias")
    // Join deleted users with content assets
    val joinedContentDF = renamedDeletedUsersDF.join(contentAssetsDF, renamedDeletedUsersDF("userIdAlias") === contentAssetsDF("userId"), "inner")
    // Join deleted users with course batch assets
    val joinedCourseDF = renamedDeletedUsersDF.join(courseAssetsDF, renamedDeletedUsersDF("userIdAlias") === courseAssetsDF("userId"), "inner")
    // Modify the concatRoles UDF to handle arrays
    val concatRoles = udf((roles: Any) => {
      roles match {
        case s: Seq[String] => s.mkString(", ")
        case _ => roles.toString
      }
    })
    // Select columns for the final output without using collect_list in UDF
    val userCols = Seq(
      renamedDeletedUsersDF("userIdAlias").alias("userId"),
      renamedDeletedUsersDF("usernameAlias").alias("username"),
      renamedDeletedUsersDF("organisationIdAlias").alias("organisationId"),
      concatRoles(renamedDeletedUsersDF("roles")).alias("roles")
    )
    // Select columns for content assets
    val contentCols = Seq(
      contentAssetsDF("identifier").alias("assetIdentifier"),
      contentAssetsDF("name").alias("assetName"),
      contentAssetsDF("status").alias("assetStatus"),
      contentAssetsDF("objectType")
    )
    // Select columns for course batch assets
    val courseCols = Seq(
      courseAssetsDF("identifier").alias("assetIdentifier"),
      courseAssetsDF("name").alias("assetName"),
      when(courseAssetsDF("status") === "0", "Upcoming Batch")
        .when(courseAssetsDF("status") === "1", "Ongoing Batch")
        .when(courseAssetsDF("status") === "2", "Batch ended").alias("assetStatus"),
      courseAssetsDF("objectType")
    )
    // Combine DataFrames for content and course batch using unionAll
    val combinedDF = joinedContentDF.select(userCols ++ contentCols: _*).unionAll(
      joinedCourseDF.select(userCols ++ courseCols: _*)
    )
    // Deduplicate the combined DataFrame based on user ID
    val finalDF = combinedDF.distinct()
    finalDF.show()
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("delete.user.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey, jobConfig)
    val formattedDate: String = {
      new SimpleDateFormat("yyyyMMdd").format(new Date())
    }
    finalDF.saveToBlobStore(storageConfig,"csv",s"delete_user_$formattedDate", Option(Map("header" -> "true")), Option(Seq("organisationId")))
  }
  def name(): String = "DeletedUsersAssetsReportJob"
  def fetchContentAssets(userIds: List[String], channels: List[String])(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside content assets")
    val apiURL = "http://10.246.3.3/search/v3/search"
    val limit = 10000 // Set the desired limit for each request
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
            "createdBy" -> userIds,
            "channel" -> channels,
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

      // Process each key in the result map
      response.asInstanceOf[Map[String, Any]].foreach {
        case (key, value) =>
          value match {
            case list: List[Map[String, Any]] =>
              // Process each entry in the list
              val entries = list.map(entry =>
                DeleteCollectionInfo(
                  entry.getOrElse("identifier", "").toString,
                  entry.getOrElse("createdBy", "").toString,
                  entry.getOrElse("name", "").toString,
                  entry.getOrElse("objectType", "").toString,
                  entry.getOrElse("status", "").toString
                )
              )
              // Create a DataFrame from the entries
              val entryDF = spark.createDataFrame(entries)
                .withColumnRenamed("createdBy", "userId")
                .select("identifier", "userId", "name", "objectType", "status")
              // Union with the existing contentDf
              contentDf = contentDf.union(entryDF)

            case _ => // Ignore other types
          }
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

  def fetchDeletedUsers(implicit spark: SparkSession): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("sunbird.user.keyspace")
    val userDf = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace), None).select(
      col("id"),
      col("username"),
      col("rootorgid"),
      col("roles"),
      col("status")).filter("status = 2").persist()
    userDf
  }

  def fetchCourseAssets(userIds: List[String], channels: List[String])(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside course assets")
    val apiUrl = "http://10.246.3.3/search/v3/search"
    val limit = 10000 // Set the desired limit for each request
    var offset = 0
    var totalRecords = 0

    var courseDataDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("identifier", StringType), StructField("userId", StringType),
        StructField("name", StringType), StructField("status", StringType), StructField("objectType", StringType)))
    )

    do {
      val requestMap = Map(
        "request" -> Map(
          "filters" -> Map(
            "createdBy" -> userIds,
            "createdFor" -> channels,
            "status" -> 1),
          "fields" -> Array("identifier", "name", "createdBy", "status"),
          "sortBy" -> Map("createdOn" -> "Desc"),
          "offset" -> offset,
          "limit" -> limit
        )
      )

      val request = JSONUtils.serialize(requestMap)
      val response = RestUtil.post[CollectionDetails](apiUrl, request).result
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
}