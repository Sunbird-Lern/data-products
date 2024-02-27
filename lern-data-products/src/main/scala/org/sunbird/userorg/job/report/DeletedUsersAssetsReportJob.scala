package org.sunbird.userorg.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.ekstep.analytics.framework.JobDriver.className
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob}
import org.mongodb.scala.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.bson.{BsonArray, BsonString}
import org.mongodb.scala.model.Aggregates.{filter, project, sort}
import org.mongodb.scala.model.Filters.{equal, in}
import org.mongodb.scala.model.{Filters, Sorts}
import org.sunbird.core.util.Constants
import org.sunbird.core.util.MongoUtil.MongoUtil
import org.sunbird.lms.exhaust.collection.{CollectionDetails, CourseBatch, DeleteCollectionInfo}
import org.sunbird.lms.job.report.BaseReportsJob

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.JavaConverters._

object DeletedUsersAssetsReportJob extends IJob with BaseReportsJob with Serializable {
  val mongoUtil = new MongoUtil("localhost", 27017, "sl-pre-prod")

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    //    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val spark = SparkSession.builder().appName(name())
      .master("local[*]").getOrCreate()
    val deletedUsersDF = fetchDeletedUsers(spark)
    System.out.println(deletedUsersDF.count())
    deletedUsersDF.show()
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]
    val userIds = getUserIdsFromDeletedUsers(fetchDeletedUsers(spark))
    val mlDF = fetchMlData()(getProgramsAggregate(userIds), spark)
    System.out.println(mlDF.count())
    val contentAssetsDF = fetchContentAssets()(spark)
    val courseAssetsDF = fetchCourseAssets()(spark)
    val renamedDeletedUsersDF = deletedUsersDF
      .withColumnRenamed("id", "userIdAlias")
      .withColumnRenamed("username", "usernameAlias")
      .withColumnRenamed("rootorgid", "organisationIdAlias")

    // Join mlDF with deleted users
    val joinedMLDF = mlDF.join(renamedDeletedUsersDF, mlDF("author") === renamedDeletedUsersDF("userIdAlias"), "inner")

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
      renamedDeletedUsersDF("userIdAlias").alias("userId"), // Use an appropriate alias for clarity
      renamedDeletedUsersDF("usernameAlias").alias("username"),
      renamedDeletedUsersDF("organisationIdAlias").alias("organisationId"),
      concatRoles(renamedDeletedUsersDF("roles")).alias("roles"),
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

    // Select columns for mlDF
    val mlCols = Seq(
      mlDF("assetIdentifier").alias("assetIdentifier"),
      mlDF("assetName").alias("assetName"),
      mlDF("assetStatus").alias("assetStatus"),
      lit("ml-program").alias("objectType")
    )

    // Combine DataFrames for content, course batch, and mlDF using unionAll
    val combinedDF = joinedContentDF.select(userCols ++ contentCols: _*).unionAll(
      joinedCourseDF.select(userCols ++ courseCols: _*)
    ).unionAll(
      joinedMLDF.select(userCols ++ mlCols: _*)
    )

    // Deduplicate the combined DataFrame based on user ID
    val finalDF = combinedDF.distinct()
    finalDF.show()
    val coalescedDF: Dataset[Row] = finalDF.coalesce(1)
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

  def fetchMlData()(data: util.List[Document], spark: SparkSession): DataFrame = {
    val rows: List[Row] = data.asScala.map { doc =>
      val createdForValues: List[String] = doc.get("createdFor") match {
        case Some(bsonArray: BsonArray) =>
          bsonArray.asScala.collect {
            case bsonString: BsonString => bsonString.getValue
          }.toList
        case _ => List.empty[String] // Handle unexpected type or provide a default value
      }
      Row(
        doc.getObjectId("_id").toString,
        doc.getString("name"),
        doc.getString("status"),
        doc.getString("author"),
        createdForValues.mkString(",")
      )
    }.toList
    val schema = StructType(
      List(
        StructField("assetIdentifier", StringType, nullable = false),
        StructField("assetName", StringType, nullable = true),
        StructField("assetStatus", StringType, nullable = true),
        StructField("author", StringType, nullable = true),
        StructField("createdFor", StringType, nullable = true)
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.show()
    df
  }

  def name(): String = "DeletedUsersAssetsReportJob"


  def getProgramsAggregate(userIds: List[String]): util.List[Document] = {
    val matchQuery = Filters.and(
      in("author", userIds: _*),
      equal("status", "active")
    )
    val sortQuery = Sorts.descending("createdAt")
    val projection1 = Document("_id" -> 1, "name" -> 1, "status" -> 1, "author" -> 1, "createdFor" -> 1)
    val pipeline: List[Bson] = List(
      filter(matchQuery),
      sort(sortQuery),
      project(projection1)
    )
    println("inside getProgramsAggregate function before calling aggregate method")
    mongoUtil.aggregate("solutions", pipeline)
  }

  def fetchContentAssets()(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside content assets")
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]

    val apiURL = Constants.CONTENT_SEARCH_URL
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

  def fetchCourseAssets()(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside course assets")
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]()
    val apiUrl = Constants.COURSE_BATCH_SEARCH_URL
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
            "createdBy" -> getUserIdsFromDeletedUsers(fetchDeletedUsers(spark)),
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