//package org.sunbird.lms.job.report
//
//import java.io.{File, PrintWriter}
//import java.sql.Timestamp
//import java.text.SimpleDateFormat
//import org.apache.spark.sql.functions.{size, _}
//import org.apache.spark.sql._
//import org.apache.spark.sql.SparkSession
//
//import java.util.Date
//import scala.collection.mutable.ListBuffer
//import cats.syntax.either._
//import ing.wbaa.druid.client.DruidClient
//import ing.wbaa.druid._
//import io.circe.Json
//import io.circe.parser.parse
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.types.StructType
//import org.ekstep.analytics.framework.util.JSONUtils
//import org.ekstep.analytics.framework._
//import org.scalamock.scalatest.MockFactory
//import org.sunbird.core.util.{EmbeddedCassandra, SparkSpec}
//import sun.util.calendar.BaseCalendar.Date
//
//import java.time.{ZoneOffset, ZonedDateTime}
//import scala.collection.mutable.ListBuffer
//import scala.concurrent.Future
//
//case class CourseBatch(courseid:String, batchid: String)
//case class UserEnrolment(userid: String, courseid: String, batchid: String, issued_certificates: List[Map[String, String]], certificates: List[Map[String, String]], status: Int, active: Boolean)
//
//class TestNishthaCourseMetrics extends SparkSpec(null){
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    // embedded cassandra setup
//    EmbeddedCassandra.loadData("src/test/resources/assessment-correction-report/nishtha_consumption_report_data.cql") // Load test data in embedded cassandra server
//
//  }
//  override def afterAll() : Unit = {
//    super.afterAll();
//    EmbeddedCassandra.close()
//  }
//
//  val cassandraFormat = "org.apache.spark.sql.cassandra";
//  val assessmentAggDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> "sunbird_courses")
//  val enrolmentsDBSettings = Map("table" -> "user_enrolments", "keyspace" -> "sunbird_courses")
//  val courseBatchSettings = Map("table" -> "course_batch", "keyspace" -> "sunbird_courses")
//
//  def migration()(implicit spark: SparkSession) = {
////    val enrolmentDf = spark.sparkContext.cassandraTable("sunbird_courses", "user_enrolments")
////    enrolmentDf.saveToCassandra("sunbird_courses", "report_user_enrolments")
////
////    val assessmentDf = spark.sparkContext.cassandraTable("sunbird_courses", "assessment_aggregator")
////    assessmentDf.saveToCassandra("sunbird_courses", "report_assessment_aggregator")
//  }
//
////  def main(args: Array[String]) = {
////    val execType = args(0)
////    val cassandraHost = args(1)
////    implicit val spark = getSparkSession()
////
////    execType match {
////      case "migration" => migration()
////      case "report" => processReport()
////    }
////  }
//
////  def getSparkSession(cassandraHost: String): SparkSession = {
////    val conf = new SparkConf()
////      .setAppName("NishthaConsumption")
////      .setMaster("local[*]")
////      .set("spark.cassandra.connection.host", cassandraHost)
////
////    val spark = SparkSession.builder().appName("NishthaConsumption").config(conf).getOrCreate()
////    spark
////  }
//
//  def loadData(settings: Map[String, String], schema: StructType)(implicit spark: SparkSession): DataFrame = {
//    if (schema.nonEmpty) {
//      spark.read.schema(schema).format(cassandraFormat).options(settings).load()
//    } else {
//      spark.read.format(cassandraFormat).options(settings).load()
//    }
//  }
//
//  "NishthaConsumption" should "execute and won't throw any errors/exception" in {
//    implicit val spark: SparkSession = getSparkSession()
//
//    processReport()
//  }
//
//  def processReport()(implicit spark: SparkSession) = {
//    val enrolments = getEnrolments()
//    val userConsumptionDf = generateAssessments(enrolments)
//
//    val resultDf = generateReport(userConsumptionDf)
//
//    resultDf.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("nishtha_course_metrics")
//    resultDf.show(false)
//  }
//
//  def generateReport(userConsumptionDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
//    userConsumptionDf
//  }
//
//  def generateAssessments(enrolmentDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
//    val assessmentDf = loadData(assessmentAggDBSettings, new StructType())
//
//    val enrolAssessmentDf = enrolmentDf.filter(col("certificate_issued") === "Yes")
//                                .join(assessmentDf,
//                                  enrolmentDf("userid") === assessmentDf("user_id") &&
//                                  enrolmentDf("courseid") === assessmentDf("course_id") &&
//                                  assessmentDf("batch_id") === enrolmentDf("batchid"),
//                                  "inner")
//
//    val assessmentScoreDf = enrolAssessmentDf
////      .filter(enrolAssessmentDf("last_issued_on") >= enrolAssessmentDf("last_attempted_on"))
//                            .withColumn("score_percent", lit((col("total_score")/col("total_max_score")) * 100).cast("int"))
//
//    val attemptsDf = assessmentScoreDf.groupBy("course_id", "batch_id", "user_id", "content_id").agg(count("attempt_id").as("attempts_count"))
//
//    val attemptCountDf = attemptsDf.groupBy("course_id","batch_id","user_id").agg(max("attempts_count").as("attempts_count"))
//
//    val attemptMetricDf = attemptCountDf.withColumn("attempt_metric",
//      when(col("attempts_count") === 1, lit("one_attempt")).otherwise(
//        when(col("attempts_count") === 2, lit("two_attempt")).otherwise(
//          when(col("attempts_count") === 3, lit("three_attempt")).otherwise(
//            lit("more_attempt")
//          )
//        )
//      )
//    )
//
//    val attemptMetricResultDf = attemptMetricDf.groupBy("course_id").pivot("attempt_metric").count()
//
//    val enrolmentCertificateDf = enrolmentDf.groupBy("courseid","certificate_issued").agg(count("userid").as("certified_count"))
//      .groupBy("courseid").pivot("certificate_issued").sum("certified_count")
//      .withColumn("enrolment_count", when(col("Yes").isNotNull, col("Yes")).otherwise(0) + when(col("No")isNotNull, col("No")).otherwise(0))
//
//
//
//    var resultDf: DataFrame = enrolmentCertificateDf.join(attemptMetricResultDf, enrolmentCertificateDf("courseid") === attemptMetricResultDf("course_id"), "left")
//
//    val columns = List[String]("one_attempt", "two_attempt", "three_attempt", "more_attempt", "Yes", "No")
//
//    columns.foreach((colName) => {
//      resultDf = if (!resultDf.columns.contains(colName)) {
//        resultDf.withColumn(colName, lit(0))
//      } else {
//        resultDf
//      }
//    })
//
//    resultDf.select("courseid", "enrolment_count", "Yes", "No", "one_attempt", "two_attempt", "three_attempt", "more_attempt").na.fill(0)
//      .withColumnRenamed("enrolment_count", "Total no. of enrolments")
//      .withColumnRenamed("Yes", "No. of users who earned a certificate")
//      .withColumnRenamed("No", "No. of users who did not get certificates")
//      .withColumnRenamed("one_attempt", "No. of users who got the certificate in a single attempt")
//      .withColumnRenamed("two_attempt", "No. of users who got the certificate in two attempts")
//      .withColumnRenamed("three_attempt", "No. of users who got the certificate in three attempts")
//      .withColumnRenamed("more_attempt", "No. of users who got the certificate in more than three attempts")
//
//  }
//
//  def getEnrolments()(implicit spark: SparkSession): DataFrame = {
//
//    import spark.implicits._
//
//    val courseDf = spark.read.format("com.databricks.spark.csv")
//      .option("delimiter", ",")
//      .option("header", "true")
//      .load("csvfile.csv")
//
//    val courseBatchSchema = Encoders.product[CourseBatch].schema
//    val courseBatchDf = loadData(courseBatchSettings, courseBatchSchema)
//
//    val courseBatchData = courseDf.join(courseBatchDf, Seq("courseid"), "inner")
//
//
//    val userEnrolmentSchema = Encoders.product[UserEnrolment].schema
//    val userEnrolmentDf = loadData(enrolmentsDBSettings, userEnrolmentSchema)
//
//    val userEnrolmentData = courseBatchData.join(userEnrolmentDf, Seq("courseid","batchid"), "inner")
//
//    val updatedUserEnrolmentDf = userEnrolmentData
//      .filter(col("active"))
////      .withColumn("certIssuedOn", certIssuedOn(col("issued_certificates")))
//      .withColumn("certificate_issued",
//        when((col("certificates").isNotNull && org.apache.spark.sql.functions.size(col("certificates").cast("array<map<string, string>>")) > 0)
//          || (col("issued_certificates").isNotNull && org.apache.spark.sql.functions.size(col("issued_certificates").cast("array<map<string, string>>")) > 0), "Yes").otherwise("No"))
//
//    updatedUserEnrolmentDf.show(10, false)
//
//    val file = new File("nishta_summary.txt" )
//    val print_Writer = new PrintWriter(file)
//
//    //Total batch created
//    print_Writer.write("Total Batch Created: " + courseBatchData.count())
//    //Total user enrolled
//    print_Writer.write("Total user enrolled: " + userEnrolmentData.count())
//
////    //User Without certificates
////    val userWithoutCertificates = userEnrolmentData.where((col("issued_certificates").isNull  || col("issued_certificates") === ""))
////
////    //User With certificates
////    val userWithCertificates = userEnrolmentData.where((col("issued_certificates").isNotNull  || col("issued_certificates") =!= ""))
////
////    //Total user count without certificates
////    print_Writer.write("Total user count without certificates: "+ userWithoutCertificates.count() )
////
////    //Total user count with certificates
////    print_Writer.write("Total user count with certificates: "+ userWithCertificates.count())
////
////    //Total batch created to csv
////    courseBatchData.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("audit_batch_created.csv")
////
////    //Total user without certificate to csv
////    userWithoutCertificates.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("audit_user_without_certificates.csv")
////
////    //Total user with certificate to csv
////    userWithCertificates.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("audit_user_with_certificates.csv")
////
//    print_Writer.close()
//
//    updatedUserEnrolmentDf
//  }
//
////  def certIssuedOnFunction(userEnrollment: Seq[scala.collection.immutable.Map[String, String]]): Timestamp = {
////    var issuedOn : ListBuffer[Date] = ListBuffer.empty
////    val utcDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
////    userEnrollment.foreach( oldUserInfo1 => {
////      val String1 = oldUserInfo1.get("lastIssuedOn").get
////      val dateConverted : Date = utcDateFormatter.parse(String1)
////      val v1 : ListBuffer[Date] = ListBuffer(dateConverted)
////      issuedOn = issuedOn ++ v1
////    })
////
////    val topDate = issuedOn.sortBy(_.getTime).head
////    new Timestamp(topDate.getTime)
////  }
////
////  val certIssuedOn = udf[Timestamp, Seq[Map[String, String]]](certIssuedOnFunction)
////
////  def time[R](block: => R): (Long, R) = {
////    val t0 = System.currentTimeMillis()
////    val result = block // call-by-name
////    val t1 = System.currentTimeMillis()
////    ((t1 - t0), result)
////  }
//}
