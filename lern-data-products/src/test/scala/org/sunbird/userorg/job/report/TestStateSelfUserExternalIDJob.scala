package org.sunbird.userorg.job.report


import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils.serialize
import org.ekstep.analytics.framework.util.{HadoopFileUtil}
import org.ekstep.analytics.framework.{FrameworkContext}
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.util.{EmbeddedCassandra, HTTPResponse}
import org.sunbird.lms.job.report.{BaseReportSpec, BaseReportsJob}

class TestStateSelfUserExternalIDJob extends BaseReportSpec with Matchers with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/user_self_test_data.cql") // Load test data in embedded cassandra server
  }
  
  override def afterAll() : Unit = {
    super.afterAll();
    (new HadoopFileUtil()).delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/admin-user-reports")
  }

  //Created data : channels ApSlug and OtherSlug contains validated users created against blocks,districts and state
  //Only TnSlug doesn't contain any validated users
  ignore /*"StateSelfUserExternalID"*/ should "generate reports" in {
    implicit val fc = new FrameworkContext()
    val reportDF = StateAdminReportJob.generateExternalIdReport()(spark, fc)
    assert(reportDF.count() === 2);

    val user1 = reportDF.filter(col("Diksha UUID") === "56c2d9a3-fae9-4341-9862-4eeeead2e9a1").first

    user1.getAs[String]("Profile Email") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user1.getAs[String]("User Type") should be ("administrator,teacher,other,parent")
    user1.getAs[String]("Profile Phone number") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user1.getAs[String]("Name") should be ("localuser118f localuser118l")
    user1.getAs[String]("provider") should be ("ap")
    user1.getAs[String]("State provided ext. ID") should be (null)
    user1.getAs[String]("Org Phone") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user1.getAs[String]("School UDISE ID") should be ("190923")
    user1.getAs[String]("School Name") should be ("mgm21")
    user1.getAs[String]("District") should be ("Chittooor")
    user1.getAs[String]("Org Email ID") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user1.getAs[String]("Root Org of user") should be ("AP")
    user1.getAs[String]("State") should be ("Andhra")
    user1.getAs[String]("User-Sub Type") should be ("hm,crp")
    user1.getAs[String]("Cluster") should be ("Chittooorblock1cluster1")
    user1.getAs[String]("Block") should be ("Chittooorblock1")

    val user2 = reportDF.filter(col("Diksha UUID") === "8eaa1621-ac15-42a4-9e26-9c846963f331").first

    user2.getAs[String]("Profile Email") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user2.getAs[String]("User Type") should be ("teacher")
    user2.getAs[String]("Profile Phone number") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user2.getAs[String]("Name") should be ("localuser117f localuser117l")
    user2.getAs[String]("provider") should be ("ka")
    user2.getAs[String]("State provided ext. ID") should be (null)
    user2.getAs[String]("Org Phone") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user2.getAs[String]("School UDISE ID") should be ("orgext2")
    user2.getAs[String]("School Name") should be ("mgm21")
    user2.getAs[String]("Diksha UUID") should be ("8eaa1621-ac15-42a4-9e26-9c846963f331")
    user2.getAs[String]("District") should be ("Gulbarga")
    user2.getAs[String]("Org Email ID") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user2.getAs[String]("Root Org of user") should be ("MPPS SIMHACHALNAGAR")
    user2.getAs[String]("State") should be ("Karnataka")
    user2.getAs[String]("User-Sub Type") should be("")
    user2.getAs[String]("Cluster") should be ("Gulbargablockcluster1")
    user2.getAs[String]("Block") should be ("Gulbargablock1")

  }
  
  ignore /*"StateSelfUserExternalIDWithZip"*/ should "execute with zip failed to generate" in {
    implicit val fc = new FrameworkContext()
    try {
      val l3LevelRespponse = createHTTPResponse("TEXT_KEY_ENCRYPTED_DATASET")
      import org.sunbird.core.util.HttpUtil
        val httpMock = mock[HttpUtil]
      (httpMock.post(_: String, _: String, _: Map[String, String])).expects(*, *, *).returning(l3LevelRespponse).anyNumberOfTimes()
      val reportDF = StateAdminReportJob.generateExternalIdReport()(spark, fc)
    } catch {
      case ex: Exception => assert(ex.getMessage === "Self-Declared user level zip generation failed with exit code 127");
    }
  }

  def createResponseBody(level: String) : String = {
    val jobData = Map[String, AnyRef]("admin-user-reports" -> level)
    val dataMap = Map[String, AnyRef]("level" -> "PLAIN_DATASET", "job" -> jobData)
    val responseMap = Map[String, AnyRef]("data" -> dataMap)
    val resultMap = Map[String, AnyRef]("response" -> responseMap)
    val responseBodyMap = Map[String, AnyRef]("result" -> resultMap)
    val responseBodyStr = serialize(responseBodyMap)
    responseBodyStr
  }

  def createHTTPResponse(level: String) : HTTPResponse = {
    val responseBody = createResponseBody(level)
    val httpResponse = HTTPResponse(200, responseBody)
    httpResponse
  }
}
