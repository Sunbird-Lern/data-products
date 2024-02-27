package org.sunbird.userorg.job.report

import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.util.{EmbeddedCassandra, SparkSpec}

class TestDeleteUsersAssetReportJob extends SparkSpec(null) with MockFactory {
  implicit var spark: SparkSession = _
  val fwServer = new MockWebServer()

  val fwDispatcher: Dispatcher = new Dispatcher() {
    @throws[InterruptedException]
    override def dispatch(request: RecordedRequest): MockResponse = {
      (request.getPath, request.getMethod) match {
        case ("/search/v3/search", "POST") =>
          new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":"api.search-service.search","ver":"3.0","ts":"2024-02-27T03:55:26ZZ","params":{"resmsgid":"4507bb91-cfa8-4d3a-85a8-75f3237d8b67","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":27276,"Question":[{"identifier":"do_21341710175921766411279","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 1 ","lastUpdatedOn":"2024-02-01T13:59:23.001+0000","objectType":"Question","status":"Live"}],"content":[{"identifier":"do_2133908432688742401472","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 00041 4291 1527953944 1527953944136","lastUpdatedOn":"2021-10-19T06:20:23.103+0000","objectType":"Content","status":"Draft"},{"identifier":"do_21349929706079846413","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 1 New question set course 4.8 ","lastUpdatedOn":"2023-01-02T12:04:57.610+0000","objectType":"Content","status":"Live"},{"identifier":"do_213427827033423872167","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 4.5 Course assessment ","lastUpdatedOn":"2023-01-02T14:30:52.292+0000","objectType":"Content","status":"Live"},{"identifier":"do_2134313619898040321558","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 4.5 course merit cert 1","lastUpdatedOn":"2023-01-02T13:49:49.688+0000","objectType":"Content","status":"Live"},{"identifier":"do_2134318607345909761794","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 4.5 No cert Course","lastUpdatedOn":"2023-01-02T13:36:12.035+0000","objectType":"Content","status":"Live"},{"identifier":"do_2134306656822722561537","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 4.5 trackable collection ","lastUpdatedOn":"2021-12-14T12:39:39.946+0000","objectType":"Content","status":"Draft"},{"identifier":"do_21344606598582272012190","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 4.6 Book with all content types","lastUpdatedOn":"2023-01-02T13:27:58.976+0000","objectType":"Content","status":"Live"},{"identifier":"do_213447466806640640165","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 4.6 Course assessment ","lastUpdatedOn":"2022-01-07T06:24:44.760+0000","objectType":"Content","status":"Draft"},{"identifier":"do_2134517704054538241960","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":" 4.6 course merit cert","lastUpdatedOn":"2023-01-02T14:27:56.774+0000","objectType":"Content","status":"Live"}]}}""")
        case ("/lms/v1/course/batch/search", "POST") =>
          new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":"api.course.batch.search","ver":"v1","ts":"2024-02-27 03:57:04:945+0000","params":{"resmsgid":null,"msgid":"9a5b8c1d-31d3-44ef-b0f6-6448757afdce","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"count":1558,"content":[{"identifier":"01308536328442675295","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"Copy of Course 58","status":1},{"identifier":"013407878578069504340","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"november","status":1},{"identifier":"0130943438114324484","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"Copy of 3.2.5 AN Book","status":1},{"identifier":"01319834012024832019","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"21233_01","status":1},{"identifier":"01341512822657024027","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"new batch","status":1},{"identifier":"0132563991562567689","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"TN Course TPD","status":1},{"identifier":"01333783330317107220","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"Untitled Course retesting","status":1},{"identifier":"01309449242632192024","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"nvn-Course","status":1},{"identifier":"01321335245581516848","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"22621 Course with assess and max attempt","status":1},{"identifier":"0134170584978063360","createdBy":"fca2925f-1eee-4654-9177-fece3fd6afc9","name":"new ","status":1}]}}}""")
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    fwServer.setDispatcher(fwDispatcher)
    fwServer.start(9100)
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/user_data.cql") // Load test data in embedded cassandra server
  }

  override def afterAll(): Unit = {
    super.afterAll();
  }


  "DeletedUsersAssetsReportJob" should "generate reports" in {
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.userorg.job.report.DeletedUsersAssetsReportJob"}}"""
    DeletedUsersAssetsReportJob.main(strConfig)
  }

  "fetchDeletedUsers" should "return a DataFrame" in {
    val deletedUsersDF: DataFrame = DeletedUsersAssetsReportJob.fetchDeletedUsers(spark)
    assert(deletedUsersDF != null)
    assert(deletedUsersDF.columns.length > 0)
  }

  "getUserIdsFromDeletedUsers" should "return a list of user ids" in {
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]
    val deletedUsersDF: DataFrame = DeletedUsersAssetsReportJob.fetchDeletedUsers(spark)
    val userIds: List[String] = DeletedUsersAssetsReportJob.getUserIdsFromDeletedUsers(deletedUsersDF)
    assert(userIds != null)
    assert(userIds.nonEmpty)
  }

  "fetchContentAssets" should "return a DataFrame" in {
    val mockedSpark: SparkSession = spark
    val contentAssetsDF: DataFrame = DeletedUsersAssetsReportJob.fetchContentAssets()(mockedSpark)
    assert(contentAssetsDF != null)
    assert(contentAssetsDF.columns.length > 0)
  }

  "fetchCourseAssets" should "return a DataFrame" in {
    val mockedSpark: SparkSession = spark
    val courseAssetsDF: DataFrame = DeletedUsersAssetsReportJob.fetchCourseAssets()(mockedSpark)
    assert(courseAssetsDF != null)
    assert(courseAssetsDF.columns.length > 0)
  }

}