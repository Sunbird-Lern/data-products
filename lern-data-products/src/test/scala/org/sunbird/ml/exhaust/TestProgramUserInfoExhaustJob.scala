package org.sunbird.ml.exhaust

import org.apache.spark.sql.{Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalamock.scalatest.MockFactory
import org.sunbird.lms.job.report.BaseReportSpec
import org.sunbird.core.util.{EmbeddedCassandra, EmbeddedPostgresql, RedisConnect}
import org.sunbird.core.exhaust.{BaseReportsJob, JobRequest}
import org.sunbird.ml.exhaust.ProgramUserInfoExhaustJob
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._
class TestProgramUserInfoExhaustJoB extends BaseReportSpec with MockFactory with BaseReportsJob {
  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  var jedis: Jedis = _
  val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")

  override def beforeAll(): Unit = {
    spark = getSparkSession();
    super.beforeAll()
    redisServer = new RedisServer(6341)
    redisServer.start()
    setupRedisData()
    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
    redisServer.stop()
    println("******** closing the redis connection **********" + redisServer.isActive)
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
    spark.close()
  }

  def setupRedisData(): Unit = {
    val redisConnect = new RedisConnect("localhost", 6341)
    val jedis = redisConnect.getConnection(0, 100000)
    jedis.hmset("user:ca0ded9d-5cd1-401c-a3ca-8c58d2bec282", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"Neeraj","userid":"ca0ded9d-5cd1-401c-a3ca-8c58d2bec282","lastname":"Doddamane","phone":"","email":"Qey38pgXtNy8iGvxLXvuczrPmy+8N8zXIAGh5VQfqALyklHiFS1DgC/rZoRvyngtgzncO8l6Ruez\nfkY/o3egFhkcKFmHNJSYA/DkqkLflSrQJTt9KYXrt2O6UV2EQ7XDT6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:19569316-7b66-4b94-9082-f2b4b8b178ee", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"Test user 1","userid":"19569316-7b66-4b94-9082-f2b4b8b178ee","lastname":"","phone":"WmjJfKzONJE99/IZ+mTH6wDSKrGEmT2IRrYySkz5BCWHXOB6kNWv8HI+IKqZDLbZ+YEzVHXd7IY2\nmXzpPeqoZA5JXp1Zr058lDZXPOh7FvGIaGYypz5SZdGUojSOqzHYT6a+wzaAmCWueMEdPmZuRg==","email":"Qey38pgXtNy8iGvxLXvuczrPmy+8N8zXIAGh5VQfqALyklHiFS1DgC/rZoRvyngtgzncO8l6Ruez\nfkY/o3egFhkcKFmHNJSYA/DkqkLflSrQJTt9KYXrt2O6UV2EQ7XDT6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:1405f334-ee59-42fc-befb-51986221881e", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"orgadmin","userid":"1405f334-ee59-42fc-befb-51986221881e","lastname":"Admin","phone":"KFQAv2vchM68MmQxVEqheyL7Ps60KkOG34X5tb6WPrbXScmOIAaYgCR5jA7jn8rPDnO6tneEM2Cl\n1Xji04U9VOp9iMrvbeV3nGPHRPJaj6cgY/VCn55i439OVshNqvMRT6a+wzaAmCWueMEdPmZuRg==","email":"Qey38pgXtNy8iGvxLXvuczrPmy+8N8zXIAGh5VQfqALyklHiFS1DgC/rZoRvyngtgzncO8l6Ruez\nfkY/o3egFhkcKFmHNJSYA/DkqkLflSrQJTt9KYXrt2O6UV2EQ7XDT6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:13b24110-af97-430a-9d2c-0dd7ef6dccaa", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"TNOrg","userid":"13b24110-af97-430a-9d2c-0dd7ef6dccaa","lastname":"Admin","phone":"9za3Pu4/Xme2GDCuiIOwx5qBPFL8TzjtgvVVn8v0hmd5BsS2idRSHiX9n+DOlgUEws3jo+GFQpe0\niDoflUf7ajx9oGbAIrEyGBx8k9I5q2J62ZEh91vNAO4UcR3Hyr3uT6a+wzaAmCWueMEdPmZuRg==","email":"zh5jwymoJUEo2ZpkR/dprBCQZNzlwS6NunsQsREJGNoacTmzfl6p13WwCYStEsLAW8rxXwNe+gNa\nCzvXba+cRs5hzHVsKaswanzFwRW8xHdn+BET3U7ZobsqAILP9HxcT6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:3fa8dd7a-6426-48fa-b87b-0f2955e174a4", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"Deepa","userid":"3fa8dd7a-6426-48fa-b87b-0f2955e174a4","lastname":"","phone":"vazvbnMAw17lrGOlSh6bCPrhqcRKONnPxb8lKzFkFLrR2VVuZZTvsQyXd35Vi0vyCQCokGYKicyt\n6MQ37BMHXbg009cRa1RrHsmGvfvRVTjo092oNEjzDOs4rgx5JSSqT6a+wzaAmCWueMEdPmZuRg==","email":""};"""))
    jedis.hmset("user:857fef6b-ff7e-42d7-b80e-53a755bed6af", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"Okok46","userid":"857fef6b-ff7e-42d7-b80e-53a755bed6af","lastname":"","phone":"","email":"z+k92nBfyS1PrQ+yFYsTyd2iUtb/+nil03uSVZ+sdbwwFWhYkfQBBKjZ4TSl4SiutftcCGtjwRzo\nacLFP/AdF09KY9CSOe31SGfXFLfHPU+S1cV3v0vYkBPcrmgb2jY3T6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:489be8be-f6ab-482d-8b31-12ac7eb5085c", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"teststag44","userid":"489be8be-f6ab-482d-8b31-12ac7eb5085c","lastname":"","phone":"","email":"0UcdyQa006kdz5WpfSLE6RqSuxhT2qvo04mSBc5PByy3i1UMBTXSDhHQhEJTJy/j3O4ATkrHWhFC\nMOyPIrsi5TWjJ36elHDt3WIISpnI+K9TxTEG/pfmWUVRjdGVtN0TT6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:e1ebd001-b9ae-4082-967a-2013fd3f32fb", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"Big dog","userid":"e1ebd001-b9ae-4082-967a-2013fd3f32fb","lastname":"","phone":"","email":"IDpOC3FSwy29GBxFKduR6i29S2m9od+eK19M3LkXVN0AyLAYlHtaz9WbIKEFe3EJi/YOwVw098C4\nOnluLNcRRYT72Zzr0LNw48nyBqSfNiNvIwiQeOT62k0erS0QBsgET6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:e96a3ad0-d52c-4a70-a36e-b8dc7529779f", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"Okok80","userid":"e96a3ad0-d52c-4a70-a36e-b8dc7529779f","lastname":"","phone":"","email":"biz4NPhbESofl0uhUkW0WrOaqIw3vFv4tgl0KGKakWMYDsnl2eBshWhql5Hx5MvhggRdqJ01do18\nQN2VqfnIBU9KY9CSOe31SGfXFLfHPU+S1cV3v0vYkBPcrmgb2jY3T6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.hmset("user:efa2ab2e-436f-4d7b-befc-daf4776b1b5f", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname":"CEO 0099","userid":"efa2ab2e-436f-4d7b-befc-daf4776b1b5f","lastname":"CCO","phone":"vazvbnMAw17lrGOlSh6bCPrhqcRKONnPxb8lKzFkFLrR2VVuZZTvsQyXd35Vi0vyCQCokGYKicyt\n6MQ37BMHXbg009cRa1RrHsmGvfvRVTjo092oNEjzDOs4rgx5JSSqT6a+wzaAmCWueMEdPmZuRg==","email":"FZN+gp5TWshh0BFz5nDqNFIu10/H3svUo6OvqZMsUdTu08g+KI/wWyRbVriy8p0HX6im8huox8yN\ndWWCUmBJi76aTXiifFDsTaz4x8kxRq/JOe8SlFEhUR4V12H/y3fBT6a+wzaAmCWueMEdPmZuRg=="};"""))
    jedis.close()
  }

  "ProgramUserInfoExhaustJob" should "generate the user info report with all the users for a given program" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('program_602512d8e6aefa27d9629bc3:01250894314817129555','37564AN8F134RR7532F125651B51S17D','program-user-exhaust','SUBMITTED','{\"type\":\"program-user-exhaust\",\"params\":{\"filters\":[{\"table_name\":\"program_enrollment\",\"table_filters\":[{\"name\":\"program_id\",\"operator\":\"=\",\"value\":\"602512d8e6aefa27d9629bc3\"}]},{\"table_name\":\"user_consent\",\"table_filters\":[{\"name\":\"object_id\",\"operator\":\"=\",\"value\":\"602512d8e6aefa27d9629bc3\"}]}]},\"title\":\"User Detail Report\"}','ml-program-test-user-01','ORG_001','2023-01-25 05:58:18.666', '{}', NULL, NULL, 0,'' ,0, 'test1234');")
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.ml.exhaust.ProgramUserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","authorizedRoles":["PROGRAM_MANAGER"],"id":"ml-program-user-exhaust","keyspace_name":"sunbird_programs","table":[{"name":"program_enrollment","columns":["user_id","program_name","program_externalId","state_name","district_name","block_name","cluster_name","school_code","school_name","user_type","user_sub_type","organisation_name","pii_consent_required"]},{"name":"user_consent","columns":["user_id","status","last_updated_on"]},{"name":"user","columns":["userid","firstname","lastname","email","phone","username"],"encrypted_columns":["email","phone"],"final_columns":["email","phone","username"]}],"label_mapping":{"user_id":"User UUID","username":"User Name(On user consent)","phone":"Mobile number(On user consent)","email":"Email ID(On user consent)","consentflag":"Consent Provided","consentprovideddate":"Consent Provided Date","program_name":"Program Name","program_externalId":"Program ID","state_name":"State","district_name":"District","block_name":"Block","cluster_name":"Cluster","school_code":"School Id","school_name":"School Name","user_type":"Usertype","user_sub_type":"Usersubtype","organisation_name":"Org Name"},"order_of_csv_column":["User UUID","User Name(On user consent)","Mobile number(On user consent)","Email ID(On user consent)","Consent Provided","Consent Provided Date","Program Name","Program ID","State","District","Block","Cluster","School Id","School Name","Usertype","Usersubtype","Org Name"],"sort":["District","Block","Cluster","School Id","User UUID"],"quote_column":["User Name(On user consent)","Program Name"],"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"Program UserInfo Exhaust"}"""
    //    val strConfig = """  {"search":{"type":"none"},"model":"org.sunbird.ml.exhaust.ProgramUserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","authorizedRoles":["PROGRAM_MANAGER"],"id":"ml-program-user-exhaust","keyspace_name":"sunbird_programs","table":[{"name":"program_enrollment","columns":["user_id","program_name","program_externalId","state_name","district_name","block_name","cluster_name","school_code","school_name","user_type","user_sub_type","organisation_name","pii_consent_required"]},{"name":"user_consent","columns":["user_id","status","last_updated_on"]},{"name":"user","columns":["userid","firstname","lastname","email","phone","username"],"encrypted_columns":["email","phone"],"final_columns":["email","phone","username"]}],"label_mapping":{"user_id":"User UUID","username":"User Name(On user consent)","phone":"Mobile number(On user consent)","email":"Email ID(On user consent)","consentflag":"Consent Provided","consentprovideddate":"Consent Provided Date","program_name":"Program Name","program_externalId":"Program ID","state_name":"State","district_name":"District","block_name":"Block","cluster_name":"Cluster","school_code":"School Id","school_name":"School Name","user_type":"Usertype","user_sub_type":"Usersubtype","organisation_name":"Org Name"},"order_of_csv_column":["User UUID","User Name(On user consent)","Mobile number(On user consent)","Email ID(On user consent)","Consent Provided","Consent Provided Date","Program Name","Program ID","State","District","Block","Cluster","School Id","School Name","Usertype","Usersubtype","Org Name"],"sort":["District","Block","Cluster","School Id","User UUID"],"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","key":"reports/","format":"csv"},"output":[{"to":"file","params":{"file":"reports/"}}],"parallelization":8,"appName":"Program UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgramUserInfoExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='program-user-exhaust'")
    while (postgresQuery.next()) {
      println(postgresQuery.getString("status"))
      println(postgresQuery.getString("download_urls"))
      println(postgresQuery.getString("err_message"))
    }
  }
}