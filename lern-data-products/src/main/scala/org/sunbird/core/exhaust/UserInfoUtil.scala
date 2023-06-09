package org.sunbird.core.exhaust

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.sunbird.lms.exhaust.collection.UDFUtils
case class UserData(userid: String, state: Option[String] = Option(""), district: Option[String] = Option(""), orgname: Option[String] = Option(""), firstname: Option[String] = Option(""), lastname: Option[String] = Option(""), email: Option[String] = Option(""),
                    phone: Option[String] = Option(""), rootorgid: String, block: Option[String] = Option(""), schoolname: Option[String] = Option(""), schooludisecode: Option[String] = Option(""), board: Option[String] = Option(""), cluster: Option[String] = Option(""),
                    usertype: Option[String] = Option(""), usersubtype: Option[String] = Option(""))
object UserInfoUtil extends BaseReportsJob {
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid");
  private val userConsentDBSettings = Map("table" -> "user_consent", "keyspace" -> AppConf.getConfig("sunbird.user.keyspace"), "cluster" -> "UserCluster");
  private val redisFormat = "org.apache.spark.sql.redis";
  val cassandraFormat = "org.apache.spark.sql.cassandra";
  def getUserCacheDF(cols: Seq[String], persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    println("user cache-redis columns "+ cols)
    val schema = Encoders.product[UserData].schema
    println("userCacheDBSettings "+ userCacheDBSettings)
    println("redisFormat "+ redisFormat)
    val df1 = loadData(userCacheDBSettings, redisFormat, schema).withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).select(cols.head, cols.tail: _*)
    println("dummy cache df start ----")
    df1.show()
    println("dummy cache df end ----")
    val df = loadData(userCacheDBSettings, redisFormat, schema).withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).select(cols.head, cols.tail: _*)
      .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt, col("userid"))
    println("redis cache query output")
    df.show()
    if (persist) df.persist() else df
  }

  def getUserConsentDF(filters: String,  persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    println("userConsentDBSettings "+ userConsentDBSettings)
    println("cassandraFormat "+ cassandraFormat)
    val df = loadData(userConsentDBSettings, cassandraFormat, new StructType())
      .where(s"""$filters""")
      .dropDuplicates("user_id", "object_id")
      .withColumn("consentflag", when(lower(col("status")) === "active", "true").otherwise("false"))
      .withColumn("last_updated_on", date_format(col("last_updated_on"), "dd/MM/yyyy"))
      .select(col("user_id").as("userid"), col("consentflag"), col("last_updated_on").as("consentprovideddate"));
    println("User Consent Cassandra Query Output")
    df.show()
    if (persist) df.persist() else df
  }

  def decryptUserInfo(pgmUserDF: DataFrame, userCacheEncryptColNames: List[String])(implicit spark: SparkSession): DataFrame = {
    println("decryptUserInfo function")
    println("program user dataframe")
    pgmUserDF.show()
    println("user cache encrypt col names "+ userCacheEncryptColNames )
    val schema = pgmUserDF.schema
    val decryptFields = schema.fields.filter(field => userCacheEncryptColNames.contains(field.name));
    var resultDF = decryptFields.foldLeft(pgmUserDF)((df, field) => {
      df.withColumn(field.name, when(col("consentflag") === "true", UDFUtils.toDecrypt(col(field.name))).otherwise(lit("")))
    })
    resultDF = resultDF.withColumn("username",when(col("consentflag") === "true", col("username")).otherwise(lit("")))
    println("decrypt user info function output")
    resultDF.show()
    resultDF
  }
}