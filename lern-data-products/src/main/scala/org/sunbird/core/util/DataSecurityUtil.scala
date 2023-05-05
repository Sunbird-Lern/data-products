package org.sunbird.core.util

import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.sunbird.core.util.EncryptFileUtil.encryptionFile

object DataSecurityUtil {

  /**
   * fetch the job security level by calling tenant preference read API using orgId
   *
   * @param jobId
   * @param orgId
   * @return
   */
  def getSecurityLevel(jobId: String, orgId: String): String = {
    val requestBody = Map("request" -> Map("orgId" -> orgId, "key" -> "dataSecurityPolicy"))
    val request = JSONUtils.serialize(requestBody)
    val headers: Map[String, String] = Map("Content-Type" -> "application/json",
    "x-authenticated-user-token" -> Constants.KEYCLOAK_ACCESS_TOKEN,
    "Authorization" -> Constants.KONG_API_KEY)
    val httpUtil = new HttpUtil
    val httpResponse = httpUtil.post(Constants.USER_ORG_BASE_URL + Constants.TENANT_PREFERENCE_URL, request, headers)
    if (httpResponse.status == 200) {
      JobLogger.log(s"dataSecurityPolicy for org=$orgId, response body=${httpResponse.body}", None, INFO)(new String())
      val responseBody = JSONUtils.deserialize[Map[String, AnyRef]](httpResponse.body)
      val data = responseBody.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("response", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("data", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      val globalLevel = data.getOrElse("level", "").asInstanceOf[String]
      val jobDetail = data.getOrElse("job", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse(jobId, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      val jobLevel = jobDetail.getOrElse("level", "").asInstanceOf[String]
      if (!StringUtils.isEmpty(jobLevel)) jobLevel else globalLevel
    } else {
      JobLogger.log(s"Error response from createUserFeed API for request :: $requestBody :: response is :: ${httpResponse.status} ::  ${httpResponse.body}", None, ERROR)(new String())
      ""
    }
  }

  def getSecuredExhaustFile(jobId: String, orgId: String, csvFile: String): Unit = {
    val level = getSecurityLevel(jobId, orgId)
    level match {
      case "L1" =>
        csvFile
      case "L2" =>
        csvFile
      case "L3" =>
        csvFile
      case "L4" =>
        val exhaustEncryptionKey = getExhaustEncryptionKey(orgId)
        //        val exhaustEncryptionKey = "https://sunbirddevbbpublic.blob.core.windows.net/sunbird-content-dev/organisation/0137774123743232000/public.pem"
        // Download the exhaustEncryptionKey
        val httpUtil = new HttpUtil
        //val downloadPath = Constants.TEMP_DIR + orgId
        val downloadPath = Constants.TEMP_DIR + orgId
        val publicPemFile = httpUtil.downloadFile(exhaustEncryptionKey, downloadPath)
        encryptionFile(publicPemFile, csvFile)
      case _ =>
        csvFile

    }
  }

  def getExhaustEncryptionKey(orgId: String): String = {
    val requestBody = Map("request" -> Map("organisationId" -> orgId))
    val request = JSONUtils.serialize(requestBody)
    val headers: Map[String, String] = Map("Content-Type" -> "application/json",
      "Authorization" -> Constants.KONG_API_KEY)
    val httpUtil = new HttpUtil
    val httpResponse = httpUtil.post(Constants.USER_ORG_BASE_URL + Constants.ORG_RRAD_URL, request, headers)
    if (httpResponse.status == 200) {
      JobLogger.log(s"getOrgDetail for org=$orgId, response body=${httpResponse.body}", None, INFO)(new String())
      val responseBody = JSONUtils.deserialize[Map[String, AnyRef]](httpResponse.body)
      val keys = responseBody.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("response", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("keys", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      val exhaustEncryptionKey = keys.getOrElse("exhaustEncryptionKey", List()).asInstanceOf[List[String]]
      if (exhaustEncryptionKey.nonEmpty) exhaustEncryptionKey.head else ""
    } else
    ""
  }
}
