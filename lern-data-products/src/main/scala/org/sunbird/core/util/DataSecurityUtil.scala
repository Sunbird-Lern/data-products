package org.sunbird.core.util

import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.EncryptionMethod
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.{FrameworkContext, StorageConfig}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.sunbird.core.exhaust.JobRequest
import org.sunbird.core.util.EncryptFileUtil.encryptionFile

import java.io.File
import java.nio.file.Paths

object DataSecurityUtil {
  val httpUtil = new HttpUtil

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
    val readTenantPrefURL = Constants.USER_ORG_BASE_URL + Constants.TENANT_PREFERENCE_URL
    val httpResponse = httpUtil.post(readTenantPrefURL, request, headers)
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

  def getSecuredExhaustFile(jobId: String, orgId: String, channel: String, csvFile: String, encryptionKey: String, storageConfig: StorageConfig, request: JobRequest) (implicit conf: Configuration, fc: FrameworkContext): Unit = {
    val level = getSecurityLevel(jobId, orgId)
    level match {
      case "L1" =>
        csvFile
      case "L2" =>
        zipAndEncrypt(csvFile, storageConfig, request)
      case "L3" =>
        //val downloadPath = Constants.TEMP_DIR + orgId
        val downloadPath = Constants.TEMP_DIR + orgId
        val publicPemFile = httpUtil.downloadFile(encryptionKey, downloadPath)
        encryptionFile(publicPemFile, csvFile)
      case "L4" =>
        val exhaustEncryptionKey = getExhaustEncryptionKey(orgId, channel)
        //        val exhaustEncryptionKey = "https://sunbirddevbbpublic.blob.core.windows.net/sunbird-content-dev/organisation/0137774123743232000/public.pem"
        // Download the exhaustEncryptionKey
        //val downloadPath = Constants.TEMP_DIR + orgId
        val downloadPath = Constants.TEMP_DIR + orgId
        val publicPemFile = httpUtil.downloadFile(exhaustEncryptionKey, downloadPath)
        encryptionFile(publicPemFile, csvFile)
      case _ =>
        csvFile

    }
  }

  def getExhaustEncryptionKey(orgId: String, channel: String): String = {
    val requestBody = Map("request" -> (if(!orgId.isEmpty) Map("organisationId" -> orgId) else Map("channel" -> channel, "isTenant" -> true)))
    val request = JSONUtils.serialize(requestBody)
    val headers: Map[String, String] = Map("Content-Type" -> "application/json",
      "Authorization" -> Constants.KONG_API_KEY)
    val httpUtil = new HttpUtil
    val httpResponse = httpUtil.post(Constants.ORG_SEARCH_URL, request, headers)
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

  @throws(classOf[Exception])
  private def zipAndEncrypt(url: String, storageConfig: StorageConfig, request: JobRequest)(implicit conf: Configuration, fc: FrameworkContext): String = {

    val path = Paths.get(url);
    val storageService = fc.getStorageService(storageConfig.store, storageConfig.accountKey.getOrElse(""), storageConfig.secretKey.getOrElse(""));
    val tempDir = AppConf.getConfig("spark_output_temp_dir") + request.request_id + "/"
    val localPath = tempDir + path.getFileName;
    fc.getHadoopFileUtil().delete(conf, tempDir);
    val filePrefix = storageConfig.store.toLowerCase() match {
      // $COVERAGE-OFF$ Disabling scoverage
      case "s3" =>
        CommonUtil.getS3File(storageConfig.container, "")
      case "azure" =>
        CommonUtil.getAzureFile(storageConfig.container, "", storageConfig.accountKey.getOrElse("azure_storage_key"))
      case "gcloud" =>
        CommonUtil.getGCloudFile(storageConfig.container, "")
      // $COVERAGE-ON$ for case: local
      case _ =>
        storageConfig.fileName
    }
    val objKey = url.replace(filePrefix, "");
    if (storageConfig.store.equals("local")) {
      fc.getHadoopFileUtil().copy(filePrefix, localPath, conf)
    }
    // $COVERAGE-OFF$ Disabling scoverage
    else {
      storageService.download(storageConfig.container, objKey, tempDir, Some(false));
    }
    // $COVERAGE-ON$
    val zipPath = localPath.replace("csv", "zip")
    val zipObjectKey = objKey.replace("csv", "zip")
    val zipLocalObjKey = url.replace("csv", "zip")

    request.encryption_key.map(key => {
      val zipParameters = new ZipParameters();
      zipParameters.setEncryptFiles(true);
      zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD); // AES encryption is not supported by default with various OS.
      val zipFile = new ZipFile(zipPath, key.toCharArray());
      zipFile.addFile(localPath, zipParameters)
    }).getOrElse({
      new ZipFile(zipPath).addFile(new File(localPath));
    })
    val resultFile = if (storageConfig.store.equals("local")) {
      fc.getHadoopFileUtil().copy(zipPath, zipLocalObjKey, conf)
    }
    // $COVERAGE-OFF$ Disabling scoverage
    else {
      storageService.upload(storageConfig.container, zipPath, zipObjectKey, Some(false), Some(0), Some(3), None);
    }
    // $COVERAGE-ON$
    fc.getHadoopFileUtil().delete(conf, tempDir);
    resultFile;
  }
}
