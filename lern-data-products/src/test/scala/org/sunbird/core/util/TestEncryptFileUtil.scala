package org.sunbird.core.util

import kong.unirest.UnirestException
import org.ekstep.analytics.framework.util.JSONUtils

import java.io.File

class TestEncryptFileUtil extends BaseSpec {

  "EncryptFileUtil" should "encrypt a file" in {
    val url = "https:/httpbin.org/post?type=test";
    val request = Map("popularity" -> 1);
    try {
      val file = new File("src/test/resources/reports/public.pem")
      EncryptFileUtil.encryptionFile(file ,"src/test/resources/reports/ap.csv","","PLAIN_DATASET")
    } catch {
      case ex: UnirestException => Console.println(s"Invalid Request for url: ${url}. The job failed with: " + ex.getMessage)
    }
  }
}