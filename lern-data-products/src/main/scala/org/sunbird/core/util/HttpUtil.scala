package org.sunbird.core.util

import kong.unirest.Unirest
import org.apache.commons.collections.CollectionUtils
import org.sunbird.core.exception.ServerException

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.channels.{Channels, ReadableByteChannel}
import scala.collection.JavaConverters._
import scala.language.postfixOps

case class HTTPResponse(status: Int, body: String) extends Serializable {
  def isSuccess:Boolean = Array(200, 201) contains status
}

class HttpUtil extends Serializable {

  def downloadFile(url: String, downloadLocation: String): File = {
    val saveFile = new File(downloadLocation)
    if (!saveFile.exists) saveFile.mkdirs
    val urlObject = new URL(url)
    val filePath = downloadLocation + "/" + Slug.makeSlug(urlObject.getPath.substring(urlObject.getPath.lastIndexOf("/")+1))
    try {
      val readableByteChannel: ReadableByteChannel = Channels.newChannel(urlObject.openStream)
      val fileOutputStream: FileOutputStream = new FileOutputStream(filePath)
      fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MaxValue);
      new File(filePath)
    } catch {
      case io: java.io.IOException => throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received : " + url)
      case fnf: java.io.FileNotFoundException => throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received : " + url)
    }
  }

  def post(url: String, requestBody: String, headers: Map[String, String] = Map[String, String]("Content-Type"->"application/json")): HTTPResponse = {
    val response = Unirest.post(url).headers(headers.asJava).body(requestBody).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }
}