package org.sunbird.core.exception

class APIException(message: String, cause: Throwable) extends Exception(message, cause)

class ServerException(code: String, msg: String, cause: Throwable = null) extends Exception(msg, cause)
