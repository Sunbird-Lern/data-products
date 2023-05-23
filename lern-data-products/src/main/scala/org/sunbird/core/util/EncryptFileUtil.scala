package org.sunbird.core.util

import org.apache.spark.sql.SparkSession

import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}
import org.bouncycastle.util.io.pem.PemReader
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, StorageConfig}
import org.sunbird.core.exhaust.JobRequest
import org.sunbird.core.util.DataSecurityUtil.downloadCsv

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import java.util.{Base64, UUID}

object EncryptFileUtil extends Serializable {

    val AES_ALGORITHM = "AES/CBC/PKCS5Padding"
    val RSA_ALGORITHM = "RSA"
    val RSA_CBC_PKCS1 = "RSA/ECB/PKCS1Padding"

    def encryptionFile(publicKeyFile: File, csvFilePath: String, keyForEncryption: String, level: String, storageConfig: StorageConfig, jobRequest: JobRequest)(implicit spark: SparkSession, fc: FrameworkContext)  : Unit = {

        val pathTuple = downloadCsv(csvFilePath, storageConfig, jobRequest, "", level)(spark.sparkContext.hadoopConfiguration, fc)
        JobLogger.log(s"encryptionFile tuple values localPath= $pathTuple._1 and objKey= $pathTuple._2, tempDir= $pathTuple._3", None, INFO)(new String())

        val uuid = generateUniqueId
        import java.security.KeyFactory
        import java.security.spec.X509EncodedKeySpec
        var encryptedUUIDBytes: Array[Byte] = Array[Byte]()
        val encryptAESCipher : Cipher = Cipher.getInstance(AES_ALGORITHM)
        var fileEncryptionKey: SecretKeySpec = null
        if(!"".equals(keyForEncryption))
        {
            //val userKey = new SecretKeySpec(keyForEncryption.getBytes, "AES")
            val userKey = generateAESKey(keyForEncryption.toCharArray)
            encryptAESCipher.init(Cipher.ENCRYPT_MODE, userKey)
            encryptedUUIDBytes = encryptAESCipher.doFinal(uuid.toString.getBytes("UTF-8"))
        } else {
            val publicKeyBytes = Files.readAllBytes(publicKeyFile.toPath)
            val pemReader = new PemReader(new java.io.StringReader(new String(publicKeyBytes)))
            val pemObject = pemReader.readPemObject()
            val keyFactory = KeyFactory.getInstance(RSA_ALGORITHM)
            val publicKeySpec = new X509EncodedKeySpec(pemObject.getContent)
            val publicKey = keyFactory.generatePublic(publicKeySpec)
            fileEncryptionKey = generateAESKey(uuid.toString.toCharArray)
            val encryptRSACipher: Cipher = Cipher.getInstance(RSA_CBC_PKCS1)
            encryptRSACipher.init(Cipher.ENCRYPT_MODE, publicKey)
            encryptedUUIDBytes = encryptRSACipher.doFinal(fileEncryptionKey.getEncoded)
        }
//        val uuidBytes = new String(ByteBuffer.wrap(new Array[Byte](16))
//          .putLong(uuid.getMostSignificantBits)
//          .putLong(uuid.getLeastSignificantBits)
//          .array()).toCharArray
//        val key = generateAESKey(uuidBytes)
        val fileBytes = Files.readAllBytes(Paths.get(pathTuple._1))
        encryptAESCipher.init(Cipher.ENCRYPT_MODE, fileEncryptionKey, new IvParameterSpec(Array.fill[Byte](16)(0)))
        val encryptedAESContent = encryptAESCipher.doFinal(fileBytes)
        try {
            val file = new File(pathTuple._1)
            val fileWriter = new FileOutputStream(file)
            try {
                fileWriter.write((level + "\n").getBytes())
                fileWriter.write((Base64.getEncoder.encodeToString(encryptedUUIDBytes) + "\n").getBytes())
                fileWriter.write(encryptedAESContent)
            }
            finally if (fileWriter != null) fileWriter.close()
        }
    }

    def generateUniqueId: UUID = UUID.randomUUID

    def generateAESKey(uuidBytes: Array[Char]): SecretKeySpec = {
        val salt = new Array[Byte](128)
        val random = new SecureRandom()
        random.nextBytes(salt)
        val pbeKeySpec = new PBEKeySpec(uuidBytes, salt, 1000, 256)
        val pbeKey = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(pbeKeySpec)
        new SecretKeySpec(pbeKey.getEncoded, "AES")
    }
}
