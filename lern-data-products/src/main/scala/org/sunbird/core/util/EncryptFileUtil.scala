package org.sunbird.core.util

import org.apache.spark.sql.SparkSession

import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{PBEKeySpec, SecretKeySpec}
import org.bouncycastle.util.io.pem.PemReader
import org.ekstep.analytics.framework.{FrameworkContext, StorageConfig}
import org.sunbird.core.exhaust.JobRequest
import org.sunbird.core.util.DataSecurityUtil.downloadCsv

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import java.util.UUID

object EncryptFileUtil extends Serializable {

    val AES_ALGORITHM = "AES/CBC/PKCS5Padding"
    val RSA_ALGORITHM = "RSA"

    def encryptionFile(publicKeyFile: File, csvFilePath: String, keyForEncryption: String, level: String, storageConfig: StorageConfig, jobRequest: JobRequest)(implicit spark: SparkSession, fc: FrameworkContext)  : Unit = {

        downloadCsv(csvFilePath, storageConfig, jobRequest, "", level)(spark.sparkContext.hadoopConfiguration, fc)

        val uuid = generateUniqueId
        import java.security.KeyFactory
        import java.security.spec.X509EncodedKeySpec
        var encryptedUUIDBytes: Array[Byte] = Array[Byte]()
        val encryptAESCipher : Cipher = Cipher.getInstance(AES_ALGORITHM)
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
            val encryptRSACipher: Cipher = Cipher.getInstance(RSA_ALGORITHM)
            encryptRSACipher.init(Cipher.ENCRYPT_MODE, publicKey)
            encryptedUUIDBytes = encryptRSACipher.doFinal(uuid.toString.getBytes("UTF-8"))
        }
        val uuidBytes = new String(ByteBuffer.wrap(new Array[Byte](16))
          .putLong(uuid.getMostSignificantBits)
          .putLong(uuid.getLeastSignificantBits)
          .array()).toCharArray
        val key = generateAESKey(uuidBytes)
        val fileBytes = Files.readAllBytes(Paths.get(csvFilePath))
        encryptAESCipher.init(Cipher.ENCRYPT_MODE, key)
        val encryptedAESContent = encryptAESCipher.doFinal(fileBytes)

        try {
            val file = new File(csvFilePath)
            val outputStream : FileOutputStream = new FileOutputStream(file)
            try {
                outputStream.write(level.getBytes)
                outputStream.write(encryptedUUIDBytes)
                outputStream.write(encryptedAESContent)
            }
            finally if (outputStream != null) outputStream.close()
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
