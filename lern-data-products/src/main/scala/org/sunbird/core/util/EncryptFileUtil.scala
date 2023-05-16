package org.sunbird.core.util

import javax.crypto.{Cipher, KeyGenerator}
import javax.crypto.spec.SecretKeySpec
import org.bouncycastle.util.io.pem.PemReader

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import java.util.UUID

object EncryptFileUtil extends Serializable {

    val AES_ALGORITHM = "AES/CBC/PKCS5Padding"
    val RSA_ALGORITHM = "RSA"

    def encryptionFile(publicKeyFile: File, csvFilePath: String, keyForEncryption: String, level: String)  : Unit = {
        val publicKeyBytes = Files.readAllBytes(publicKeyFile.toPath)

        val pemReader = new PemReader(new java.io.StringReader(new String(publicKeyBytes)))
        val pemObject = pemReader.readPemObject()

        val password = generateUniqueId
        import java.security.KeyFactory
        import java.security.spec.X509EncodedKeySpec
        var encryptedUUIDBytes: Array[Byte] = Array[Byte]()
        val encryptAESCipher : Cipher = Cipher.getInstance(AES_ALGORITHM)
        if(!keyForEncryption.isBlank)
        {
            val keyFactory = KeyFactory.getInstance(RSA_ALGORITHM)
            val publicKeySpec = new X509EncodedKeySpec(pemObject.getContent)
            val publicKey = keyFactory.generatePublic(publicKeySpec)
            val encryptRSACipher: Cipher = Cipher.getInstance(RSA_ALGORITHM)
            encryptRSACipher.init(Cipher.ENCRYPT_MODE, publicKey)
            encryptedUUIDBytes = encryptRSACipher.doFinal(password.toString.getBytes("UTF-8"))
        } else {
            val publicKey = new SecretKeySpec(keyForEncryption.getBytes, AES_ALGORITHM)
            encryptAESCipher.init(Cipher.ENCRYPT_MODE, publicKey)
            encryptedUUIDBytes = encryptAESCipher.doFinal(password.toString.getBytes("UTF-8"))
        }
        val key = generateAESKey(password)
        val fileBytes = Files.readAllBytes(Paths.get(csvFilePath))
        encryptAESCipher.init(Cipher.ENCRYPT_MODE, key)
        val encryptedAESContent = encryptAESCipher.doFinal(fileBytes)
        val levelAESContent = encryptAESCipher.doFinal(level.getBytes)

        try {
            val file = new File(csvFilePath)
            val outputStream : FileOutputStream = new FileOutputStream(file)
            try {
                outputStream.write(levelAESContent)
                outputStream.write(encryptedUUIDBytes)
                outputStream.write(encryptedAESContent)
            }
            finally if (outputStream != null) outputStream.close()
        }
    }

    def generateUniqueId: UUID = UUID.randomUUID

    def generateAESKey(uuid: UUID): SecretKeySpec = {
        val keyGenerator = KeyGenerator.getInstance("AES")
        val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
          .putLong(uuid.getMostSignificantBits)
          .putLong(uuid.getLeastSignificantBits)
          .array()
        val secureRandom = new SecureRandom(uuidBytes)
        keyGenerator.init(128, secureRandom)
        new SecretKeySpec(uuidBytes, "AES")
    }
}