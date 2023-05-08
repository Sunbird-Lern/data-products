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

    def encryptionFile(publicKeyFile: File, csvFilePath: String)  : Unit = {
        val publicKeyBytes = Files.readAllBytes(publicKeyFile.toPath)

        val pemReader = new PemReader(new java.io.StringReader(new String(publicKeyBytes)))
        val pemObject = pemReader.readPemObject()


        import java.security.KeyFactory
        import java.security.spec.EncodedKeySpec
        import java.security.spec.X509EncodedKeySpec
        val keyFactory = KeyFactory.getInstance("RSA")
        val publicKeySpec = new X509EncodedKeySpec(pemObject.getContent)
        val publicKey = keyFactory.generatePublic(publicKeySpec)
        val password = generateUniqueId
        val encryptCipher : Cipher = Cipher.getInstance("RSA")
        encryptCipher.init(Cipher.ENCRYPT_MODE, publicKey)
        val encryptedUUIDBytes = encryptCipher.doFinal(password.toString.getBytes("UTF-8"))


        val key = generateAESKey(password)
        val encryptAESCipher : Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
        val fileBytes = Files.readAllBytes(Paths.get(csvFilePath))
        encryptAESCipher.init(Cipher.ENCRYPT_MODE, key)
        val encryptedAESContent = encryptAESCipher.doFinal(fileBytes)

        try {
            val file = new File(csvFilePath)
            val stream1 : FileOutputStream = new FileOutputStream(file)
            try {
                stream1.write(encryptedUUIDBytes)
                stream1.write(encryptedAESContent)
            }
            finally if (stream1 != null) stream1.close()
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