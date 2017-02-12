package org.sl.utils.avro.validator.test

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

class ResourceUtils {

    companion object {
        fun resource(path: String): String {
            return File(ResourceUtils::class.java.classLoader.getResource(path).toURI()).absolutePath
        }

        fun readResource(path: String): ByteArray {
            val filePath = resource(path)
            return Files.readAllBytes(Paths.get(filePath))
        }
    }
}