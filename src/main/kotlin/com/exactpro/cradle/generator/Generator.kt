/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.generator

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleStorage
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.mstore.ShutdownManager
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.path
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.io.path.absolutePathString
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger {}

class Generator : CliktCommand() {
    val th2ConfigDir: Path by option(names = arrayOf("-c", "--th2-config-dir"))
        .path(mustExist = true, canBeDir = true, canBeFile = false)
        .required()
        .help { "path to th2 common config directory" }
    val messagesMetadataDir: Path? by option(names = arrayOf("-m", "--messages-metadata-dir"))
        .path(mustExist = true, canBeDir = true, canBeFile = false)
        .help { "path to messages metadata directory" }
    val eventsMetadataDir: Path? by option(names = arrayOf("-e", "--events-metadata-dir"))
        .path(mustExist = true, canBeDir = true, canBeFile = false)
        .help { "path to events metadata directory" }
    val pagesCount: Int by option(names = arrayOf("-pc", "--page-count"))
        .int()
        .default(0)
        .help { "number of pages for creating" }
    val pageLenMin: Int by option(names = arrayOf("-pl", "--page-length"))
        .int()
        .default(100)
        .help { "page duration in minutes" }

    override fun run() {
        val shutdownManager = ShutdownManager()
        try {
            val factory = CommonFactory.createFromArguments(*arrayOf("--configs", th2ConfigDir.absolutePathString()))
                .also(shutdownManager::registerResource)

            val bookName = factory.boxConfiguration.bookName
            val storage: CradleStorage = factory.cradleManager.storage
            val bookId = BookId(bookName)
            val startNanos: Long = Instant.now().run { epochSecond * SECONDS.toNanos(1) + nano }

            if (pagesCount > 0) {
                LOGGER.info { "createPages" }
                generatePages(storage, PageGeneratorSettings(
                    bookId,
                    pagesCount,
                    pageLenMin,
                    startMillis = startNanos / 1_000_000,
                ))
            }

            messagesMetadataDir?.let {
                LOGGER.info { "processMessagesCsvs" }
                generateMessages(storage, MessageGeneratorSettings(
                    directoryPath = it,
                    bookId = bookId,
                    startNanos = startNanos,
                ))
            }
            eventsMetadataDir?.let {
                LOGGER.info { "processEventsCsvs" }
                generateEvents(storage, EventGeneratorSettings(
                    directoryPath = it,
                    bookId = bookId,
                    startNanos = startNanos,
                ))
            }
            shutdownManager.closeResources()
        } catch (e: Exception) {
            LOGGER.error(e) { "Error during loading from Cradle" }
            shutdownManager.closeResources()
            exitProcess(2)
        }
    }
}

open class GeneratorSettings(
    val directoryPath: Path,
    val bookId: BookId,
    val startNanos: Long = System.nanoTime(),
) {
    val startMillis = startNanos / 1_000_000L
}

fun main(args: Array<String>) = Generator().main(args)
