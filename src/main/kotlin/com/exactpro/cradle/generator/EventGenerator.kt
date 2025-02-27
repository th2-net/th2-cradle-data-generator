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
import com.exactpro.cradle.Direction
import com.exactpro.cradle.counters.Interval
import com.exactpro.cradle.filters.FilterForGreater
import com.exactpro.cradle.filters.FilterForLess
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventBatchToStore
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder
import com.exactpro.cradle.testevents.TestEventFilter
import com.exactpro.cradle.testevents.TestEventSingle
import com.exactpro.cradle.testevents.TestEventSingleToStore
import com.exactpro.cradle.testevents.TestEventToStore
import com.exactpro.th2.estore.Configuration
import com.exactpro.th2.estore.EventPersistor
import com.exactpro.th2.estore.ErrorCollector
import com.exactpro.th2.estore.Persistor
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.time.Instant
import kotlin.io.path.nameWithoutExtension
import kotlin.random.Random

private val LOGGER = KotlinLogging.logger {}

fun generateEvents(storage: CradleStorage, config: EventGeneratorSettings) {
    EventPersistor(
        EventErrorCollector,
        Configuration(128, Runtime.getRuntime().totalMemory() / 2L, 15, 5000L, 1, 5000L),
        storage
    ).also { it.start() }.use { persistor ->
        val dummyMessageIds = buildList {
            for (sequence in 1L..100L) {
                add(StoredMessageId(config.bookId, config.sessionAlias, Direction.FIRST, Instant.now(), sequence))
            }
        }

        if (Files.exists(config.directoryPath) && Files.isDirectory(config.directoryPath)) {
            Files.list(config.directoryPath).forEach { file ->
                if (Files.isRegularFile(file) && file.toString().endsWith(".csv")) {
                    processCsvEventsFile(storage, config, dummyMessageIds, file)
                }
            }
        } else {
            throw NoSuchFileException("Directory does not exists")
        }
    }
}

@Suppress("unused")
fun loadEvents(storage: CradleStorage, bookId: BookId, from: Instant, to: Instant) {
    val scopes =
        storage.getScopes(bookId, Interval(from, to)).asIterable().mapIndexed { i, scope -> Pair(scope, i) }.toMap()
    val fromMillis = from.toEpochMilli()

    for (scope in scopes.entries) {
        val filter = TestEventFilter(bookId, scope.key).apply {
            startTimestampFrom = FilterForGreater.forGreaterOrEquals(from)
            startTimestampTo = FilterForLess.forLess(to)
        }

        val events = storage.getTestEvents(filter)

        val file = File("scope" + scope.value + ".csv")
        BufferedWriter(FileWriter(file)).use { writer ->
            for (event in events) {
                if (event.isBatch) {
                    val eventBatch = event.asBatch()
                    val parentIdString = event.parentId.toString(fromMillis, scopes)
                    writer.write(parentIdString) // batch start
                    writer.newLine()
                    for (eventSingle in eventBatch.testEvents) {
                        writeEvent(writer, eventSingle, fromMillis, scopes)
                    }
                    writer.write(".\n") // batch end
                } else {
                    writeEvent(writer, event.asSingle(), fromMillis, scopes)
                }
            }
        }
    }
}

private fun processCsvEventsFile(
    storage: CradleStorage,
    config: EventGeneratorSettings,
    dummyMessageIds: List<StoredMessageId>,
    filePath: Path
) {
    val scope = filePath.nameWithoutExtension
    val lines = Files.readAllLines(filePath)

    val batchBuilder = TestEventBatchToStoreBuilder(Int.MAX_VALUE, Long.MAX_VALUE)
    var batchCounter = 1
    var batch: TestEventBatchToStore? = null

    for (line in lines) {
        val values = line.split(",")

        if (values.size == 1) {
            if (values[0] == ".") {
                storage.storeTestEvent(batch)
                batch = null
            } else {
                val parentIdParts = values[0].split(':')
                val parentScope = parentIdParts[0]
                val parentTimestampNano = parentIdParts[1].toLong() + config.startNanos
                val parentId = parentIdParts[2]

                batch = batchBuilder
                    .idRandom(config.bookId, parentScope)
                    .parentId(
                        StoredTestEventId(
                            config.bookId,
                            parentScope,
                            Instant.ofEpochSecond(
                                parentTimestampNano / 1_000_000_000,
                                parentTimestampNano % 1_000_000_000
                            ),
                            parentId
                        )
                    )
                    .type("batch")
                    .name("batch_" + batchCounter++)
                    .build()
            }
        } else {
            val uid = values[0]
            val nameLen = values[1].toInt()
            val msgCnt = values[2].toInt()
            val contentSize = values[3].toInt()
            val succeed = values[4].toInt() != 0

            val parentIdString = values[5]
            val parentId = if (parentIdString == "0") {
                null
            } else {
                val parentIdParts = values[5].split(':')
                val parentTimestampNano = parentIdParts[1].toLong() + config.startNanos
                StoredTestEventId(
                    config.bookId,
                    parentIdParts[0],
                    Instant.ofEpochSecond(parentTimestampNano / 1_000_000_000, parentTimestampNano % 1_000_000_000),
                    parentIdParts[2]
                )
            }

            val startTimestampNano = values[6].toLong() + config.startNanos
            val startTime =
                Instant.ofEpochSecond(startTimestampNano / 1_000_000_000, startTimestampNano % 1_000_000_000)
            val endTime = Instant.ofEpochMilli(values[7].toLong() + config.startMillis + 1)

            val name = generateRandomString(nameLen)
            val messages = dummyMessageIds.take(msgCnt).toSet()
            val content = Random.nextBytes(contentSize)

            val id = StoredTestEventId(config.bookId, scope, startTime, uid)

            val event = TestEventSingleToStore.builder(config.rejectionThresholdMillis)
                .id(id)
                .name(name)
                .messages(messages)
                .content(content)
                .success(succeed)
                .parentId(parentId)
                .endTimestamp(endTime)
                .build()

            if (batch != null) {
                batch.addTestEvent(event)
            } else {
                storage.storeTestEvent(event)
            }
        }
    }
}

private fun writeEvent(writer: BufferedWriter, event: TestEventSingle, startMillis: Long, scopes: Map<String, Int>) {
    val uid = event.id.id
    val nameLen = event.name.length
    val msgCnt = event.messages?.size ?: 0
    val contentSize = event.content.size
    val succeed = if (event.isSuccess) 1 else 0
    val parentId = event.parentId.toString(startMillis, scopes)

    val startTimeOffsetNano =
        (event.startTimestamp.toEpochMilli() - startMillis) * 1_000_000 + event.startTimestamp.nano % 1_000_000
    val endTimeOffset = event.endTimestamp.toEpochMilli() - startMillis

    writer.write("$uid,$nameLen,$msgCnt,$contentSize,$succeed,$parentId,$startTimeOffsetNano,$endTimeOffset\n")
}

private fun generateRandomString(length: Int) = String(CharArray(length) { Random.nextInt(32, 127).toChar() })

private fun StoredTestEventId?.toString(startMillis: Long, scopes: Map<String, Int>) = this?.let {
    val parentTimestampOffset = startTimestamp.minusMillis(startMillis)
    val parentTimestampOffsetNano =
        parentTimestampOffset.epochSecond * 1_000_000_000 + parentTimestampOffset.nano
    val parentScope = "scope${scopes[scope] ?: 0}"
    "$parentScope:$parentTimestampOffsetNano:$id"
} ?: "0"

class EventGeneratorSettings(
    val directoryPath: Path,
    val bookId: BookId,
    val sessionAlias: String = "alias_00",
    val startNanos: Long = System.nanoTime(),
    val startMillis: Long = startNanos / 1_000_000L,
    val rejectionThresholdMillis: Long = 1_000 * 60 * 60 * 24 * 365,
)

private object EventErrorCollector: ErrorCollector {
    override fun init(
        persistor: Persistor<TestEventToStore>,
        rootEvent: StoredTestEventId
    ) {
        LOGGER.info { "inited error collector" }
    }

    override fun collect(logger: org.slf4j.Logger, error: String, cause: Throwable) {
        LOGGER.error(cause) { "Collected: $error" }
    }

    override fun collect(error: String) {
        LOGGER.error { "Collected: $error" }
    }

    override fun close() {
        LOGGER.info { "closed error collector" }
    }
}