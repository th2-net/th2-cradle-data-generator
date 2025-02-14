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

@file:JvmName("Main")

package com.exactpro.th2.kafka.client

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Direction
import com.exactpro.cradle.PageToAdd
import com.exactpro.cradle.Direction as CradleDirection
import com.exactpro.cradle.counters.Interval
import com.exactpro.cradle.filters.FilterForGreater
import com.exactpro.cradle.filters.FilterForLess
import com.exactpro.cradle.messages.GroupedMessageBatchToStore
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageToStore
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventBatchToStore
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder
import com.exactpro.cradle.testevents.TestEventFilter
import com.exactpro.cradle.testevents.TestEventSingle
import com.exactpro.cradle.testevents.TestEventSingleToStore
import com.exactpro.th2.common.schema.factory.CommonFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.io.path.nameWithoutExtension
import kotlin.system.exitProcess
import kotlin.random.Random

private val LOGGER = KotlinLogging.logger {}

fun main(args: Array<String>) {
    val resources: Deque<Pair<String, () -> Unit>> = ConcurrentLinkedDeque()

    val factory = runCatching { CommonFactory.createFromArguments(*args) }.getOrElse {
        LOGGER.error(it) { "Failed to create common factory with arguments: ${args.joinToString(" ")}" }
        CommonFactory()
    }.apply { resources += "factory" to ::close }

    runCatching {
        val msgMetaDir = args[2]
        val eventsMetaDir = args[3]
        val (pagesCnt, pageLenMin) = if (args.size < 6) Pair(0, 0) else Pair(args[4].toInt(), args[5].toInt())

        val bookName = factory.boxConfiguration.bookName
        val storage = factory.cradleManager.storage
        val bookId = BookId(bookName)

        createPages(storage, bookId, pagesCnt, pageLenMin)
        processMessagesCsvs(storage, bookId, msgMetaDir)
        processEventsCsvs(storage, bookId, eventsMetaDir)
        factory.close()
    }.onFailure {
        LOGGER.error(it) { "Error during loading from Cradle" }
        exitProcess(2)
    }
}

private const val MINUTE_MS: Int = 60_000
private const val REJECTION_THRESHOLD_SECONDS: Long = 60 * 60 * 24 * 365
private const val REJECTION_THRESHOLD_MILLIS: Long = REJECTION_THRESHOLD_SECONDS * 1_000

fun createPages(storage: CradleStorage, bookId: BookId, pagesCnt: Int, pageLenMin: Int) {
    val pages = (0 until pagesCnt).map {
        PageToAdd(
            "autopage_$it",
            Instant.ofEpochMilli(startMillis + it * pageLenMin * MINUTE_MS),
            "test page"
        )
    }

    storage.addPages(bookId, pages)
}

fun processCsvMessagesFile(storage: CradleStorage, bookId: BookId, filePath: Path) {
    val group = filePath.nameWithoutExtension
    val lines = Files.readAllLines(filePath)

    var batch: GroupedMessageBatchToStore? = null

    var firstSequence = 0L
    var secondSequence = 0L

    for (line in lines) {
        val values = line.split(",")

        if (values.size == 1) {
            if (batch !=null) {
                storage.storeGroupedMessageBatch(batch)
            }
            batch = GroupedMessageBatchToStore(group, Int.MAX_VALUE, REJECTION_THRESHOLD_SECONDS)

        } else {
            val timeOffsetMillis = values[0].toLong()
            val alias = "alias" + values[1]
            val cradleDirection = CradleDirection.values()[values[2].toInt()]

            val sequence = when (cradleDirection) {
                CradleDirection.FIRST -> ++firstSequence
                CradleDirection.SECOND -> ++secondSequence
            }

            val timestamp = Instant.ofEpochMilli(timeOffsetMillis + startMillis)

            val storedMessageId = StoredMessageId(bookId, alias, cradleDirection, timestamp, sequence)

            val fixMessage = generateFixMessage(sequence, timestamp)

            val message = MessageToStore.builder()
                .id(storedMessageId)
                .sessionAlias(alias)
                .direction(cradleDirection)
                .sequence(sequence)
                .bookId(bookId)
                .content(fixMessage.toByteArray(Charsets.US_ASCII))
                .protocol("FIX")
                .timestamp(timestamp)
                .build()

            batch!!.addMessage(message)
        }
    }
}

private val dummyMessageIds = mutableListOf<StoredMessageId>()
private val startMillis = Instant.now().toEpochMilli()
private val startNanos = startMillis * 1_000_000

fun generateRandomString(length: Int) = String(CharArray(length) { Random.nextInt(32, 127).toChar() })

fun processCsvEventsFile(storage: CradleStorage, bookId: BookId, filePath: Path) {
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
                val parentTimestampNano = parentIdParts[1].toLong() + startNanos
                val parentId = parentIdParts[2]

                batch = batchBuilder
                    .idRandom(bookId, parentScope)
                    .parentId(StoredTestEventId(bookId, parentScope, Instant.ofEpochSecond(parentTimestampNano / 1_000_000_000, parentTimestampNano % 1_000_000_000), parentId))
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
                val parentTimestampNano = parentIdParts[1].toLong() + startNanos
                StoredTestEventId(bookId, parentIdParts[0], Instant.ofEpochSecond(parentTimestampNano / 1_000_000_000, parentTimestampNano % 1_000_000_000), parentIdParts[2])
            }

            val startTimestampNano = values[6].toLong() + startNanos
            val startTime = Instant.ofEpochSecond(startTimestampNano / 1_000_000_000, startTimestampNano % 1_000_000_000)
            val endTime = Instant.ofEpochMilli(values[7].toLong() + startMillis + 1)

            val name = generateRandomString(nameLen)
            val messages = dummyMessageIds.take(msgCnt).toSet()
            val content = Random.nextBytes(contentSize)

            val id = StoredTestEventId(bookId, scope, startTime, uid)

            val event = TestEventSingleToStore.builder(REJECTION_THRESHOLD_MILLIS)
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

fun processEventsCsvs(storage: CradleStorage, bookId: BookId, directoryPath: String) {
    for (sequence in 1L..100L) {
        dummyMessageIds.add(
            StoredMessageId(bookId, "alias_00", Direction.FIRST, Instant.now(), sequence)
        )
    }

    val dirPath: Path = Paths.get(directoryPath)

    if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
        Files.list(dirPath).forEach { file ->
            if (Files.isRegularFile(file) && file.toString().endsWith(".csv")) {
                processCsvEventsFile(storage, bookId, file)
            }
        }
    } else {
        throw NoSuchFileException("Directory does not exists")
    }
}

fun calculateFixMessageChecksum(message: String): Int = message.toByteArray(Charsets.US_ASCII).sum() % 256

fun processMessagesCsvs(storage: CradleStorage, bookId: BookId, directoryPath: String) {
    val dirPath: Path = Paths.get(directoryPath)

    if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
        Files.list(dirPath).forEach { file ->
            if (Files.isRegularFile(file) && file.toString().endsWith(".csv")) {
                processCsvMessagesFile(storage, bookId, file)
            }
        }
    } else {
        throw NoSuchFileException("Directory does not exists")
    }
}

fun loadMessages(storage: CradleStorage, bookId: BookId, from: Instant, to: Instant) {
    val groups = storage.getGroups(bookId).mapIndexed { i, group -> Pair(group, i) }.toMap()
    val aliases = storage.getSessionAliases(bookId).mapIndexed { i, group -> Pair(group, i) }.toMap()
    val fromMillis = from.toEpochMilli()

    for (group in groups) {
        val filter = GroupedMessageFilter(bookId, null, group.key)
        filter.from = FilterForGreater.forGreaterOrEquals(from)
        filter.to = FilterForLess.forLess(to)

        val batches = storage.getGroupedMessageBatches(filter)

        val file = File("group" + group.value + ".csv")
        BufferedWriter(FileWriter(file)).use { writer ->
            for (batch in batches) {
                writer.write("${batch.lastTimestamp.toEpochMilli()}\n")

                batch.messages.forEach {
                    val timeOffsetMillis = it.timestamp.toEpochMilli() - fromMillis
                    val aliasIndex = aliases[it.sessionAlias]
                    val directionOrdinal = it.direction.ordinal

                    writer.write("$timeOffsetMillis,$aliasIndex,$directionOrdinal\n")
                }
            }
        }
    }
}

fun parentIdToString(parentId: StoredTestEventId?, startMillis: Long, scopes: Map<String, Int>) = if (parentId == null) {
    "0"
} else {
    val parentTimestampOffset = parentId.startTimestamp.minusMillis(startMillis)
    val parentTimestampOffsetNano = parentTimestampOffset.epochSecond * 1_000_000_000 + parentTimestampOffset.nano
    val parentScope = "scope${scopes[parentId.scope] ?: 0}"
    "$parentScope:$parentTimestampOffsetNano:${parentId.id}"
}

fun writeEvent(writer: BufferedWriter, event: TestEventSingle, startMillis: Long, scopes: Map<String, Int>) {
    val uid = event.id.id
    val nameLen = event.name.length
    val msgCnt = event.messages?.size ?: 0
    val contentSize = event.content.size
    val succeed = if (event.isSuccess) 1 else 0
    val parentId = parentIdToString(event.parentId, startMillis, scopes)

    val startTimeOffsetNano = (event.startTimestamp.toEpochMilli() - startMillis) * 1_000_000 + event.startTimestamp.nano % 1_000_000
    val endTimeOffset = event.endTimestamp.toEpochMilli() - startMillis

    writer.write("$uid,$nameLen,$msgCnt,$contentSize,$succeed,$parentId,$startTimeOffsetNano,$endTimeOffset\n")
}

fun loadEvents(storage: CradleStorage, bookId: BookId, from: Instant, to: Instant) {
    val scopes = storage.getScopes(bookId, Interval(from, to)).asIterable().mapIndexed { i, scope -> Pair(scope, i) }.toMap()
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
                    val parentIdString = parentIdToString(event.parentId, fromMillis, scopes)
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

private val fixFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS")
private val utcTimeZone = ZoneId.of("UTC")
private const val lagMillis = 300L

fun generateFixMessage(sequence: Long, timestamp: Instant): String {

    val transactTime = LocalDateTime.ofInstant(timestamp, utcTimeZone)
    val sendingTime = LocalDateTime.ofInstant(timestamp.plusMillis(lagMillis), utcTimeZone)
    sendingTime.format(fixFormatter)
    transactTime.format(fixFormatter)

    val qty = Random.nextInt(1, 1000)
    val price = Random.nextInt(100, 200)
    val clOrdID = Random.nextLong(1_000_000, 10_000_000)
    val secondaryClOrdID = Random.nextLong(1_000_000, 10_000_000)
    val securityID = Random.nextLong(1_000_000, 10_000_000)
    val partyID = "PARTY_" + Random.nextInt(100, 1000)

    val fixBody = "35=D\u000134=${sequence}\u000149=$partyID\u000152=${sendingTime.format(fixFormatter)}\u000156=GATEWAY\u000111=$clOrdID\u000122=8\u000138=$qty\u000140=2\u000144=$price\u000148=$securityID\u000154=1\u000160=${transactTime.format(fixFormatter)}\u0001526=$secondaryClOrdID\u0001528=A\u0001581=1\u00011138=$qty\u0001453=4\u0001448=$partyID\u0001447=D\u0001452=76\u0001448=0\u0001447=P\u0001452=3\u0001448=0\u0001447=P\u0001452=122\u0001448=3\u0001447=P\u0001452=12\u0001"

    val fixHeader = "8=FIXT.1.1\u00019=${fixBody.length}\u0001"
    val fixMessageNoChecksum = fixHeader + fixBody
    val checksum = calculateFixMessageChecksum(fixMessageNoChecksum)
    val fixMessage = fixMessageNoChecksum + "10=${String.format("%03d", checksum)}\u0001"

    return fixMessage
}