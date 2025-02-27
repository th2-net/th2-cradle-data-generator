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
import com.exactpro.cradle.filters.FilterForGreater
import com.exactpro.cradle.filters.FilterForLess
import com.exactpro.cradle.messages.GroupedMessageBatchToStore
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageToStore
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.mstore.Callback
import com.exactpro.th2.mstore.Configuration
import com.exactpro.th2.mstore.ErrorCollector
import com.exactpro.th2.mstore.MessagePersistor
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.nameWithoutExtension
import kotlin.random.Random

private val LOGGER = KotlinLogging.logger {}
private val FIX_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS")

fun generateMessages(storage: CradleStorage, config: MessageGeneratorSettings) {
    MessagePersistor(
        MessageErrorCollector,
        storage,
        Configuration.builder().withMaxRetryCount(15).withRebatching(false).build(),
    ).also { it.start() }.use { persistor ->
        if (Files.exists(config.directoryPath) && Files.isDirectory(config.directoryPath)) {
            Files.list(config.directoryPath).forEach { file ->
                if (Files.isRegularFile(file) && file.toString().endsWith(".csv")) {
                    processCsvMessagesFile(persistor, config, file)
                }
            }
        } else {
            throw NoSuchFileException("Directory does not exists")
        }
    }
}

@Suppress("unused")
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

private fun processCsvMessagesFile(persistor: MessagePersistor, config: MessageGeneratorSettings, filePath: Path) {
    val group = filePath.nameWithoutExtension
    val lines = Files.readAllLines(filePath)

    var batch: GroupedMessageBatchToStore? = null

    var firstSequence = 0L
    var secondSequence = 0L

    for (line in lines) {
        val values = line.split(",")

        if (values.size == 1) {
            if (batch != null) {
                persistor.persist(batch, MessageCallBack)
            }
            batch = GroupedMessageBatchToStore(group, Int.MAX_VALUE, config.rejectionThresholdSeconds)

        } else {
            val timeOffsetMillis = values[0].toLong()
            val alias = "alias" + values[1]
            val cradleDirection = Direction.values()[values[2].toInt()]

            val sequence = when (cradleDirection) {
                Direction.FIRST -> ++firstSequence
                Direction.SECOND -> ++secondSequence
            }

            val timestamp = Instant.ofEpochMilli(timeOffsetMillis + config.startMillis)

            val storedMessageId = StoredMessageId(config.bookId, alias, cradleDirection, timestamp, sequence)

            val fixMessage = generateFixMessage(config, sequence, timestamp)

            val message = MessageToStore.builder()
                .id(storedMessageId)
                .sessionAlias(alias)
                .direction(cradleDirection)
                .sequence(sequence)
                .bookId(config.bookId)
                .content(fixMessage.toByteArray(Charsets.US_ASCII))
                .protocol("FIX")
                .timestamp(timestamp)
                .build()

            batch!!.addMessage(message)
        }
    }
}

private fun generateFixMessage(config: MessageGeneratorSettings, sequence: Long, timestamp: Instant): String {

    val transactTime = LocalDateTime.ofInstant(timestamp, ZoneOffset.UTC)
    val sendingTime = LocalDateTime.ofInstant(timestamp.plusMillis(config.lagMillis), ZoneOffset.UTC)
    sendingTime.format(FIX_TIMESTAMP_FORMAT)
    transactTime.format(FIX_TIMESTAMP_FORMAT)

    val qty = Random.nextInt(1, 1000)
    val price = Random.nextInt(100, 200)
    val clOrdID = Random.nextLong(1_000_000, 10_000_000)
    val secondaryClOrdID = Random.nextLong(1_000_000, 10_000_000)
    val securityID = Random.nextLong(1_000_000, 10_000_000)
    val partyID = "PARTY_" + Random.nextInt(100, 1000)

    val fixBody =
        "35=D\u000134=${sequence}\u000149=$partyID\u000152=${sendingTime.format(FIX_TIMESTAMP_FORMAT)}\u000156=GATEWAY\u000111=$clOrdID\u000122=8\u000138=$qty\u000140=2\u000144=$price\u000148=$securityID\u000154=1\u000160=${
            transactTime.format(FIX_TIMESTAMP_FORMAT)
        }\u0001526=$secondaryClOrdID\u0001528=A\u0001581=1\u00011138=$qty\u0001453=4\u0001448=$partyID\u0001447=D\u0001452=76\u0001448=0\u0001447=P\u0001452=3\u0001448=0\u0001447=P\u0001452=122\u0001448=3\u0001447=P\u0001452=12\u0001"

    val fixHeader = "8=FIXT.1.1\u00019=${fixBody.length}\u0001"
    val fixMessageNoChecksum = fixHeader + fixBody
    val checksum = calculateFixMessageChecksum(fixMessageNoChecksum)
    val fixMessage = fixMessageNoChecksum + "10=${String.format("%03d", checksum)}\u0001"

    return fixMessage
}

private fun calculateFixMessageChecksum(message: String): Int = message.toByteArray(Charsets.US_ASCII).sum() % 256

class MessageGeneratorSettings(
    val directoryPath: Path,
    val bookId: BookId,
    val lagMillis: Long = 300L,
    val startMillis: Long = System.currentTimeMillis(),
    val rejectionThresholdSeconds: Long = 60 * 60 * 24 * 365,
)

private object MessageCallBack : Callback<GroupedMessageBatchToStore> {
    @Volatile
    private var previousCount = 0L
    private val count = AtomicLong(0)
    private val size = AtomicLong(0)

    override fun onSuccess(p0: GroupedMessageBatchToStore) {
        val count = p0.messages.size
        val size = p0.batchSize
        val currentCount = this.count.addAndGet(count.toLong())
        this.size.addAndGet(size.toLong())
        LOGGER.trace { "Stored group batch ${p0.bookId.name}:${p0.group} with $count items / $size bytes" }
        if (currentCount - previousCount > 100_000L) {
            previousCount = currentCount
            LOGGER.info { "Stored messages $this" }
        }
    }

    override fun onFail(p0: GroupedMessageBatchToStore) {
        LOGGER.error { "Failure storing group batch ${p0.bookId.name}:${p0.group} with ${p0.messages.size} item / ${p0.batchSize} bytes" }
    }

    override fun toString(): String {
        return "total count: ${count.get()}, size: ${size.get()}"
    }
}

private object MessageErrorCollector: ErrorCollector {
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