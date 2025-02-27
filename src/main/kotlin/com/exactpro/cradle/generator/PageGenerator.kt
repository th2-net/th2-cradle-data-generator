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
import com.exactpro.cradle.PageToAdd
import java.time.Instant

private const val MINUTE_MS: Int = 60_000

fun generatePages(storage: CradleStorage, config: PageGeneratorSettings) {
    val pages = (0 .. config.pagesCount).map {
        PageToAdd(
            "autopage_$it",
            Instant.ofEpochMilli(config.startMillis + it * config.pageLenMin * MINUTE_MS),
            "test page"
        )
    }

    storage.addPages(config.bookId, pages)
}

class PageGeneratorSettings(
    val bookId: BookId,
    val pagesCount: Int = 0,
    val pageLenMin: Int = 0,
    val startMillis: Long = System.currentTimeMillis(),
)
