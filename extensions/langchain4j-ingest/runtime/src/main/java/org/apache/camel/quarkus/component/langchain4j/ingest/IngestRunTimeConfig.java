/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.quarkus.component.langchain4j.ingest;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigDocMapKey;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithParentName;

/**
 * Runtime configuration of ingestion pipelines, configured and {@code @Ingest} ones alike: every
 * property but the consumer URI, the parser and the modality, which are build-time, see
 * {@link IngestBuildTimeConfig}.
 */
@ConfigMapping(prefix = "quarkus.camel.langchain4j.ingest")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface IngestRunTimeConfig {

    /**
     * Ingestion pipelines by name.
     */
    @WithParentName
    @ConfigDocMapKey("pipeline-name")
    Map<String, PipelineRunTimeConfig> pipelines();

    interface PipelineRunTimeConfig {

        /**
         * Whether this pipeline starts. Useful to switch ingestion off in dev mode.
         */
        @WithDefault("true")
        boolean enabled();

        /**
         * The document source.
         */
        SourceRunTimeConfig source();

        /**
         * Name of the `EmbeddingStore` bean to write to. When not set, the only one present is
         * used.
         */
        Optional<String> embeddingStore();

        /**
         * Name of the `EmbeddingModel` bean to embed with. When not set, the only one present is
         * used.
         */
        Optional<String> embeddingModel();

        /**
         * MIME type of a media payload, such as `audio/wav` or `image/png`, handed to the
         * embedding model. When not set, it is derived from the document id's file extension
         * through Camel's MIME table; a document whose type cannot be determined, or whose
         * medium the model does not declare, fails the exchange. Only valid with
         * `modality=media`: set without it, the pipeline fails to start.
         */
        Optional<String> contentType();

        /**
         * Maximum size of one segment, in characters; 500 when not set.
         */
        Optional<Integer> maxSegmentSize();

        /**
         * How much of the previous segment each segment repeats, in characters; 50 when not set.
         * Overlap keeps a sentence split across a boundary retrievable from either side.
         */
        Optional<Integer> maxOverlapSize();

        /**
         * How many segments are embedded per request to the embedding model. Providers with
         * generous per-request limits ingest large documents faster with a bigger batch; a batch
         * carries at most `embedding-batch-size` × `max-segment-size` characters, so tune the two
         * together against the provider's token limits.
         */
        @WithDefault("32")
        int embeddingBatchSize();

        /**
         * Maximum size of one document in characters, applied to the text about to be split;
         * 0, the default, means no limit. The pipeline holds a document in memory whole, so the
         * cap is the protection against oversized — on a consumer-fed pipeline, attacker-sized —
         * payloads. An oversized document fails the exchange cleanly.
         */
        @WithDefault("0")
        int maxDocumentSize();

        /**
         * Name of the `DocumentSplitter` bean replacing the default recursive splitting;
         * `max-segment-size` and `max-overlap-size` are then ignored. Looked up by name only —
         * an application may hold unrelated splitters. Segments returned without the identity
         * metadata are re-stamped, so a custom splitter cannot break citation.
         */
        Optional<String> documentSplitter();

        /**
         * Filters deciding which deliveries are ingested. A rejected delivery is answered with
         * a `filtered` outcome and never keeps a dedup claim.
         */
        FilterRunTimeConfig filter();

        interface FilterRunTimeConfig {

            /**
             * Comma-separated Ant-style patterns the document id must match to be ingested,
             * for example `*.pdf,*.md`. A non-matching delivery is rejected before the dedup
             * claim and without reading the body. When not set, every id is accepted.
             */
            Optional<String> includeId();

            /**
             * Comma-separated Ant-style patterns for document ids to skip, for example
             * `draft-*`. Exclusion wins over `include-id`.
             */
            Optional<String> excludeId();

            /**
             * Minimum size of one document in characters; 0, the default, means no minimum. A
             * shorter document is answered `filtered` instead of being written.
             */
            @WithDefault("0")
            int minDocumentSize();

            /**
             * Name of a Camel `Predicate` bean deciding whether a delivery is ingested,
             * evaluated with the body available. Looked up by name only.
             */
            Optional<String> documentFilter();
        }

        interface SourceRunTimeConfig {

            /**
             * The directory to ingest documents from, for a pipeline that has no `source.uri`. A
             * path is a deployment concern, so unlike the URI it stays runtime configuration.
             * Setting both is an error.
             */
            Optional<String> directory();

            /**
             * Whether subdirectories are ingested too, when reading a directory.
             */
            @WithDefault("true")
            boolean recursive();

            /**
             * Name of the `IdempotentRepository` bean remembering already ingested documents,
             * instead of the built-in in-memory one (100 000 keys, lost on restart). Looked up
             * by name only. On a pipeline consuming from a component it deduplicates deliveries
             * by document id, first write wins.
             */
            Optional<String> idempotentRepository();

            /**
             * When `true`, an in-memory register (100 000 keys) is created and bound under the
             * `idempotent-repository` name, unless a bean with that name exists — the existing
             * bean wins.
             */
            @WithDefault("false")
            boolean idempotentRepositoryAutoCreate();

            /**
             * Where the document id lives in the exchange the consumer delivers: normally the
             * name of a header, such as `CamelAwsS3Key` for an S3 consumer or `CamelKafkaKey` for
             * a Kafka one. For an id that is not a plain header, write a simple-language
             * expression in the `+$simple{...}+` form — MicroProfile Config passes it through
             * untouched, while a `+${...}+` in a properties file would be consumed as a config
             * expansion before Camel ever saw it. When not set, a pipeline reading a directory
             * uses the file name, and one consuming from a component uses the
             * `CamelLangChain4jIngestDocumentId` header (the deprecated 3.39 name
             * `CamelIngestDocumentId` is still read as a fallback).
             */
            Optional<String> documentId();
        }
    }
}
