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

import java.util.Optional;

/**
 * One ingestion pipeline as the composition consumes it, configured or declared with
 * {@code @Ingest}: see {@link #of}. The constructor holds the rules a pipeline is checked against
 * at startup; the size bounds and the media conflicts are the component's to check, and the
 * build-time keys are checked by the build. Exactly one of {@code directory} and {@code uri} is
 * set; bean references are names, resolved by {@link IngestRoutes}. A {@code null} size is left
 * to the sink Kamelet's default.
 */
record PipelineSpec(
        String name,
        String directory,
        String uri,
        boolean recursive,
        String documentId,
        String idempotentRepository,
        boolean idempotentRepositoryAutoCreate,
        IngestParser parser,
        boolean media,
        String contentType,
        Integer maxSegmentSize,
        Integer maxOverlapSize,
        Integer embeddingBatchSize,
        int maxDocumentSize,
        String documentSplitter,
        String embeddingStore,
        String embeddingModel,
        Filters filters) {

    /** What may be substituted into a Kamelet URI or a simple expression without escaping. */
    private static final String PLAIN_NAME = "[A-Za-z0-9._-]+";

    PipelineSpec {
        // substituted into the sink Kamelet's inner endpoint URI and into registry references
        if (name == null || !name.matches(PLAIN_NAME)) {
            throw new IllegalArgumentException(
                    "Ingestion pipeline name '" + name + "' may only contain letters, digits, '.', '_' and '-'");
        }
        // a consumer URI says "consume from this"; its absence says "read that directory"
        if (uri != null && directory != null) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name + "' sets both source.uri ('" + uri
                    + "') and source.directory ('" + directory + "'). A pipeline reads one source: keep the URI,"
                    + " or drop it to read the directory.");
        }
        if (uri == null && directory == null) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name + "' has no source.directory. Set"
                    + " quarkus.camel.langchain4j.ingest." + name + ".source.directory");
        }
        // substituted into the file-source Kamelet's endpoint URI: a '?' or '#' could inject
        // consumer options - delete=true would consume the user's documents
        if (directory != null && (directory.contains("?") || directory.contains("#"))) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "': the directory must not contain '?' or '#' (got '" + directory + "')");
        }
        if (uri != null && !uri.contains(":")) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name + "': '" + uri + "' is not a consumer URI");
        }
        // the parser actions substitute a plain header name into a simple expression, so it is held
        // to a charset that cannot break out of it; anything else is a simple expression
        if (uri != null && documentId != null && !IngestCompositionSupport.isSimpleExpression(documentId)
                && !documentId.matches(PLAIN_NAME)) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name + "': source.document-id '" + documentId
                    + "' is not a plain header name - it may only contain letters, digits, '.', '_' and '-';"
                    + " write anything else as a $simple{...} expression");
        }
        if (idempotentRepositoryAutoCreate && idempotentRepository == null) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "' sets source.idempotent-repository-auto-create but no source.idempotent-repository name to"
                    + " create the register under.");
        }
    }

    /**
     * A pipeline from its configuration - either config root may lack the name - and, for an
     * {@code @Ingest} pipeline, the method's {@link IngestPipeline}. Configuration owns every
     * property but the source, which comes from exactly one place: the method, else the
     * configuration.
     */
    static PipelineSpec of(String name, IngestBuildTimeConfig.PipelineBuildTimeConfig buildTime,
            IngestRunTimeConfig.PipelineRunTimeConfig runTime, IngestPipeline java) {
        Optional<IngestRunTimeConfig.PipelineRunTimeConfig> config = Optional.ofNullable(runTime);
        Optional<IngestRunTimeConfig.PipelineRunTimeConfig.SourceRunTimeConfig> configured = config.map(c -> c.source());
        Source source = java == null ? null : java.source();
        return new PipelineSpec(name,
                source != null ? source.directory() : configured.flatMap(s -> s.directory()).orElse(null),
                source != null ? source.uri() : Optional.ofNullable(buildTime).flatMap(b -> b.source().uri()).orElse(null),
                source != null ? source.isRecursive() : configured.map(s -> s.recursive()).orElse(true),
                source != null ? source.documentId() : configured.flatMap(s -> s.documentId()).orElse(null),
                source != null ? source.idempotentRepository()
                        : configured.flatMap(s -> s.idempotentRepository()).orElse(null),
                source != null ? source.isIdempotentRepositoryAutoCreate()
                        : configured.map(s -> s.idempotentRepositoryAutoCreate()).orElse(false),
                buildTime == null ? null : IngestParser.fromLabel(buildTime.parser().orElse(null)),
                buildTime != null && "media".equals(buildTime.modality()),
                config.flatMap(c -> c.contentType()).orElse(null),
                onePlace(name, java == null ? null : java.maxSegmentSize(),
                        config.flatMap(c -> c.maxSegmentSize()).orElse(null), "splitter", "max-segment-size"),
                onePlace(name, java == null ? null : java.maxOverlapSize(),
                        config.flatMap(c -> c.maxOverlapSize()).orElse(null), "splitter", "max-overlap-size"),
                config.map(c -> c.embeddingBatchSize()).orElse(null),
                config.map(c -> c.maxDocumentSize()).orElse(0),
                config.flatMap(c -> c.documentSplitter()).orElse(null),
                onePlace(name, java == null ? null : java.embeddingStoreName(),
                        config.flatMap(c -> c.embeddingStore()).orElse(null), "embeddingStore", "embedding-store"),
                onePlace(name, java == null ? null : java.embeddingModelName(),
                        config.flatMap(c -> c.embeddingModel()).orElse(null), "embeddingModel", "embedding-model"),
                config.map(c -> Filters.of(c.filter())).orElse(Filters.NONE));
    }

    /** A deprecated builder setter still fills its property, unless the configuration sets it too. */
    private static <T> T onePlace(String name, T fromJava, T fromConfig, String setter, String key) {
        if (fromJava != null && fromConfig != null) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name + "' sets " + key + " twice: through the"
                    + " deprecated IngestPipeline." + setter + "() in its @Ingest method and in quarkus.camel.langchain4j"
                    + ".ingest." + name + "." + key + ". Keep the configuration property.");
        }
        return fromJava != null ? fromJava : fromConfig;
    }

    /** The {@code filter.*} options, forwarded to the sink Kamelet and enforced by the component. */
    record Filters(String includeId, String excludeId, int minDocumentSize, String documentFilter) {

        static final Filters NONE = new Filters(null, null, 0, null);

        static Filters of(IngestRunTimeConfig.PipelineRunTimeConfig.FilterRunTimeConfig filter) {
            return new Filters(filter.includeId().orElse(null), filter.excludeId().orElse(null),
                    filter.minDocumentSize(), filter.documentFilter().orElse(null));
        }
    }
}
