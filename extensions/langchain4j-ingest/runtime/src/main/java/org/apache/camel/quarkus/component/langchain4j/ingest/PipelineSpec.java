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

/**
 * One ingestion pipeline as the composition consumes it, whichever way it was declared:
 * {@link #fromConfig} or {@link #fromBuilder}. The constructor holds every rule a pipeline is
 * checked against at startup, so both declaration paths are checked alike. Exactly one of
 * {@code directory} and {@code uri} is set; bean references are names, resolved by
 * {@link IngestRoutes}.
 */
record PipelineSpec(
        String name,
        boolean declaredInJava,
        String directory,
        String uri,
        boolean recursive,
        String documentId,
        String idempotentRepository,
        boolean idempotentRepositoryAutoCreate,
        IngestParser parser,
        boolean media,
        String contentType,
        int maxSegmentSize,
        int maxOverlapSize,
        int embeddingBatchSize,
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
        if (maxSegmentSize <= 0 || maxOverlapSize < 0 || maxOverlapSize >= maxSegmentSize) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name + "': max-segment-size must be positive"
                    + " and max-overlap-size must be smaller than it (got " + maxSegmentSize + " / " + maxOverlapSize
                    + ")");
        }
        if (embeddingBatchSize < 1) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "': embedding-batch-size must be positive (got " + embeddingBatchSize + ")");
        }
        if (maxDocumentSize < 0) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "': max-document-size must not be negative, 0 meaning no limit (got " + maxDocumentSize + ")");
        }
        // a media document is embedded whole: nothing to parse, nothing to split
        if (media && parser != null) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "' sets modality 'media' together with a parser. A media document is embedded whole and"
                    + " never parsed; remove one of them.");
        }
        if (media && documentSplitter != null) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "' sets modality 'media' together with a document splitter. A media document is embedded"
                    + " whole and never split; remove one of them.");
        }
        if (!media && contentType != null) {
            // a content type types a media payload; with text it is a sign that media was forgotten
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "' sets a content type without modality 'media'. A content type types a media payload;"
                    + " set modality(\"media\") or remove it.");
        }
        if (filters.minDocumentSize() < 0) {
            throw new IllegalArgumentException("Ingestion pipeline '" + name
                    + "': filter.min-document-size must not be negative (got " + filters.minDocumentSize() + ")");
        }
    }

    /** A pipeline declared in configuration; either config root may lack the name. */
    static PipelineSpec fromConfig(String name, IngestBuildTimeConfig.PipelineBuildTimeConfig buildTime,
            IngestRunTimeConfig.PipelineRunTimeConfig runTime) {
        IngestRunTimeConfig.PipelineRunTimeConfig.SourceRunTimeConfig source = runTime == null ? null : runTime.source();
        return new PipelineSpec(name, false,
                source == null ? null : source.directory().orElse(null),
                buildTime == null ? null : buildTime.source().uri().orElse(null),
                source == null || source.recursive(),
                source == null ? null : source.documentId().orElse(null),
                source == null ? null : source.idempotentRepository().orElse(null),
                source != null && source.idempotentRepositoryAutoCreate(),
                buildTime == null ? null : IngestParser.fromLabel(buildTime.parser().orElse(null)),
                buildTime != null && "media".equals(buildTime.modality()),
                buildTime == null ? null : buildTime.contentType().orElse(null),
                buildTime == null ? IngestBuildTimeConfig.DEFAULT_MAX_SEGMENT_SIZE : buildTime.maxSegmentSize(),
                buildTime == null ? IngestBuildTimeConfig.DEFAULT_MAX_OVERLAP_SIZE : buildTime.maxOverlapSize(),
                buildTime == null ? IngestBuildTimeConfig.DEFAULT_EMBEDDING_BATCH_SIZE : buildTime.embeddingBatchSize(),
                buildTime == null ? IngestBuildTimeConfig.DEFAULT_MAX_DOCUMENT_SIZE : buildTime.maxDocumentSize(),
                buildTime == null ? null : buildTime.documentSplitter().orElse(null),
                buildTime == null ? null : buildTime.embeddingStore().orElse(null),
                buildTime == null ? null : buildTime.embeddingModel().orElse(null),
                runTime == null ? Filters.NONE : Filters.of(runTime.filter()));
    }

    /**
     * A pipeline an {@code @Ingest} method returned. The builder has no filter API, so the
     * {@code filter.*} configuration of the same name supplies the filters.
     */
    static PipelineSpec fromBuilder(String name, IngestPipeline pipeline,
            IngestRunTimeConfig.PipelineRunTimeConfig configured) {
        Source source = pipeline.source();
        return new PipelineSpec(name, true,
                source.directory(),
                source.uri(),
                source.isRecursive(),
                source.documentId(),
                source.idempotentRepository(),
                source.isIdempotentRepositoryAutoCreate(),
                IngestParser.fromLabel(pipeline.parser().orElse(null)),
                "media".equals(pipeline.modality().orElse(null)),
                pipeline.contentType().orElse(null),
                pipeline.maxSegmentSize(),
                pipeline.maxOverlapSize(),
                pipeline.embeddingBatchSize(),
                pipeline.maxDocumentSize(),
                pipeline.documentSplitterName().orElse(null),
                pipeline.embeddingStoreName().orElse(null),
                pipeline.embeddingModelName().orElse(null),
                configured == null ? Filters.NONE : Filters.of(configured.filter()));
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
