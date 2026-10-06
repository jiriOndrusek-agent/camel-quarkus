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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.component.langchain4j.ingest.IngestResult;
import org.apache.camel.component.langchain4j.ingest.LangChain4jIngestHeaders;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.util.URISupport;
import org.jboss.logging.Logger;

/**
 * The route pieces between the Kamelets: the pre-parse guards and parser actions, the
 * empty-outcome tail, the Kamelet parameters and URI assembly. Static and stateless — everything
 * a step needs arrives as an argument — so {@link IngestRoutes} stays the one narrative from
 * configuration to topology.
 */
final class IngestCompositionSupport {

    static final String FILE_SOURCE_KAMELET = "langchain4j-ingest-file-source";

    static final String SINK_KAMELET = "langchain4j-ingest-sink";

    private static final Logger LOG = Logger.getLogger(IngestCompositionSupport.class);

    private IngestCompositionSupport() {
    }

    /** The file-source Kamelet's parameters; directory pipelines only. */
    static Map<String, Object> fileSourceParameters(PipelineSpec spec) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("directory", spec.directory());
        source.put("recursive", String.valueOf(spec.recursive()));
        if (spec.parser() == null && !spec.media()) {
            // text is read as UTF-8; a parser or a media model receives the raw bytes instead -
            // the format is its business, and a charset conversion would corrupt a binary document
            source.put("charset", "UTF-8");
        }
        source.put("idempotentRepository", "#bean:" + registerRef(spec));
        return source;
    }

    /**
     * The parser action Kamelet's parameters. The sink's {@code maxDocumentSize} counts the
     * extracted text, which protects the splitter and the model but not the parse, so the action
     * rejects the raw payload first, in bytes, before tika or docling materialize it.
     */
    static Map<String, Object> actionParameters(PipelineSpec spec) {
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("documentIdHeader", documentIdHeader(spec));
        if (spec.maxDocumentSize() > 0) {
            action.put("maxDocumentSize", String.valueOf(spec.maxDocumentSize()));
        }
        return action;
    }

    /**
     * The sink Kamelet's parameters: whatever is set is forwarded, and the component checks the
     * bounds and the media conflicts - it ignores what a modality does not use and rejects what
     * contradicts it.
     */
    static Map<String, Object> sinkParameters(PipelineSpec spec) {
        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("pipelineName", spec.name());
        if (spec.media()) {
            sink.put("modality", "media");
        }
        putIfSet(sink, "contentType", spec.contentType());
        putIfSet(sink, "maxSegmentSize", spec.maxSegmentSize());
        putIfSet(sink, "maxOverlapSize", spec.maxOverlapSize());
        putIfSet(sink, "embeddingBatchSize", spec.embeddingBatchSize());
        if (spec.maxDocumentSize() != 0) {
            sink.put("maxDocumentSize", String.valueOf(spec.maxDocumentSize()));
        }
        if (spec.documentSplitter() != null) {
            sink.put("documentSplitter", "#bean:" + spec.documentSplitter());
        }
        // enforced by the component: id patterns act before the dedup claim, the size floor and the
        // predicate answer FILTERED and release theirs
        PipelineSpec.Filters filters = spec.filters();
        putIfSet(sink, "includeId", filters.includeId());
        putIfSet(sink, "excludeId", filters.excludeId());
        if (filters.minDocumentSize() != 0) {
            sink.put("minDocumentSize", String.valueOf(filters.minDocumentSize()));
        }
        if (filters.documentFilter() != null) {
            sink.put("documentFilter", "#bean:" + filters.documentFilter());
        }
        sink.put("embeddingStore", "#bean:" + generatedName(spec.name(), "store"));
        sink.put("embeddingModel", "#bean:" + generatedName(spec.name(), "model"));
        if (spec.uri() != null) {
            sink.put("documentIdHeader", documentIdHeader(spec));
            // deduplication by document id happens inside the sink's producer: a duplicate is
            // answered SKIPPED, a blank delivery releases its claim. A directory pipeline's file
            // consumer discards the reply and its source register already keeps the same file
            // version from being ingested twice, so no repository goes to its sink
            String register = registerRef(spec);
            if (register != null) {
                sink.put("idempotentRepository", "#bean:" + register);
            }
        }
        return sink;
    }

    private static void putIfSet(Map<String, Object> parameters, String key, Object value) {
        if (value != null) {
            parameters.put(key, String.valueOf(value));
        }
    }

    /**
     * Where the parser action and the sink read the document id: a consumer pipeline's plain
     * header name as configured; otherwise the canonical header, into which the route normalises
     * a directory pipeline's id or a simple expression before any parse.
     */
    static String documentIdHeader(PipelineSpec spec) {
        String documentId = spec.documentId();
        return spec.uri() != null && documentId != null && !isSimpleExpression(documentId)
                ? documentId
                : LangChain4jIngestHeaders.DOCUMENT_ID;
    }

    /**
     * The register the pipeline's Kamelets reference: the named repository, the built-in one of a
     * directory pipeline naming none, or none at all.
     */
    static String registerRef(PipelineSpec spec) {
        if (spec.idempotentRepository() != null) {
            return spec.idempotentRepository();
        }
        return spec.directory() != null ? generatedName(spec.name(), "register") : null;
    }

    /** The registry name of a bean the extension binds for a pipeline. */
    static String generatedName(String name, String what) {
        return "langchain4j-ingest-" + name + "-" + what;
    }

    /**
     * The optional parse stage: the id guard, then the parser action Kamelet, which caps the raw
     * payload and captures the document id into the exchange property before the parse. The route is
     * returned unchanged when the pipeline has no parser. A directory pipeline passes no register:
     * its file source already filtered duplicates out.
     */
    static ProcessorDefinition<?> parseSteps(ProcessorDefinition<?> route, PipelineSpec spec,
            IdempotentRepository register) {
        IngestParser parser = spec.parser();
        if (parser == null) {
            return route;
        }
        String name = spec.name();
        String documentIdHeader = documentIdHeader(spec);
        // a parser must never run without a captured id: the header the action reads would be
        // absent, and headers written by a parsed document could take its place - the previous
        // engine failed such a delivery, and so does this guard
        route = route.process(exchange -> {
            String id = exchange.getMessage().getHeader(documentIdHeader, String.class);
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("Ingestion pipeline '" + name
                        + "': the document id resolved to nothing, and a parser pipeline requires it before"
                        + " the parse - a parsed document must not supply its own identity");
            }
        });
        if (register != null) {
            // advisory: a known duplicate is answered SKIPPED before the (possibly remote) parse
            // is paid for; the sink producer's eager claim stays authoritative, so a duplicate
            // racing this check is still caught there
            route = route.choice()
                    .when(exchange -> register.contains(exchange.getMessage().getHeader(documentIdHeader, String.class)))
                    .process(exchange -> exchange.getMessage().setBody(new IngestResult(name,
                            exchange.getMessage().getHeader(documentIdHeader, String.class), 0,
                            IngestResult.Outcome.SKIPPED)))
                    .stop()
                    .end();
        }
        return route.to(kameletUri(parser.actionKamelet(), actionParameters(spec)));
    }

    /**
     * A directory pipeline's file consumer discards the reply, so an EMPTY outcome would leave
     * no trace at all: with a parser it is warned about — a parse to nothing typically means a
     * missing Tika parser module or an image-only document, and the file's register key is
     * committed, so it is not retried until the file changes — without one it is debug-logged.
     */
    static void emptyOutcomeTail(ProcessorDefinition<?> tail, PipelineSpec spec) {
        String name = spec.name();
        IngestParser parser = spec.parser();
        tail.process(exchange -> {
            IngestResult result = exchange.getMessage().getBody(IngestResult.class);
            if (result == null || result.outcome() != IngestResult.Outcome.EMPTY) {
                return;
            }
            if (parser != null) {
                LOG.warnf("Ingestion pipeline '%s': document '%s' parsed to no text and was skipped; its key is"
                        + " committed, so it is not retried until the file changes (missing parser module?"
                        + " image-only document?)", name, result.documentId());
            } else {
                LOG.debugf("Ingestion pipeline '%s': document '%s' contained no text, nothing was written",
                        name, result.documentId());
            }
        });
    }

    /**
     * Built through {@code createQueryString} rather than concatenated, so no Kamelet property can be injected.
     * Every value travels as a {@code RAW(...)} token: {@code createQueryString} leaves those unencoded and the
     * Kamelet's own URI parsing unwraps them, so a {@code #bean:} prefix, an Ant pattern's slashes and commas, or
     * a directory path arrive at the template verbatim — percent-encoded they would survive the substitution
     * literally.
     */
    static String kameletUri(String kamelet, Map<String, Object> properties) {
        Map<String, Object> raw = new LinkedHashMap<>();
        properties.forEach((key, value) -> raw.put(key, raw(String.valueOf(value))));
        return "kamelet:" + kamelet + "?" + URISupport.createQueryString(raw);
    }

    private static String raw(String value) {
        if (!value.contains(")")) {
            return "RAW(" + value + ")";
        }
        if (!value.contains("}")) {
            return "RAW{" + value + "}";
        }
        throw new IllegalStateException(
                "A Kamelet property value containing both ')' and '}' cannot be passed: " + value);
    }

    static String routeId(String name) {
        return "langchain4j-ingest-" + name;
    }

    static boolean isSimpleExpression(String value) {
        return value.contains("${") || value.contains("$simple{");
    }
}
