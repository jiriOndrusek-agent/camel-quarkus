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

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
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

    /** The parser action Kamelet's parameters. */
    static Map<String, Object> actionParameters(PipelineSpec spec) {
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("documentIdHeader", documentIdHeader(spec));
        return action;
    }

    /** The sink Kamelet's parameters. */
    static Map<String, Object> sinkParameters(PipelineSpec spec) {
        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("pipelineName", spec.name());
        if (spec.media()) {
            // embedded whole, as one vector: the splitter options do not apply
            sink.put("modality", "media");
            if (spec.contentType() != null) {
                sink.put("contentType", spec.contentType());
            }
        } else {
            sink.put("maxSegmentSize", String.valueOf(spec.maxSegmentSize()));
            sink.put("maxOverlapSize", String.valueOf(spec.maxOverlapSize()));
            sink.put("embeddingBatchSize", String.valueOf(spec.embeddingBatchSize()));
        }
        if (spec.maxDocumentSize() > 0) {
            sink.put("maxDocumentSize", String.valueOf(spec.maxDocumentSize()));
        }
        if (spec.documentSplitter() != null) {
            sink.put("documentSplitter", "#bean:" + spec.documentSplitter());
        }
        // enforced by the component: id patterns act before the dedup claim, the size floor and the
        // predicate answer FILTERED and release theirs
        PipelineSpec.Filters filters = spec.filters();
        if (filters.includeId() != null) {
            sink.put("includeId", filters.includeId());
        }
        if (filters.excludeId() != null) {
            sink.put("excludeId", filters.excludeId());
        }
        if (filters.minDocumentSize() > 0) {
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
     * The optional parse stage: the raw-size guard, then the parser action Kamelet, which
     * captures the document id into the exchange property before the parse. The route is
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
        int maxDocumentSize = spec.maxDocumentSize();
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
        if (maxDocumentSize > 0) {
            // the endpoint's own cap counts extracted characters, which protects the splitter and
            // the model but not the parse: this guard rejects the raw payload first, before tika
            // or docling materialize it
            route = route.process(rawSizeGuard(name, maxDocumentSize, spec.directory() != null, documentIdHeader));
        }
        return route.to(kameletUri(parser.actionKamelet(), actionParameters(spec)));
    }

    private static Processor rawSizeGuard(String name, int maxDocumentSize, boolean trustDeclaredLength,
            String documentIdHeader) {
        // the declared-length header spares even the read, but only the directory pipeline's own
        // file consumer is trusted to have set it: on a consumer pipeline every header may be
        // attacker-supplied along with the payload, so its body is always measured - a forged
        // CamelFileLength must not talk an oversized payload past the guard and into the parser
        return exchange -> {
            Long declared = trustDeclaredLength
                    ? exchange.getMessage().getHeader(Exchange.FILE_LENGTH, Long.class)
                    : null;
            long size;
            byte[] bounded = null;
            if (declared != null) {
                size = declared;
            } else {
                // every body is measured through a stream - a GenericFile streams from disk, a
                // byte[] merely wraps - so an attacker-sized payload is rejected after
                // maxDocumentSize + 1 bytes instead of being materialized whole in the heap just
                // to be measured; an accepted stream is consumed here, so the bytes replace it
                // as the body. A null body carries no bytes to guard; it flows on and becomes
                // the EMPTY outcome
                InputStream stream = exchange.getMessage().getBody(InputStream.class);
                if (stream == null) {
                    size = 0;
                } else {
                    int limit = maxDocumentSize == Integer.MAX_VALUE ? Integer.MAX_VALUE : maxDocumentSize + 1;
                    bounded = stream.readNBytes(limit);
                    size = bounded.length;
                }
            }
            if (size > maxDocumentSize) {
                throw new IllegalArgumentException(
                        "Ingestion pipeline '" + name + "': document '"
                                + exchange.getMessage().getHeader(documentIdHeader, String.class)
                                + "' exceeds maxDocumentSize (" + size + " > " + maxDocumentSize + " bytes)");
            }
            if (bounded != null) {
                exchange.getMessage().setBody(bounded);
            }
        };
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
