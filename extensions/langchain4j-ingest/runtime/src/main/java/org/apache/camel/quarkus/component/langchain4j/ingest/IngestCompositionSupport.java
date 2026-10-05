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
 * empty-outcome tail and the Kamelet URI assembly. Static and stateless — everything a step
 * needs arrives as an argument — so {@link IngestRoutes} stays the one narrative from
 * configuration to topology.
 */
final class IngestCompositionSupport {

    private static final Logger LOG = Logger.getLogger(IngestCompositionSupport.class);

    private IngestCompositionSupport() {
    }

    /**
     * The optional parse stage: the raw-size guard, then the parser action Kamelet, which
     * captures the document id into the exchange property before the parse. The route is
     * returned unchanged when the pipeline has no parser.
     */
    static ProcessorDefinition<?> parseSteps(ProcessorDefinition<?> route, String name, IngestParser parser,
            int maxDocumentSize, boolean directory) {
        // the file-source Kamelet normalised the id into the upstream header already; no
        // duplicate pre-check - the endpoint register already filtered duplicates out
        return parseSteps(route, name, parser, maxDocumentSize, directory, LangChain4jIngestHeaders.DOCUMENT_ID, null);
    }

    static ProcessorDefinition<?> parseSteps(ProcessorDefinition<?> route, String name, IngestParser parser,
            int maxDocumentSize, boolean directory, String documentIdHeader, IdempotentRepository register) {
        if (parser == null) {
            return route;
        }
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
            route = route.process(rawSizeGuard(name, maxDocumentSize, directory, documentIdHeader));
        }
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("documentIdHeader", documentIdHeader);
        return route.to(kameletUri(parser.actionKamelet(), action));
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
    static void emptyOutcomeTail(ProcessorDefinition<?> tail, String name, IngestParser parser) {
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

    static String required(String name, String value, String property) {
        if (value == null) {
            throw new IllegalStateException("Ingestion pipeline '" + name + "' has no " + property
                    + ". Set quarkus.camel.langchain4j.ingest." + name + "." + property);
        }
        return value;
    }

    static boolean isSimpleExpression(String value) {
        return value.contains("${") || value.contains("$simple{");
    }
}
