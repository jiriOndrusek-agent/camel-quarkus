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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Camel puts a URI parameter the Kamelet does not declare into the Kamelet's properties
 * unchecked, so an option the pinned camel-kamelets lacks would be dropped silently. This holds
 * every parameter the composition can forward to the properties the Kamelets on the classpath
 * declare: a version gap fails here, offline, instead of in a running pipeline.
 */
class KameletContractTest {

    @Test
    void everyForwardedParameterIsDeclared() {
        Map<String, Set<String>> forwarded = new TreeMap<>();
        for (PipelineSpec spec : List.of(
                spec("target/docs", null, null, false),
                spec("target/docs", null, IngestParser.TIKA, false),
                spec(null, "direct:feed", IngestParser.DOCLING, false),
                spec("target/media", null, null, true),
                spec(null, "direct:media", null, true))) {
            if (spec.directory() != null) {
                collect(forwarded, IngestCompositionSupport.FILE_SOURCE_KAMELET,
                        IngestCompositionSupport.fileSourceParameters(spec));
            }
            if (spec.parser() != null) {
                collect(forwarded, spec.parser().actionKamelet(), IngestCompositionSupport.actionParameters(spec));
            }
            collect(forwarded, IngestCompositionSupport.SINK_KAMELET, IngestCompositionSupport.sinkParameters(spec));
        }
        assertEquals(Set.of("docling-convert-action", "langchain4j-ingest-file-source", "langchain4j-ingest-sink",
                "tika-extract-text-action"), forwarded.keySet());

        forwarded.forEach((kamelet, parameters) -> {
            Set<String> undeclared = new TreeSet<>(parameters);
            undeclared.removeAll(declaredProperties(kamelet));
            assertTrue(undeclared.isEmpty(), () -> "The camel-kamelets on the classpath declare no " + undeclared
                    + " on " + kamelet + ", which the composition forwards");
        });
    }

    /** Every optional value set, so every conditional parameter is forwarded. */
    private static PipelineSpec spec(String directory, String uri, IngestParser parser, boolean media) {
        return new PipelineSpec("contract", directory, uri, true, uri == null ? null : "CamelKafkaKey",
                "register", false, parser, media, media ? "audio/wav" : null, 500, 50, 32, 1000,
                media ? null : "splitter", "store", "model",
                new PipelineSpec.Filters("**/*.md", "**/draft-*", 10, "documentFilter"));
    }

    private static void collect(Map<String, Set<String>> forwarded, String kamelet, Map<String, Object> parameters) {
        forwarded.computeIfAbsent(kamelet, k -> new TreeSet<>()).addAll(parameters.keySet());
    }

    @SuppressWarnings("unchecked")
    private static Set<String> declaredProperties(String kamelet) {
        String resource = "kamelets/" + kamelet + ".kamelet.yaml";
        try (InputStream yaml = KameletContractTest.class.getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(yaml, resource + " is not on the classpath; check the camel-kamelets version");
            Map<String, Object> root = (Map<String, Object>) new Load(LoadSettings.builder().build())
                    .loadFromInputStream(yaml);
            Map<String, Object> definition = (Map<String, Object>) ((Map<String, Object>) root.get("spec"))
                    .get("definition");
            return ((Map<String, Object>) definition.get("properties")).keySet();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
