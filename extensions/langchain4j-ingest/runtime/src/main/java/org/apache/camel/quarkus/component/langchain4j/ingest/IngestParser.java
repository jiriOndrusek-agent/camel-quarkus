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

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The document parsers a pipeline may put between the consumer and the splitter. Each value
 * knows the action Kamelet performing the parse, so adding a parser here is the whole wiring —
 * there is no switch to keep in step.
 */
public enum IngestParser {

    TIKA("tika-extract-text-action"),
    DOCLING("docling-convert-action");

    private final String actionKamelet;

    IngestParser(String actionKamelet) {
        this.actionKamelet = actionKamelet;
    }

    /** The configuration value — and, by design, the name of the Camel component the parser needs. */
    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** The action Kamelet performing this parse in the composition route. */
    String actionKamelet() {
        return actionKamelet;
    }

    /** The labels, for validation messages and the builder's {@code SUPPORTED_PARSERS} view. */
    public static Set<String> labels() {
        return Stream.of(values()).map(IngestParser::label).collect(Collectors.toUnmodifiableSet());
    }

    /** Resolves a configuration value; {@code null} stays {@code null}, an unknown label names the options. */
    static IngestParser fromLabel(String label) {
        if (label == null) {
            return null;
        }
        for (IngestParser parser : values()) {
            if (parser.label().equals(label)) {
                return parser;
            }
        }
        throw new IllegalArgumentException("Unknown parser '" + label + "'. Supported parsers: "
                + labels().stream().sorted().collect(Collectors.joining(", ")));
    }
}
