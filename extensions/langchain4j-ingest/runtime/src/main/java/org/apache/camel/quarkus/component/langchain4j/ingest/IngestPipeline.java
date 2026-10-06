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

import java.util.Set;

/**
 * A pipeline declared in Java, returned from an {@link Ingest} method: its {@link Source}. Every
 * other property is configuration, {@code quarkus.camel.langchain4j.ingest.<name>.*}, as for a
 * pipeline declared there.
 */
public final class IngestPipeline {

    /** The values the {@code parser} configuration property accepts. */
    public static final Set<String> SUPPORTED_PARSERS = IngestParser.labels();

    private final Source source;
    private String embeddingStoreName;
    private String embeddingModelName;
    private Integer maxSegmentSize;
    private Integer maxOverlapSize;

    private IngestPipeline(Source source) {
        this.source = source;
    }

    public static IngestPipeline from(Source source) {
        return new IngestPipeline(source);
    }

    /**
     * @deprecated set {@code quarkus.camel.langchain4j.ingest.<name>.embedding-store} instead; setting
     *             both fails the start
     */
    @Deprecated(since = "4.0.0", forRemoval = true)
    public IngestPipeline embeddingStore(String beanName) {
        this.embeddingStoreName = beanName;
        return this;
    }

    /**
     * @deprecated set {@code quarkus.camel.langchain4j.ingest.<name>.embedding-model} instead; setting
     *             both fails the start
     */
    @Deprecated(since = "4.0.0", forRemoval = true)
    public IngestPipeline embeddingModel(String beanName) {
        this.embeddingModelName = beanName;
        return this;
    }

    /**
     * @deprecated set {@code quarkus.camel.langchain4j.ingest.<name>.max-segment-size} and
     *             {@code max-overlap-size} instead; setting both fails the start
     */
    @Deprecated(since = "4.0.0", forRemoval = true)
    public IngestPipeline splitter(int maxSegmentSize, int maxOverlapSize) {
        if (maxSegmentSize <= 0 || maxOverlapSize < 0 || maxOverlapSize >= maxSegmentSize) {
            throw new IllegalArgumentException("max-segment-size must be positive and max-overlap-size must be "
                    + "smaller than it (got " + maxSegmentSize + " / " + maxOverlapSize + ")");
        }
        this.maxSegmentSize = maxSegmentSize;
        this.maxOverlapSize = maxOverlapSize;
        return this;
    }

    Source source() {
        return source;
    }

    String embeddingStoreName() {
        return embeddingStoreName;
    }

    String embeddingModelName() {
        return embeddingModelName;
    }

    Integer maxSegmentSize() {
        return maxSegmentSize;
    }

    Integer maxOverlapSize() {
        return maxOverlapSize;
    }
}
