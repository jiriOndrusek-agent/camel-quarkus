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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.inject.Qualifier;

/**
 * Declares the source of an ingestion pipeline in Java, type-safe and IDE-assisted:
 *
 * <pre>
 * &#64;Ingest("products")
 * IngestPipeline productDocs() {
 *     return IngestPipeline.from(Source.endpoint(dsl -&gt; dsl.aws2S3("product-docs").region("eu-west-1").deleteAfterRead(false))
 *             .documentId("CamelAwsS3Key"));
 * }
 * </pre>
 *
 * Typing {@code dsl.} lists a factory for every Camel component — {@code aws2S3}, {@code kafka},
 * {@code ftp} and some 300 more — so a source is written with the component's own typed options
 * rather than a URI string, and with nothing to import. Every other property of the pipeline is
 * configuration, {@code quarkus.camel.langchain4j.ingest.products.*}, as for a configured one.
 *
 * {@code @Ingest} is a CDI qualifier, so the method is a producer method of {@link IngestPipeline}
 * without {@code @Produces} (Quarkus' {@code quarkus.arc.auto-producer-methods}, on by default;
 * add {@code @Produces} when it is off). Its parameters are injection points. It runs once at
 * startup, never for a pipeline switched off with {@code enabled=false}. Pipeline names must be
 * unique, and configuration must not set a {@code source.*} property for them.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Ingest {

    /** The pipeline name. */
    String value();
}
