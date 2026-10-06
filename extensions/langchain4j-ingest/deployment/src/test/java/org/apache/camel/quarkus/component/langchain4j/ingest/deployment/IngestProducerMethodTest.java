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
package org.apache.camel.quarkus.component.langchain4j.ingest.deployment;

import java.util.concurrent.atomic.AtomicInteger;

import io.quarkus.test.QuarkusExtensionTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.apache.camel.quarkus.component.langchain4j.ingest.Ingest;
import org.apache.camel.quarkus.component.langchain4j.ingest.IngestPipeline;
import org.apache.camel.quarkus.component.langchain4j.ingest.Source;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * An {@code @Ingest} method is a CDI producer method: its class needs no bean-defining annotation,
 * its parameters are injection points, it runs once although the pre-start check and the route
 * builder both read the pipeline, and a pipeline switched off never runs. Configuration supplies
 * every other property, as for a configured pipeline.
 */
class IngestProducerMethodTest {

    @RegisterExtension
    static final QuarkusExtensionTest CONFIG = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(TestEmbeddingBeans.class, Pipelines.class))
            .overrideConfigKey("producer.test.directory", "target/producer-docs")
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.embedding-store", "store")
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.embedding-model", "model")
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.off.enabled", "false");

    @Inject
    CamelContext context;

    @Test
    void producerMethodsDeclarePipelines() {
        Assertions.assertNotNull(context.getRoute("langchain4j-ingest-docs"));
        Assertions.assertNull(context.getRoute("langchain4j-ingest-off"));
        Assertions.assertEquals(1, Pipelines.CALLS.get(), "the @Ingest method must run exactly once");
    }

    // no bean-defining annotation: the producer method makes the class a bean
    public static class Pipelines {

        static final AtomicInteger CALLS = new AtomicInteger();

        @Ingest("docs")
        IngestPipeline docs(@ConfigProperty(name = "producer.test.directory") String directory) {
            CALLS.incrementAndGet();
            return IngestPipeline.from(Source.file(directory));
        }

        @Ingest("off")
        IngestPipeline off() {
            throw new IllegalStateException("a switched-off pipeline's method must not run");
        }
    }
}
