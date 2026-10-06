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

import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A media pipeline embeds a document whole, so the component rejects a document splitter beside it
 * at startup.
 */
class IngestModalitySplitterConflictTest {

    @RegisterExtension
    static final QuarkusExtensionTest CONFIG = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(TestEmbeddingBeans.class, Splitters.class))
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.source.directory", "target/media-splitter")
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.modality", "media")
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.document-splitter", "mySplitter")
            .assertException(t -> ValidationTestSupport.assertFailure(t,
                    "Ingestion pipeline 'docs': documentSplitter does not apply to modality=media"));

    @Test
    void startMustFail() {
        Assertions.fail("The application start was expected to fail");
    }

    @ApplicationScoped
    public static class Splitters {

        @Produces
        @Singleton
        @Named("mySplitter")
        DocumentSplitter splitter() {
            return DocumentSplitters.recursive(100, 10);
        }
    }
}
