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

import io.quarkus.test.QuarkusExtensionTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Splitter bounds are forwarded to the component, which rejects them at startup. */
class IngestSplitterBoundsConfigTest {

    @RegisterExtension
    static final QuarkusExtensionTest CONFIG = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(TestEmbeddingBeans.class))
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.source.directory", "target/splitter-bounds")
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.max-segment-size", "100")
            .overrideConfigKey("quarkus.camel.langchain4j.ingest.docs.max-overlap-size", "100")
            .assertException(t -> ValidationTestSupport.assertFailure(t,
                    "Ingestion pipeline 'docs': maxSegmentSize must be positive and maxOverlapSize must be non-negative and smaller than it (got 100 / 100)"));

    @Test
    void startMustFail() {
        Assertions.fail("The application start was expected to fail");
    }
}
