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

import org.apache.camel.quarkus.component.langchain4j.ingest.IngestPipeline;
import org.apache.camel.quarkus.component.langchain4j.ingest.Source;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The builder rejects an unsupported modality eagerly, like the configuration path at build time. */
class IngestPipelineModalityValidationTest {

    @Test
    void unknownModalityRejected() {
        IngestPipeline pipeline = IngestPipeline.from(Source.file("target/music"));
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> pipeline.modality("video"));
        Assertions.assertTrue(e.getMessage().contains("modality must be one of"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("video"), e.getMessage());
    }

    @Test
    void supportedModalitiesAccepted() {
        for (String modality : IngestPipeline.SUPPORTED_MODALITIES) {
            IngestPipeline.from(Source.file("target/music")).modality(modality);
        }
    }
}
