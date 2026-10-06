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
package org.apache.camel.quarkus.component.langchain4j.ingest.it;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The filter configuration, forwarded through the sink Kamelet to the component: id patterns,
 * the size floor and the documentFilter predicate each answer {@code filtered}.
 */
@QuarkusTest
class Langchain4jIngestFilterTest {

    @Test
    void filtersGovernIngestion() {
        assertEquals("filtered", feed("notes.txt", "A text file the id patterns must reject."));
        assertEquals("filtered", feed("draft-plan.md", "Excluded although the include patterns match."));
        assertEquals("filtered", feed("stub.md", "too short"));
        assertEquals("filtered", feed("brief.md", "The CONFIDENTIAL relay specification stays out."));

        assertEquals("ingested", feed("camels.md", "Camels are resilient desert animals with two rows of eyelashes."));

        // only the accepted document reached the store
        assertNotNull(Langchain4jIngestTest.hit("What is resilient?", "filtered", "Camels"));
        assertTrue(Langchain4jIngestTest.hits("What is resilient?", "filtered").stream()
                .allMatch(hit -> hit.get("documentId").equals("camels.md")),
                "no filtered delivery may have been written");
    }

    /** A builder ({@code @Ingest}) pipeline is filtered through configuration the same way. */
    @Test
    void configurationFiltersApplyToBuilderPipelines() {
        String excluded = RestAssured.given().contentType(ContentType.TEXT)
                .body("The OMEGA-3 valve datasheet is not for the knowledge base.")
                .post("/langchain4j-ingest/feed/datasheets/secret-omega.txt")
                .then().statusCode(200).extract().asString();
        assertEquals("filtered", excluded);

        String accepted = RestAssured.given().contentType(ContentType.TEXT)
                .body("The OMEGA-3 valve datasheet lists coolant tolerances.")
                .post("/langchain4j-ingest/feed/datasheets/sheet-omega.txt")
                .then().statusCode(200).extract().asString();
        assertEquals("ingested", accepted);
    }

    private static String feed(String documentId, String body) {
        return RestAssured.given().contentType(ContentType.TEXT)
                .body(body)
                .post("/langchain4j-ingest/feed/filtered/" + documentId)
                .then().statusCode(200).extract().asString();
    }
}
