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
package org.apache.camel.quarkus.component.ai.resource.it;

import org.apache.camel.builder.RouteBuilder;

public class AiResourceRoutes extends RouteBuilder {
    @Override
    public void configure() {
        from("ai-resource:appConfig?resourceUri=camel:///config/app.json&tags=config"
                + "&description=Current application configuration"
                + "&mimeType=application/json")
                .setBody(constant("{\"env\":\"test\"}"));

        from("ai-resource:latestReport?resourceUri=camel:///reports/latest.pdf&tags=reports"
                + "&description=Latest sales report"
                + "&mimeType=application/pdf")
                .setBody(constant(new byte[] { 0x25, 0x50, 0x44, 0x46 }));

        from("ai-resource:readme?resourceUri=camel:///readme.txt"
                + "&description=Project readme")
                .setBody(constant("Read me"));

        from("ai-resource:failingResource?resourceUri=camel:///fail&tags=test"
                + "&description=A resource that always fails")
                .throwException(new IllegalStateException("Intentional failure"));
    }
}
