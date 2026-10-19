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
package org.apache.camel.quarkus.component.mcp.server.it;

import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class McpServerRoutes extends RouteBuilder {

    @Override
    public void configure() {
        from("ai-tool:say_hello?tags=conformance&description=Say hello"
                + "&parameter.name=string&parameter.name.description=Who to greet&parameter.name.required=true")
                .routeId("say-hello-route")
                .setBody(simple("Hello ${header.name}"));

        from("ai-tool:fail_tool?tags=conformance&description=Always fails")
                .process(e -> {
                    throw new IllegalStateException("secret internal detail");
                });

        // between tool-timeout and resource-timeout, like delayed_resource
        from("ai-tool:slow_tool?tags=conformance&description=Exceeds the tool timeout")
                .delay(3000)
                .setBody(constant("done"));

        from("ai-tool:hidden_tool?description=Untagged tool, must not be exposed")
                .setBody(constant("hidden"));

        from("ai-tool:other_tool?tags=untrusted&description=Not a selected tag, must not be exposed")
                .setBody(constant("other"));

        from("ai-tool:annotated_tool?tags=conformance&description=Tool with annotation hints"
                + "&title=Annotated tool"
                + "&readOnlyHint=true"
                + "&idempotentHint=true")
                .setBody(constant("annotated"));

        from("ai-tool:create_order?tags=conformance&description=Create an order"
                + "&argSchema=classpath:schema/create-order.json")
                .process(e -> {
                    Map<?, ?> customer = e.getMessage().getHeader("customer", Map.class);
                    List<?> items = e.getMessage().getHeader("items", List.class);
                    e.getMessage().setBody("order for customer " + customer.get("id") + " with " + items.size() + " item(s)");
                });

        from("ai-resource:app_config?resourceUri=camel:///config/app.json&tags=conformance"
                + "&description=Application configuration&mimeType=application/json&title=App config")
                .routeId("app-config-route")
                .setBody(constant("{\"env\":\"test\"}"));

        from("ai-resource:latest_report?resourceUri=camel:///reports/latest.pdf&tags=conformance"
                + "&description=Latest report&mimeType=application/pdf")
                .setBody(constant(new byte[] { 0x25, 0x50, 0x44, 0x46 }));

        from("ai-resource:fail_resource?resourceUri=camel:///fail&tags=conformance&description=Always fails")
                .process(e -> {
                    throw new IllegalStateException("secret internal detail");
                });

        from("ai-resource:slow_resource?resourceUri=camel:///slow&tags=conformance"
                + "&description=Exceeds the resource timeout")
                .delay(6000)
                .setBody(constant("done"));

        // between tool-timeout and resource-timeout, like slow_tool
        from("ai-resource:delayed_resource?resourceUri=camel:///delayed&tags=conformance"
                + "&description=Exceeds the tool timeout, not the resource timeout")
                .delay(3000)
                .setBody(constant("delayed"));

        from("ai-resource:hidden_resource?resourceUri=camel:///hidden"
                + "&description=Untagged resource, must not be exposed")
                .setBody(constant("hidden"));

        from("ai-resource:other_resource?resourceUri=camel:///other&tags=untrusted"
                + "&description=Not a selected tag, must not be exposed")
                .setBody(constant("other"));

        from("ai-resource:same_name?resourceUri=camel:///same/first&tags=conformance"
                + "&description=First resource named same_name")
                .setBody(constant("first"));

        from("ai-resource:same_name?resourceUri=camel:///same/second&tags=conformance"
                + "&description=Name already taken, must be refused")
                .setBody(constant("second"));

        // the uri is taken by the resource registered in QuarkusDefinedResources, so it is refused once started
        from("ai-resource:clashing_resource?resourceUri=camel:///user/clash&tags=conformance"
                + "&description=Uri already taken by an application resource, must be refused")
                .routeId("clashing-resource-route")
                .autoStartup(false)
                .setBody(constant("camel"));
    }
}
