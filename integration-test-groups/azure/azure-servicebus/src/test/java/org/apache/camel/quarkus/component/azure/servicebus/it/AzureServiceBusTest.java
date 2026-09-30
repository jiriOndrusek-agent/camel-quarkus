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
package org.apache.camel.quarkus.component.azure.servicebus.it;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.camel.quarkus.test.EnabledIf;
import org.apache.camel.quarkus.test.mock.backend.MockBackendDisabled;
import org.apache.camel.quarkus.test.support.azure.AzureServiceBusTestResource;
import org.awaitility.Awaitility;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@QuarkusTest
@QuarkusTestResource(AzureServiceBusTestResource.class)
class AzureServiceBusTest {
    // NOTE: Consumer endpoints are started / stopped manually to prevent them from inferring with each other

    private static final Logger LOG = Logger.getLogger(AzureServiceBusTest.class);

    // How long a single drain receive waits for the queue to yield a message before concluding it is empty.
    private static final Duration DRAIN_RECEIVE_TIMEOUT = Duration.ofSeconds(1);
    // Max time to wait for a sent message to reach its consumer. Generous because a freshly started consumer,
    // especially with token credential authentication, can take a while to establish its receiver link.
    private static final Duration MESSAGE_RECEIVE_TIMEOUT = Duration.ofMinutes(2);

    @BeforeEach
    public void beforeEach() {
        // The queue is shared by all consumer routes. Drain it before every test so a message left behind or
        // redelivered by a previous test cannot be picked up here and mask the message under test.
        drainQueue();
    }

    private void drainQueue() {
        // Drain the shared queue of messages lingering from previous runs or consumers. A pull receiver lets us stop
        // as soon as the queue is empty (one short receive) instead of always waiting a fixed amount of time.
        ServiceBusReceiverClient receiver = new ServiceBusClientBuilder()
                .connectionString(AzureServiceBusHelper.getConnectionString())
                .receiver()
                .queueName(AzureServiceBusHelper.getDestination("queue"))
                .buildClient();
        try {
            int received;
            do {
                received = 0;
                for (ServiceBusReceivedMessage message : receiver.receiveMessages(10, DRAIN_RECEIVE_TIMEOUT)) {
                    received++;
                    try {
                        receiver.complete(message);
                        LOG.infof("Purged old message: %s", message.getMessageId());
                    } catch (Exception e) {
                        // Best-effort cleanup: a failed completion (e.g. expired lock) must not fail the drain.
                        // The message stays uncompleted and is redelivered later.
                        LOG.warnf(e, "Could not complete drained message %s", message.getMessageId());
                    }
                }
            } while (received > 0);
        } finally {
            receiver.close();
        }
    }

    @ParameterizedTest
    @MethodSource("produceConsumeOptions")
    void produceConsumeMessage(
            String destinationType,
            AmqpTransportType transportType,
            String payloadType) {

        final String consumerRouteId = "servicebus-%s-consumer-%s".formatted(destinationType, transportType);
        final String destination = AzureServiceBusHelper.getDestination(destinationType);
        final String mockEndpointUri = "mock:%s-%s-%s-%s-results".formatted(destinationType, destination, transportType.name(),
                payloadType);
        final String messageBody = UUID.randomUUID().toString();

        try {
            RestAssured.given()
                    .post("/azure-servicebus/route/" + consumerRouteId + "/start")
                    .then()
                    .statusCode(204);

            RestAssured.given()
                    .contentType(ContentType.TEXT)
                    .queryParam("serviceBusType", destinationType)
                    .queryParam("payloadType", payloadType)
                    .queryParam("transportType", transportType.name())
                    .body(messageBody)
                    .post("/azure-servicebus/send/message/" + destination)
                    .then()
                    .statusCode(201);

            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).atMost(MESSAGE_RECEIVE_TIMEOUT).untilAsserted(() -> {
                RestAssured.given()
                        .queryParam("endpointUri", mockEndpointUri)
                        .get("/azure-servicebus/receive/messages")
                        .then()
                        .statusCode(200)
                        .body(
                                "[0].body", is(messageBody),
                                "[0].sequenceNumber", greaterThanOrEqualTo(1));
            });
        } finally {
            RestAssured.given()
                    .post("/azure-servicebus/route/" + consumerRouteId + "/stop")
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    void multipleMessages() {
        final String consumerRouteId = "servicebus-queue-consumer-" + AmqpTransportType.AMQP;
        final String destination = AzureServiceBusHelper.getDestination("queue");
        final String mockEndpointUri = "mock:%s-%s-%s-%s-results".formatted("queue", destination, AmqpTransportType.AMQP.name(),
                List.class.getSimpleName());
        final List<String> messages = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            messages.add("cq-azure-servicebus-test-" + UUID.randomUUID().toString());
        }
        try {
            RestAssured.given()
                    .post("/azure-servicebus/route/" + consumerRouteId + "/start")
                    .then()
                    .statusCode(204);

            RestAssured.given()
                    .contentType(ContentType.JSON)
                    .queryParam("serviceBusType", "queue")
                    .queryParam("payloadType", List.class.getSimpleName())
                    .queryParam("transportType", AmqpTransportType.AMQP.name())
                    .body(messages)
                    .post("/azure-servicebus/send/message/" + destination)
                    .then()
                    .statusCode(201);

            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).atMost(MESSAGE_RECEIVE_TIMEOUT).untilAsserted(() -> {
                RestAssured.given()
                        .queryParam("endpointUri", mockEndpointUri)
                        .get("/azure-servicebus/receive/messages")
                        .then()
                        .statusCode(200)
                        .body(
                                "[0].body", is(messages.get(0)),
                                "[0].sequenceNumber", greaterThanOrEqualTo(1),
                                "[1].body", is(messages.get(1)),
                                "[1].sequenceNumber", greaterThanOrEqualTo(1),
                                "[2].body", is(messages.get(2)),
                                "[2].sequenceNumber", greaterThanOrEqualTo(1));
            });
        } finally {
            RestAssured.given()
                    .post("/azure-servicebus/route/" + consumerRouteId + "/stop")
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    void produceConsumeWithCustomClients() {
        final String messageBody = UUID.randomUUID().toString();
        try {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-consumer-custom-processor/start")
                    .then()
                    .statusCode(204);

            RestAssured.given()
                    .contentType(ContentType.TEXT)
                    .queryParam("directEndpointUri", "direct:send-message-custom-client")
                    .queryParam("payloadType", String.class.getSimpleName())
                    .body(messageBody)
                    .post("/azure-servicebus/send/message/" + AzureServiceBusHelper.getDestination("queue"))
                    .then()
                    .statusCode(201);

            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).atMost(MESSAGE_RECEIVE_TIMEOUT).untilAsserted(() -> {
                RestAssured.given()
                        .queryParam("endpointUri", AzureServiceBusProducers.MOCK_ENDPOINT_URI)
                        .get("/azure-servicebus/receive/messages")
                        .then()
                        .statusCode(200)
                        .body(
                                "[0].body", is(messageBody),
                                "[0].sequenceNumber", greaterThanOrEqualTo(1));
            });
        } finally {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-consumer-custom-processor/stop")
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    @EnabledIf({ MockBackendDisabled.class }) //only connection string is enabled on emulator, see https://github.com/Azure/azure-service-bus-emulator-installer/issues/30#issuecomment-2500163047
    void tokenCredentialAuthentication() {
        final String messageBody = UUID.randomUUID().toString();
        try {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-consumer-token-credential/start")
                    .then()
                    .statusCode(204);

            RestAssured.given()
                    .contentType(ContentType.TEXT)
                    .queryParam("directEndpointUri", "direct:token-credential")
                    .queryParam("payloadType", String.class.getSimpleName())
                    .body(messageBody)
                    .post("/azure-servicebus/send/message/" + AzureServiceBusHelper.getDestination("queue"))
                    .then()
                    .statusCode(201);

            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).atMost(MESSAGE_RECEIVE_TIMEOUT).untilAsserted(() -> {
                RestAssured.given()
                        .queryParam("endpointUri", "mock:servicebus-token-credential-results")
                        .get("/azure-servicebus/receive/messages")
                        .then()
                        .statusCode(200)
                        .body(
                                "[0].body", is(messageBody),
                                "[0].sequenceNumber", greaterThanOrEqualTo(1));
            });
        } finally {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-consumer-token-credential/stop")
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    void scheduled() {
        final String messageBody = UUID.randomUUID().toString();
        try {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-scheduled-consumer/start")
                    .then()
                    .statusCode(204);

            // Schedule message for 15 seconds in the future
            long scheduledEnqueueTime = Instant.now()
                    .plus(Duration.of(15, ChronoUnit.SECONDS))
                    .toEpochMilli();

            RestAssured.given()
                    .contentType(ContentType.TEXT)
                    .queryParam("directEndpointUri", "direct:scheduled")
                    .queryParam("payloadType", String.class.getSimpleName())
                    .queryParam("scheduledEnqueueTime", scheduledEnqueueTime)
                    .body(messageBody)
                    .post("/azure-servicebus/send/message/" + AzureServiceBusHelper.getDestination("queue"))
                    .then()
                    .statusCode(201);

            //we are checking that there is no message in the next 10 seconds.
            //we should rather avoid checking the message near the scheduled time, because process can be delayed and receives the message
            while (Instant.now().toEpochMilli() < (scheduledEnqueueTime - 5000)) {
                // No message should be received before the scheduled time
                RestAssured.given()
                        .queryParam("endpointUri", "mock:servicebus-queue-scheduled-consumer-results")
                        .get("/azure-servicebus/receive/messages")
                        .then()
                        .statusCode(200)
                        .body("size()", is(0));
                try {
                    //wait those 5 seconds, we were not checking
                    Thread.sleep(5250);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Message should be enqueued and eventually consumed
            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).atMost(MESSAGE_RECEIVE_TIMEOUT).untilAsserted(() -> {
                RestAssured.given()
                        .queryParam("endpointUri", "mock:servicebus-queue-scheduled-consumer-results")
                        .get("/azure-servicebus/receive/messages")
                        .then()
                        .statusCode(200)
                        .body(
                                "[0].body", is(messageBody),
                                "[0].sequenceNumber", greaterThanOrEqualTo(1));
            });
        } finally {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-scheduled-consumer/stop")
                    .then()
                    .statusCode(204);
        }
    }

    @EnabledIf({ MockBackendDisabled.class }) //only connection string is enabled on emulator, see https://github.com/Azure/azure-service-bus-emulator-installer/issues/30#issuecomment-2500163047
    @Test
    void azureIdentityCredentials() {
        Assumptions.assumeTrue(AzureServiceBusHelper.isAzureIdentityCredentialsAvailable());

        final String messageBody = UUID.randomUUID().toString();
        try {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-consumer-azure-identity/start")
                    .then()
                    .statusCode(204);

            RestAssured.given()
                    .contentType(ContentType.TEXT)
                    .queryParam("directEndpointUri", "direct:azure-identity")
                    .queryParam("payloadType", String.class.getSimpleName())
                    .body(messageBody)
                    .post("/azure-servicebus/send/message/" + AzureServiceBusHelper.getDestination("queue"))
                    .then()
                    .statusCode(201);

            Awaitility.await().pollInterval(1, TimeUnit.SECONDS).atMost(MESSAGE_RECEIVE_TIMEOUT).untilAsserted(() -> {
                RestAssured.given()
                        .queryParam("endpointUri", "mock:servicebus-azure-identity-results")
                        .get("/azure-servicebus/receive/messages")
                        .then()
                        .statusCode(200)
                        .body(
                                "[0].body", is(messageBody),
                                "[0].sequenceNumber", greaterThanOrEqualTo(1));
            });
        } finally {
            RestAssured.given()
                    .post("/azure-servicebus/route/servicebus-queue-consumer-azure-identity/stop")
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    void dynamicExceptionInstantiation() {
        RestAssured.get("/azure-servicebus/exception/cache")
                .then()
                .statusCode(200)
                .body(is("true"));
    }

    static Stream<Arguments> produceConsumeOptions() {
        String destinationTypes = "queue";

        // Topics aren't available on the Azure basic pricing tier
        if (AzureServiceBusHelper.isAzureServiceBusTopicConfigPresent()) {
            destinationTypes += ",topic";
        } else {
            LOG.warnf(
                    "Configuration options azure.servicebus.topic.name & azure.servicebus.topic.subscription.name are not present. Azure Service Bus topic testing will be disabled");
        }

        String[] payloadTypes = { String.class.getSimpleName(), byte[].class.getSimpleName(),
                BinaryData.class.getSimpleName() };
        //in mocked backend, the  AMQP_WEB_SOCKET is not supported, see https://github.com/Azure/azure-service-bus-emulator-installer/issues/51
        AmqpTransportType[] transportTypes;
        if (AzureServiceBusHelper.isMockBackEnd()) {
            transportTypes = new AmqpTransportType[] { AmqpTransportType.AMQP };
        } else {
            transportTypes = AmqpTransportType.values();
        }
        return Stream.of(destinationTypes.split(","))
                .flatMap(s1 -> Stream.of(transportTypes)
                        .flatMap(s2 -> Stream.of(payloadTypes)
                                .map(s3 -> Arguments.of(s1, s2, s3))));
    }
}
