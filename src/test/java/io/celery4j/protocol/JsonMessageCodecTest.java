/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link JsonMessageCodec}.
 *
 * @since 0.1.0
 */
final class JsonMessageCodecTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "3802f860-8d3c-4dad-b18c-597fb2ac728b";

    /**
     * Task name used across the cases.
     */
    private static final String NAME = "proj.tasks.add";

    /**
     * A well formed envelope of the smallest shape.
     */
    private static final String MINIMAL = String.join(
        "",
        "{\"body\":\"e30=\",",
        "\"content-type\":\"application/json\",",
        "\"content-encoding\":\"utf-8\",",
        "\"headers\":{\"id\":\"a\",\"task\":\"t\"},",
        "\"properties\":{\"correlation_id\":\"a\"}}"
    );

    @Test
    void writesTheBody() throws IOException {
        Assertions.assertEquals(
            "e30=", JsonMessageCodecTest.written().get("body").textValue()
        );
    }

    @Test
    void writesTheContentType() throws IOException {
        Assertions.assertEquals(
            MessageProperties.JSON,
            JsonMessageCodecTest.written().get(MessageProperties.TYPE).textValue()
        );
    }

    @Test
    void writesTheContentEncoding() throws IOException {
        Assertions.assertEquals(
            MessageProperties.UTF8,
            JsonMessageCodecTest.written().get(MessageProperties.ENCODING).textValue()
        );
    }

    @Test
    void writesTheTaskIntoTheHeaders() throws IOException {
        Assertions.assertEquals(
            JsonMessageCodecTest.NAME,
            JsonMessageCodecTest.written().get("headers").get(MessageHeaders.TASK).textValue()
        );
    }

    @Test
    void writesTheCorrelationIntoTheProperties() throws IOException {
        Assertions.assertEquals(
            JsonMessageCodecTest.ID,
            JsonMessageCodecTest.written()
                .get("properties").get(MessageProperties.CORRELATION).textValue()
        );
    }

    @Test
    void keepsTheContentTypeOutOfTheProperties() throws IOException {
        Assertions.assertNull(
            JsonMessageCodecTest.written().get("properties").get(MessageProperties.TYPE)
        );
    }

    @Test
    void writesADefaultContentTypeWhenNoneWasSet() throws IOException {
        Assertions.assertEquals(
            MessageProperties.JSON,
            new ObjectMapper().readTree(
                new JsonMessageCodec().encode(
                    new Message(
                        new MessageProperties(
                            Map.of(MessageProperties.CORRELATION, JsonMessageCodecTest.ID)
                        ),
                        new MessageHeaders(
                            Map.of(
                                MessageHeaders.ID, JsonMessageCodecTest.ID,
                                MessageHeaders.TASK, JsonMessageCodecTest.NAME
                            )
                        ),
                        "e30="
                    )
                )
            ).get(MessageProperties.TYPE).textValue()
        );
    }

    @Test
    void readsBackWhatItWrote() {
        final MessageCodec codec = new JsonMessageCodec();
        Assertions.assertEquals(
            JsonMessageCodecTest.message(),
            codec.decode(codec.encode(JsonMessageCodecTest.message()))
        );
    }

    @Test
    void readsTheTaskOfAMessageWrittenElsewhere() throws IOException {
        Assertions.assertEquals(
            "proj.tasks.add", new JsonMessageCodec().decode(JsonMessageCodecTest.fixture()).task()
        );
    }

    @Test
    void readsTheIdentifierOfAMessageWrittenElsewhere() throws IOException {
        Assertions.assertEquals(
            "6ad689d4-3e0e-4b57-a1cf-4a2f0cca5c53",
            new JsonMessageCodec().decode(JsonMessageCodecTest.fixture()).id()
        );
    }

    @Test
    void readsTheContentTypeIntoTheProperties() throws IOException {
        Assertions.assertEquals(
            Optional.of(MessageProperties.JSON),
            new JsonMessageCodec().decode(JsonMessageCodecTest.fixture())
                .properties().text(MessageProperties.TYPE)
        );
    }

    @Test
    void readsTheRoutingOfAMessageWrittenElsewhere() throws IOException {
        Assertions.assertEquals(
            "celery",
            ((Map<?, ?>) new JsonMessageCodec().decode(JsonMessageCodecTest.fixture())
                .properties().asMap().get(MessageProperties.DELIVERY))
                .get(MessageProperties.ROUTING)
        );
    }

    @Test
    void readsTheParametersOfAMessageWrittenElsewhere() throws IOException {
        Assertions.assertEquals(
            List.of(2, 2),
            new JsonTaskBody().decode(
                new JsonMessageCodec().decode(JsonMessageCodecTest.fixture()).body()
            ).args()
        );
    }

    @Test
    void carriesHeadersItDoesNotKnowThroughACycle() throws IOException {
        final MessageCodec codec = new JsonMessageCodec();
        final Message message = codec.decode(JsonMessageCodecTest.fixture());
        Assertions.assertEquals(
            message.headers().asMap(),
            codec.decode(codec.encode(message)).headers().asMap()
        );
    }

    @Test
    void refusesBytesThatAreNoJson() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = "{oops".getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void refusesAnEnvelopeThatIsNoObject() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = "[]".getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void refusesAnEnvelopeWithoutHeaders() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = JsonMessageCodecTest.MINIMAL
            .replace("\"headers\":{\"id\":\"a\",\"task\":\"t\"},", "")
            .getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void refusesAnEnvelopeWithoutProperties() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = JsonMessageCodecTest.MINIMAL
            .replace(",\"properties\":{\"correlation_id\":\"a\"}", "")
            .getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void refusesAnEnvelopeWithABodyOfAnotherKind() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = JsonMessageCodecTest.MINIMAL
            .replace("\"body\":\"e30=\"", "\"body\":7")
            .getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void refusesAnEnvelopeWithHeadersOfAnotherKind() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = JsonMessageCodecTest.MINIMAL
            .replace("\"headers\":{\"id\":\"a\",\"task\":\"t\"}", "\"headers\":[]")
            .getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void readsAnEnvelopeOfTheSmallestShape() {
        Assertions.assertEquals(
            "t",
            new JsonMessageCodec()
                .decode(JsonMessageCodecTest.MINIMAL.getBytes(StandardCharsets.UTF_8))
                .task()
        );
    }

    @Test
    void refusesAnEnvelopeWithNullHeaders() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = JsonMessageCodecTest.MINIMAL
            .replace("\"headers\":{\"id\":\"a\",\"task\":\"t\"}", "\"headers\":null")
            .getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void refusesAnEnvelopeWithoutContentType() {
        final MessageCodec codec = new JsonMessageCodec();
        final byte[] raw = JsonMessageCodecTest.MINIMAL
            .replace("\"content-type\":\"application/json\",", "")
            .getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(raw));
    }

    @Test
    void writesTheContentEncodingItWasGiven() throws IOException {
        Assertions.assertEquals(
            "binary",
            new ObjectMapper().readTree(
                new JsonMessageCodec().encode(
                    new Message(
                        new MessageProperties(
                            Map.of(
                                MessageProperties.CORRELATION, JsonMessageCodecTest.ID,
                                MessageProperties.ENCODING, "binary"
                            )
                        ),
                        new MessageHeaders(
                            Map.of(
                                MessageHeaders.ID, JsonMessageCodecTest.ID,
                                MessageHeaders.TASK, JsonMessageCodecTest.NAME
                            )
                        ),
                        "e30="
                    )
                )
            ).get(MessageProperties.ENCODING).textValue()
        );
    }

    private static Message message() {
        return new Message(
            new MessageProperties(
                Map.of(
                    MessageProperties.CORRELATION, JsonMessageCodecTest.ID,
                    MessageProperties.WRAPPING, MessageProperties.BASE64,
                    MessageProperties.TYPE, MessageProperties.JSON,
                    MessageProperties.ENCODING, MessageProperties.UTF8
                )
            ),
            new MessageHeaders(
                Map.of(
                    MessageHeaders.ID, JsonMessageCodecTest.ID,
                    MessageHeaders.TASK, JsonMessageCodecTest.NAME,
                    MessageHeaders.LANG, ProtocolV2.LANG
                )
            ),
            "e30="
        );
    }

    private static JsonNode written() throws IOException {
        return new ObjectMapper().readTree(
            new JsonMessageCodec().encode(JsonMessageCodecTest.message())
        );
    }

    private static byte[] fixture() throws IOException {
        try (
            InputStream stream = JsonMessageCodecTest.class.getResourceAsStream(
                "/protocol/v2-documented-example.json"
            )
        ) {
            return stream.readAllBytes();
        }
    }
}
