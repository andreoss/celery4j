/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case reading the message a foreign producer actually wrote, captured by
 * the live suite and committed unchanged.
 *
 * <p>These cases need no container: the capture is what lets the wire shape be
 * checked on every build, and what would show a difference if the other side
 * ever changed it.</p>
 *
 * @since 0.1.1
 */
final class CapturedMessageTest {

    @Test
    void readsTheTaskName() {
        Assertions.assertEquals(
            "proj.java.echo", CapturedMessageTest.captured().task()
        );
    }

    @Test
    void readsTheLanguageOfTheProducer() {
        Assertions.assertEquals(
            Optional.of("py"),
            CapturedMessageTest.captured().headers().text(MessageHeaders.LANG)
        );
    }

    @Test
    void readsThePositionalArguments() {
        Assertions.assertEquals(
            List.of(2, 3), new Protocols().task(CapturedMessageTest.captured()).args()
        );
    }

    @Test
    void readsTheKeywordArguments() {
        Assertions.assertEquals(
            Map.of("debug", true),
            new Protocols().task(CapturedMessageTest.captured()).kwargs()
        );
    }

    @Test
    void readsTheEmbeddedWorkflowFields() {
        Assertions.assertTrue(
            new ProtocolV2().body(CapturedMessageTest.captured())
                .embed()
                .containsKey(TaskBody.CHAIN)
        );
    }

    @Test
    void readsTheRetryCount() {
        Assertions.assertEquals(
            0L, CapturedMessageTest.captured().headers().number(MessageHeaders.RETRIES, -1L)
        );
    }

    @Test
    void readsTheRoutingTheProducerChose() {
        Assertions.assertEquals(
            "fromforeign",
            ((Map<?, ?>) CapturedMessageTest.captured()
                .properties()
                .asMap()
                .get(MessageProperties.DELIVERY))
                .get(MessageProperties.ROUTING)
        );
    }

    @Test
    void keepsAHeaderThisVersionDoesNotKnow() {
        Assertions.assertTrue(
            CapturedMessageTest.captured().headers().asMap().containsKey("stamped_headers")
        );
    }

    @Test
    void carriesAnUnknownHeaderThroughACycle() {
        final MessageCodec codec = new JsonMessageCodec();
        Assertions.assertEquals(
            CapturedMessageTest.captured().headers().asMap(),
            codec.decode(codec.encode(CapturedMessageTest.captured())).headers().asMap()
        );
    }

    @Test
    void readsBackTheWholeMessageAfterACycle() {
        final MessageCodec codec = new JsonMessageCodec();
        Assertions.assertEquals(
            CapturedMessageTest.captured(),
            codec.decode(codec.encode(CapturedMessageTest.captured()))
        );
    }

    private static Message captured() {
        return new JsonMessageCodec().decode(CapturedMessageTest.bytes());
    }

    private static byte[] bytes() {
        try (
            InputStream stream = CapturedMessageTest.class.getResourceAsStream(
                "/protocol/v2-captured-example.json"
            )
        ) {
            return stream.readAllBytes();
        } catch (final IOException ex) {
            throw new IllegalStateException("the capture cannot be read", ex);
        }
    }
}
