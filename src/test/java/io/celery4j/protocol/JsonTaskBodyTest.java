/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link JsonTaskBody}.
 *
 * @since 0.1.0
 */
final class JsonTaskBodyTest {

    /**
     * A body written by a foreign producer.
     */
    private static final String FOREIGN =
        "W1siZml6eiJdLCB7ImIiOiAiYmF6eiJ9LCB7ImNhbGxiYWNrcyI6IG51bGx9XQ==";

    @Test
    void writesTheThreeParts() {
        Assertions.assertEquals(
            "[[\"fizz\"],{\"b\":\"bazz\"},{}]",
            JsonTaskBodyTest.unwrap(
                new JsonTaskBody().encode(
                    new TaskBody(List.of("fizz"), Map.of("b", "bazz"), Map.of())
                )
            )
        );
    }

    @Test
    void readsBackWhatItWrote() {
        final TaskBody body = new TaskBody(List.of("fizz"), Map.of("b", "bazz"), Map.of());
        Assertions.assertEquals(
            body, new JsonTaskBody().decode(new JsonTaskBody().encode(body))
        );
    }

    @Test
    void readsArgumentsWrittenElsewhere() {
        Assertions.assertEquals(
            List.of("fizz"), new JsonTaskBody().decode(JsonTaskBodyTest.FOREIGN).args()
        );
    }

    @Test
    void readsKeywordArgumentsWrittenElsewhere() {
        Assertions.assertEquals(
            Map.of("b", "bazz"), new JsonTaskBody().decode(JsonTaskBodyTest.FOREIGN).kwargs()
        );
    }

    @Test
    void readsEmbeddedFieldsWrittenElsewhere() {
        Assertions.assertTrue(
            new JsonTaskBody().decode(JsonTaskBodyTest.FOREIGN)
                .embed().containsKey(TaskBody.CALLBACKS)
        );
    }

    @Test
    void readsNullPartsAsEmpty() {
        Assertions.assertEquals(
            List.of(),
            new JsonTaskBody().decode(JsonTaskBodyTest.wrap("[null,null,null]")).args()
        );
    }

    @Test
    void readsNullKeywordArgumentsAsEmpty() {
        Assertions.assertEquals(
            Map.of(),
            new JsonTaskBody().decode(JsonTaskBodyTest.wrap("[null,null,null]")).kwargs()
        );
    }

    @Test
    void refusesTextThatIsNotWrapped() {
        final TaskBodyCodec codec = new JsonTaskBody();
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode("not wrapped!"));
    }

    @Test
    void refusesABodyThatIsNoJson() {
        final TaskBodyCodec codec = new JsonTaskBody();
        final String body = JsonTaskBodyTest.wrap("{oops");
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(body));
    }

    @Test
    void refusesABodyThatIsNoArray() {
        final TaskBodyCodec codec = new JsonTaskBody();
        final String body = JsonTaskBodyTest.wrap("{}");
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(body));
    }

    @Test
    void refusesABodyOfTheWrongLength() {
        final TaskBodyCodec codec = new JsonTaskBody();
        final String body = JsonTaskBodyTest.wrap("[[],{}]");
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(body));
    }

    @Test
    void refusesArgumentsOfAnotherKind() {
        final TaskBodyCodec codec = new JsonTaskBody();
        final String body = JsonTaskBodyTest.wrap("[{},{},{}]");
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(body));
    }

    @Test
    void refusesKeywordArgumentsOfAnotherKind() {
        final TaskBodyCodec codec = new JsonTaskBody();
        final String body = JsonTaskBodyTest.wrap("[[],[],{}]");
        Assertions.assertThrows(ProtocolException.class, () -> codec.decode(body));
    }

    private static String wrap(final String json) {
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    private static String unwrap(final String body) {
        return new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
    }
}
