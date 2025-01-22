/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2025
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link JsonResults}.
 *
 * @since 0.1.0
 */
final class JsonResultsTest {

    /**
     * A result written by a foreign worker.
     */
    private static final String FOREIGN = String.join(
        "",
        "{\"status\":\"SUCCESS\",\"result\":4,\"traceback\":null,",
        "\"children\":[],\"date_done\":\"2026-09-19T12:00:00.000000\",",
        "\"task_id\":\"6ad689d4-3e0e-4b57-a1cf-4a2f0cca5c53\"}"
    );

    @Test
    void readsBackWhatItWrote() {
        final JsonResults codec = new JsonResults();
        final TaskResult result = JsonResultsTest.result();
        Assertions.assertEquals(result, codec.decode(codec.encode(result)));
    }

    @Test
    void writesTheFieldsAStoreHolds() {
        Assertions.assertEquals(
            State.SUCCESS,
            new JsonResults().decode(new JsonResults().encode(JsonResultsTest.result()))
                .state()
                .name()
        );
    }

    @Test
    void readsTheStateOfAResultWrittenElsewhere() {
        Assertions.assertEquals(
            new State(State.SUCCESS),
            new JsonResults().decode(JsonResultsTest.bytes()).state()
        );
    }

    @Test
    void readsTheValueOfAResultWrittenElsewhere() {
        Assertions.assertEquals(
            Optional.of(4), new JsonResults().decode(JsonResultsTest.bytes()).value()
        );
    }

    @Test
    void readsTheIdentifierOfAResultWrittenElsewhere() {
        Assertions.assertEquals(
            "6ad689d4-3e0e-4b57-a1cf-4a2f0cca5c53",
            new JsonResults().decode(JsonResultsTest.bytes()).id()
        );
    }

    @Test
    void readsANullTracebackOfAResultWrittenElsewhere() {
        Assertions.assertEquals(
            Optional.empty(), new JsonResults().decode(JsonResultsTest.bytes()).traceback()
        );
    }

    @Test
    void refusesBytesThatAreNoJson() {
        final JsonResults codec = new JsonResults();
        final byte[] raw = "{oops".getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ResultException.class, () -> codec.decode(raw));
    }

    @Test
    void refusesAResultThatIsNoObject() {
        final JsonResults codec = new JsonResults();
        final byte[] raw = "[]".getBytes(StandardCharsets.UTF_8);
        Assertions.assertThrows(ResultException.class, () -> codec.decode(raw));
    }

    @Test
    void usesTheMapperItWasGiven() {
        final JsonResults codec = new JsonResults(new ObjectMapper());
        Assertions.assertEquals(
            JsonResultsTest.result(), codec.decode(codec.encode(JsonResultsTest.result()))
        );
    }

    private static TaskResult result() {
        return new TaskResult(
            Map.of(
                TaskResult.ID, "task-one",
                TaskResult.STATUS, State.SUCCESS,
                TaskResult.VALUE, 42,
                TaskResult.CHILDREN, List.of()
            )
        );
    }

    private static byte[] bytes() {
        return JsonResultsTest.FOREIGN.getBytes(StandardCharsets.UTF_8);
    }
}
