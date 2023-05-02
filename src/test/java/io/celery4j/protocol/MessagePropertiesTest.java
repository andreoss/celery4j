/*
 * SPDX-FileCopyrightText: Copyright (c) 2023
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link MessageProperties}.
 *
 * @since 0.1.0
 */
final class MessagePropertiesTest {

    /**
     * Identifier used across the cases.
     */
    private static final String ID = "3802f860-8d3c-4dad-b18c-597fb2ac728b";

    @Test
    void readsTheCorrelationIdentifier() {
        Assertions.assertEquals(
            MessagePropertiesTest.ID, MessagePropertiesTest.properties().correlation()
        );
    }

    @Test
    void refusesPropertiesWithoutCorrelationIdentifier() {
        Assertions.assertThrows(
            ProtocolException.class, () -> new MessageProperties(Map.of()).correlation()
        );
    }

    @Test
    void readsATextProperty() {
        Assertions.assertEquals(
            Optional.of(MessageProperties.BASE64),
            MessagePropertiesTest.properties().text(MessageProperties.WRAPPING)
        );
    }

    @Test
    void readsANumberProperty() {
        Assertions.assertEquals(
            2L,
            MessagePropertiesTest.properties()
                .with(MessageProperties.MODE, MessageProperties.PERSISTENT)
                .number(MessageProperties.MODE, 0L)
        );
    }

    @Test
    void readsAnAbsentNumberPropertyAsTheOneGiven() {
        Assertions.assertEquals(
            1L, MessagePropertiesTest.properties().number(MessageProperties.MODE, 1L)
        );
    }

    @Test
    void carriesAPropertyItDoesNotKnow() {
        Assertions.assertEquals(
            Optional.of("cluster"),
            MessagePropertiesTest.properties().with("delivery_cluster", "cluster")
                .text("delivery_cluster")
        );
    }

    @Test
    void leavesThePropertiesItCameFromAlone() {
        final MessageProperties props = MessagePropertiesTest.properties();
        props.with(MessageProperties.PRIORITY, 5);
        Assertions.assertFalse(props.asMap().containsKey(MessageProperties.PRIORITY));
    }

    @Test
    void refusesToBeModifiedThroughItsMap() {
        final Map<String, Object> values = MessagePropertiesTest.properties().asMap();
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> values.put(MessageProperties.PRIORITY, 1)
        );
    }

    @Test
    void comparesEqualByValue() {
        Assertions.assertEquals(
            MessagePropertiesTest.properties(), MessagePropertiesTest.properties()
        );
    }

    @Test
    void comparesEqualToItself() {
        final MessageProperties props = MessagePropertiesTest.properties();
        Assertions.assertEquals(props, props);
    }

    @Test
    void comparesUnequalToOtherProperties() {
        Assertions.assertNotEquals(
            MessagePropertiesTest.properties(),
            MessagePropertiesTest.properties().with(MessageProperties.PRIORITY, 9)
        );
    }

    @Test
    void comparesUnequalToAnotherKind() {
        Assertions.assertNotEquals(MessagePropertiesTest.properties(), "not properties");
    }

    @Test
    void hashesByValue() {
        Assertions.assertEquals(
            MessagePropertiesTest.properties().hashCode(),
            MessagePropertiesTest.properties().hashCode()
        );
    }

    @Test
    void describesItself() {
        Assertions.assertTrue(
            MessagePropertiesTest.properties().toString().contains(MessagePropertiesTest.ID)
        );
    }

    private static MessageProperties properties() {
        final Map<String, Object> values = new HashMap<>();
        values.put(MessageProperties.CORRELATION, MessagePropertiesTest.ID);
        values.put(MessageProperties.WRAPPING, MessageProperties.BASE64);
        return new MessageProperties(values);
    }
}
