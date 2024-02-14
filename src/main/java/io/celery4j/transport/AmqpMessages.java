/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2024
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.transport;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.LongString;
import io.celery4j.protocol.Message;
import io.celery4j.protocol.MessageHeaders;
import io.celery4j.protocol.MessageProperties;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * How a message is written for a transport that carries headers and bytes of
 * its own, and how it is read back.
 *
 * <p>Here the task headers are the delivery's headers and the body is the
 * parameters as they are. The base64 wrapping the other transport needs is
 * undone on the way out and put back on the way in, so that a message means
 * the same thing whichever transport carried it.</p>
 *
 * <p>A message that says nothing about how its body is written is taken to
 * mean the type and encoding this was made with.</p>
 *
 * @since 0.2.0
 */
public final class AmqpMessages {

    /**
     * Content type assumed when a message names none.
     */
    private final String type;

    /**
     * Character encoding assumed when a message names none.
     */
    private final String encoding;

    /**
     * Ctor.
     */
    public AmqpMessages() {
        this(MessageProperties.JSON, MessageProperties.UTF8);
    }

    /**
     * Ctor.
     *
     * @param type Content type assumed when a message names none
     * @param encoding Character encoding assumed when a message names none
     */
    public AmqpMessages(final String type, final String encoding) {
        this.type = type;
        this.encoding = encoding;
    }

    /**
     * Delivery properties of a message.
     *
     * @param message Message to describe
     * @return The properties a delivery carries
     */
    public AMQP.BasicProperties properties(final Message message) {
        final MessageProperties props = message.properties();
        return new AMQP.BasicProperties.Builder()
            .contentType(props.text(MessageProperties.TYPE).orElse(this.type))
            .contentEncoding(props.text(MessageProperties.ENCODING).orElse(this.encoding))
            .correlationId(props.text(MessageProperties.CORRELATION).orElse(null))
            .replyTo(props.text(MessageProperties.REPLY).orElse(null))
            .deliveryMode((int) props.number(MessageProperties.MODE, MessageProperties.PERSISTENT))
            .priority((int) props.number(MessageProperties.PRIORITY, 0L))
            .headers(message.headers().asMap())
            .build();
    }

    /**
     * Body of a message, unwrapped.
     *
     * <p>A body the other transport wrapped in base64 is unwrapped. A body
     * that says it is written another way is taken as text.</p>
     *
     * @param message Message to take the body of
     * @return The parameters as bytes
     */
    public byte[] body(final Message message) {
        final byte[] bytes;
        if (message.properties()
            .text(MessageProperties.WRAPPING)
            .filter(MessageProperties.BASE64::equals)
            .isPresent()) {
            bytes = Base64.getDecoder().decode(message.body());
        } else {
            bytes = message.body().getBytes(this.charset(message));
        }
        return bytes;
    }

    /**
     * The message a delivery carries.
     *
     * @param props Properties of the delivery
     * @param body Body of the delivery
     * @param routing Queue it was delivered from
     * @return The message
     */
    public Message message(
        final AMQP.BasicProperties props, final byte[] body, final String routing
    ) {
        return new Message(
            new MessageProperties(this.delivery(props, routing)),
            new MessageHeaders(AmqpMessages.headers(props)),
            Base64.getEncoder().encodeToString(body)
        );
    }

    private Charset charset(final Message message) {
        return Charset.forName(
            message.properties().text(MessageProperties.ENCODING).orElse(this.encoding)
        );
    }

    private Map<String, Object> delivery(
        final AMQP.BasicProperties props, final String routing
    ) {
        final Map<String, Object> delivery = new LinkedHashMap<>();
        delivery.put(MessageProperties.CORRELATION, props.getCorrelationId());
        delivery.put(MessageProperties.REPLY, props.getReplyTo());
        delivery.put(
            MessageProperties.TYPE, AmqpMessages.named(props.getContentType(), this.type)
        );
        delivery.put(
            MessageProperties.ENCODING,
            AmqpMessages.named(props.getContentEncoding(), this.encoding)
        );
        if (props.getDeliveryMode() != null) {
            delivery.put(MessageProperties.MODE, props.getDeliveryMode());
        }
        if (props.getPriority() != null) {
            delivery.put(MessageProperties.PRIORITY, props.getPriority());
        }
        delivery.put(
            MessageProperties.DELIVERY,
            Map.of(MessageProperties.EXCHANGE, "", MessageProperties.ROUTING, routing)
        );
        return delivery;
    }

    private static String named(final String value, final String fallback) {
        final String named;
        if (value == null) {
            named = fallback;
        } else {
            named = value;
        }
        return named;
    }

    private static Map<String, Object> headers(final AMQP.BasicProperties props) {
        final Map<String, Object> headers = new LinkedHashMap<>();
        if (props.getHeaders() != null) {
            props.getHeaders().forEach(
                (name, value) -> headers.put(name, AmqpMessages.plain(value))
            );
        }
        return headers;
    }

    private static Object plain(final Object value) {
        final Object read;
        if (value instanceof LongString text) {
            read = text.toString();
        } else if (value instanceof List<?> items) {
            read = items.stream().map(AmqpMessages::plain).toList();
        } else {
            read = value;
        }
        return read;
    }
}
