/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The body codecs a consumer knows, looked up by the content type a message
 * declares.
 *
 * <p>A message whose content type is not registered is refused rather than
 * guessed, since a body read with the wrong codec is worse than one that was
 * never read.</p>
 *
 * @since 0.1.0
 */
public final class BodyCodecs {

    /**
     * Short name of the JSON content type, which some producers use in place
     * of the mime type.
     */
    public static final String JSON = "json";

    /**
     * Codecs a registry knows when it was given none.
     */
    private static final Map<String, TaskBodyCodec> DEFAULT = Map.of(
        MessageProperties.JSON, new JsonTaskBody(),
        BodyCodecs.JSON, new JsonTaskBody()
    );

    /**
     * Codecs by content type.
     */
    private final Map<String, TaskBodyCodec> codecs;

    /**
     * Ctor.
     */
    public BodyCodecs() {
        this(BodyCodecs.DEFAULT);
    }

    /**
     * Ctor.
     *
     * @param codecs Codecs by content type
     */
    public BodyCodecs(final Map<String, TaskBodyCodec> codecs) {
        this.codecs = codecs;
    }

    /**
     * The codec a content type is read and written with.
     *
     * @param type Content type a message declares
     * @return The codec for that type
     * @throws ProtocolException If no codec is registered for the type
     */
    public TaskBodyCodec of(final String type) throws ProtocolException {
        final TaskBodyCodec codec = this.codecs.get(type);
        if (codec == null) {
            throw new ProtocolException(
                String.format("no body codec for content type %s", type)
            );
        }
        return codec;
    }

    /**
     * The codec the body of a message is read with.
     *
     * @param message Message whose content type decides
     * @return The codec for that message
     * @throws ProtocolException If the message declares no known content type
     */
    public TaskBodyCodec of(final Message message) throws ProtocolException {
        return this.of(
            message.properties().text(MessageProperties.TYPE).orElseThrow(
                () -> new ProtocolException("property content-type is required")
            )
        );
    }

    /**
     * The same registry with one more codec.
     *
     * @param type Content type the codec reads and writes
     * @param codec Codec for that type
     * @return A registry knowing that codec
     */
    public BodyCodecs with(final String type, final TaskBodyCodec codec) {
        final Map<String, TaskBodyCodec> extended = new LinkedHashMap<>(this.codecs);
        extended.put(type, codec);
        return new BodyCodecs(extended);
    }
}
