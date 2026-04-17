/*
 * SPDX-FileCopyrightText: Copyright (c) 2023-2026
 * SPDX-License-Identifier: MIT
 */
package io.celery4j.protocol;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A mapper that refuses to write anything, for cases about what a codec says
 * when the writing itself fails.
 *
 * <p>A value that cannot be turned into a tree is one failure and is covered
 * by handing one over. A tree that cannot be written as bytes is another, and
 * nothing a caller passes causes it, so the mapper has to.</p>
 *
 * @since 1.0.1
 */
public final class RefusingMapper extends ObjectMapper {

    /**
     * Ctor.
     */
    public RefusingMapper() {
        super();
    }

    @Override
    public byte[] writeValueAsBytes(final Object value) throws JsonMappingException {
        throw new JsonMappingException(null, "this mapper writes nothing");
    }
}
