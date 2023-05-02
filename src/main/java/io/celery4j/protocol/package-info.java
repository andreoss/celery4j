/**
 * Wire records of the task message protocol and the codecs that read and
 * write them.
 *
 * <p>A {@link io.celery4j.protocol.Message} is what a broker carries: the
 * delivery properties, the task headers and the encoded body. Codecs turn a
 * message into bytes and back, and turn a body into the parameters of a
 * {@link io.celery4j.protocol.Task}.</p>
 *
 * @since 0.1.0
 */
package io.celery4j.protocol;
