/**
 * Jackson utility classes
 *
 * <p>{@link com.github.fge.jackson.JsonLoader} contains various methods to load
 * JSON documents as {@link com.fasterxml.jackson.databind.JsonNode}s. It uses
 * a {@link com.github.fge.jackson.JsonNodeReader} (as such, parsing {@code []]}
 * will generate an error where Jackson normally does not).</p>
 *
 * <p>You will also want to use {@link com.github.fge.jackson.JacksonUtils}
 * to grab a node factory, reader and pretty printer for anything JSON. Compared
 * to the basic Jackson's {@link com.fasterxml.jackson.databind.ObjectMapper},
 * the one provided by {@link com.github.fge.jackson.JacksonUtils} deserializes
 * all floating point numbers as {@link java.math.BigDecimal}s by default. This
 * is done using {@link
 * com.fasterxml.jackson.databind.DeserializationFeature#USE_BIG_DECIMAL_FOR_FLOATS}.
 * </p>
 *
 * <p>{@link com.github.fge.jackson.JsonNumEquals} is an equivalence over {@link
 * com.fasterxml.jackson.databind.JsonNode} for recursive equivalence of JSON
 * number values.</p>
 *
 * <p>Finally, {@link com.github.fge.jackson.NodeType} is a utility enumeration
 * which distinguishes between all JSON node types defined by RFC 7159, plus
 * {@code integer} (used by JSON Schema). Note that since Jackson 2.2, there is
 * also {@link com.fasterxml.jackson.databind.JsonNode#getNodeType()}, but it
 * does not make a difference between {@code number} and {@code integer}.</p>
 */
package org.xbib.json.jackson;
