package org.xbib.json.jackson;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * Class dedicated to reading JSON values from {@link java.io.InputStream}s and {@link
 * java.io.Reader}s
 * <p>
 * <p>This class wraps a Jackson {@link com.fasterxml.jackson.databind.ObjectMapper} so that it read one, and
 * only one, JSON text from a source. By default, when you read and map an
 * input source, Jackson will stop after it has read the first valid JSON text;
 * this means, for instance, that with this as an input:</p>
 * <p>
 * <pre>
 *     []]]
 * </pre>
 * <p>
 * <p>it will read the initial empty array ({@code []}) and stop there. This
 * class, instead, will peek to see whether anything is after the initial array,
 * and throw an exception if it finds anything.</p>
 * <p>
 * <p>Note: the input sources are closed by the read methods.</p>
 *
 * @see com.fasterxml.jackson.databind.ObjectMapper#readValues(com.fasterxml.jackson.core.JsonParser, Class)
 */
public final class JsonNodeReader {
    private final ObjectReader reader;

    public JsonNodeReader(final ObjectMapper mapper) {
        reader = mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true)
                .reader(JsonNode.class);
    }

    /**
     * No-arg constructor (see description)
     */
    public JsonNodeReader() {
        this(JacksonUtils.newMapper());
    }

    /**
     * Read a JSON value from an {@link java.io.InputStream}
     *
     * @param in the input stream
     * @return the value
     * @throws java.io.IOException malformed input, or problem encountered when reading
     *                             from the stream
     */
    public JsonNode fromInputStream(final InputStream in)
            throws IOException {
        JsonParser parser = null;
        MappingIterator<JsonNode> iterator = null;

        try {
            parser = reader.getFactory().createParser(in);
            iterator = reader.readValues(parser);
            return readNode(iterator);
        } finally {
            if (parser != null) {
                parser.close();
            }
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    /**
     * Read a JSON value from a {@link java.io.Reader}
     *
     * @param r the reader
     * @return the value
     * @throws java.io.IOException malformed input, or problem encountered when reading
     *                             from the reader
     */
    public JsonNode fromReader(final Reader r)
            throws IOException {
        JsonParser parser = null;
        MappingIterator<JsonNode> iterator = null;

        try {
            parser = reader.getFactory().createParser(r);
            iterator = reader.readValues(parser);
            return readNode(iterator);
        } finally {
            if (parser != null) {
                parser.close();
            }
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    private static JsonNode readNode(final MappingIterator<JsonNode> iterator)
            throws IOException {
        final Object source = iterator.getParser().getInputSource();
        final JsonParseExceptionBuilder builder
                = new JsonParseExceptionBuilder(source);


        if (!iterator.hasNextValue()) {
            throw builder.build();
        }

        final JsonNode ret = iterator.nextValue();

        builder.setLocation(iterator.getCurrentLocation());

        try {
            if (iterator.hasNextValue()) {
                throw builder.build();
            }
        } catch (JsonParseException e) {
            throw builder.setLocation(e.getLocation()).build();
        }

        return ret;
    }

    private static final class JsonParseExceptionBuilder {
        private String message = "";
        private JsonLocation location;

        private JsonParseExceptionBuilder(final Object source) {
            location = new JsonLocation(source, 0L, 1, 1);
        }

        private JsonParseExceptionBuilder setMessage(
                final String message) {
            this.message = message;
            return this;
        }

        private JsonParseExceptionBuilder setLocation(
                final JsonLocation location) {
            this.location = location;
            return this;
        }

        public JsonParseException build() {
            return new JsonParseException(message, location);
        }
    }
}
