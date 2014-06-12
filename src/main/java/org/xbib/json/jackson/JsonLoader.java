package org.xbib.json.jackson;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;

/**
 * Utility class to load JSON values from various sources as {@link com.fasterxml.jackson.databind.JsonNode}s.
 * <p>
 * <p>This class uses a {@link JsonNodeReader} to parse JSON inputs.</p>
 *
 * @see JsonNodeReader
 */
public final class JsonLoader {
    /**
     * The reader
     */
    private static final JsonNodeReader READER = new JsonNodeReader();

    private JsonLoader() {
    }

    /**
     * Read a {@link com.fasterxml.jackson.databind.JsonNode} from a resource path.
     * <p>
     * <p>This method first tries and loads the resource using {@link
     * Class#getResource(String)}; if not found, is tries and uses the context
     * classloader and if this is not found, this class's classloader.</p>
     * <p>
     * <p>This method throws an {@link java.io.IOException} if the resource does not
     * exist.</p>
     *
     * @param classLoader the class loader
     * @param resource the path to the resource (<strong>must</strong> begin
     *                 with a {@code /})
     * @return the JSON document at the resource
     * @throws IllegalArgumentException resource path does not begin with a
     *                                  {@code /}
     * @throws java.io.IOException      there was a problem loading the resource, or the JSON
     *                                  document is invalid
     */
    public static JsonNode fromResource(ClassLoader classLoader, final String resource)
            throws IOException {
        URL url = JsonLoader.class.getResource(resource);
        InputStream in = url != null ?  url.openStream() : classLoader.getResourceAsStream(resource);
        final JsonNode ret;
        try {
            ret = READER.fromInputStream(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return ret;
    }

    /**
     * Read a {@link com.fasterxml.jackson.databind.JsonNode} from a user supplied {@link java.io.Reader}
     *
     * @param reader The reader
     * @return the document
     * @throws java.io.IOException if the reader has problems
     */
    public static JsonNode fromReader(final Reader reader)
            throws IOException {
        return READER.fromReader(reader);
    }

    /**
     * Read a {@link com.fasterxml.jackson.databind.JsonNode} from a string input
     *
     * @param json the JSON as a string
     * @return the document
     * @throws java.io.IOException could not read from string
     */
    public static JsonNode fromString(final String json)
            throws IOException {
        return fromReader(new StringReader(json));
    }
}
