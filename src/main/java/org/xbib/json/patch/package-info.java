/**
 * Implementation of JSON Patch
 *
 * <p>As its name implies, JSON Patch is a mechanism designed to modify JSON
 * documents. It consists of a series of operations to apply in order to the
 * source JSON document until all operations are applied or an error has been
 * encountered.</p>
 *
 * <p>The main class is {@link org.xbib.json.patch.JsonPatch}.</p>
 *
 * <p>Note that at this moment, the only way to build a patch is from a JSON
 * representation (as a {@link com.fasterxml.jackson.databind.JsonNode}).</p>
 *
 */
package org.xbib.json.patch;
