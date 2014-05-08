package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_BOOLEAN;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NULL;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NUMBER;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_STRING;

public class RiverState implements ToXContent, Streamable, Comparable<RiverState> {

    /**
     * The date the river was started
     */
    private Date started;

    /**
     * The name of the river
     */
    private String name;

    /**
     * The type of the river
     */
    private String type;

    /**
     * Last timestamp of river activity
     */
    private Date timestamp;

    /**
     * Is river instance enabled?
     */
    private boolean enabled;

    /**
     * A counter for river activity
     */
    private long counter;
    /**
     * A flag for signalling river activity
     */
    private boolean active;

    /**
     * A custom map for more information about the river
     */
    private Map<String, Object> custom;

    /**
     * The settings of the river
     */
    private Settings settings;

    /**
     * Coordinate for the state persistence document
     */
    private String coordinateIndex;

    /**
     * Coordinate for the state persistence document
     */
    private String coordinateType;

    /**
     * Coordinate for the state persistence document
     */
    private String coordinateId;

    public RiverState() {
    }

    public RiverState setStarted(Date started) {
        this.started = started;
        return this;
    }

    public Date getStarted() {
        return started;
    }

    public RiverState setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public RiverState setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    public RiverState setCounter(long counter) {
        this.counter = counter;
        return this;
    }

    public long getCounter() {
        return counter;
    }

    public RiverState setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public RiverState setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public RiverState setActive(boolean active) {
        this.active = active;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public RiverState setCustom(Map<String, Object> custom) {
        this.custom = custom;
        return this;
    }

    public Map<String, Object> getCustom() {
        return custom;
    }

    public RiverState setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public Settings getSettings() {
        return settings;
    }

    public RiverState setCoordinates(String index, String type, String id) {
        this.coordinateIndex = index;
        this.coordinateType = type;
        this.coordinateId = id;
        return this;
    }

    public RiverState save(Client client) throws IOException {
        if (coordinateIndex == null || coordinateType == null || coordinateId == null) {
            return this;
        }
        XContentBuilder builder = jsonBuilder();
        builder = toXContent(builder, ToXContent.EMPTY_PARAMS);
        client.prepareIndex()
                .setIndex(coordinateIndex)
                .setType(coordinateType)
                .setId(coordinateId)
                .setSource(builder.string())
                .execute()
                .actionGet();
        // refresh index to make this visible
        client.admin().indices().prepareRefresh(coordinateIndex).execute().actionGet();
        return this;
    }

    public RiverState load(Client client) throws IOException {
        if (coordinateIndex == null || coordinateType == null || coordinateId == null) {
            return this;
        }
        GetResponse get = null;
        try {
            get = client.prepareGet(coordinateIndex, coordinateType, coordinateId).execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        if (get != null && get.isExists()) {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(get.getSourceAsBytes());
            fromXContent(parser);
        } else {
            counter = 0L;
        }
        return this;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        DateMathParser dateParser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != END_OBJECT) {
            if (token == null) {
                break;
            } else if (token == FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == VALUE_NULL || token == VALUE_STRING
                    || token == VALUE_BOOLEAN || token == VALUE_NUMBER) {
                if (currentFieldName.equals("name")) {
                    setName(parser.text());

                } else if (currentFieldName.equals("type")) {
                    setType(parser.text());

                } else if (currentFieldName.equals("started")) {
                    try {
                        this.started = new Date(dateParser.parse(parser.text(), 0));
                    } catch (Exception e) {
                        // ignore
                    }

                } else if (currentFieldName.equals("timestamp")) {
                    try {
                        setTimestamp(new Date(dateParser.parse(parser.text(), 0)));
                    } catch (Exception e) {
                        // ignore
                    }

                } else if (currentFieldName.equals("counter")) {
                    try {
                        setCounter(parser.longValue());
                    } catch (Exception e) {
                        // ignore
                    }

                } else if (currentFieldName.equals("enabled")) {
                    setEnabled(parser.booleanValue());

                } else if (currentFieldName.equals("active")) {
                    setActive(parser.booleanValue());

                }
            } else if (token == START_OBJECT) {
                if ("custom".equals(currentFieldName)) {
                    setCustom(parser.map());
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field("name", name)
                .field("type", type)
                .field("enabled", enabled)
                .field("started", started)
                .field("timestamp", timestamp)
                .field("counter", counter)
                .field("active", active)
                .field("custom", custom)
                .endObject();
        return builder;
    }

    public String toString() {
        try {
            XContentBuilder builder = jsonBuilder();
            toXContent(builder, EMPTY_PARAMS);
            return builder.string();
        } catch (IOException e) {
            return "";
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readOptionalString();
        type = in.readOptionalString();
        started = new Date(in.readLong());
        timestamp = new Date(in.readLong());
        enabled = in.readBoolean();
        active = in.readBoolean();
        counter = in.readLong();
        custom = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(type);
        out.writeLong(started.getTime());
        out.writeLong(timestamp.getTime());
        out.writeBoolean(enabled);
        out.writeBoolean(active);
        out.writeLong(counter);
        out.writeMap(custom);
    }

    @Override
    public int compareTo(RiverState o) {
        return toString().compareTo(o.toString());
    }
}
