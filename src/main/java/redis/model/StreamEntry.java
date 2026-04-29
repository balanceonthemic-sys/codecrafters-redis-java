package redis.model;

import java.util.Map;

/**
 * Represents a single entry in a Redis Stream.
 * Each entry has an ID and a set of key-value fields.
 */
public class StreamEntry {
    public final String id;
    public final Map<String, String> fields;

    public StreamEntry(String id, Map<String, String> fields) {
        this.id = id;
        this.fields = fields;
    }
}
