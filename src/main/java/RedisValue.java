package redis.model;

/**
 * Wraps any Redis value (String, List, Stream) with an optional expiry.
 */
public class RedisValue {
    public final Object data;
    private final long expiryAt; // -1 means no expiry

    public RedisValue(Object data, long expiryAt) {
        this.data = data;
        this.expiryAt = expiryAt;
    }

    public boolean isExpired() {
        return expiryAt != -1 && System.currentTimeMillis() > expiryAt;
    }
}
