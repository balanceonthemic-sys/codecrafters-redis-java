class RedisValue {
    Object data;
    long expiryTime; // System.currentTimeMillis() + TTL

    RedisValue(Object data, long expiryTime) {
        this.data = data;
        this.expiryTime = expiryTime;
    }

    boolean isExpired() {
        return expiryTime != -1 && System.currentTimeMillis() > expiryTime;
    }
}