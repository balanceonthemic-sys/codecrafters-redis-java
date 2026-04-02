class RedisValue {
    String data;
    long expiryTime; // System.currentTimeMillis() + TTL

    RedisValue(String data, long expiryTime) {
        this.data = data;
        this.expiryTime = expiryTime;
    }

    boolean isExpired() {
        return expiryTime != -1 && System.currentTimeMillis() > expiryTime;
    }
}