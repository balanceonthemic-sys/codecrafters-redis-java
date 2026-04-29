package redis.storage;

import redis.model.RedisValue;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Central thread-safe key-value store for all Redis data.
 * Singleton — one shared storage for the entire server.
 */
public class RedisStorage {

    // Single shared instance across all client handler threads
    private static final ConcurrentHashMap<String, RedisValue> store = new ConcurrentHashMap<>();

    // Private constructor — no instantiation
    private RedisStorage() {}

    public static RedisValue get(String key) {
        return store.get(key);
    }

    public static void put(String key, RedisValue value) {
        store.put(key, value);
    }

    public static void remove(String key) {
        store.remove(key);
    }

    public static boolean containsKey(String key) {
        return store.containsKey(key);
    }

    /**
     * Returns the raw store for synchronized blocks (e.g. BLPOP, RPUSH notify).
     * Use with care — always synchronize on this object when calling wait/notifyAll.
     */
    public static ConcurrentHashMap<String, RedisValue> getRawStore() {
        return store;
    }
}
