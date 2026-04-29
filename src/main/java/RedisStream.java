package redis.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a Redis Stream — an ordered list of StreamEntry objects.
 */
public class RedisStream {
    public final List<StreamEntry> entries = new ArrayList<>();
}
