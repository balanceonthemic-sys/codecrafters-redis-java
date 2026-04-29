package redis.command;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.model.RedisStream;
import redis.model.RedisValue;
import redis.model.StreamEntry;
import redis.storage.RedisStorage;

/**
 * Handles all Redis commands.
 * Each method corresponds to one Redis command.
 * Stateless — all state lives in RedisStorage.
 */
public class CommandHandler {

    // ─── STRING COMMANDS ────────────────────────────────────────────────────────

    public static void handleSet(List<String> commands, OutputStream out) throws IOException {
        String key   = commands.get(1);
        String value = commands.get(2);
        long expiryAt = -1;

        for (int i = 3; i < commands.size(); i++) {
            String arg = commands.get(i).toUpperCase();
            if (arg.equals("EX")) {
                expiryAt = System.currentTimeMillis() + (Long.parseLong(commands.get(i + 1)) * 1000);
                break;
            } else if (arg.equals("PX")) {
                expiryAt = System.currentTimeMillis() + Long.parseLong(commands.get(i + 1));
                break;
            }
        }

        RedisStorage.put(key, new RedisValue(value, expiryAt));
        out.write("+OK\r\n".getBytes());
    }

    public static void handleGet(List<String> commands, OutputStream out) throws IOException {
        String key = commands.get(1);
        RedisValue val = RedisStorage.get(key);

        if (val == null || val.isExpired()) {
            if (val != null) RedisStorage.remove(key);
            out.write("$-1\r\n".getBytes());
        } else {
            String response = "$" + ((String) val.data).length() + "\r\n" + val.data + "\r\n";
            out.write(response.getBytes());
        }
    }

    // ─── TYPE COMMAND ────────────────────────────────────────────────────────────

    public static void handleType(List<String> commands, OutputStream out) throws IOException {
        String key = commands.get(1);
        RedisValue val = RedisStorage.get(key);

        if (val == null) {
            out.write("+none\r\n".getBytes());
        } else if (val.data instanceof String) {
            out.write("+string\r\n".getBytes());
        } else if (val.data instanceof RedisStream) {
            out.write("+stream\r\n".getBytes());
        } else if (val.data instanceof List) {
            out.write("+list\r\n".getBytes());
        }
    }
    /**
 * Compares two Redis stream IDs in "ms-seq" format.
 * Returns negative, zero, or positive like compareTo.
 */
private static int compareIds(String id1, String id2) {
    String[] parts1 = id1.split("-");
    String[] parts2 = id2.split("-");

    long ms1  = Long.parseLong(parts1[0]);
    long ms2  = Long.parseLong(parts2[0]);
    long seq1 = Long.parseLong(parts1[1]);
    long seq2 = Long.parseLong(parts2[1]);

    if (ms1 != ms2) return Long.compare(ms1, ms2);
    return Long.compare(seq1, seq2);
}
        public static void handleXread(List<String> commands, OutputStream out) throws IOException {
    int idx = 1;
    int count = Integer.MAX_VALUE; // no limit by default

    // Parse optional COUNT argument
    if (commands.get(idx).equalsIgnoreCase("COUNT")) {
        count = Integer.parseInt(commands.get(idx + 1));
        idx += 2;
    }

    // Skip STREAMS keyword
    idx++; // skip "STREAMS"

    // Remaining args: stream keys then IDs
    // e.g. STREAMS stream1 stream2 id1 id2
    int numStreams = (commands.size() - idx) / 2;
    List<String> keys = new ArrayList<>();
    List<String> startIds = new ArrayList<>();

    for (int i = 0; i < numStreams; i++) {
        keys.add(commands.get(idx + i));
        startIds.add(commands.get(idx + numStreams + i));
    }

    // Build response — array of [streamName, entries] per stream
    StringBuilder response = new StringBuilder();
    int streamsWithResults = 0;
    List<String> streamResponses = new ArrayList<>();

    for (int s = 0; s < numStreams; s++) {
        String key     = keys.get(s);
        String startId = startIds.get(s);

        // $ means start from last entry — treat as max current ID
        if (startId.equals("$")) {
            RedisValue val = RedisStorage.get(key);
            if (val == null || !(val.data instanceof RedisStream)) {
                startId = "0-0";
            } else {
                RedisStream st = (RedisStream) val.data;
                if (st.entries.isEmpty()) {
                    startId = "0-0";
                } else {
                    // $ means after last entry — use last ID as exclusive start
                    startId = st.entries.get(st.entries.size() - 1).id;
                }
            }
        }

        // Normalize startId — if no seq, default to 0
        if (!startId.contains("-")) startId = startId + "-0";

        RedisValue val = RedisStorage.get(key);
        if (val == null || !(val.data instanceof RedisStream)) {
            continue; // skip streams that don't exist
        }

        RedisStream stream = (RedisStream) val.data;
        List<StreamEntry> result = new ArrayList<>();

        for (StreamEntry entry : stream.entries) {
            // XREAD is EXCLUSIVE of the start ID (unlike XRANGE which is inclusive)
            if (compareIds(entry.id, startId) > 0) {
                result.add(entry);
                if (result.size() >= count) break;
            }
        }

        if (result.isEmpty()) continue; // don't include empty streams

        // Build this stream's response
        StringBuilder streamResp = new StringBuilder();

        // Stream name
        streamResp.append("$").append(key.length()).append("\r\n")
                  .append(key).append("\r\n");

        // Entries array
        streamResp.append("*").append(result.size()).append("\r\n");
        for (StreamEntry entry : result) {
            streamResp.append("*2\r\n");
            streamResp.append("$").append(entry.id.length())
                      .append("\r\n").append(entry.id).append("\r\n");

            int fieldCount = entry.fields.size() * 2;
            streamResp.append("*").append(fieldCount).append("\r\n");
            for (Map.Entry<String, String> field : entry.fields.entrySet()) {
                streamResp.append("$").append(field.getKey().length())
                          .append("\r\n").append(field.getKey()).append("\r\n");
                streamResp.append("$").append(field.getValue().length())
                          .append("\r\n").append(field.getValue()).append("\r\n");
            }
        }

        streamResponses.add(streamResp.toString());
        streamsWithResults++;
    }

    // If no results at all
    if (streamsWithResults == 0) {
        out.write("*-1\r\n".getBytes());
        return;
    }

    // Write outer array — one entry per stream
    response.append("*").append(streamsWithResults).append("\r\n");
    for (String sr : streamResponses) {
        response.append("*2\r\n"); // [streamName, entries]
        response.append(sr);
    }

    out.write(response.toString().getBytes());
}

    public static void handleXrange(List<String> commands, OutputStream out) throws IOException {
    String key     = commands.get(1);
    String startId = commands.get(2);
    String endId   = commands.get(3);

    RedisValue val = RedisStorage.get(key);

    if (val == null || !(val.data instanceof RedisStream)) {
        out.write("*0\r\n".getBytes());
        return;
    }

    RedisStream stream = (RedisStream) val.data;

    // Normalise - and + to actual boundary IDs
    if (startId.equals("-")) startId = "0-0";
    if (endId.equals("+"))   endId   = Long.MAX_VALUE + "-" + Long.MAX_VALUE;

    // If only ms given (no -seq), default seq to 0 for start, MAX for end
    if (!startId.contains("-")) startId = startId + "-0";
    if (!endId.contains("-"))   endId   = endId   + "-" + Long.MAX_VALUE;

    List<StreamEntry> result = new ArrayList<>();

    for (StreamEntry entry : stream.entries) {
        if (compareIds(entry.id, startId) >= 0 &&
            compareIds(entry.id, endId)   <= 0) {
            result.add(entry);
        }
    }

    // Write RESP response
    out.write(("*" + result.size() + "\r\n").getBytes());
    for (StreamEntry entry : result) {
        // Each entry is a 2-element array: [id, [field, value, field, value...]]
        out.write("*2\r\n".getBytes());

        // Write the ID
        out.write(("$" + entry.id.length() + "\r\n" + entry.id + "\r\n").getBytes());

        // Write the fields as a flat array
        int fieldCount = entry.fields.size() * 2; // key + value per field
        out.write(("*" + fieldCount + "\r\n").getBytes());
        for (Map.Entry<String, String> field : entry.fields.entrySet()) {
            out.write(("$" + field.getKey().length()   + "\r\n" + field.getKey()   + "\r\n").getBytes());
            out.write(("$" + field.getValue().length() + "\r\n" + field.getValue() + "\r\n").getBytes());
        }
    }
}

    // ─── LIST COMMANDS ───────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public static void handleRpush(List<String> commands, OutputStream out) throws IOException {
        String key = commands.get(1);
        RedisValue existing = RedisStorage.get(key);
        List<String> list = (existing != null && existing.data instanceof List)
                ? (List<String>) existing.data : new ArrayList<>();

        for (int i = 2; i < commands.size(); i++) {
            list.add(commands.get(i));
        }

        synchronized (RedisStorage.getRawStore()) {
            RedisStorage.put(key, new RedisValue(list, -1));
            RedisStorage.getRawStore().notifyAll();
        }
        out.write((":" + list.size() + "\r\n").getBytes());
    }

    @SuppressWarnings("unchecked")
    public static void handleLpush(List<String> commands, OutputStream out) throws IOException {
        String key = commands.get(1);
        RedisValue existing = RedisStorage.get(key);
        List<String> list = (existing != null && existing.data instanceof List)
                ? (List<String>) existing.data : new ArrayList<>();

        for (int i = 2; i < commands.size(); i++) {
            list.add(0, commands.get(i));
        }

        synchronized (RedisStorage.getRawStore()) {
            RedisStorage.put(key, new RedisValue(list, -1));
            RedisStorage.getRawStore().notifyAll();
        }
        out.write((":" + list.size() + "\r\n").getBytes());
    }

    @SuppressWarnings("unchecked")
    public static void handleLrange(List<String> commands, OutputStream out) throws IOException {
        String key   = commands.get(1);
        int start    = Integer.parseInt(commands.get(2));
        int stop     = Integer.parseInt(commands.get(3));
        RedisValue val = RedisStorage.get(key);

        if (val == null || !(val.data instanceof List)) {
            out.write("*0\r\n".getBytes());
            return;
        }

        List<String> list = (List<String>) val.data;
        int size = list.size();

        if (start < 0) start = size + start;
        if (stop  < 0) stop  = size + stop;
        start = Math.max(0, start);
        if (stop >= size) stop = size - 1;

        if (start >= size || start > stop) {
            out.write("*0\r\n".getBytes());
            return;
        }

        int count = stop - start + 1;
        StringBuilder response = new StringBuilder("*" + count + "\r\n");
        for (int i = start; i <= stop; i++) {
            String item = list.get(i);
            response.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
        }
        out.write(response.toString().getBytes());
    }

    @SuppressWarnings("unchecked")
    public static void handleLlen(List<String> commands, OutputStream out) throws IOException {
        String key = commands.get(1);
        RedisValue val = RedisStorage.get(key);

        if (val == null) {
            out.write(":0\r\n".getBytes());
        } else if (val.data instanceof List) {
            out.write((":" + ((List<String>) val.data).size() + "\r\n").getBytes());
        } else {
            out.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
        }
    }

    @SuppressWarnings("unchecked")
    public static void handleLpop(List<String> commands, OutputStream out) throws IOException {
        String key = commands.get(1);
        RedisValue val = RedisStorage.get(key);

        if (val == null || !(val.data instanceof List)) {
            out.write(commands.size() == 2 ? "$-1\r\n".getBytes() : "*0\r\n".getBytes());
            return;
        }

        List<String> list = (List<String>) val.data;

        if (commands.size() == 2) {
            // Single pop
            if (list.isEmpty()) {
                out.write("$-1\r\n".getBytes());
            } else {
                String element = list.remove(0);
                out.write(("$" + element.length() + "\r\n" + element + "\r\n").getBytes());
            }
        } else {
            // Multi-pop: LPOP key count
            int count = Integer.parseInt(commands.get(2));
            int actualToPop = Math.min(count, list.size());

            if (actualToPop == 0) {
                out.write("*0\r\n".getBytes());
            } else {
                StringBuilder response = new StringBuilder("*" + actualToPop + "\r\n");
                for (int i = 0; i < actualToPop; i++) {
                    String element = list.remove(0);
                    response.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
                }
                out.write(response.toString().getBytes());
            }
        }

        if (list.isEmpty()) RedisStorage.remove(key);
    }

    @SuppressWarnings("unchecked")
    public static void handleBlpop(List<String> commands, OutputStream out) throws IOException, InterruptedException {
        String key = commands.get(1);
        double timeoutSeconds = Double.parseDouble(commands.get(2));
        long endTime = (timeoutSeconds == 0) ? 0
                : System.currentTimeMillis() + (long) (timeoutSeconds * 1000);

        synchronized (RedisStorage.getRawStore()) {
            while (true) {
                RedisValue val = RedisStorage.get(key);
                if (val != null && val.data instanceof List && !((List<?>) val.data).isEmpty()) {
                    List<String> list = (List<String>) val.data;
                    String element = list.remove(0);
                    if (list.isEmpty()) RedisStorage.remove(key);
                    out.write(("*2\r\n$" + key.length() + "\r\n" + key +
                            "\r\n$" + element.length() + "\r\n" + element + "\r\n").getBytes());
                    break;
                }

                if (timeoutSeconds > 0) {
                    long remaining = endTime - System.currentTimeMillis();
                    if (remaining <= 0) {
                        out.write("*-1\r\n".getBytes());
                        break;
                    }
                    RedisStorage.getRawStore().wait(remaining);
                } else {
                    RedisStorage.getRawStore().wait();
                }
            }
        }
    }

    // ─── STREAM COMMANDS ─────────────────────────────────────────────────────────

    public static void handleXadd(List<String> commands, OutputStream out) throws IOException {
        String key     = commands.get(1);
        String idInput = commands.get(2);

        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 3; i < commands.size(); i += 2) {
            fields.put(commands.get(i), commands.get(i + 1));
        }

        try {
            synchronized (RedisStorage.getRawStore()) {
                RedisValue val = RedisStorage.get(key);
                RedisStream stream = (val == null) ? new RedisStream() : (RedisStream) val.data;
                if (val == null) RedisStorage.put(key, new RedisValue(stream, -1));
                    // Handle fully auto-generated ID — bare "*"
                    if (idInput.equals("*")) {
                        long currentMs = System.currentTimeMillis();
                        long nextSeq;

                        if (stream.entries.isEmpty()) {
                            nextSeq = 0;
                        } else {
                            StreamEntry last = stream.entries.get(stream.entries.size() - 1);
                            String[] lastParts = last.id.split("-");
                            long lastMs  = Long.parseLong(lastParts[0]);
                            long lastSeq = Long.parseLong(lastParts[1]);

                            if (currentMs == lastMs) {
                                nextSeq = lastSeq + 1; // same millisecond — increment seq
                            } else {
                                nextSeq = 0; // new millisecond — reset seq
                            }
                        }

                        idInput = currentMs + "-" + nextSeq; // e.g. "1713000000000-0"
                        // fall through to success path below
                    }

                String[] parts = idInput.split("-");
                String finalId;

                if (parts[1].equals("*")) {
                    // Auto-generate sequence
                    long currentMs = Long.parseLong(parts[0]);
                    long nextSeq;

                    if (stream.entries.isEmpty()) {
                        nextSeq = (currentMs == 0) ? 1 : 0;
                    } else {
                        StreamEntry last = stream.entries.get(stream.entries.size() - 1);
                        String[] lastParts = last.id.split("-");
                        long lastMs  = Long.parseLong(lastParts[0]);
                        long lastSeq = Long.parseLong(lastParts[1]);

                        if (currentMs == lastMs) {
                            nextSeq = lastSeq + 1;
                        } else if (currentMs > lastMs) {
                            nextSeq = 0;
                        } else {
                            out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                            out.flush();
                            return;
                        }
                    }
                    finalId = currentMs + "-" + nextSeq;

                } else {
                    // Explicit ID
                    long newMs  = Long.parseLong(parts[0]);
                    long newSeq = Long.parseLong(parts[1]);

                    if (newMs == 0 && newSeq == 0) {
                        out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes());
                        out.flush();
                        return;
                    }

                    if (!stream.entries.isEmpty()) {
                        StreamEntry last = stream.entries.get(stream.entries.size() - 1);
                        String[] lastParts = last.id.split("-");
                        long lastMs  = Long.parseLong(lastParts[0]);
                        long lastSeq = Long.parseLong(lastParts[1]);

                        if (newMs < lastMs || (newMs == lastMs && newSeq <= lastSeq)) {
                            out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                            out.flush();
                            return;
                        }
                    }
                    finalId = idInput;
                }

                stream.entries.add(new StreamEntry(finalId, fields));
                RedisStorage.getRawStore().notifyAll(); // wake up any blocking XREAD threads

                out.write(("$" + finalId.length() + "\r\n" + finalId + "\r\n").getBytes());
                out.flush();
            }
        } catch (NumberFormatException e) {
            out.write("-ERR Invalid ID format\r\n".getBytes());
            out.flush();
        }
    }

    // ─── UTILITY COMMANDS ────────────────────────────────────────────────────────

    public static void handlePing(OutputStream out) throws IOException {
        out.write("+PONG\r\n".getBytes());
    }

    public static void handleEcho(List<String> commands, OutputStream out) throws IOException {
        String payload = commands.get(1);
        out.write(("$" + payload.length() + "\r\n" + payload + "\r\n").getBytes());
    }
}
