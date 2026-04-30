package redis.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.model.RedisStream;
import redis.model.RedisValue;
import redis.model.StreamEntry;
import redis.server.ServerConfig;
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
    public static void handleIncr(List<String> commands, OutputStream out) throws IOException {
    String key = commands.get(1);
    RedisValue val = RedisStorage.get(key);

    // Key doesn't exist — start from 0
    if (val == null) {
        RedisStorage.put(key, new RedisValue("1", -1));
        out.write(":1\r\n".getBytes());
        return;
    }

    // Key exists but isn't a string
    if (!(val.data instanceof String)) {
        out.write("-ERR value is not an integer or out of range\r\n".getBytes());
        return;
    }

    // Try to parse as integer
    try {
        long current = Long.parseLong((String) val.data);
        long newValue = current + 1;
        RedisStorage.put(key, new RedisValue(String.valueOf(newValue), -1));
        out.write((":" + newValue + "\r\n").getBytes());
    } catch (NumberFormatException e) {
        out.write("-ERR value is not an integer or out of range\r\n".getBytes());
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
private static String buildXreadResponse(
        List<String> keys, List<String> startIds, int count) {

    List<String> streamResponses = new ArrayList<>();

    for (int s = 0; s < keys.size(); s++) {
        String key     = keys.get(s);
        String startId = startIds.get(s);

        RedisValue val = RedisStorage.get(key);
        if (val == null || !(val.data instanceof RedisStream)) continue;

        RedisStream stream = (RedisStream) val.data;
        List<StreamEntry> result = new ArrayList<>();

        for (StreamEntry entry : stream.entries) {
            if (compareIds(entry.id, startId) > 0) {
                result.add(entry);
                if (result.size() >= count) break;
            }
        }

        if (result.isEmpty()) continue;

        StringBuilder sr = new StringBuilder();
        sr.append("$").append(key.length()).append("\r\n")
          .append(key).append("\r\n");
        sr.append("*").append(result.size()).append("\r\n");

        for (StreamEntry entry : result) {
            sr.append("*2\r\n");
            sr.append("$").append(entry.id.length())
              .append("\r\n").append(entry.id).append("\r\n");
            sr.append("*").append(entry.fields.size() * 2).append("\r\n");
            for (Map.Entry<String, String> field : entry.fields.entrySet()) {
                sr.append("$").append(field.getKey().length())
                  .append("\r\n").append(field.getKey()).append("\r\n");
                sr.append("$").append(field.getValue().length())
                  .append("\r\n").append(field.getValue()).append("\r\n");
            }
        }
        streamResponses.add(sr.toString());
    }

    if (streamResponses.isEmpty()) return null;

    StringBuilder response = new StringBuilder();
    response.append("*").append(streamResponses.size()).append("\r\n");
    for (String sr : streamResponses) {
        response.append("*2\r\n").append(sr);
    }
    return response.toString();
}
public class ClientHandler implements Runnable {

    private final Socket clientSocket;

    // Transaction state — per client
    private boolean inTransaction = false;
    private final List<List<String>> commandQueue = new ArrayList<>();

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handle() throws InterruptedException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()));
             OutputStream out = clientSocket.getOutputStream()) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("*")) continue;

                int numElements = Integer.parseInt(line.substring(1));
                List<String> commands = new ArrayList<>();

                for (int i = 0; i < numElements; i++) {
                    reader.readLine(); // skip $N
                    commands.add(reader.readLine());
                }

                if (commands.isEmpty()) continue;
                String commandName = commands.get(0).toUpperCase();

                // ─── TRANSACTION HANDLING ────────────────────────────
             if (commandName.equals("MULTI")) {
                    if (inTransaction) {
                        // Queue an error result so EXEC returns it in the array
                        out.write("-ERR MULTI calls can not be nested\r\n".getBytes());
                    } else {
                        inTransaction = true;
                        out.write("+OK\r\n".getBytes());
                    }
                    out.flush();
                    continue;
                }
                if (commandName.equals("DISCARD")) {
                    if (!inTransaction) {
                        out.write("-ERR DISCARD without MULTI\r\n".getBytes());
                    } else {
                        inTransaction = false;
                        commandQueue.clear();
                        out.write("+OK\r\n".getBytes());
                    }
                    out.flush();
                    continue;
                }

              if (commandName.equals("EXEC")) {
                    if (!inTransaction) {
                        out.write("-ERR EXEC without MULTI\r\n".getBytes());
                        out.flush();
                        continue;
                    }

                    inTransaction = false;

                    // Write outer array size FIRST
                    out.write(("*" + commandQueue.size() + "\r\n").getBytes());

                    // Then execute each command — results go directly into the array
                    for (List<String> queuedCmd : commandQueue) {
                        String queuedName = queuedCmd.get(0).toUpperCase();
                        route(queuedName, queuedCmd, out);
                        // NO flush here — flush only once at the end
                    }

                    commandQueue.clear();
                    out.flush(); // single flush at the very end
                    continue;
                }

                // ─── QUEUE COMMANDS IF IN TRANSACTION ────────────────
                if (inTransaction) {
                    commandQueue.add(commands);
                    out.write("+QUEUED\r\n".getBytes());
                    out.flush();
                    continue;
                }

                // ─── NORMAL COMMAND ROUTING ───────────────────────────
                route(commandName, commands, out);
                out.flush();
            }

        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
        }
    }

    private void route(String command, List<String> commands, OutputStream out)
            throws IOException, InterruptedException {
        switch (command) {
            case "PING"   -> CommandHandler.handlePing(out);
            case "ECHO"   -> CommandHandler.handleEcho(commands, out);
            case "SET"    -> CommandHandler.handleSet(commands, out);
            case "GET"    -> CommandHandler.handleGet(commands, out);
            case "INCR"   -> CommandHandler.handleIncr(commands, out);
            case "TYPE"   -> CommandHandler.handleType(commands, out);
            case "RPUSH"  -> CommandHandler.handleRpush(commands, out);
            case "LPUSH"  -> CommandHandler.handleLpush(commands, out);
            case "LRANGE" -> CommandHandler.handleLrange(commands, out);
            case "LLEN"   -> CommandHandler.handleLlen(commands, out);
            case "LPOP"   -> CommandHandler.handleLpop(commands, out);
            case "BLPOP"  -> CommandHandler.handleBlpop(commands, out);
            case "XADD"   -> CommandHandler.handleXadd(commands, out);
            case "XRANGE" -> CommandHandler.handleXrange(commands, out);
            case "XREAD"  -> CommandHandler.handleXread(commands, out);
            case "INFO" -> CommandHandler.handleInfo(commands, out);

            default       -> out.write(
                    ("-ERR unknown command '" + command + "'\r\n").getBytes());
        }
    }
}
public static void handleInfo(List<String> commands, OutputStream out) throws IOException {
    // Build the replication info string
    String info = null;
    if (ServerConfig.getRole().equals("master")) {
        // Master info
        info = """
               # Replication\r
               role:master\r
               master_replid:""" + ServerConfig.getMasterReplId() + "\r\n" +
               "master_repl_offset:" + ServerConfig.getMasterReplOffset() + "\r\n";
    } else {
        // Slave info
        info = """
               # Replication\r
               role:slave\r
               master_host:""" + ServerConfig.getMasterHost() + "\r\n" +
               "master_port:" + ServerConfig.getMasterPort() + "\r\n" +
               "master_link_status:up\r\n" +
               "master_last_io_seconds_ago:0\r\n" +
               "master_sync_in_progress:0\r\n" +
               "slave_repl_offset:" + ServerConfig.getMasterReplOffset() + "\r\n";
    }
}

    public static void handleXread(List<String> commands, OutputStream out)
        throws IOException, InterruptedException {

    int idx = 1;
    int count = Integer.MAX_VALUE;
    long blockMs = -1; // -1 means not blocking

    // Parse optional BLOCK argument
    if (commands.get(idx).equalsIgnoreCase("BLOCK")) {
        blockMs = Long.parseLong(commands.get(idx + 1));
        idx += 2;
    }

    // Parse optional COUNT argument
    if (commands.get(idx).equalsIgnoreCase("COUNT")) {
        count = Integer.parseInt(commands.get(idx + 1));
        idx += 2;
    }

    // Skip STREAMS keyword
    idx++;

    // Parse stream keys and start IDs
    int numStreams = (commands.size() - idx) / 2;
    List<String> keys      = new ArrayList<>();
    List<String> startIds  = new ArrayList<>();

    for (int i = 0; i < numStreams; i++) {
        keys.add(commands.get(idx + i));
        startIds.add(commands.get(idx + numStreams + i));
    }

    // Resolve $ to the current last ID for each stream
    // $ means "only entries added AFTER this command" 
    for (int s = 0; s < numStreams; s++) {
        if (startIds.get(s).equals("$")) {
            RedisValue val = RedisStorage.get(keys.get(s));
            if (val instanceof RedisValue && val.data instanceof RedisStream) {
                RedisStream st = (RedisStream) val.data;
                String lastId = st.entries.isEmpty() ? "0-0"
                        : st.entries.get(st.entries.size() - 1).id;
                startIds.set(s, lastId);
            } else {
                startIds.set(s, "0-0");
            }
        }
        // Normalize — if no seq given default to 0
        if (!startIds.get(s).contains("-"))
            startIds.set(s, startIds.get(s) + "-0");
    }

    // Blocking mode
    if (blockMs >= 0) {
        long deadline = (blockMs == 0) ? 0
                : System.currentTimeMillis() + blockMs;

        synchronized (RedisStorage.getRawStore()) {
            while (true) {
                // Try to read — same logic as non-blocking
                String response = buildXreadResponse(keys, startIds, count);

                if (response != null) {
                    // Found data — write and return
                    out.write(response.getBytes());
                    out.flush();
                    return;
                }

                // No data yet — wait
                if (blockMs == 0) {
                    // Block forever until notified
                    RedisStorage.getRawStore().wait();
                } else {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        // Timeout expired — return null
                        out.write("*-1\r\n".getBytes());
                        out.flush();
                        return;
                    }
                    RedisStorage.getRawStore().wait(remaining);
                }
            }
        }
    }

    // Non-blocking mode — read immediately
    String response = buildXreadResponse(keys, startIds, count);
    if (response == null) {
        out.write("*-1\r\n".getBytes());
    } else {
        out.write(response.getBytes());
    }
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
