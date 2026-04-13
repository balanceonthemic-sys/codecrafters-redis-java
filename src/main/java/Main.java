import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class Main {
    static class RedisStream {
    List<StreamEntry> entries = new ArrayList<>();
}

static class StreamEntry {
    String id;
    @SuppressWarnings("unused")
    Map<String, String> fields;

    StreamEntry(String id, Map<String, String> fields) {
        this.id = id;
        this.fields = fields;
    }
}
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment the code below to pass the first stage
      
       int port = 6379;
       // Create a pool of 10 threads to handle 10 clients at once
       ExecutorService executor = Executors.newFixedThreadPool(10);
       try (ServerSocket serverSocket = new ServerSocket(port)) { 
         
         // Since the tester restarts your program quite often, setting SO_REUSEADDR
         // ensures that we don't run into 'Address already in use' errors
         serverSocket.setReuseAddress(true);
         // Wait for connection from client.

      while (true) {
                // Main thread just waits for new connections
                Socket clientSocket = serverSocket.accept();
                
                // Hand the client to a background worker thread
                executor.submit(() -> {
                    try {
                        handleClient(clientSocket);
                    } catch (InterruptedException ex) {
                        System.getLogger(Main.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
                    }
                });
            }
         
         }catch (IOException e) {
            System.out.println("Server Error: " + e.getMessage());
        }
       }
  
private static final ConcurrentHashMap<String, RedisValue> storage = new ConcurrentHashMap<>();

 private static void handleClient(Socket clientSocket) throws InterruptedException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         OutputStream output = clientSocket.getOutputStream()) {
        
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("*")) {
                int numElements = Integer.parseInt(line.substring(1)); 
                List<String> commands = new ArrayList<>();

                for (int i = 0; i < numElements; i++) {
                    reader.readLine(); 
                    commands.add(reader.readLine()); 
                }

                String commandName = commands.get(0).toUpperCase();

                // --- START CONSOLIDATED COMMAND CHAIN ---
                if (commandName.equals("SET")) {
                    String key = commands.get(1);
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
                    storage.put(key, new RedisValue(value, expiryAt));
                    output.write("+OK\r\n".getBytes());
                } 
                else if (commandName.equals("GET")) {
                    String key = commands.get(1);
                    RedisValue val = storage.get(key);
                    if (val == null || val.isExpired()) {
                        if (val != null) storage.remove(key);
                        output.write("$-1\r\n".getBytes());
                    } else {
                        String response = "$" + ((String)val.data).length() + "\r\n" + val.data + "\r\n";
                        output.write(response.getBytes());
                    }
                }
                else if (commandName.equals("RPUSH") && commands.size() >= 3) {
                    String key = commands.get(1);
                    RedisValue existingValue = storage.get(key);
                    List<String> list = (existingValue != null && existingValue.data instanceof List) 
                                        ? (List<String>) existingValue.data : new ArrayList<>();

                    for (int i = 2; i < commands.size(); i++) {
                        list.add(commands.get(i));
                    }

                    synchronized (storage) {
                        storage.put(key, new RedisValue(list, -1));
                        storage.notifyAll(); 
                    }
                    output.write((":" + list.size() + "\r\n").getBytes());
                }
                else if (commandName.equals("LRANGE")) {
                            String key = commands.get(1);
                            int start = Integer.parseInt(commands.get(2));
                            int stop = Integer.parseInt(commands.get(3));

                            RedisValue val = storage.get(key);
                            
                            // Ensure we ALWAYS write a response, even if null
                            if (val == null || !(val.data instanceof List)) {
                                output.write("*0\r\n".getBytes());
                            } else {
                                List<String> list = (List<String>) val.data;
                                int size = list.size();

                                // Normalize indices
                                if (start < 0) start = size + start;
                                if (stop < 0) stop = size + stop;
                                start = Math.max(0, start);
                                if (stop >= size) stop = size - 1;

                                if (start >= size || start > stop) {
                                    output.write("*0\r\n".getBytes());
                                } else {
                                    int count = stop - start + 1;
                                    StringBuilder response = new StringBuilder("*" + count + "\r\n");
                                    for (int i = start; i <= stop; i++) {
                                        String item = list.get(i);
                                        response.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
                                    }
                                    output.write(response.toString().getBytes());
                                }
                            }
                            // No 'return' here! Let it hit the final flush.
                        }
                else if (commandName.equals("LPUSH") && commands.size() >= 3) {
                    String key = commands.get(1);
                    RedisValue existingValue = storage.get(key);
                    List<String> list = (existingValue != null && existingValue.data instanceof List) 
                                        ? (List<String>) existingValue.data : new ArrayList<>();

                    for (int i = 2; i < commands.size(); i++) {
                        list.add(0, commands.get(i));
                    }

                    synchronized (storage) {
                        storage.put(key, new RedisValue(list, -1));
                        storage.notifyAll(); 
                    }
                    output.write((":" + list.size() + "\r\n").getBytes());
                }
                else if (commandName.equals("TYPE") && commands.size() >= 2) {
                    String key = commands.get(1);
                    RedisValue val = storage.get(key);
                    if (val == null) {
                        output.write("+none\r\n".getBytes());
                    } else if (val.data instanceof String) {
                        output.write("+string\r\n".getBytes());
                    } else if (val.data instanceof RedisStream) {
                        output.write("+stream\r\n".getBytes());
                    } else if (val.data instanceof List) {
                        output.write("+list\r\n".getBytes());
                    }
                }
                else if (commandName.equals("LLEN")) {
                    String key = commands.get(1);
                    RedisValue val = storage.get(key);
                    if (val == null) {
                        output.write(":0\r\n".getBytes());
                    } else if (val.data instanceof List) {
                        List<String> list = (List<String>) val.data;
                        output.write((":" + list.size() + "\r\n").getBytes());
                    } else {
                        output.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
                    }
                    // No 'return' here!
                }
               else if (commandName.equals("XADD") && commands.size() >= 4) {
    String key = commands.get(1);
    String idInput = commands.get(2); // e.g., "1000-*"

    Map<String, String> fields = new LinkedHashMap<>();
    for (int i = 3; i < commands.size(); i += 2) {
        fields.put(commands.get(i), commands.get(i + 1));
    }

    try {
        synchronized (storage) {
            RedisValue val = storage.get(key);
            RedisStream stream = (val == null) ? new RedisStream() : (RedisStream) val.data;
            if (val == null) storage.put(key, new RedisValue(stream, -1));

            String finalId;
            String[] parts = idInput.split("-");
            
            // --- NEW: Sequence Auto-generation Logic ---
            if (parts[1].equals("*")) {
                long currentMs = Long.parseLong(parts[0]);
                long nextSeq;

                if (stream.entries.isEmpty()) {
                    // Rule: 0-0 is forbidden, start at 0-1
                    nextSeq = (currentMs == 0) ? 1 : 0;
                } else {
                    StreamEntry last = stream.entries.get(stream.entries.size() - 1);
                    String[] lastParts = last.id.split("-");
                    long lastMs = Long.parseLong(lastParts[0]);
                    long lastSeq = Long.parseLong(lastParts[1]);

                    if (currentMs == lastMs) {
                        nextSeq = lastSeq + 1;
                    } else if (currentMs > lastMs) {
                        nextSeq = 0;
                    } else {
                        output.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                        output.flush();
                        return;
                    }
                }
                finalId = currentMs + "-" + nextSeq;
            } else {
                // Handle Explicit IDs (Same logic as before)
                finalId = idInput;
                long newMs = Long.parseLong(parts[0]);
                long newSeq = Long.parseLong(parts[1]);

                if (newMs == 0 && newSeq == 0) {
                    output.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes());
                    output.flush();
                    return;
                }

                if (!stream.entries.isEmpty()) {
                    StreamEntry last = stream.entries.get(stream.entries.size() - 1);
                    String[] lastParts = last.id.split("-");
                    if (newMs < Long.parseLong(lastParts[0]) || (newMs == Long.parseLong(lastParts[0]) && newSeq <= Long.parseLong(lastParts[1]))) {
                        output.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                        output.flush();
                        return;
                    }
                }
            }

            // Success: Use finalId
            stream.entries.add(new StreamEntry(finalId, fields));
            output.write(("$" + finalId.length() + "\r\n" + finalId + "\r\n").getBytes());
        }
    } catch (IOException | NumberFormatException e) {
        output.write("-ERR Invalid ID format\r\n".getBytes());
    }
    output.flush();
}
                else if (commandName.equals("BLPOP") && commands.size() >= 3) {
                    String key = commands.get(1);
                    double timeoutSeconds = Double.parseDouble(commands.get(2));
                    long endTime = (timeoutSeconds == 0) ? 0 : System.currentTimeMillis() + (long)(timeoutSeconds * 1000);

                    synchronized (storage) {
                        while (true) {
                            RedisValue val = storage.get(key);
                            if (val != null && val.data instanceof List && !((List)val.data).isEmpty()) {
                                List<String> list = (List<String>) val.data;
                                String element = list.remove(0);
                                if (list.isEmpty()) storage.remove(key);
                                output.write(("*2\r\n$" + key.length() + "\r\n" + key + "\r\n$" + element.length() + "\r\n" + element + "\r\n").getBytes());
                                break; 
                            }
                            if (timeoutSeconds > 0) {
                                long remaining = endTime - System.currentTimeMillis();
                                if (remaining <= 0) {
                                    output.write("*-1\r\n".getBytes());
                                    break;
                                }
                                storage.wait(remaining);
                            } else {
                                storage.wait(); 
                            }
                        }
                    }
                }
                    else if (commandName.equals("LPOP") && commands.size() >= 2) {
                    String key = commands.get(1);
                    RedisValue val = storage.get(key);

                    if (val == null || !(val.data instanceof List)) {
                        // If key doesn't exist, Redis returns a Null Bulk String for single pop
                        // or an empty array if it was a multi-pop. The tester expects a response.
                        if (commands.size() == 2) {
                            output.write("$-1\r\n".getBytes());
                        } else {
                            output.write("*0\r\n".getBytes());
                        }
                    } else {
                        List<String> list = (List<String>) val.data;
                        
                        if (commands.size() == 2) {
                            // Standard single pop
                            if (list.isEmpty()) {
                                output.write("$-1\r\n".getBytes());
                            } else {
                                String element = list.remove(0);
                                output.write(("$" + element.length() + "\r\n" + element + "\r\n").getBytes());
                            }
                        } else {
                            // Multi-pop logic (LPOP key count)
                            int count = Integer.parseInt(commands.get(2));
                            int actualToPop = Math.min(count, list.size());
                            
                            if (actualToPop == 0) {
                                output.write("*0\r\n".getBytes());
                            } else {
                                StringBuilder response = new StringBuilder("*" + actualToPop + "\r\n");
                                for (int i = 0; i < actualToPop; i++) {
                                    String element = list.remove(0);
                                    response.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
                                }
                                output.write(response.toString().getBytes());
                            }
                        }
                        // Cleanup empty keys to save memory
                        if (list.isEmpty()) storage.remove(key);
                    }
                    // Critical: Ensure the response is pushed out
                    output.flush(); 
                }
                else if (commandName.equals("PING")) {
                    output.write("+PONG\r\n".getBytes());
                } 
                else if (commandName.equals("ECHO") && commands.size() > 1) {
                    String payload = commands.get(1);
                    output.write(("$" + payload.length() + "\r\n" + payload + "\r\n").getBytes());
                }

                // SINGLE FLUSH FOR ALL COMMANDS
                output.flush();
            }
        }
    } catch (IOException e) {
        System.out.println("Client Error: " + e.getMessage());
    }
}
}