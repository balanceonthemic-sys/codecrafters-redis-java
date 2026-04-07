import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
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
                executor.submit(() -> handleClient(clientSocket));
            }
         
         }catch (IOException e) {
            System.out.println("Server Error: " + e.getMessage());
        }
       }
  
private static final ConcurrentHashMap<String, RedisValue> storage = new ConcurrentHashMap<>();

  private static void handleClient(Socket clientSocket) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         OutputStream output = clientSocket.getOutputStream()) {
        
        String line;
        while ((line = reader.readLine()) != null) {
            // 1. Detect an Array (Starts with '*')
            if (line.startsWith("*")) {
    // Read the number of elements in the array (e.g., *2)
    int numElements = Integer.parseInt(line.substring(1)); 
    
    // Create a list to store the parts of the command
    java.util.List<String> commands = new java.util.ArrayList<>();

    // Use numElements to drive the loop
    for (int i = 0; i < numElements; i++) {
        reader.readLine(); // Read and discard the '$' length line (e.g., $4)
        commands.add(reader.readLine()); // Read the actual data (e.g., ECHO)
    }

    // Now process based on the first element (the command name)
    String commandName = commands.get(0).toUpperCase();
    if (commandName.equals("SET")) {
    String key = commands.get(1);
    String value = commands.get(2);
    long expiryAt = -1; // Default: No expiry

    // Check for EX or PX arguments
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
else if (commandName.equals("RPUSH") && commands.size() >= 3) {
    String key = commands.get(1);
    
    // Retrieve existing value
    RedisValue existingValue = storage.get(key);
    java.util.List<String> list;

    // Check if we can reuse the existing list
    if (existingValue != null && existingValue.data instanceof java.util.List) {
        list = (java.util.List<String>) existingValue.data;
    } else {
        // If it's null or a String, create a fresh List
        list = new java.util.ArrayList<>();
    }

    // Add all new elements from the RPUSH command
    for (int i = 2; i < commands.size(); i++) {
        list.add(commands.get(i));
    }

    // Store it back
    storage.put(key, new RedisValue(list, -1));

    // Respond with the new Integer length: :<length>\r\n
    String response = ":" + list.size() + "\r\n";
    output.write(response.getBytes());
    output.flush();
}
else if (commandName.equals("LRANGE") && commands.size() >= 4) {
    String key = commands.get(1);
    int start = Integer.parseInt(commands.get(2));
    int stop = Integer.parseInt(commands.get(3));

    RedisValue val = storage.get(key);
    
    // Constraint: If list doesn't exist or is the wrong type, return empty array
    if (val == null || !(val.data instanceof java.util.List)) {
        output.write("*0\r\n".getBytes());
    } else {
        java.util.List<String> list = (java.util.List<String>) val.data;
        int size = list.size();

        // --- NEW: Normalize Negative Indices ---
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        // --- NEW: Basic Safety Bounds ---
        if (start < 0) start = 0;
        // ---------------------------------------

        // Constraint: Start index greater than or equal to length
        if (start >= size || start > stop) {
            output.write("*0\r\n".getBytes());
        } else {
            // Constraint: Adjust stop index if it exceeds list length
            if (stop >= size) {
                stop = size - 1;
            }
            // Ensure start isn't negative (common Redis behavior)
            start = Math.max(0, start);

            // Build the RESP Array response
            int count = stop - start + 1;
            StringBuilder response = new StringBuilder("*" + count + "\r\n");
            
            for (int i = start; i <= stop; i++) {
                String item = list.get(i);
                response.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
            }
            output.write(response.toString().getBytes());
        }
    }
    output.flush();
}
else if (commandName.equals("LLEN") && commands.size() >= 2) {
    String key = commands.get(1);
    RedisValue val = storage.get(key);

    // 1. If key doesn't exist, length is 0
    if (val == null) {
        output.write(":0\r\n".getBytes());
    } 
    // 2. Check if the data is actually a List
    else if (val.data instanceof java.util.List) {
        java.util.List<String> list = (java.util.List<String>) val.data;
        // Respond with the size as a RESP Integer (:size\r\n)
        String response = ":" + list.size() + "\r\n";
        output.write(response.getBytes());
    } 
    // 3. Return WRONGTYPE error if it's a String (from SET)
    else {
        output.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
    }
    output.flush();
}
else if (commandName.equals("LPOP") && commands.size() >= 2) {
    String key = commands.get(1);
    RedisValue val = storage.get(key);

    if (val == null || !(val.data instanceof java.util.List)) {
        output.write("$-1\r\n".getBytes());
    } else {
        java.util.List<String> list = (java.util.List<String>) val.data;
        
        // 1. Determine if a 'count' was provided
        if (commands.size() == 2) {
            // Standard single pop
            if (list.isEmpty()) {
                output.write("$-1\r\n".getBytes());
            } else {
                String element = list.remove(0);
                String response = "$" + element.length() + "\r\n" + element + "\r\n";
                output.write(response.getBytes());
            }
        } else {
            // 2. Multi-pop logic
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
        
        // Cleanup if list becomes empty
        if (list.isEmpty()) storage.remove(key);
    }
    output.flush();
}
else if (commandName.equals("BLPOP") && commands.size() >= 3) {
    String key = commands.get(1);
    // 1. Parse the timeout (Redis allows decimals like 0.5)
    double timeoutSeconds = Double.parseDouble(commands.get(2));
    long timeoutMillis = (long) (timeoutSeconds * 1000);
    
    long endTime = (timeoutSeconds == 0) ? 0 : System.currentTimeMillis() + timeoutMillis;

    synchronized (storage) {
        while (true) {
            RedisValue val = storage.get(key);
            
            // 2. Check if data is available
            if (val != null && val.data instanceof java.util.List && !((java.util.List)val.data).isEmpty()) {
                java.util.List<String> list = (java.util.List<String>) val.data;
                String element = list.remove(0);
                
                // Response is an Array: [key, value]
                String response = "*2\r\n$" + key.length() + "\r\n" + key + "\r\n" +
                                  "$" + element.length() + "\r\n" + element + "\r\n";
                output.write(response.getBytes());
                break;
            }

            // 3. Handle the Timeout Logic
            long remaining = (endTime == 0) ? 0 : endTime - System.currentTimeMillis();
            
           if (endTime != 0 && remaining <= 0) {
            // FIX: Changed from $-1 (Bulk String) to *-1 (Array)
            output.write("*-1\r\n".getBytes()); 
            output.flush();
            break;
        }

            try {
                // 4. Wait for the 'remaining' time or until notifyAll()
                storage.wait(remaining); 
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    output.flush();
}
else if (commandName.equals("LPUSH") && commands.size() >= 3) {
    String key = commands.get(1);
    
    // 1. Get existing value or handle new key
    RedisValue existingValue = storage.get(key);
    java.util.List<String> list;

    if (existingValue != null && existingValue.data instanceof java.util.List) {
        list = (java.util.List<String>) existingValue.data;
    } else {
        list = new java.util.ArrayList<>();
    }

    // 2. Prepend values to the front (index 0)
    // Note: To match Redis behavior, we loop through the arguments
    for (int i = 2; i < commands.size(); i++) {
        list.add(0, commands.get(i)); // 0 is the start of the list
    }

    // 3. Update storage
    storage.put(key, new RedisValue(list, -1));

    // 4. Respond with new length
    String response = ":" + list.size() + "\r\n";
    output.write(response.getBytes());
    output.flush();
}
else if (commandName.equals("TYPE") && commands.size() >= 2) {
    String key = commands.get(1);
    RedisValue val = storage.get(key);

    if (val == null) {
        output.write("+none\r\n".getBytes());
    } else {
        // Use your Object-based polymorphism to identify the type
        if (val.data instanceof String) {
            output.write("+string\r\n".getBytes());
        } else if (val.data instanceof java.util.List) {
            output.write("+list\r\n".getBytes());
        } else {
            // Default for types not yet implemented
            output.write("+unknown\r\n".getBytes());
        }
    }
    output.flush();
}
  else if (commandName.equals("GET")) {
    String key = commands.get(1);
    RedisValue val = storage.get(key);

    if (val == null || val.isExpired()) {
        if (val != null) storage.remove(key); // Cleanup expired data
        output.write("$-1\r\n".getBytes());
    } else {
        String response = "$" + ((String)val.data).length()+ "\r\n" + val.data + "\r\n";
        output.write(response.getBytes());
    }
}
    if (commandName.equals("PING")) {
        output.write("+PONG\r\n".getBytes());
    } 
    else if (commandName.equals("ECHO") && commands.size() > 1) {
        String payload = commands.get(1);
        String response = "$" + payload.length() + "\r\n" + payload + "\r\n";
        output.write(response.getBytes());
    }
    output.flush();
}
        }
    } catch (IOException e) {
        System.out.println("Client Error: " + e.getMessage());
    }
}
}