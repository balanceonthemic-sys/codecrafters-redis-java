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
  else if (commandName.equals("GET")) {
    String key = commands.get(1);
    RedisValue val = storage.get(key);

    if (val == null || val.isExpired()) {
        if (val != null) storage.remove(key); // Cleanup expired data
        output.write("$-1\r\n".getBytes());
    } else {
        String response = "$" + val.data.length() + "\r\n" + val.data + "\r\n";
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