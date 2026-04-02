import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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
  
private static void handleClient(Socket clientSocket) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         OutputStream output = clientSocket.getOutputStream()) {
        
        String line;
        while (null != (line = reader.readLine())) {
            // 1. Detect an Array (Starts with '*')
            if (line.startsWith("*")) {
                int numElements = Integer.parseInt(line.substring(1));
                
                // 2. Read the first element (The Command Name)
                reader.readLine(); // Skip the '$' length line
                String command = reader.readLine().toUpperCase(); 

                if (command.equals("PING")) {
                    output.write("+PONG\r\n".getBytes());
                } 
                else if (command.equals("ECHO")) {
                    // 3. Read the second element (The Argument)
                    reader.readLine(); // Skip the '$' length line
                    String argument = reader.readLine();
                    
                    // 4. Respond with a Bulk String: $<length>\r\n<data>\r\n
                    String response = "$" + argument.length() + "\r\n" + argument + "\r\n";
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