import java.io.IOException;
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
    // Logic to handle the client connection and process commands
    try(java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(clientSocket.getInputStream()))){
        String line;
        while ((line= reader.readLine() ) != null) {
    // Logic to handle the command based on what 'line' contains
    if (line.toUpperCase().contains("PING")) {
        java.io.OutputStream output = clientSocket.getOutputStream();
        output.write("+PONG\r\n".getBytes());
        output.flush();        
    }
}
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       } finally {
            try { clientSocket.close(); } catch (IOException ignored) {}
        }
    }

       
}